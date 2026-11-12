// Package recovery contains the operator-gated recovery coordinator. Resource
// mutations remain owned by Runtime, Registry, and Scheduler authorities.
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/scheduler"
)

const storeVersion = 2

var (
	ErrNotFound                     = errors.New("recovery object not found")
	ErrAdministrativeAuthentication = errors.New("administrative authentication is not verifiable")
	ErrPlanStale                    = errors.New("recovery plan is stale")
	ErrAuditPersistence             = errors.New("recovery audit persistence failed")
	ErrSnapshotUnstable             = errors.New("recovery snapshot changed during capture")
)

type IncidentType string

const (
	IncidentInferenceUnknown          IncidentType = "INFERENCE_UNKNOWN"
	IncidentInferenceIncomplete       IncidentType = "INFERENCE_INCOMPLETE"
	IncidentLeaseProtected            IncidentType = "LEASE_PROTECTED"
	IncidentJournalDirty              IncidentType = "JOURNAL_DIRTY"
	IncidentJournalInconsistent       IncidentType = "JOURNAL_INCONSISTENT"
	IncidentJournalCorrupted          IncidentType = "JOURNAL_CORRUPTED"
	IncidentPersistenceFailure        IncidentType = "PERSISTENCE_FAILURE"
	IncidentProcessIdentityUnverified IncidentType = "PROCESS_IDENTITY_UNVERIFIED"
)

type IncidentStatus string

const (
	StatusDetected         IncidentStatus = "DETECTED"
	StatusDiagnosing       IncidentStatus = "DIAGNOSING"
	StatusReadyForReview   IncidentStatus = "READY_FOR_REVIEW"
	StatusBlocked          IncidentStatus = "BLOCKED"
	StatusAuthorized       IncidentStatus = "AUTHORIZED"
	StatusResolving        IncidentStatus = "RESOLVING"
	StatusResolved         IncidentStatus = "RESOLVED"
	StatusRecoveryRequired IncidentStatus = "RECOVERY_REQUIRED"
)

type VerificationStatus string

const (
	EvidenceRegistered    VerificationStatus = "REGISTERED"
	EvidenceVerified      VerificationStatus = "VERIFIED"
	EvidenceInsufficient  VerificationStatus = "INSUFFICIENT"
	EvidenceContradictory VerificationStatus = "CONTRADICTORY"
)

type Evidence struct {
	Type              string                         `json:"type"`
	Source            string                         `json:"source"`
	SubjectKind       corecontract.SubjectKind       `json:"subject_kind,omitempty"`
	SubjectID         string                         `json:"subject_id,omitempty"`
	RequestID         string                         `json:"request_id,omitempty"`
	ExecutionID       string                         `json:"execution_id,omitempty"`
	LeaseID           string                         `json:"lease_id,omitempty"`
	ProcessInstanceID string                         `json:"process_instance_id,omitempty"`
	ModelID           string                         `json:"model_id,omitempty"`
	ObservedAt        time.Time                      `json:"observed_at"`
	Verification      VerificationStatus             `json:"verification_status"`
	Guarantee         corecontract.GuaranteeScope    `json:"guarantee_scope"`
	ProcessAssignment corecontract.ProcessAssignment `json:"process_assignment,omitempty"`
}

func ValidateEvidence(e Evidence) error {
	if strings.TrimSpace(e.Type) == "" || strings.TrimSpace(e.Source) == "" || e.ObservedAt.IsZero() {
		return errors.New("recovery evidence requires type, source and timestamp")
	}
	if e.Verification != EvidenceRegistered && e.Verification != EvidenceVerified && e.Verification != EvidenceInsufficient && e.Verification != EvidenceContradictory {
		return fmt.Errorf("unknown recovery evidence verification %q", e.Verification)
	}
	if e.SubjectID == "" && e.RequestID == "" && e.ExecutionID == "" && e.LeaseID == "" {
		return errors.New("recovery evidence requires a correlated identity")
	}
	return nil
}

type Resolution struct {
	Action        string `json:"action"`
	OperatorID    string `json:"operator_id,omitempty"`
	Authorization string `json:"authorization_mechanism"`
	Authenticated bool   `json:"authenticated"`
	Result        string `json:"result"`
	Reason        string `json:"reason,omitempty"`
}

type Incident struct {
	IncidentID         string                         `json:"incident_id"`
	IncidentType       IncidentType                   `json:"incident_type"`
	Status             IncidentStatus                 `json:"status"`
	CreatedAt          time.Time                      `json:"created_at"`
	UpdatedAt          time.Time                      `json:"updated_at"`
	LARMInstanceID     string                         `json:"larm_instance_id,omitempty"`
	RequestIDs         []string                       `json:"request_ids,omitempty"`
	ExecutionIDs       []string                       `json:"execution_ids,omitempty"`
	LeaseIDs           []string                       `json:"lease_ids,omitempty"`
	ModelIDs           []string                       `json:"model_ids,omitempty"`
	ProcessInstanceIDs []string                       `json:"process_instance_ids,omitempty"`
	ProcessAssignment  corecontract.ProcessAssignment `json:"process_assignment"`
	Evidence           []Evidence                     `json:"evidence"`
	BlockingReasons    []string                       `json:"blocking_reasons"`
	JournalReferences  []string                       `json:"journal_references,omitempty"`
	RecoveryPlanID     string                         `json:"recovery_plan_id,omitempty"`
	Resolution         *Resolution                    `json:"resolution,omitempty"`
	AuditReferences    []string                       `json:"audit_references,omitempty"`
}

// RecoveryIncident is the public contract name; Incident remains the concise
// internal spelling used by the service methods.
type RecoveryIncident = Incident

type SafetyClassification string

const (
	SafeToResolve               SafetyClassification = "SAFE_TO_RESOLVE"
	BlockedInsufficientEvidence SafetyClassification = "BLOCKED_INSUFFICIENT_EVIDENCE"
	BlockedInconsistentState    SafetyClassification = "BLOCKED_INCONSISTENT_STATE"
	BlockedProcessIdentity      SafetyClassification = "BLOCKED_PROCESS_IDENTITY"
)

type RecoveryPlan struct {
	PlanID                string               `json:"plan_id"`
	IncidentID            string               `json:"incident_id"`
	CreatedAt             time.Time            `json:"created_at"`
	SnapshotVersion       string               `json:"snapshot_version"`
	EvidenceDigest        string               `json:"evidence_digest"`
	RequiredPreconditions []string             `json:"required_preconditions"`
	VerifiedPreconditions []string             `json:"verified_preconditions"`
	MissingEvidence       []string             `json:"missing_evidence"`
	ProposedAction        string               `json:"proposed_action"`
	SafetyClassification  SafetyClassification `json:"safety_classification"`
	BlockingReasons       []string             `json:"blocking_reasons"`
}

type AuditEvent struct {
	EventID                string    `json:"event_id"`
	IncidentID             string    `json:"incident_id"`
	PlanID                 string    `json:"plan_id,omitempty"`
	ModelID                string    `json:"model_id,omitempty"`
	Timestamp              time.Time `json:"timestamp"`
	Actor                  string    `json:"actor"`
	OperatorIdentity       string    `json:"operator_identity,omitempty"`
	CapabilityID           string    `json:"capability_id,omitempty"`
	EvidenceDigest         string    `json:"evidence_digest,omitempty"`
	InstanceID             string    `json:"instance_id,omitempty"`
	AuthorizationMechanism string    `json:"authorization_mechanism,omitempty"`
	Action                 string    `json:"action"`
	Result                 string    `json:"result"`
	Evidence               []string  `json:"evidence,omitempty"`
	Rejection              string    `json:"rejection_reason,omitempty"`
}

type ReconciliationPhase string

const (
	PhasePrepared         ReconciliationPhase = "PREPARED"
	PhaseReconciling      ReconciliationPhase = "RECONCILING"
	PhaseRuntimeDone      ReconciliationPhase = "RUNTIME_COMMITTED"
	PhaseRegistryDone     ReconciliationPhase = "REGISTRY_COMMITTED"
	PhaseSchedulerDone    ReconciliationPhase = "SCHEDULER_COMMITTED"
	PhaseAuditPending     ReconciliationPhase = "AUDIT_PENDING"
	PhaseComplete         ReconciliationPhase = "COMPLETE"
	PhaseRecoveryRequired ReconciliationPhase = "RECOVERY_REQUIRED"
)

type ReconciliationOperation struct {
	OperationID        string              `json:"operation_id"`
	IncidentID         string              `json:"incident_id"`
	PlanID             string              `json:"plan_id"`
	SnapshotVersion    string              `json:"snapshot_version"`
	EvidenceDigest     string              `json:"evidence_digest"`
	Phase              ReconciliationPhase `json:"phase"`
	RequestIDs         []string            `json:"request_ids,omitempty"`
	LeaseIDs           []string            `json:"lease_ids,omitempty"`
	ProcessInstanceIDs []string            `json:"process_instance_ids,omitempty"`
	UpdatedAt          time.Time           `json:"updated_at"`
	Error              string              `json:"error,omitempty"`
}

type Authorization struct {
	IncidentID      string
	PlanID          string
	SnapshotVersion string
	EvidenceDigest  string
	OperatorID      string
	Authenticated   bool
	Mechanism       string
	CapabilityID    string
	Action          string
}

type diskState struct {
	Version      int                                `json:"version"`
	Incidents    map[string]Incident                `json:"incidents"`
	Plans        map[string]RecoveryPlan            `json:"plans"`
	Audit        []AuditEvent                       `json:"audit"`
	Capabilities map[string]capabilityRecord        `json:"capabilities,omitempty"`
	RevokedNonce map[string]bool                    `json:"revoked_nonces,omitempty"`
	Operations   map[string]ReconciliationOperation `json:"operations,omitempty"`
}

type Service struct {
	mu                sync.Mutex
	registry          *inference.Registry
	scheduler         *scheduler.Scheduler
	path              string
	incidents         map[string]Incident
	plans             map[string]RecoveryPlan
	audit             []AuditEvent
	capabilities      map[string]capabilityRecord
	revokedNonce      map[string]bool
	operations        map[string]ReconciliationOperation
	storeError        error
	startupDirty      bool
	processes         ProcessProvider
	processReconciler ProcessReconciler
	// fault is nil in production. Package-local validation fixtures use it to
	// stop at a durable phase boundary without weakening any guard.
	fault func(ReconciliationPhase) error
}

// ProcessProvider is read-only. Recovery can diagnose survivors but cannot
// adopt, signal, or release a process.
type ProcessProvider interface {
	ProcessInstances() []corecontract.ProcessInstance
}

type ProcessReconciliationRequest = corecontract.ProcessReconciliationRequest

type ProcessReconciler interface {
	ProcessProvider
	ReconcileProcess(context.Context, ProcessReconciliationRequest) error
}

type runtimeRecoveryAuthority interface {
	SchedulerRecoveryReady() error
}

func Open(registry *inference.Registry, sched *scheduler.Scheduler, path string, providers ...ProcessProvider) (*Service, error) {
	if registry == nil || path == "" {
		return nil, errors.New("recovery service requires registry and path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	s := &Service{registry: registry, scheduler: sched, path: path, incidents: map[string]Incident{}, plans: map[string]RecoveryPlan{}, audit: []AuditEvent{}, capabilities: map[string]capabilityRecord{}, revokedNonce: map[string]bool{}, operations: map[string]ReconciliationOperation{}}
	if len(providers) > 0 {
		s.processes = providers[0]
		if reconciler, ok := providers[0].(ProcessReconciler); ok {
			s.processReconciler = reconciler
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("read recovery store: %w", err)
	} else {
		if info, statErr := os.Stat(path); statErr != nil || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("recovery store must be a private file")
		}
		var state diskState
		if err := json.Unmarshal(data, &state); err != nil || (state.Version != storeVersion && state.Version != 1) || state.Incidents == nil || state.Plans == nil || state.Audit == nil {
			return nil, errors.New("recovery store is corrupt or has an unsupported version")
		}
		if err := validateStoredState(state); err != nil {
			return nil, fmt.Errorf("recovery store is invalid: %w", err)
		}
		s.incidents, s.plans, s.audit = state.Incidents, state.Plans, state.Audit
		if state.Capabilities != nil {
			s.capabilities = state.Capabilities
		}
		if state.RevokedNonce != nil {
			s.revokedNonce = state.RevokedNonce
		}
		if state.Operations != nil {
			s.operations = state.Operations
		}
	}
	s.startupDirty = !registry.RecoveryStatus().CleanShutdown
	if err := s.Refresh(); err != nil {
		return nil, err
	}
	if err := s.recoverEmptySchedulerStore(); err != nil {
		return nil, err
	}
	return s, nil
}

// recoverEmptySchedulerStore is the narrow, cross-authority recovery path for
// a stale dirty marker with no durable acquisition state. It does not resolve
// leases, processes, incidents, or inference records.
func (s *Service) recoverEmptySchedulerStore() error {
	if s.scheduler == nil || s.processes == nil {
		return nil
	}
	diagnostics := s.scheduler.RecoveryDiagnostics()
	if !diagnostics.RecoveryBlocked || diagnostics.CleanShutdown || len(diagnostics.Records) != 0 {
		return nil
	}
	status := s.registry.RecoveryStatus()
	if s.startupDirty || status.CleanShutdown == false || status.RecoveryRequired || !status.AdmissionAllowed || status.PendingInferenceCount != 0 || status.ProtectedLeaseCount != 0 || status.JournalError != "" {
		return nil
	}
	for _, incident := range s.incidents {
		if incident.Status != StatusResolved {
			return nil
		}
	}
	authority, ok := s.processes.(runtimeRecoveryAuthority)
	if !ok {
		return nil
	}
	if err := authority.SchedulerRecoveryReady(); err != nil {
		return nil
	}
	if err := s.scheduler.RecoverEmptyStore(); err != nil {
		return fmt.Errorf("recover empty scheduler store: %w", err)
	}
	return nil
}

func (s *Service) Incidents() ([]Incident, error) {
	if err := s.Refresh(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Incident, 0, len(s.incidents))
	for _, incident := range s.incidents {
		result = append(result, cloneIncident(incident))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IncidentID < result[j].IncidentID })
	return result, nil
}

func (s *Service) Incident(id string) (Incident, error) {
	incidents, err := s.Incidents()
	if err != nil {
		return Incident{}, err
	}
	for _, incident := range incidents {
		if incident.IncidentID == id {
			return incident, nil
		}
	}
	return Incident{}, ErrNotFound
}

func (s *Service) Plan(incidentID string) (RecoveryPlan, error) {
	if err := s.Refresh(); err != nil {
		return RecoveryPlan{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	incident, ok := s.incidents[incidentID]
	if !ok {
		return RecoveryPlan{}, ErrNotFound
	}
	plan, ok := s.plans[incident.RecoveryPlanID]
	if !ok {
		return RecoveryPlan{}, ErrNotFound
	}
	return plan, nil
}

func (s *Service) Audit() ([]AuditEvent, error) {
	if err := s.Refresh(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AuditEvent(nil), s.audit...), nil
}

func (s *Service) Operation(incidentID, planID string) (ReconciliationOperation, error) {
	if err := s.Refresh(); err != nil {
		return ReconciliationOperation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, operation := range s.operations {
		if operation.IncidentID == incidentID && operation.PlanID == planID {
			return operation, nil
		}
	}
	return ReconciliationOperation{}, ErrNotFound
}

func (s *Service) Execute(auth Authorization) error {
	return s.ExecuteContext(context.Background(), auth)
}

// ExecuteContext coordinates durable owner commits. It never touches a
// process, lease map, or inference record directly.
func (s *Service) ExecuteContext(ctx context.Context, auth Authorization) error {
	if err := s.Refresh(); err != nil {
		return err
	}
	s.mu.Lock()
	plan, ok := s.plans[auth.PlanID]
	if !ok || plan.IncidentID != auth.IncidentID {
		s.mu.Unlock()
		return ErrNotFound
	}
	current := s.planForIncidentLocked(auth.IncidentID)
	if current.PlanID != auth.PlanID || current.SnapshotVersion != auth.SnapshotVersion || current.EvidenceDigest != auth.EvidenceDigest {
		if err := s.auditLocked(AuditEvent{IncidentID: auth.IncidentID, PlanID: auth.PlanID, Actor: "UNVERIFIED", AuthorizationMechanism: auth.Mechanism, Action: "PLAN_REJECTED", Result: "REJECTED", Rejection: ErrPlanStale.Error()}); err != nil {
			s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
			s.mu.Unlock()
			return fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		}
		s.mu.Unlock()
		return ErrPlanStale
	}
	if !auth.Authenticated || strings.TrimSpace(auth.Mechanism) == "" || strings.TrimSpace(auth.CapabilityID) == "" || auth.Action != RecoveryReconcileAction {
		if err := s.auditLocked(AuditEvent{IncidentID: auth.IncidentID, PlanID: auth.PlanID, Actor: "UNVERIFIED", AuthorizationMechanism: auth.Mechanism, Action: "AUTHORIZATION_REJECTED", Result: "REJECTED", Rejection: ErrAdministrativeAuthentication.Error()}); err != nil {
			s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
			s.mu.Unlock()
			return fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		}
		s.mu.Unlock()
		return ErrAdministrativeAuthentication
	}
	if plan.SafetyClassification != SafeToResolve {
		s.mu.Unlock()
		return errors.New("reconciliation plan is not eligible")
	}
	opKey := reconciliationOperationKey(auth.IncidentID, auth.PlanID, auth.EvidenceDigest)
	op, exists := s.operations[opKey]
	if !exists {
		op = ReconciliationOperation{OperationID: opKey, IncidentID: auth.IncidentID, PlanID: auth.PlanID, SnapshotVersion: auth.SnapshotVersion, EvidenceDigest: auth.EvidenceDigest, Phase: PhasePrepared, RequestIDs: append([]string(nil), s.incidents[auth.IncidentID].RequestIDs...), LeaseIDs: append([]string(nil), s.incidents[auth.IncidentID].LeaseIDs...), ProcessInstanceIDs: append([]string(nil), s.incidents[auth.IncidentID].ProcessInstanceIDs...), UpdatedAt: time.Now().UTC()}
		s.operations[opKey] = op
		if err := s.persistLocked(); err != nil {
			s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
			s.mu.Unlock()
			return s.storeError
		}
		if s.fault != nil {
			if err := s.fault(PhasePrepared); err != nil {
				s.mu.Unlock()
				return err
			}
		}
	}
	if op.Phase == PhaseComplete {
		s.mu.Unlock()
		if s.scheduler != nil {
			return s.scheduler.CompleteReconciliation(op.OperationID)
		}
		return nil
	}
	if op.Phase == PhasePrepared || op.Phase == PhaseRecoveryRequired || op.Phase == PhaseAuditPending {
		op.Phase, op.Error, op.UpdatedAt = PhaseReconciling, "", time.Now().UTC()
		if err := s.persistOperationLocked(op); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.fault != nil {
			if err := s.fault(PhaseReconciling); err != nil {
				s.mu.Unlock()
				return err
			}
		}
	}
	incident := cloneIncident(s.incidents[auth.IncidentID])
	s.mu.Unlock()

	if op.Phase != PhaseComplete {
		if !phaseAtLeast(op.Phase, PhaseRuntimeDone) {
			if s.processReconciler == nil {
				return s.failOperation(opKey, "runtime reconciliation authority is unavailable")
			}
			for _, processID := range op.ProcessInstanceIDs {
				if err := s.processReconciler.ReconcileProcess(ctx, corecontract.ProcessReconciliationRequest{OperationKey: opKey, ProcessInstanceID: processID, OwnerInstanceID: incident.LARMInstanceID}); err != nil {
					return s.failOperation(opKey, err.Error())
				}
			}
			if err := s.advanceOperation(opKey, PhaseRuntimeDone); err != nil {
				return err
			}
			if s.fault != nil {
				if err := s.fault(PhaseRuntimeDone); err != nil {
					return err
				}
			}
			op.Phase = PhaseRuntimeDone
		}
		if !phaseAtLeast(op.Phase, PhaseRegistryDone) {
			for _, leaseID := range op.LeaseIDs {
				if err := s.registry.ReconcilePhysical(inference.ReconciliationRequest{OperationKey: opKey, LeaseID: leaseID, ModelID: firstModel(incident), RequestIDs: incident.RequestIDs, ProcessInstanceIDs: incident.ProcessInstanceIDs, ProcessEvidence: s.processEvidence(incident)}); err != nil {
					return s.failOperation(opKey, err.Error())
				}
			}
			if err := s.advanceOperation(opKey, PhaseRegistryDone); err != nil {
				return err
			}
			if s.fault != nil {
				if err := s.fault(PhaseRegistryDone); err != nil {
					return err
				}
			}
			op.Phase = PhaseRegistryDone
		}
		if !phaseAtLeast(op.Phase, PhaseSchedulerDone) {
			if s.scheduler == nil {
				return s.failOperation(opKey, "scheduler reconciliation authority is unavailable")
			}
			for _, leaseID := range op.LeaseIDs {
				if _, err := s.scheduler.ReconcileLease(scheduler.ReconciliationRequest{OperationKey: opKey, IncidentID: incident.IncidentID, PlanID: op.PlanID, EvidenceDigest: op.EvidenceDigest, LeaseID: leaseID, ModelID: firstModel(incident), RequestIDs: incident.RequestIDs}); err != nil {
					return s.failOperation(opKey, err.Error())
				}
			}
			if err := s.advanceOperation(opKey, PhaseSchedulerDone); err != nil {
				return err
			}
			if s.fault != nil {
				if err := s.fault(PhaseSchedulerDone); err != nil {
					return err
				}
			}
			op.Phase = PhaseSchedulerDone
		}
	}
	if err := s.completeOperation(opKey, auth); err != nil {
		return err
	}
	return nil
}

func reconciliationOperationKey(incident, plan, digestValue string) string {
	return "reconcile-" + shortDigest(incident+"|"+plan+"|"+digestValue)
}

func phaseAtLeast(actual, wanted ReconciliationPhase) bool {
	rank := map[ReconciliationPhase]int{PhasePrepared: 0, PhaseReconciling: 1, PhaseRuntimeDone: 2, PhaseRegistryDone: 3, PhaseSchedulerDone: 4, PhaseAuditPending: 5, PhaseComplete: 6, PhaseRecoveryRequired: 1}
	return rank[actual] >= rank[wanted]
}

func (s *Service) persistOperationLocked(op ReconciliationOperation) error {
	previous, existed := s.operations[op.OperationID]
	op.RequestIDs = append([]string(nil), op.RequestIDs...)
	op.LeaseIDs = append([]string(nil), op.LeaseIDs...)
	op.ProcessInstanceIDs = append([]string(nil), op.ProcessInstanceIDs...)
	op.UpdatedAt = time.Now().UTC()
	s.operations[op.OperationID] = op
	if err := s.persistLocked(); err != nil {
		if existed {
			s.operations[op.OperationID] = previous
		} else {
			delete(s.operations, op.OperationID)
		}
		return fmt.Errorf("%w: %v", ErrAuditPersistence, err)
	}
	return nil
}

func (s *Service) advanceOperation(key string, phase ReconciliationPhase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[key]
	if !ok {
		return ErrNotFound
	}
	op.Phase, op.Error = phase, ""
	return s.persistOperationLocked(op)
}

func (s *Service) failOperation(key, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[key]
	if ok {
		op.Phase, op.Error = PhaseRecoveryRequired, reason
		_ = s.persistOperationLocked(op)
	}
	return errors.New(reason)
}

func (s *Service) completeOperation(key string, auth Authorization) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[key]
	if !ok {
		return ErrNotFound
	}
	op.Phase = PhaseAuditPending
	if err := s.persistOperationLocked(op); err != nil {
		return err
	}
	if s.fault != nil {
		if err := s.fault(PhaseAuditPending); err != nil {
			return err
		}
	}
	incident, ok := s.incidents[op.IncidentID]
	if !ok {
		return ErrNotFound
	}
	incident.Status = StatusResolved
	incident.Resolution = &Resolution{Action: "RECONCILE_PHYSICAL_CAPACITY", OperatorID: auth.OperatorID, Authorization: auth.Mechanism, Authenticated: true, Result: "RECONCILED", Reason: "physical process and lease disposition committed; semantic UNKNOWN retained"}
	incident.AuditReferences = appendUnique(incident.AuditReferences, "reconciliation:"+key)
	s.incidents[op.IncidentID] = incident
	if err := s.addAuditOnceLocked(AuditEvent{IncidentID: op.IncidentID, PlanID: op.PlanID, Actor: auth.OperatorID, OperatorIdentity: auth.OperatorID, CapabilityID: auth.CapabilityID, EvidenceDigest: op.EvidenceDigest, InstanceID: incident.LARMInstanceID, AuthorizationMechanism: auth.Mechanism, Action: "RECONCILIATION_COMPLETED", Result: "RECONCILED", Evidence: append([]string(nil), incident.JournalReferences...)}); err != nil {
		return fmt.Errorf("%w: %v", ErrAuditPersistence, err)
	}
	op.Phase = PhaseComplete
	if err := s.persistOperationLocked(op); err != nil {
		return err
	}
	if s.scheduler != nil {
		if err := s.scheduler.CompleteReconciliation(op.OperationID); err != nil {
			return err
		}
	}
	if s.fault != nil {
		if err := s.fault(PhaseComplete); err != nil {
			return err
		}
	}
	return nil
}

func firstModel(incident Incident) string {
	if len(incident.ModelIDs) == 0 {
		return ""
	}
	return incident.ModelIDs[0]
}

func (s *Service) processEvidence(incident Incident) []corecontract.TerminalEvidence {
	if s.processes == nil {
		return nil
	}
	wanted := make(map[string]struct{}, len(incident.ProcessInstanceIDs))
	for _, id := range incident.ProcessInstanceIDs {
		wanted[id] = struct{}{}
	}
	var result []corecontract.TerminalEvidence
	for _, process := range s.processes.ProcessInstances() {
		if _, ok := wanted[process.ProcessInstanceID]; !ok {
			continue
		}
		for _, evidence := range process.Evidence {
			if evidence.EvidenceID == process.TerminationEvidenceID {
				result = append(result, evidence)
				break
			}
		}
	}
	return result
}

func (s *Service) operationForIncidentLocked(incidentID string) *ReconciliationOperation {
	var result *ReconciliationOperation
	for _, operation := range s.operations {
		if operation.IncidentID == incidentID && (result == nil || operation.UpdatedAt.After(result.UpdatedAt)) {
			copy := operation
			result = &copy
		}
	}
	return result
}

func (s *Service) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storeError != nil {
		return s.storeError
	}
	status, all, schedulerState, schedulerLeases, processes, snapshotVersion := s.captureSnapshot()
	stable := false
	for attempt := 0; attempt < 3; attempt++ {
		_, _, _, _, _, secondVersion := s.captureSnapshot()
		if snapshotVersion == secondVersion {
			stable = true
			break
		}
		status, all, schedulerState, schedulerLeases, processes, snapshotVersion = s.captureSnapshot()
	}
	if !stable {
		return ErrSnapshotUnstable
	}
	newIncidents := make(map[string]Incident, len(s.incidents))
	for id, old := range s.incidents {
		newIncidents[id] = old
	}
	add := func(typ IncidentType, key string, reason string, evidence []Evidence, req, exec, lease, model string, process corecontract.ProcessAssignment) Incident {
		id := "incident-" + shortDigest(string(typ)+"|"+key)
		incident := newIncidents[id]
		if incident.IncidentID == "" {
			incident = Incident{IncidentID: id, IncidentType: typ, CreatedAt: time.Now().UTC(), Status: StatusBlocked}
		}
		incident.UpdatedAt = time.Now().UTC()
		incident.LARMInstanceID = status.LARMInstanceID
		incident.Status = StatusBlocked
		incident.RequestIDs = appendUnique(incident.RequestIDs, req)
		incident.ExecutionIDs = appendUnique(incident.ExecutionIDs, exec)
		incident.LeaseIDs = appendUnique(incident.LeaseIDs, lease)
		incident.ModelIDs = appendUnique(incident.ModelIDs, model)
		if process != "" {
			incident.ProcessAssignment = process
		}
		incident.BlockingReasons = appendUnique(incident.BlockingReasons, reason)
		for _, ev := range evidence {
			if validateErr := ValidateEvidence(ev); validateErr == nil {
				incident.Evidence = appendEvidence(incident.Evidence, ev)
			}
		}
		incident.JournalReferences = appendUnique(incident.JournalReferences, status.JournalPath)
		newIncidents[id] = incident
		return incident
	}
	if status.JournalError != "" {
		add(IncidentPersistenceFailure, "journal", status.JournalError, []Evidence{journalEvidence("journal", status.JournalUpdatedAt, EvidenceContradictory)}, "", "", "", "", "")
	} else if s.startupDirty {
		add(IncidentJournalDirty, "journal", "journal was not closed cleanly", []Evidence{journalEvidence("journal", status.JournalUpdatedAt, EvidenceInsufficient)}, "", "", "", "", "")
	}

	schedulerByLease := map[string]scheduler.RecoveryRecord{}
	for _, record := range schedulerState.Records {
		schedulerByLease[record.Lease.ID] = record
	}
	leaseByID := map[string]scheduler.Lease{}
	for _, lease := range schedulerLeases {
		leaseByID[lease.ID] = lease
	}
	processByID := map[string]corecontract.ProcessInstance{}
	for _, process := range processes {
		processByID[process.ProcessInstanceID] = process
	}
	processLeases := map[string]map[string]struct{}{}
	for _, record := range all {
		if record.ProcessInstanceID == "" {
			continue
		}
		if processLeases[record.ProcessInstanceID] == nil {
			processLeases[record.ProcessInstanceID] = map[string]struct{}{}
		}
		processLeases[record.ProcessInstanceID][record.LeaseID] = struct{}{}
	}
	for _, record := range all {
		needs := !record.IsTerminal() || record.IsAmbiguous() || record.RecoveryState == corecontract.RecoveryRequired || len(record.TerminalEvidence) == 0
		if !needs {
			continue
		}
		typ := IncidentInferenceIncomplete
		reason := "inference is incomplete or lacks verified terminal evidence"
		if record.IsAmbiguous() {
			typ, reason = IncidentInferenceUnknown, "inference is UNKNOWN or cancellation is UNKNOWN"
		}
		if schedulerRecord, ok := schedulerByLease[record.LeaseID]; !ok && leaseByID[record.LeaseID].ID == "" {
			typ, reason = IncidentJournalInconsistent, "inference has no correlated lease in the scheduler state"
		} else if ok && schedulerRecord.Lease.Model != record.ModelID {
			typ, reason = IncidentJournalInconsistent, "model identity differs between inference and scheduler stores"
		} else if (ok && schedulerRecord.Lease.State == scheduler.RecoveryRequired) || schedulerState.RecoveryBlocked {
			typ, reason = IncidentLeaseProtected, "scheduler lease remains recovery-protected"
		} else if ok && leaseTerminal(schedulerRecord.Lease.State) && !record.IsTerminal() {
			typ, reason = IncidentJournalInconsistent, "scheduler persisted a terminal lease while inference remains non-terminal"
		}
		if record.ProcessAssignment == corecontract.ProcessUnverified && !record.IsAmbiguous() {
			typ, reason = IncidentProcessIdentityUnverified, "process identity is UNVERIFIED; backend termination and lease reuse are not proven"
		}
		ev := []Evidence{{Type: "INFERENCE_STATE", Source: "inference-registry", SubjectKind: corecontract.SubjectRequest, SubjectID: record.RequestID, RequestID: record.RequestID, ExecutionID: record.ExecutionID, LeaseID: record.LeaseID, ProcessInstanceID: record.ProcessInstanceID, ModelID: record.ModelID, ObservedAt: record.UpdatedAt, Verification: EvidenceRegistered, Guarantee: corecontract.GuaranteeRecovery, ProcessAssignment: record.ProcessAssignment}}
		if record.ProcessAssignment == corecontract.ProcessUnverified {
			reason += "; process identity is UNVERIFIED"
		}
		processVerified := false
		if record.ProcessInstanceID != "" {
			if process, ok := processByID[record.ProcessInstanceID]; ok {
				processVerified = verifiedProcessEvidence(process)
				verification := EvidenceInsufficient
				if processVerified {
					verification = EvidenceVerified
				}
				ev = append(ev, Evidence{Type: "PROCESS_TERMINATION_VERIFIED", Source: "process-supervisor", SubjectKind: corecontract.SubjectProcess, SubjectID: process.ProcessInstanceID, ProcessInstanceID: process.ProcessInstanceID, ModelID: process.ModelID, ObservedAt: process.UpdatedAt, Verification: verification, Guarantee: corecontract.GuaranteeProcess, ProcessAssignment: record.ProcessAssignment})
			}
		}
		if !processVerified {
			reason += "; correlated verified process termination is required"
		}
		if len(processLeases[record.ProcessInstanceID]) > 1 {
			reason += "; one process is correlated with multiple leases"
		}
		key := "lease:" + record.LeaseID
		if key == "" {
			key = "lease:" + record.LeaseID
		}
		incident := add(typ, key, reason, ev, record.RequestID, record.ExecutionID, record.LeaseID, record.ModelID, record.ProcessAssignment)
		if record.ProcessInstanceID != "" {
			incident.ProcessInstanceIDs = appendUnique(incident.ProcessInstanceIDs, record.ProcessInstanceID)
			newIncidents[incident.IncidentID] = incident
		}
		if record.ProcessAssignment == corecontract.ProcessUnverified && typ == IncidentInferenceUnknown {
			incident.BlockingReasons = appendUnique(incident.BlockingReasons, "ProcessAssignment=UNVERIFIED cannot prove backend termination or lease reuse")
			newIncidents[incident.IncidentID] = incident
		}
		if processVerified && record.ProcessAssignment == corecontract.ProcessAssigned && leaseByID[record.LeaseID].ID != "" {
			incident.BlockingReasons = removeReason(incident.BlockingReasons, "; correlated verified process termination is required")
			newIncidents[incident.IncidentID] = incident
		}
	}
	for _, schedulerRecord := range schedulerState.Records {
		if schedulerRecord.Lease.State != scheduler.RecoveryRequired && !schedulerState.RecoveryBlocked {
			continue
		}
		found := false
		for _, record := range all {
			if record.LeaseID == schedulerRecord.Lease.ID {
				found = true
				break
			}
		}
		if !found {
			add(IncidentLeaseProtected, schedulerRecord.Lease.ID, "durable scheduler lease has no identifiable inference", []Evidence{{Type: "SCHEDULER_LEASE", Source: "scheduler-idempotency-store", SubjectKind: corecontract.SubjectLease, SubjectID: schedulerRecord.Lease.ID, LeaseID: schedulerRecord.Lease.ID, ModelID: schedulerRecord.Lease.Model, ObservedAt: schedulerRecord.UpdatedAt, Verification: EvidenceRegistered, Guarantee: corecontract.GuaranteeRecovery}}, "", "", schedulerRecord.Lease.ID, schedulerRecord.Lease.Model, "")
		}
	}
	if s.processes != nil {
		for _, process := range s.processes.ProcessInstances() {
			if process.State != corecontract.ProcessRecoveryRequired && process.State != corecontract.ProcessIdentityUnknown && process.State != corecontract.ProcessTerminationUnknown && process.State != corecontract.ProcessLost {
				continue
			}
			incident := add(IncidentProcessIdentityUnverified, "process|"+process.ProcessInstanceID, "process survivor, PID reuse, or process identity requires explicit recovery; no adoption or signal is permitted", []Evidence{{Type: "PROCESS_STATE", Source: "process-supervisor", SubjectKind: corecontract.SubjectProcess, SubjectID: process.ProcessInstanceID, ProcessInstanceID: process.ProcessInstanceID, ModelID: process.ModelID, ObservedAt: process.UpdatedAt, Verification: EvidenceRegistered, Guarantee: corecontract.GuaranteeProcess, ProcessAssignment: corecontract.ProcessAssigned}}, "", "", "", process.ModelID, corecontract.ProcessAssigned)
			incident.ProcessInstanceIDs = appendUnique(incident.ProcessInstanceIDs, process.ProcessInstanceID)
			newIncidents[incident.IncidentID] = incident
		}
	}

	changed := false
	for id, incident := range newIncidents {
		if old, exists := s.incidents[id]; exists && old.Status == StatusResolved {
			newIncidents[id] = old
			continue
		}
		if old, exists := s.incidents[id]; exists {
			if operation := s.operationForIncidentLocked(id); operation != nil && operation.Phase != PhaseComplete {
				if _, planExists := s.plans[operation.PlanID]; planExists {
					newIncidents[id] = old
					continue
				}
			}
		}
		if _, existed := s.incidents[id]; !existed {
			if err := s.addAuditOnceLocked(AuditEvent{IncidentID: id, Actor: "UNVERIFIED", Action: "INCIDENT_DETECTED", Result: "BLOCKED", Evidence: evidenceIDs(incident.Evidence), Rejection: strings.Join(incident.BlockingReasons, "; ")}); err != nil {
				s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
				return s.storeError
			}
			incident.AuditReferences = appendUnique(incident.AuditReferences, s.auditIDLocked(id, "", "INCIDENT_DETECTED"))
			if err := s.addAuditOnceLocked(AuditEvent{IncidentID: id, Actor: "UNVERIFIED", Action: "DIAGNOSIS_COMPLETED", Result: "BLOCKED", Evidence: evidenceIDs(incident.Evidence), Rejection: strings.Join(incident.BlockingReasons, "; ")}); err != nil {
				s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
				return s.storeError
			}
			incident.AuditReferences = appendUnique(incident.AuditReferences, s.auditIDLocked(id, "", "DIAGNOSIS_COMPLETED"))
			newIncidents[id] = incident
			changed = true
		}
		plan := buildPlan(incident, snapshotVersion)
		if isEligiblePlan(incident) {
			plan.SafetyClassification = SafeToResolve
			plan.RequiredPreconditions = []string{"correlated process termination is verified", "all associated inferences are included", "scheduler lease is present", "administrative authorization is verified"}
			plan.VerifiedPreconditions = append([]string(nil), plan.RequiredPreconditions...)
			plan.MissingEvidence = nil
			plan.ProposedAction = "RECONCILE_PHYSICAL_CAPACITY"
			plan.BlockingReasons = nil
			incident.Status = StatusReadyForReview
			newIncidents[id] = incident
		}
		if old, ok := s.plans[plan.PlanID]; !ok {
			changed = true
			s.plans[plan.PlanID] = plan
			incident.RecoveryPlanID = plan.PlanID
			if err := s.addAuditOnceLocked(AuditEvent{IncidentID: id, PlanID: plan.PlanID, Actor: "UNVERIFIED", Action: "PLAN_GENERATED", Result: "BLOCKED", Evidence: evidenceIDs(incident.Evidence), Rejection: strings.Join(plan.BlockingReasons, "; ")}); err != nil {
				s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
				return s.storeError
			}
			incident.AuditReferences = appendUnique(incident.AuditReferences, s.auditIDLocked(id, plan.PlanID, "PLAN_GENERATED"))
			newIncidents[id] = incident
		} else if incident.RecoveryPlanID == "" {
			incident.RecoveryPlanID = old.PlanID
			newIncidents[id] = incident
		}
	}
	if len(newIncidents) != len(s.incidents) {
		changed = true
	}
	for id, incident := range newIncidents {
		if old, ok := s.incidents[id]; !ok || old.RecoveryPlanID != incident.RecoveryPlanID || old.BlockingReasons == nil {
			changed = true
		}
	}
	if changed {
		if err := s.persistStateLocked(newIncidents, s.plans, s.audit); err != nil {
			s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
			return s.storeError
		}
		s.incidents = newIncidents
	}
	return nil
}

func (s *Service) captureSnapshot() (inference.RecoveryStatus, []inference.Snapshot, scheduler.RecoveryDiagnostics, []scheduler.Lease, []corecontract.ProcessInstance, string) {
	status := s.registry.RecoveryStatus()
	all := s.registry.List()
	var schedulerState scheduler.RecoveryDiagnostics
	var schedulerLeases []scheduler.Lease
	if s.scheduler != nil {
		schedulerState, schedulerLeases = s.scheduler.RecoveryDiagnostics(), s.scheduler.Leases()
	}
	processes := []corecontract.ProcessInstance{}
	if s.processes != nil {
		processes = s.processes.ProcessInstances()
	}
	version := digest(struct {
		Registry  []inference.Snapshot
		Scheduler scheduler.RecoveryDiagnostics
		Leases    []scheduler.Lease
		Processes []corecontract.ProcessInstance
		Status    inference.RecoveryStatus
	}{all, schedulerState, schedulerLeases, processes, status})
	return status, all, schedulerState, schedulerLeases, processes, version
}

func buildPlan(incident Incident, snapshot string) RecoveryPlan {
	digestValue := digest(struct {
		IncidentID         string
		RequestIDs         []string
		ExecutionIDs       []string
		LeaseIDs           []string
		ModelIDs           []string
		ProcessInstanceIDs []string
		Evidence           []Evidence
		Reasons            []string
	}{incident.IncidentID, sortedStrings(incident.RequestIDs), sortedStrings(incident.ExecutionIDs), sortedStrings(incident.LeaseIDs), sortedStrings(incident.ModelIDs), sortedStrings(incident.ProcessInstanceIDs), incident.Evidence, incident.BlockingReasons})
	classification := BlockedInsufficientEvidence
	if incident.IncidentType == IncidentJournalInconsistent || incident.IncidentType == IncidentJournalCorrupted {
		classification = BlockedInconsistentState
	}
	if incident.IncidentType == IncidentProcessIdentityUnverified || incident.ProcessAssignment == corecontract.ProcessUnverified {
		classification = BlockedProcessIdentity
	}
	return RecoveryPlan{PlanID: "plan-" + shortDigest(incident.IncidentID+"|"+snapshot+"|"+digestValue), IncidentID: incident.IncidentID, CreatedAt: time.Now().UTC(), SnapshotVersion: snapshot, EvidenceDigest: digestValue, RequiredPreconditions: []string{"all correlated stores agree", "all associated inferences have verified terminal evidence", "scheduler protection is revalidated", "operator authentication is verified"}, VerifiedPreconditions: nil, MissingEvidence: []string{"verified backend/process termination or durable non-process proof where required", "authenticated operator identity"}, ProposedAction: "NO_RELEASE_OR_UNLOCK_WITHOUT_VERIFIED_EVIDENCE", SafetyClassification: classification, BlockingReasons: append([]string(nil), incident.BlockingReasons...)}
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func verifiedProcessEvidence(process corecontract.ProcessInstance) bool {
	if process.State != corecontract.ProcessTerminatedVerified || process.ProcessIdentityStatus != "VERIFIED" || process.OwnershipStatus != "OWNED_VERIFIED" || process.TerminationStatus != "TERMINATED_VERIFIED" || process.TerminatedAt.IsZero() || process.WaitError == "" || process.DescendantStatus != "VERIFIED_OBSERVED_SCOPE" || process.TerminationEvidenceID == "" {
		return false
	}
	if process.WaitError != "exited_zero" && process.WaitError != "nonzero_exit" && process.WaitError != "signaled_exit" {
		return false
	}
	for _, evidence := range process.Evidence {
		if evidence.EvidenceID == process.TerminationEvidenceID && evidence.Type == corecontract.EvidenceProcessTerminationVerified && evidence.SubjectID == process.ProcessInstanceID && evidence.CorrelationID == process.ProcessInstanceID && evidence.ProcessAbsent && evidence.ProcessIdentityVerified && evidence.Result == corecontract.EvidenceSuccess && corecontract.ValidateEvidence(evidence) == nil {
			return true
		}
	}
	return false
}

func isEligiblePlan(incident Incident) bool {
	if len(incident.RequestIDs) == 0 || len(incident.LeaseIDs) != 1 || len(incident.ProcessInstanceIDs) == 0 || incident.ProcessAssignment != corecontract.ProcessAssigned {
		return false
	}
	verified := false
	for _, evidence := range incident.Evidence {
		if evidence.Type == "PROCESS_TERMINATION_VERIFIED" && evidence.Verification == EvidenceVerified {
			verified = true
		}
	}
	for _, reason := range incident.BlockingReasons {
		lower := strings.ToLower(reason)
		for _, blocker := range []string{"journal", "identity is unverified", "ownership", "model identity", "incomplete correlated", "survivor", "descendant", "contradict", "multiple leases"} {
			if strings.Contains(lower, blocker) {
				return false
			}
		}
	}
	return verified
}

func removeReason(values []string, suffix string) []string {
	result := values[:0]
	for _, value := range values {
		if !strings.Contains(value, suffix) {
			result = append(result, value)
		}
	}
	return result
}

func (s *Service) planForIncidentLocked(id string) RecoveryPlan {
	if incident, ok := s.incidents[id]; ok {
		return s.plans[incident.RecoveryPlanID]
	}
	return RecoveryPlan{}
}

func (s *Service) addAuditOnceLocked(event AuditEvent) error {
	for _, old := range s.audit {
		if old.IncidentID == event.IncidentID && old.PlanID == event.PlanID && old.Action == event.Action && old.Result == event.Result && old.Rejection == event.Rejection {
			return nil
		}
	}
	id, err := corecontract.NewID()
	if err != nil {
		return err
	}
	event.EventID, event.Timestamp = id, time.Now().UTC()
	s.audit = append(s.audit, event)
	return nil
}

func (s *Service) auditIDLocked(incidentID, planID, action string) string {
	for i := len(s.audit) - 1; i >= 0; i-- {
		if s.audit[i].IncidentID == incidentID && s.audit[i].PlanID == planID && s.audit[i].Action == action {
			return s.audit[i].EventID
		}
	}
	return ""
}

func (s *Service) auditLocked(event AuditEvent) error {
	if err := s.addAuditOnceLocked(event); err != nil {
		return err
	}
	return s.persistLocked()
}

func (s *Service) appendAuditLocked(event AuditEvent) error {
	id, err := corecontract.NewID()
	if err != nil {
		return err
	}
	event.EventID, event.Timestamp = id, time.Now().UTC()
	if event.Actor == "" {
		event.Actor = "UNVERIFIED"
	}
	s.audit = append(s.audit, event)
	if err := s.persistLocked(); err != nil {
		s.audit = s.audit[:len(s.audit)-1]
		return err
	}
	return nil
}

func (s *Service) persistLocked() error { return s.persistStateLocked(s.incidents, s.plans, s.audit) }

func (s *Service) persistStateLocked(incidents map[string]Incident, plans map[string]RecoveryPlan, audit []AuditEvent) error {
	data, err := json.MarshalIndent(diskState{Version: storeVersion, Incidents: incidents, Plans: plans, Audit: audit, Capabilities: s.capabilities, RevokedNonce: s.revokedNonce, Operations: s.operations}, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".larm-recovery-operator-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, s.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func journalEvidence(id string, at time.Time, verification VerificationStatus) Evidence {
	if at.IsZero() {
		at = time.Unix(0, 0).UTC()
	}
	return Evidence{Type: "JOURNAL_STATE", Source: "inference-journal", SubjectID: id, ObservedAt: at, Verification: verification, Guarantee: corecontract.GuaranteeRecovery}
}

func leaseTerminal(state scheduler.LeaseState) bool {
	switch state {
	case scheduler.Completed, scheduler.Failed, scheduler.Cancelled, scheduler.Expired:
		return true
	default:
		return false
	}
}
func evidenceIDs(values []Evidence) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.Type+":"+value.SubjectID)
	}
	sort.Strings(result)
	return result
}
func appendUnique(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, old := range values {
		if old == value {
			return values
		}
	}
	return append(values, value)
}
func appendEvidence(values []Evidence, value Evidence) []Evidence {
	for _, old := range values {
		if old.Type == value.Type && old.SubjectID == value.SubjectID && old.Verification == value.Verification {
			return values
		}
	}
	return append(values, value)
}
func cloneIncident(value Incident) Incident {
	value.RequestIDs = append([]string(nil), value.RequestIDs...)
	value.ExecutionIDs = append([]string(nil), value.ExecutionIDs...)
	value.LeaseIDs = append([]string(nil), value.LeaseIDs...)
	value.ModelIDs = append([]string(nil), value.ModelIDs...)
	value.ProcessInstanceIDs = append([]string(nil), value.ProcessInstanceIDs...)
	value.Evidence = append([]Evidence(nil), value.Evidence...)
	value.BlockingReasons = append([]string(nil), value.BlockingReasons...)
	value.JournalReferences = append([]string(nil), value.JournalReferences...)
	value.AuditReferences = append([]string(nil), value.AuditReferences...)
	return value
}

func validateStoredState(state diskState) error {
	for key, incident := range state.Incidents {
		if key == "" || incident.IncidentID != key || incident.CreatedAt.IsZero() || incident.UpdatedAt.IsZero() || incident.UpdatedAt.Before(incident.CreatedAt) {
			return errors.New("invalid incident identity or timestamps")
		}
		for _, evidence := range incident.Evidence {
			if err := ValidateEvidence(evidence); err != nil {
				return err
			}
		}
	}
	for key, plan := range state.Plans {
		if key == "" || plan.PlanID != key || plan.IncidentID == "" || plan.CreatedAt.IsZero() || plan.SnapshotVersion == "" || plan.EvidenceDigest == "" {
			return errors.New("invalid recovery plan")
		}
		if _, ok := state.Incidents[plan.IncidentID]; !ok {
			return errors.New("recovery plan references an unknown incident")
		}
	}
	for _, event := range state.Audit {
		if event.EventID == "" || event.IncidentID == "" || event.Timestamp.IsZero() || event.Actor == "" || event.Action == "" || event.Result == "" {
			return errors.New("invalid recovery audit event")
		}
	}
	for key, operation := range state.Operations {
		if key == "" || operation.OperationID != key || operation.IncidentID == "" || operation.PlanID == "" || operation.SnapshotVersion == "" || operation.EvidenceDigest == "" || operation.UpdatedAt.IsZero() {
			return errors.New("invalid reconciliation operation")
		}
		if operation.Phase == "" {
			return errors.New("reconciliation operation has no phase")
		}
		if _, ok := state.Incidents[operation.IncidentID]; !ok {
			return errors.New("reconciliation operation references an unknown incident")
		}
	}
	return nil
}

func digest(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func shortDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:24]
}
