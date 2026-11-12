package inference

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"local-ai-runtime/internal/corecontract"
)

var (
	ErrNotFound           = errors.New("inference not found")
	ErrRequestIDCollision = errors.New("request ID is already associated with an inference")
	ErrInvalidRequestID   = errors.New("request ID must be a canonical UUIDv4")
)

const (
	EventRequestCreated       = "REQUEST_CREATED"
	EventAdmissionAccepted    = "REQUEST_ADMITTED"
	EventWaitingLease         = "WAITING_LEASE"
	EventLeaseActive          = "LEASE_ACTIVE"
	EventInferenceStarted     = "INFERENCE_STARTED"
	EventTransportPreparing   = "UPSTREAM_PREPARING"
	EventBackendConnected     = "BACKEND_CONNECTION_STARTED"
	EventHeadersReceived      = "HEADERS_RECEIVED"
	EventBodyStarted          = "BODY_STARTED"
	EventBodyCompleted        = "BODY_COMPLETED"
	EventHTTPCompleted        = "HTTP_COMPLETED"
	EventHTTPError            = "HTTP_ERROR"
	EventTransportError       = "TRANSPORT_ERROR"
	EventCancellationRequest  = "CANCELLATION_REQUESTED"
	EventCancellationObserved = "CANCELLATION_OBSERVED"
	EventDownstreamDisconnect = "DOWNSTREAM_DISCONNECTED"
	EventSSEDone              = "SSE_DONE_DETECTED"
	EventSSEEOF               = "SSE_EOF"
	EventSSETruncated         = "SSE_TRUNCATED"
)

type Options struct {
	MaxTerminalRecords int
	Journal            *Journal
}

type CreateInput struct {
	RequestID          string
	ExecutionID        string
	ExecutionDeclared  bool
	ExecutionOwnership string
	ModelID            string
	LeaseID            string
	Stream             bool
}

type Observation struct {
	Type       string    `json:"type"`
	ObservedAt time.Time `json:"observed_at"`
	HTTPStatus int       `json:"http_status,omitempty"`
}

type Snapshot struct {
	corecontract.Inference
	ExecutionDeclared           bool                            `json:"execution_declared"`
	ExecutionOwnership          string                          `json:"execution_ownership"`
	ExecutionLifetime           corecontract.ExecutionLifetime  `json:"execution_lifetime"`
	ProcessIdentityStatus       string                          `json:"process_identity_status"`
	ProcessPort                 int                             `json:"process_port,omitempty"`
	ProcessPID                  int                             `json:"process_pid,omitempty"`
	Observations                []Observation                   `json:"observations,omitempty"`
	TerminalEvidence            []corecontract.TerminalEvidence `json:"terminal_evidence,omitempty"`
	RecoveryState               corecontract.RecoveryState      `json:"recovery_state"`
	RecoveryReason              string                          `json:"recovery_reason,omitempty"`
	PhysicalReconciliationKey   string                          `json:"physical_reconciliation_key,omitempty"`
	PhysicalReconciliationState string                          `json:"physical_reconciliation_state,omitempty"`
	PhysicalReconciliationAt    time.Time                       `json:"physical_reconciliation_at,omitempty"`
}

type entry struct {
	inference          corecontract.Inference
	executionDeclared  bool
	executionOwnership string
	executionLifetime  corecontract.ExecutionLifetime
	processStatus      string
	processPort        int
	processPID         int
	observations       []Observation
	evidence           []corecontract.TerminalEvidence
	recoveryState      corecontract.RecoveryState
	recoveryReason     string
	physicalKey        string
	physicalState      string
	physicalAt         time.Time
}

type Registry struct {
	mu               sync.RWMutex
	records          map[string]*entry
	reserved         map[string]struct{}
	order            []string
	max              int
	journal          *Journal
	recoveryRequired bool
	recoveryReason   string
	journalError     string
	blockedLeases    map[string]string
}

func New(opts Options) *Registry {
	if opts.MaxTerminalRecords <= 0 {
		opts.MaxTerminalRecords = 1000
	}
	return &Registry{records: make(map[string]*entry), reserved: make(map[string]struct{}), max: opts.MaxTerminalRecords, journal: opts.Journal, blockedLeases: make(map[string]string)}
}

// Open reconstructs only diagnostic metadata. It never restores scheduler
// leases or backend processes. Non-terminal records remain protected after a
// restart and new admission is blocked until an explicit future recovery phase.
func Open(opts Options) (*Registry, error) {
	r := New(opts)
	if opts.Journal == nil {
		return r, nil
	}
	records := opts.Journal.Records()
	for _, persisted := range records {
		e := entryFromSnapshot(persisted)
		if _, exists := r.records[e.inference.RequestID]; exists {
			return nil, fmt.Errorf("duplicate persisted request %q", e.inference.RequestID)
		}
		r.records[e.inference.RequestID] = e
		r.order = append(r.order, e.inference.RequestID)
		if (!e.inference.IsTerminal() && e.physicalState != "RECONCILED") || (persisted.RecoveryState == corecontract.RecoveryRequired && e.physicalState != "RECONCILED") {
			r.recoveryRequired = true
			if r.recoveryReason == "" {
				r.recoveryReason = "journal contains an incomplete or ambiguous inference"
			}
			e.recoveryState = corecontract.RecoveryRequired
			e.recoveryReason = r.recoveryReason
		}
	}
	// A dirty journal is only a gate until every non-terminal record has an
	// independently durable physical disposition. The journal itself remains
	// historical evidence and is never rewritten to claim a clean shutdown.
	r.recomputeRecoveryLocked()
	return r, nil
}

// Reserve validates and claims a request identity before scheduler side effects.
// A reservation is not an inference record until Create succeeds.
func (r *Registry) Reserve(requestID string) (string, error) {
	if requestID == "" {
		var err error
		requestID, err = corecontract.NewID()
		if err != nil {
			return "", err
		}
	} else if !validID(requestID) {
		return "", ErrInvalidRequestID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recoveryRequired || r.journalError != "" {
		return "", ErrRecoveryRequired
	}
	if _, ok := r.records[requestID]; ok {
		return "", ErrRequestIDCollision
	}
	if _, ok := r.reserved[requestID]; ok {
		return "", ErrRequestIDCollision
	}
	r.reserved[requestID] = struct{}{}
	return requestID, nil
}

func (r *Registry) CancelReservation(requestID string) {
	r.mu.Lock()
	delete(r.reserved, requestID)
	r.mu.Unlock()
}

func (r *Registry) Create(input CreateInput) (Snapshot, error) {
	now := time.Now().UTC()
	if !validID(input.RequestID) || !validID(input.ExecutionID) || !validID(input.LeaseID) {
		return Snapshot{}, ErrInvalidRequestID
	}
	i := corecontract.Inference{
		SchemaVersion: corecontract.ContractVersion,
		RequestID:     input.RequestID, ExecutionID: input.ExecutionID, LeaseID: input.LeaseID,
		ModelID: input.ModelID, ProcessAssignment: corecontract.ProcessPending,
		Stream: input.Stream, Admission: corecontract.AdmissionPending,
		Transport: corecontract.TransportNotStarted, Semantic: corecontract.SemanticNotEvaluated,
		Cancellation: corecontract.CancelNone, CreatedAt: now, UpdatedAt: now,
	}
	if err := corecontract.ValidateInference(i); err != nil {
		return Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recoveryRequired || r.journalError != "" {
		return Snapshot{}, ErrRecoveryRequired
	}
	if _, ok := r.records[input.RequestID]; ok {
		return Snapshot{}, ErrRequestIDCollision
	}
	delete(r.reserved, input.RequestID)
	e := &entry{inference: i, executionDeclared: input.ExecutionDeclared, executionOwnership: input.ExecutionOwnership, executionLifetime: corecontract.ExecutionEphemeral, processStatus: "UNVERIFIED", recoveryState: corecontract.RecoveryNormal}
	e.observations = append(e.observations, Observation{Type: EventRequestCreated, ObservedAt: now})
	if err := r.persistCandidateLocked(input.RequestID, e, false); err != nil {
		r.blockedLeases[input.LeaseID] = "inference admission was not durably recorded"
		r.markJournalFailureLocked(err)
		return Snapshot{}, err
	}
	r.records[input.RequestID] = e
	r.order = append(r.order, input.RequestID)
	return snapshot(e), nil
}

func (r *Registry) Get(requestID string) (Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.records[requestID]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return snapshot(e), nil
}

func (r *Registry) List() []Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Snapshot, 0, len(r.records))
	for _, id := range r.order {
		if e := r.records[id]; e != nil {
			result = append(result, snapshot(e))
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (r *Registry) AdmissionAccepted(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Admission = corecontract.AdmissionAccepted
		p.UpdatedAt = now
		return p, nil, EventAdmissionAccepted, nil
	})
}

func (r *Registry) WaitingLease(id string) error { return r.event(id, EventWaitingLease) }
func (r *Registry) LeaseActive(id string) error  { return r.event(id, EventLeaseActive) }

func (r *Registry) MarkProcessUnverified(id string, port int) error {
	return r.MarkProcessUnverifiedWithPID(id, port, 0)
}

func (r *Registry) MarkProcessUnverifiedWithPID(id string, port, pid int) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.ProcessAssignment = corecontract.ProcessUnverified
		p.ProcessInstanceID = ""
		p.UpdatedAt = now
		e.processStatus, e.processPort, e.processPID = "UNVERIFIED", port, pid
		return p, nil, EventInferenceStarted, nil
	})
}

// AssignProcess records the durable supervisor identity before any upstream
// bytes are sent. A READY process with verified ownership is required; the
// registry never manufactures an assignment from PID or port metadata.
func (r *Registry) AssignProcess(id string, process corecontract.ProcessInstance, port int) error {
	if process.State != corecontract.ProcessReady || process.OwnershipStatus != "OWNED_VERIFIED" || process.ProcessInstanceID == "" || process.StartIdentity == "" {
		return errors.New("process assignment is not verified")
	}
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.ProcessAssignment = corecontract.ProcessAssigned
		p.ProcessInstanceID = process.ProcessInstanceID
		p.UpdatedAt = now
		e.processStatus, e.processPort, e.processPID = "VERIFIED", port, process.PID
		return p, nil, EventInferenceStarted, nil
	})
}

func (r *Registry) StartTransport(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport = corecontract.TransportStarted
		p.UpdatedAt = now
		return p, nil, EventBackendConnected, nil
	})
}

// PrepareTransport records the phase immediately before the upstream attempt.
// The transport state remains NOT_STARTED until the client call is made.
func (r *Registry) PrepareTransport(id string) error { return r.event(id, EventTransportPreparing) }

func (r *Registry) Headers(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport = corecontract.TransportHeaders
		p.UpdatedAt = now
		return p, nil, EventHeadersReceived, nil
	})
}

func (r *Registry) BodyStarted(id string) error { return r.event(id, EventBodyStarted) }

func (r *Registry) BodyCompleted(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport = corecontract.TransportBodyCompleted
		p.UpdatedAt = now
		return p, nil, EventBodyCompleted, nil
	})
}

func (r *Registry) CompleteJSON(id string, status int) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		ev := r.evidence(id, corecontract.EvidenceHTTPCompleted, corecontract.EvidenceSuccess, corecontract.GuaranteeSemantic, now, status)
		p := e.inference
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportCompleted, corecontract.SemanticSucceeded, corecontract.ErrorNone
		p.UpdatedAt = now
		return p, &ev, EventHTTPCompleted, nil
	})
}

func (r *Registry) FailHTTP(id string, status int) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportCompleted, corecontract.SemanticFailed, corecontract.ErrorBackend
		p.UpdatedAt = now
		ev := r.evidence(id, corecontract.EvidenceBackendError, corecontract.EvidenceFailure, corecontract.GuaranteeSemantic, now, status)
		return p, &ev, EventHTTPError, nil
	})
}

func (r *Registry) FailTransport(id string, class corecontract.ErrorClass) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportFailed, corecontract.SemanticUnknown, class
		p.UpdatedAt = now
		evType := corecontract.EvidenceTransportError
		if class == corecontract.ErrorBackend {
			evType = corecontract.EvidenceBackendError
		}
		ev := r.evidence(id, evType, corecontract.EvidenceAmbiguous, corecontract.GuaranteeTransport, now, 0)
		return p, &ev, EventTransportError, nil
	})
}

// FailBeforeUpstream is terminal because the caller has evidence that no
// upstream attempt was made. It is deliberately distinct from FailTransport,
// which remains ambiguous after an attempted write or connection.
func (r *Registry) FailBeforeUpstream(id string, class corecontract.ErrorClass) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportFailed, corecontract.SemanticFailed, class
		p.UpdatedAt = now
		ev := r.evidence(id, corecontract.EvidenceTransportError, corecontract.EvidenceFailure, corecontract.GuaranteeTransport, now, 0)
		ev.TransportTerminated = true
		return p, &ev, EventTransportError, nil
	})
}

func (r *Registry) MarkUnknown(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.ProcessAssignment = corecontract.ProcessUnverified
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportUnknown, corecontract.SemanticUnknown, corecontract.ErrorUnknown
		p.UpdatedAt = now
		return p, nil, EventTransportError, nil
	})
}

func (r *Registry) RequestCancellation(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Cancellation = corecontract.CancelRequested
		p.UpdatedAt = now
		return p, nil, EventCancellationRequest, nil
	})
}

// CancelBeforeUpstream confirms cancellation only for the pre-send phase.
// Calling context.CancelFunc alone never reaches this method.
func (r *Registry) CancelBeforeUpstream(id string) error {
	if err := r.RequestCancellation(id); err != nil {
		return err
	}
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Cancellation = corecontract.CancelConfirmed
		p.ErrorClass = corecontract.ErrorCancellation
		p.UpdatedAt = now
		ev := r.evidence(id, corecontract.EvidenceCancellationConfirmed, corecontract.EvidenceSuccess, corecontract.GuaranteeCancellation, now, 0)
		ev.TransportTerminated = true
		return p, &ev, EventCancellationObserved, nil
	})
}

func (r *Registry) MarkDownstreamDisconnected(id string) error {
	if s, err := r.Get(id); err == nil && s.Cancellation == corecontract.CancelNone {
		if err := r.RequestCancellation(id); err != nil {
			return err
		}
	}
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Transport, p.Semantic, p.Cancellation, p.ErrorClass = corecontract.TransportUnknown, corecontract.SemanticUnknown, corecontract.CancelUnknown, corecontract.ErrorClientDisconnect
		if p.SSE != nil {
			s := cloneSSE(p.SSE)
			s.ClientDisconnected = true
			p.SSE = &s
		}
		p.UpdatedAt = now
		return p, nil, EventDownstreamDisconnect, nil
	})
}

func (r *Registry) MarkCancellationUnknown(id string) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		p := e.inference
		p.Cancellation = corecontract.CancelUnknown
		p.UpdatedAt = now
		return p, nil, EventCancellationObserved, nil
	})
}

// ReconciliationRequest is the owner-checked physical disposition recorded by
// the Registry. It never changes the semantic result of an inference.
type ReconciliationRequest struct {
	OperationKey       string
	LeaseID            string
	ModelID            string
	RequestIDs         []string
	ProcessInstanceIDs []string
	ProcessEvidence    []corecontract.TerminalEvidence
}

// ReconcilePhysical records that the correlated inference protection was
// evaluated after the owning Runtime Manager verified process termination.
func (r *Registry) ReconcilePhysical(req ReconciliationRequest) error {
	if req.OperationKey == "" || req.LeaseID == "" || req.ModelID == "" || len(req.RequestIDs) == 0 {
		return errors.New("physical reconciliation requires operation, lease, model and request identities")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	wanted := make(map[string]struct{}, len(req.RequestIDs))
	processes := make(map[string]struct{}, len(req.ProcessInstanceIDs))
	for _, processID := range req.ProcessInstanceIDs {
		if processID == "" {
			return errors.New("physical reconciliation contains an empty process identity")
		}
		processes[processID] = struct{}{}
	}
	evidenceByProcess := make(map[string]corecontract.TerminalEvidence, len(req.ProcessEvidence))
	for _, evidence := range req.ProcessEvidence {
		if evidence.Type != corecontract.EvidenceProcessTerminationVerified || evidence.SubjectKind != corecontract.SubjectProcess || evidence.SubjectID == "" || evidence.SubjectID != evidence.ProcessInstanceID || evidence.CorrelationID != evidence.SubjectID || evidence.Result != corecontract.EvidenceSuccess || !evidence.ProcessAbsent || !evidence.ProcessIdentityVerified || corecontract.ValidateEvidence(evidence) != nil {
			return errors.New("invalid correlated process termination evidence")
		}
		evidenceByProcess[evidence.SubjectID] = evidence
	}
	for _, id := range req.RequestIDs {
		if !validID(id) {
			return ErrInvalidRequestID
		}
		if _, exists := wanted[id]; exists {
			return errors.New("physical reconciliation contains duplicate request identity")
		}
		wanted[id] = struct{}{}
	}
	var matched []string
	for id, e := range r.records {
		if e.inference.LeaseID != req.LeaseID {
			continue
		}
		matched = append(matched, id)
		if _, ok := wanted[id]; !ok {
			return errors.New("physical reconciliation omitted a correlated inference")
		}
		if e.inference.ModelID != req.ModelID || e.inference.ProcessAssignment != corecontract.ProcessAssigned || e.inference.ProcessInstanceID == "" {
			return errors.New("physical reconciliation has inconsistent inference ownership")
		}
		if _, ok := processes[e.inference.ProcessInstanceID]; !ok {
			return errors.New("physical reconciliation omitted a correlated process")
		}
		if _, ok := evidenceByProcess[e.inference.ProcessInstanceID]; !ok {
			return errors.New("physical reconciliation omitted process termination evidence")
		}
	}
	if len(matched) != len(wanted) {
		return errors.New("physical reconciliation references an unknown or missing inference")
	}
	for _, id := range matched {
		e := r.records[id]
		if e.physicalState == "RECONCILED" && e.physicalKey != req.OperationKey {
			return errors.New("inference was physically reconciled by another operation")
		}
	}
	updated := make(map[string]*entry, len(matched))
	now := time.Now().UTC()
	for _, id := range matched {
		copy := cloneEntry(r.records[id])
		copy.physicalKey, copy.physicalState, copy.physicalAt = req.OperationKey, "RECONCILED", now
		copy.recoveryState = corecontract.RecoveryResolved
		updated[id] = copy
	}
	if r.journal != nil {
		records := make([]Snapshot, 0, len(r.order))
		for _, id := range r.order {
			if candidate, ok := updated[id]; ok {
				records = append(records, snapshot(candidate))
			} else if existing := r.records[id]; existing != nil {
				records = append(records, snapshot(existing))
			}
		}
		if err := r.journal.Persist(records, false); err != nil {
			r.markJournalFailureLocked(err)
			return err
		}
	}
	for id, candidate := range updated {
		*r.records[id] = *candidate
	}
	r.recomputeRecoveryLocked()
	return nil
}

// ReconciliationAllowed is consulted by Scheduler. The Registry remains the
// authority for the complete inference set attached to a lease.
func (r *Registry) ReconciliationAllowed(leaseID, model string, requestIDs []string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	wanted := make(map[string]struct{}, len(requestIDs))
	for _, id := range requestIDs {
		wanted[id] = struct{}{}
	}
	count := 0
	for id, e := range r.records {
		if e.inference.LeaseID != leaseID {
			continue
		}
		count++
		if e.inference.ModelID != model || e.physicalState != "RECONCILED" || e.inference.IsTerminal() || e.physicalKey == "" {
			return errors.New("correlated inference is not physically reconciled")
		}
		if _, ok := wanted[id]; !ok {
			return errors.New("correlated inference was omitted")
		}
	}
	if count == 0 || count != len(wanted) {
		return errors.New("lease has an incomplete correlated inference set")
	}
	return nil
}

// Protected is the scheduler's read-only release guard. It intentionally
// treats missing or mismatched request identity as protected (fail closed).
func (r *Registry) Protected(leaseID, requestID, model string) (bool, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if reason := r.blockedLeases[leaseID]; reason != "" {
		return true, reason
	}
	if r.journalError != "" {
		return true, "inference journal is unavailable"
	}
	if r.recoveryRequired {
		return true, "runtime recovery is required"
	}
	if requestID != "" {
		e, ok := r.records[requestID]
		if !ok || e.inference.LeaseID != leaseID || (model != "" && e.inference.ModelID != model) {
			return true, "release request identity is missing or does not own the lease"
		}
	}
	for _, id := range r.order {
		e := r.records[id]
		if e == nil || e.inference.LeaseID != leaseID {
			continue
		}
		if e.inference.IsTerminal() && len(e.evidence) == 0 {
			return true, "terminal inference has no terminal evidence"
		}
		if !e.inference.IsTerminal() || e.inference.IsAmbiguous() {
			return true, "an inference is active or has unresolved upstream state"
		}
	}
	return false, ""
}

func (r *Registry) SSEHeaders(id string) error {
	return r.updateSSE(id, func(p *corecontract.Inference, s *corecontract.SSEObservation, now time.Time) {
		s.HeadersReceived = true
		p.UpdatedAt = now
	}, EventHeadersReceived)
}

func (r *Registry) SSEChunk(id string, done bool, count uint64) error {
	return r.updateSSE(id, func(p *corecontract.Inference, s *corecontract.SSEObservation, now time.Time) {
		s.FirstChunk = s.FirstChunk || count == 1
		s.ChunkCount = count
		s.DoneDetected = s.DoneDetected || done
		p.UpdatedAt = now
	}, func() string {
		if done {
			return EventSSEDone
		}
		return EventBodyStarted
	}())
}

func (r *Registry) SSEEOF(id string, done bool, count uint64) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		s := cloneSSE(e.inference.SSE)
		s.EOF, s.DoneDetected, s.ChunkCount = true, done, count
		p := e.inference
		p.SSE = &s
		p.Transport = corecontract.TransportCompleted
		p.UpdatedAt = now
		var ev corecontract.TerminalEvidence
		if done {
			p.Semantic, p.ErrorClass = corecontract.SemanticSucceeded, corecontract.ErrorNone
			ev = r.evidence(id, corecontract.EvidenceSSECompleted, corecontract.EvidenceSuccess, corecontract.GuaranteeSemantic, now, 0)
			return p, &ev, EventSSEEOF, nil
		}
		p.Semantic, p.ErrorClass, s.Truncated = corecontract.SemanticTruncated, corecontract.ErrorSSETruncated, true
		p.SSE = &s
		ev = r.evidence(id, corecontract.EvidenceSSETruncated, corecontract.EvidenceFailure, corecontract.GuaranteeSemantic, now, 0)
		return p, &ev, EventSSETruncated, nil
	})
}

func (r *Registry) SSEReadError(id string, count uint64) error {
	return r.update(id, func(e *entry, now time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error) {
		s := cloneSSE(e.inference.SSE)
		s.ReadError, s.Truncated, s.ChunkCount = true, true, count
		p := e.inference
		p.SSE = &s
		p.Transport, p.Semantic, p.ErrorClass = corecontract.TransportFailed, corecontract.SemanticTruncated, corecontract.ErrorSSETruncated
		p.UpdatedAt = now
		ev := r.evidence(id, corecontract.EvidenceSSETruncated, corecontract.EvidenceFailure, corecontract.GuaranteeSemantic, now, 0)
		return p, &ev, EventTransportError, nil
	})
}

func (r *Registry) event(id, kind string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.records[id]
	if !ok {
		return ErrNotFound
	}
	proposed := cloneEntry(e)
	proposed.observations = append(proposed.observations, Observation{Type: kind, ObservedAt: time.Now().UTC()})
	if err := r.persistCandidateLocked(id, proposed, false); err != nil {
		r.markJournalFailureLocked(err)
		return err
	}
	*e = *proposed
	return nil
}

func (r *Registry) update(id string, fn func(*entry, time.Time) (corecontract.Inference, *corecontract.TerminalEvidence, string, error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.records[id]
	if !ok {
		return ErrNotFound
	}
	now := time.Now().UTC()
	working := cloneEntry(e)
	proposed, ev, kind, err := fn(working, now)
	if err != nil {
		return err
	}
	if err := corecontract.ValidateInferenceTransition(e.inference, proposed, ev); err != nil {
		return fmt.Errorf("inference transition: %w", err)
	}
	updated := cloneEntry(e)
	updated.inference = proposed
	updated.observations = append(updated.observations, Observation{Type: kind, ObservedAt: now})
	if ev != nil {
		updated.evidence = append(updated.evidence, *ev)
		updated.inference.TerminalEvidenceIDs = append(updated.inference.TerminalEvidenceIDs, ev.EvidenceID)
	}
	if err := r.persistCandidateLocked(id, updated, false); err != nil {
		r.markJournalFailureLocked(err)
		return err
	}
	*e = *updated
	r.retireLocked()
	return nil
}

func (r *Registry) updateSSE(id string, fn func(*corecontract.Inference, *corecontract.SSEObservation, time.Time), kind string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.records[id]
	if !ok {
		return ErrNotFound
	}
	now := time.Now().UTC()
	p := e.inference
	s := cloneSSE(p.SSE)
	p.SSE = &s
	fn(&p, &s, now)
	if err := corecontract.ValidateInferenceTransition(e.inference, p, nil); err != nil {
		return fmt.Errorf("inference transition: %w", err)
	}
	updated := cloneEntry(e)
	updated.inference = p
	updated.observations = append(updated.observations, Observation{Type: kind, ObservedAt: now})
	if err := r.persistCandidateLocked(id, updated, false); err != nil {
		r.markJournalFailureLocked(err)
		return err
	}
	*e = *updated
	return nil
}

func (r *Registry) evidence(requestID string, typ corecontract.EvidenceType, result corecontract.EvidenceResult, guarantee corecontract.GuaranteeScope, now time.Time, status int) corecontract.TerminalEvidence {
	id, _ := corecontract.NewID()
	return corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: id, SubjectKind: corecontract.SubjectRequest, SubjectID: requestID, RequestID: requestID, Source: "larm-inference-registry", Type: typ, ObservedAt: now, Result: result, CorrelationID: requestID, Guarantee: guarantee, HTTPStatus: status, SSEDone: typ == corecontract.EvidenceSSECompleted, EOF: typ == corecontract.EvidenceHTTPCompleted || typ == corecontract.EvidenceSSECompleted, TransportTerminated: typ == corecontract.EvidenceHTTPCompleted || typ == corecontract.EvidenceSSECompleted}
}

func (r *Registry) retireLocked() {
	terminal := 0
	for _, id := range r.order {
		if e := r.records[id]; e != nil && e.inference.IsTerminal() && len(e.evidence) > 0 && e.recoveryState != corecontract.RecoveryRequired {
			terminal++
		}
	}
	for terminal > r.max {
		for i, id := range r.order {
			e := r.records[id]
			if e != nil && e.inference.IsTerminal() {
				delete(r.records, id)
				r.order = append(r.order[:i], r.order[i+1:]...)
				terminal--
				break
			}
		}
	}
}

func snapshot(e *entry) Snapshot {
	i := e.inference
	s := Snapshot{Inference: i, ExecutionDeclared: e.executionDeclared, ExecutionOwnership: e.executionOwnership, ExecutionLifetime: e.executionLifetime, ProcessIdentityStatus: e.processStatus, ProcessPort: e.processPort, ProcessPID: e.processPID, RecoveryState: e.recoveryState, RecoveryReason: e.recoveryReason, PhysicalReconciliationKey: e.physicalKey, PhysicalReconciliationState: e.physicalState, PhysicalReconciliationAt: e.physicalAt}
	s.Observations = append([]Observation(nil), e.observations...)
	s.TerminalEvidence = append([]corecontract.TerminalEvidence(nil), e.evidence...)
	if i.SSE != nil {
		v := *i.SSE
		s.SSE = &v
	}
	return s
}

func cloneEntry(e *entry) *entry {
	copy := *e
	copy.observations = append([]Observation(nil), e.observations...)
	copy.evidence = append([]corecontract.TerminalEvidence(nil), e.evidence...)
	if e.inference.SSE != nil {
		s := *e.inference.SSE
		copy.inference.SSE = &s
	}
	copy.inference.TerminalEvidenceIDs = append([]string(nil), e.inference.TerminalEvidenceIDs...)
	return &copy
}

func entryFromSnapshot(s Snapshot) *entry {
	if s.RecoveryState == "" {
		s.RecoveryState = corecontract.RecoveryNormal
	}
	return &entry{inference: s.Inference, executionDeclared: s.ExecutionDeclared, executionOwnership: s.ExecutionOwnership, executionLifetime: s.ExecutionLifetime, processStatus: s.ProcessIdentityStatus, processPort: s.ProcessPort, processPID: s.ProcessPID, observations: append([]Observation(nil), s.Observations...), evidence: append([]corecontract.TerminalEvidence(nil), s.TerminalEvidence...), recoveryState: s.RecoveryState, recoveryReason: s.RecoveryReason, physicalKey: s.PhysicalReconciliationKey, physicalState: s.PhysicalReconciliationState, physicalAt: s.PhysicalReconciliationAt}
}

func (r *Registry) persistCandidateLocked(requestID string, candidate *entry, clean bool) error {
	if r.journal == nil {
		return nil
	}
	records := make([]Snapshot, 0, len(r.records)+1)
	for _, id := range r.order {
		if id == requestID {
			records = append(records, snapshot(candidate))
			continue
		}
		if existing := r.records[id]; existing != nil {
			records = append(records, snapshot(existing))
		}
	}
	if _, exists := r.records[requestID]; !exists {
		records = append(records, snapshot(candidate))
	}
	records = trimTerminalSnapshots(records, r.max)
	return r.journal.Persist(records, clean)
}

func trimTerminalSnapshots(records []Snapshot, max int) []Snapshot {
	if max <= 0 {
		return records
	}
	terminal := 0
	for _, record := range records {
		if record.Inference.IsTerminal() && len(record.TerminalEvidence) > 0 && record.RecoveryState != corecontract.RecoveryRequired {
			terminal++
		}
	}
	for terminal > max {
		removed := false
		for i, record := range records {
			if record.Inference.IsTerminal() && len(record.TerminalEvidence) > 0 && record.RecoveryState != corecontract.RecoveryRequired {
				records = append(records[:i], records[i+1:]...)
				terminal--
				removed = true
				break
			}
		}
		if !removed {
			break
		}
	}
	return records
}

func (r *Registry) markJournalFailureLocked(err error) {
	r.journalError = err.Error()
	r.recoveryRequired = true
	r.recoveryReason = "journal write or synchronization failed"
}

func (r *Registry) recomputeRecoveryLocked() {
	if r.journalError != "" {
		r.recoveryRequired = true
		return
	}
	for _, id := range r.order {
		if e := r.records[id]; e != nil && (!e.inference.IsTerminal() && e.physicalState != "RECONCILED") {
			r.recoveryRequired = true
			return
		}
	}
	r.recoveryRequired = false
	r.recoveryReason = ""
}

// ProtectLease is used when scheduler acceptance precedes inference creation
// but durable admission fails. The lease remains protected in this process.
func (r *Registry) ProtectLease(leaseID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if leaseID != "" {
		r.blockedLeases[leaseID] = reason
	}
}

func (r *Registry) AdmissionBlocked() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.recoveryRequired || r.journalError != ""
}

func (r *Registry) MarkCleanShutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.journal == nil {
		return nil
	}
	if r.recoveryRequired || r.journalError != "" {
		return ErrRecoveryRequired
	}
	if r.hasProtectedLocked() {
		return ErrRecoveryRequired
	}
	records := make([]Snapshot, 0, len(r.order))
	for _, id := range r.order {
		if e := r.records[id]; e != nil {
			records = append(records, snapshot(e))
		}
	}
	if err := r.journal.Persist(records, true); err != nil {
		r.markJournalFailureLocked(err)
		return err
	}
	return nil
}

func (r *Registry) hasProtectedLocked() bool {
	for _, id := range r.order {
		if e := r.records[id]; e != nil && (!e.inference.IsTerminal() && e.physicalState != "RECONCILED") {
			return true
		}
	}
	return false
}

func cloneSSE(s *corecontract.SSEObservation) corecontract.SSEObservation {
	if s == nil {
		return corecontract.SSEObservation{}
	}
	return *s
}

func validID(value string) bool {
	var zero corecontract.Inference
	zero.SchemaVersion, zero.RequestID, zero.ExecutionID, zero.LeaseID = corecontract.ContractVersion, value, value, value
	zero.ModelID, zero.Admission, zero.Transport, zero.Semantic, zero.Cancellation = "x", corecontract.AdmissionPending, corecontract.TransportNotStarted, corecontract.SemanticNotEvaluated, corecontract.CancelNone
	zero.ProcessAssignment = corecontract.ProcessPending
	now := time.Now()
	zero.CreatedAt, zero.UpdatedAt = now, now
	return corecontract.ValidateInference(zero) == nil
}

func ValidID(value string) bool { return validID(value) }
