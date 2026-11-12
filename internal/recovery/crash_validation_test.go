package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

// TestCrashPrefixesPersistAndRetry covers the reachable durable prefixes. The
// callback is a controlled interruption at the same persistence boundaries a
// child-process crash would cross; it never edits state or fabricates process
// evidence.
func TestCrashPrefixesPersistAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point ReconciliationPhase
		want  ReconciliationPhase
	}{
		{name: "during preparation", point: PhasePrepared, want: PhasePrepared},
		{name: "before runtime", point: PhaseReconciling, want: PhaseReconciling},
		{name: "after runtime", point: PhaseRuntimeDone, want: PhaseRuntimeDone},
		{name: "after registry", point: PhaseRegistryDone, want: PhaseRegistryDone},
		{name: "after scheduler", point: PhaseSchedulerDone, want: PhaseSchedulerDone},
		{name: "before final audit", point: PhaseAuditPending, want: PhaseAuditPending},
		{name: "after final durable commit", point: PhaseComplete, want: PhaseComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCrashBoundaryFixture(t)
			f.service.fault = func(phase ReconciliationPhase) error {
				if phase == tc.point {
					return errors.New("controlled crash boundary")
				}
				return nil
			}
			if err := f.service.Execute(f.auth); err == nil {
				t.Fatal("fault boundary unexpectedly completed")
			}
			restarted := f.reopen(t)
			op, err := restarted.service.Operation(f.incident.IncidentID, f.plan.PlanID)
			if err != nil {
				t.Fatal(err)
			}
			if op.Phase != tc.want {
				t.Fatalf("durable phase = %s, want %s", op.Phase, tc.want)
			}
			restarted.service.fault = nil
			if err := restarted.service.Execute(f.auth); err != nil {
				t.Fatalf("retry after boundary: %v", err)
			}
			final, err := restarted.service.Operation(f.incident.IncidentID, f.plan.PlanID)
			if err != nil || final.Phase != PhaseComplete {
				t.Fatalf("retry operation = %+v, err=%v", final, err)
			}
			got, err := restarted.registry.Get(f.requestID)
			if err != nil || got.Semantic != corecontract.SemanticUnknown {
				t.Fatalf("retry changed UNKNOWN = %+v, err=%v", got, err)
			}
		})
	}
}

type crashBoundaryFixture struct {
	dir       string
	registry  *inference.Registry
	scheduler *scheduler.Scheduler
	service   *Service
	auth      Authorization
	incident  Incident
	plan      RecoveryPlan
	requestID string
	process   corecontract.ProcessInstance
}

type crashProcessAuthority struct {
	process corecontract.ProcessInstance
}

func (a crashProcessAuthority) ProcessInstances() []corecontract.ProcessInstance {
	return []corecontract.ProcessInstance{a.process}
}

func (a crashProcessAuthority) ReconcileProcess(_ context.Context, _ corecontract.ProcessReconciliationRequest) error {
	return nil
}

func newCrashBoundaryFixture(t *testing.T) *crashBoundaryFixture {
	t.Helper()
	dir := t.TempDir()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	owner := journal.OwnerInstanceID()
	processID, requestID, executionID, leaseID := crashID(t), crashID(t), crashID(t), crashID(t)
	process := corecontract.ProcessInstance{SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: processID, OwnerInstanceID: owner, LARMInstanceID: owner, ModelID: "model-1", PID: 1, PGID: 1, StartIdentity: "fixture-start", OSProcessStartIdentity: "fixture-start", ConfigFingerprint: "sha256:fixture", State: corecontract.ProcessReady, ProcessIdentityStatus: "VERIFIED", OwnershipStatus: "OWNED_VERIFIED", TerminationStatus: "NOT_REQUESTED", DescendantStatus: "SUPERVISED_GROUP", ReadinessEvidenceID: crashID(t), RecoveryState: corecontract.RecoveryNormal, UpdatedAt: time.Now().UTC()}
	if _, err := registry.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model-1"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := registry.AssignProcess(requestID, process, 1); err != nil {
		t.Fatal(err)
	}
	if err := registry.FailTransport(requestID, corecontract.ErrorTransport); err != nil {
		t.Fatal(err)
	}
	process.State = corecontract.ProcessTerminatedVerified
	process.TerminationStatus = "TERMINATED_VERIFIED"
	process.TerminatedAt = time.Now().UTC()
	process.WaitError = "exited_zero"
	process.DescendantStatus = "VERIFIED_OBSERVED_SCOPE"
	evidence := corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: crashID(t), SubjectKind: corecontract.SubjectProcess, SubjectID: processID, ProcessInstanceID: processID, Source: "fixture-supervisor", Type: corecontract.EvidenceProcessTerminationVerified, ObservedAt: time.Now().UTC(), Result: corecontract.EvidenceSuccess, CorrelationID: processID, Guarantee: corecontract.GuaranteeProcess, ProcessAbsent: true, ProcessIdentityVerified: true}
	process.TerminationEvidenceID, process.Evidence = evidence.EvidenceID, []corecontract.TerminalEvidence{evidence}
	if err := journal.PersistProcess(process); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(dir, "scheduler.json")
	writeRecoverySchedulerStore(t, store, leaseID)
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: "/bin/true"}, Models: map[string]config.ModelConfig{"model-1": {Path: filepath.Join(dir, "fixture.gguf"), Port: 1, Context: 1, Batch: 1, UBatch: 1, GPULayers: "all", Parallel: 1}}}, journal)
	sched, err := scheduler.NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 2, LeaseTTL: "1h", Heartbeat: "10m", MaxLease: "2h"}, store)
	if err != nil {
		t.Fatal(err)
	}
	sched.SetReleaseProtector(registry)
	sched.SetAdmissionGate(registry)
	service, err := Open(registry, sched, filepath.Join(dir, "recovery.json"), crashProcessAuthority{process: process})
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := service.Incidents()
	if err != nil {
		t.Fatal(err)
	}
	var incident Incident
	for _, candidate := range incidents {
		if len(candidate.RequestIDs) == 1 && candidate.RequestIDs[0] == requestID {
			incident = candidate
			break
		}
	}
	if incident.IncidentID == "" {
		t.Fatalf("request incident missing: %+v", incidents)
	}
	plan, err := service.Plan(incident.IncidentID)
	if err != nil || plan.SafetyClassification != SafeToResolve {
		t.Fatalf("crash plan = %+v, err=%v", plan, err)
	}
	return &crashBoundaryFixture{dir: dir, registry: registry, scheduler: sched, service: service, auth: Authorization{IncidentID: incident.IncidentID, PlanID: plan.PlanID, SnapshotVersion: plan.SnapshotVersion, EvidenceDigest: plan.EvidenceDigest, OperatorID: "uid:fixture", Authenticated: true, Mechanism: "fixture-capability", CapabilityID: "capability-fixture", Action: RecoveryReconcileAction}, incident: incident, plan: plan, requestID: requestID, process: process}
}

func (f *crashBoundaryFixture) reopen(t *testing.T) *crashBoundaryFixture {
	t.Helper()
	_ = f.scheduler.Shutdown(context.Background())
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(f.dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: "/bin/true"}}, journal)
	sched, err := scheduler.NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 2, LeaseTTL: "1h", Heartbeat: "10m", MaxLease: "2h"}, filepath.Join(f.dir, "scheduler.json"))
	if err != nil {
		t.Fatal(err)
	}
	sched.SetReleaseProtector(registry)
	sched.SetAdmissionGate(registry)
	service, err := Open(registry, sched, filepath.Join(f.dir, "recovery.json"), crashProcessAuthority{process: f.process})
	if err != nil {
		t.Fatal(err)
	}
	return &crashBoundaryFixture{dir: f.dir, registry: registry, scheduler: sched, service: service, auth: f.auth, incident: f.incident, plan: f.plan, requestID: f.requestID, process: f.process}
}

func writeRecoverySchedulerStore(t *testing.T, path, leaseID string) {
	t.Helper()
	key := crashID(t)
	now := time.Now().UTC()
	canonical, _ := json.Marshal(struct {
		Model string `json:"model"`
	}{"model-1"})
	hash := sha256.Sum256(canonical)
	file := map[string]any{"version": 1, "clean_shutdown": false, "records": map[string]any{key: map[string]any{"key": key, "fingerprint": hex.EncodeToString(hash[:]), "lease": map[string]any{"lease_id": leaseID, "model": "model-1", "state": "QUEUED", "created_at": now}, "created_at": now, "updated_at": now}}, "reconciliations": map[string]any{}}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func crashID(t *testing.T) string {
	t.Helper()
	id, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
