package inference

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"local-ai-runtime/internal/corecontract"
)

func journalRecord(t *testing.T, r *Registry) string {
	t.Helper()
	requestID, err := r.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := r.Create(CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model"}); err != nil {
		t.Fatal(err)
	}
	if err := r.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	return requestID
}

func TestJournalPersistsInferenceAndLeaseCorrelation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	j, err := OpenJournal(JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(Options{Journal: j})
	if err != nil {
		t.Fatal(err)
	}
	requestID := journalRecord(t, r)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	j2, err := OpenJournal(JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Options{Journal: j2})
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.RecoveryStatus()
	if !status.RecoveryRequired || status.AdmissionAllowed || status.PendingInferenceCount != 1 || status.ProtectedLeaseCount != 1 {
		t.Fatalf("recovery status = %+v", status)
	}
	got, err := restarted.Get(requestID)
	if err != nil || got.LeaseID == "" || got.ModelID != "model" || got.RecoveryState != corecontract.RecoveryRequired {
		t.Fatalf("recovered inference = %+v, err=%v", got, err)
	}
	if _, err := restarted.Reserve(""); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("reserve during recovery = %v", err)
	}
}

func TestCleanShutdownDoesNotRequireRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	j, _ := OpenJournal(JournalOptions{Path: path})
	r, _ := Open(Options{Journal: j})
	id := journalRecord(t, r)
	if err := r.MarkProcessUnverified(id, 8080); err != nil {
		t.Fatal(err)
	}
	if err := r.StartTransport(id); err != nil {
		t.Fatal(err)
	}
	if err := r.Headers(id); err != nil {
		t.Fatal(err)
	}
	if err := r.BodyCompleted(id); err != nil {
		t.Fatal(err)
	}
	if err := r.CompleteJSON(id, 200); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkCleanShutdown(); err != nil {
		t.Fatal(err)
	}
	j2, err := OpenJournal(JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Options{Journal: j2})
	if err != nil {
		t.Fatal(err)
	}
	if status := restarted.RecoveryStatus(); status.RecoveryRequired || !status.AdmissionAllowed {
		t.Fatalf("clean restart status = %+v", status)
	}
}

func TestJournalFailureFailsClosedWithoutPublishingTransition(t *testing.T) {
	j, err := OpenJournal(JournalOptions{Path: filepath.Join(t.TempDir(), "recovery.json")})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := Open(Options{Journal: j})
	id := journalRecord(t, r)
	j.fault = func() error { return errors.New("injected sync failure") }
	if err := r.MarkProcessUnverified(id, 8080); err == nil {
		t.Fatal("expected journal failure")
	}
	snapshot, _ := r.Get(id)
	if snapshot.ProcessAssignment != corecontract.ProcessPending || !r.AdmissionBlocked() {
		t.Fatalf("failed transition was published: %+v", snapshot)
	}
	if protected, _ := r.Protected(snapshot.LeaseID, id, snapshot.ModelID); !protected {
		t.Fatal("journal failure released protection")
	}
}

func TestPhysicalReconciliationPreservesUnknown(t *testing.T) {
	j, err := OpenJournal(JournalOptions{Path: filepath.Join(t.TempDir(), "recovery.json")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(Options{Journal: j})
	if err != nil {
		t.Fatal(err)
	}
	id := journalRecord(t, r)
	snapshot, err := r.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	processID, _ := corecontract.NewID()
	ownerID, _ := corecontract.NewID()
	process := corecontract.ProcessInstance{SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: processID, OwnerInstanceID: ownerID, ModelID: snapshot.ModelID, ConfigFingerprint: "sha256:test", State: corecontract.ProcessReady, PID: 1, PGID: 1, StartIdentity: "start", OwnershipStatus: "OWNED_VERIFIED", ProcessIdentityStatus: "VERIFIED", ReadinessEvidenceID: "ready", RecoveryState: corecontract.RecoveryNormal}
	if err := r.AssignProcess(id, process, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.FailTransport(id, corecontract.ErrorTransport); err != nil {
		t.Fatal(err)
	}
	evidenceID, _ := corecontract.NewID()
	evidence := corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: evidenceID, SubjectKind: corecontract.SubjectProcess, SubjectID: processID, ProcessInstanceID: processID, Source: "supervisor-fixture", Type: corecontract.EvidenceProcessTerminationVerified, ObservedAt: time.Now().UTC(), Result: corecontract.EvidenceSuccess, CorrelationID: processID, Guarantee: corecontract.GuaranteeProcess, ProcessAbsent: true, ProcessIdentityVerified: true}
	if err := r.ReconcilePhysical(ReconciliationRequest{OperationKey: "op-1", LeaseID: snapshot.LeaseID, ModelID: snapshot.ModelID, RequestIDs: []string{id}, ProcessInstanceIDs: []string{processID}, ProcessEvidence: []corecontract.TerminalEvidence{evidence}}); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Semantic != corecontract.SemanticUnknown || got.PhysicalReconciliationState != "RECONCILED" || r.AdmissionBlocked() {
		t.Fatalf("physical reconciliation changed semantic or gate: %+v, blocked=%v", got, r.AdmissionBlocked())
	}
	if err := r.ReconciliationAllowed(snapshot.LeaseID, snapshot.ModelID, []string{id}); err != nil {
		t.Fatal(err)
	}
}

func TestJournalCorruptionAndVersionAreExplicit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recovery.json")
	if err := os.WriteFile(path, []byte(`{"version":999,"larm_instance_id":"x","clean_shutdown":true,"updated_at":"2026-01-01T00:00:00Z","records":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(JournalOptions{Path: path}); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("unsupported version error = %v", err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(JournalOptions{Path: path}); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("corrupt journal error = %v", err)
	}
}
