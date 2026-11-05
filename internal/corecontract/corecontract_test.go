package corecontract

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

var contractNow = time.Date(2026, time.November, 3, 12, 0, 0, 0, time.UTC)

func testID(n string) string { return "00000000-0000-4000-8000-000000000" + n }

func execution(state ExecutionState) Execution {
	return Execution{SchemaVersion: ContractVersion, ExecutionID: testID("001"), Initiator: "test", Lifetime: ExecutionPersistent, ModelID: "model-a", State: state, CreatedAt: contractNow, UpdatedAt: contractNow, RecoveryState: RecoveryNormal}
}

func inference(request string, transport TransportState, semantic SemanticState) Inference {
	return Inference{SchemaVersion: ContractVersion, RequestID: request, ExecutionID: testID("001"), LeaseID: testID("002"), ModelID: "model-a", ProcessInstanceID: testID("003"), ProcessAssignment: ProcessAssigned, Admission: AdmissionAccepted, Transport: transport, Semantic: semantic, Cancellation: CancelNone, CreatedAt: contractNow, UpdatedAt: contractNow}
}

func lease(state FIFOState, release ReleaseState) Lease {
	return Lease{SchemaVersion: ContractVersion, LeaseID: testID("002"), ExecutionID: testID("001"), Ownership: LeasePersistentOwnership, ModelID: "model-a", FIFO: state, Activation: ActivationActive, Release: release, RecoveryState: RecoveryNormal}
}

func evidence(id, subject string, kind EvidenceType, result EvidenceResult) *TerminalEvidence {
	return &TerminalEvidence{SchemaVersion: ContractVersion, EvidenceID: testID(id), SubjectKind: SubjectRequest, SubjectID: subject, Source: "test-observer", Type: kind, ObservedAt: contractNow, Result: result, CorrelationID: subject, Guarantee: GuaranteeSemantic, EOF: true, SSEDone: kind == EvidenceSSECompleted}
}

func executionEvidence(id string, result EvidenceResult) *TerminalEvidence {
	return &TerminalEvidence{SchemaVersion: ContractVersion, EvidenceID: testID(id), SubjectKind: SubjectExecution, SubjectID: testID("001"), Source: "test-observer", Type: EvidenceExecutionCompleted, ObservedAt: contractNow, Result: result, CorrelationID: testID("001"), Guarantee: GuaranteeSemantic}
}

func TestExecutionLifecycle(t *testing.T) {
	created := execution(ExecutionCreated)
	waiting := created
	waiting.State = ExecutionWaitingModel
	active := waiting
	active.State = ExecutionActive
	finalizing := active
	finalizing.State = ExecutionFinalizing
	completed := finalizing
	completed.State = ExecutionCompleted
	completed.Completion = &Completion{Result: CompletionSucceeded, EvidenceIDs: []string{testID("010")}, At: contractNow}
	for _, step := range []struct{ from, to Execution }{{created, waiting}, {waiting, active}, {active, finalizing}} {
		if err := ValidateExecutionTransition(step.from, step.to, nil, nil); err != nil {
			t.Fatalf("lifecycle transition: %v", err)
		}
	}
	if err := ValidateExecutionTransition(finalizing, completed, executionEvidence("010", EvidenceSuccess), nil); err != nil {
		t.Fatalf("completion: %v", err)
	}
}

func TestExecutionWithMultipleInferences(t *testing.T) {
	current := execution(ExecutionFinalizing)
	current.InferenceIDs = []string{testID("101"), testID("102")}
	proposed := current
	proposed.State = ExecutionCompleted
	proposed.Completion = &Completion{Result: CompletionSucceeded, EvidenceIDs: []string{testID("010")}, At: contractNow}
	first := inference(testID("101"), TransportCompleted, SemanticSucceeded)
	second := inference(testID("102"), TransportCompleted, SemanticFailed)
	if err := ValidateExecutionTransition(current, proposed, executionEvidence("010", EvidenceSuccess), []Inference{first, second}); err != nil {
		t.Fatalf("expected terminal multi-inference execution: %v", err)
	}
	second.Transport = TransportHeaders
	if err := ValidateExecutionTransition(current, proposed, executionEvidence("010", EvidenceSuccess), []Inference{first, second}); err == nil {
		t.Fatal("active inference must block execution completion")
	}
}

func TestLeaseOwnershipRejectsForeignLease(t *testing.T) {
	i := inference(testID("101"), TransportNotStarted, SemanticNotEvaluated)
	e := execution(ExecutionActive)
	l := lease(FIFOActive, ReleaseNotRequested)
	l.ExecutionID = testID("999")
	if err := ValidateInferenceOwnership(i, e, l); err == nil {
		t.Fatal("foreign lease was accepted")
	}
}

func TestCancellationRequestedIsNotTermination(t *testing.T) {
	current := inference(testID("101"), TransportStarted, SemanticNotEvaluated)
	proposed := current
	proposed.Cancellation = CancelRequested
	if err := ValidateInferenceTransition(current, proposed, nil); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	if proposed.IsTerminal() {
		t.Fatal("cancel request must not be terminal")
	}
}

func TestCancellationConfirmedRequiresCorrelatedEvidence(t *testing.T) {
	current := inference(testID("101"), TransportStarted, SemanticNotEvaluated)
	requested := current
	requested.Cancellation = CancelRequested
	if err := ValidateInferenceTransition(current, requested, nil); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	proposed := requested
	proposed.Cancellation = CancelConfirmed
	if err := ValidateInferenceTransition(requested, proposed, evidence("011", current.RequestID, EvidenceCancellationConfirmed, EvidenceSuccess)); err == nil {
		t.Fatal("uncorroborated cancellation was accepted")
	}
	confirmed := evidence("011", current.RequestID, EvidenceCancellationConfirmed, EvidenceSuccess)
	confirmed.Guarantee = GuaranteeCancellation
	confirmed.TransportTerminated = true
	if err := ValidateInferenceTransition(requested, proposed, confirmed); err != nil {
		t.Fatalf("confirmed cancellation: %v", err)
	}
}

func TestSSECompleteRequiresDoneAndEOF(t *testing.T) {
	current := inference(testID("101"), TransportHeaders, SemanticNotEvaluated)
	current.Stream = true
	proposed := current
	proposed.Transport = TransportCompleted
	proposed.Semantic = SemanticSucceeded
	proposed.SSE = &SSEObservation{HeadersReceived: true, FirstChunk: true, ChunkCount: 2, DoneDetected: true, EOF: true}
	ev := evidence("012", current.RequestID, EvidenceSSECompleted, EvidenceSuccess)
	if err := ValidateInferenceTransition(current, proposed, ev); err != nil {
		t.Fatalf("complete SSE: %v", err)
	}
}

func TestSSETruncatedCannotBecomeSuccess(t *testing.T) {
	current := inference(testID("101"), TransportHeaders, SemanticNotEvaluated)
	current.Stream = true
	proposed := current
	proposed.Transport = TransportCompleted
	proposed.Semantic = SemanticSucceeded
	proposed.SSE = &SSEObservation{HeadersReceived: true, FirstChunk: true, ChunkCount: 1, EOF: true}
	truncated := evidence("013", current.RequestID, EvidenceSSETruncated, EvidenceFailure)
	if err := ValidateInferenceTransition(current, proposed, truncated); err == nil {
		t.Fatal("EOF without DONE was classified as success")
	}
	proposed.Semantic = SemanticTruncated
	proposed.SSE.Truncated = true
	if err := ValidateInferenceTransition(current, proposed, truncated); err != nil {
		t.Fatalf("truncated SSE: %v", err)
	}
}

func TestUnknownInferenceKeepsOwnership(t *testing.T) {
	i := inference(testID("101"), TransportUnknown, SemanticUnknown)
	if !i.IsAmbiguous() || i.IsTerminal() {
		t.Fatal("unknown inference classification is unsafe")
	}
	l := lease(FIFOActive, ReleaseInProgress)
	l.InferenceIDs = []string{i.RequestID}
	l.Release = ReleaseReleased
	l.FIFO = FIFOCompleted
	l.Activation = ActivationInactive
	if err := ValidateLeaseTransition(lease(FIFOActive, ReleaseInProgress), l, nil, []Inference{i}); err == nil {
		t.Fatal("lease released with unknown inference")
	}
}

func TestProcessInstanceIdentityAndTermination(t *testing.T) {
	p := ProcessInstance{SchemaVersion: ContractVersion, ProcessInstanceID: testID("003"), OwnerInstanceID: testID("004"), ModelID: "model-a", ConfigFingerprint: "sha256:test", State: ProcessPreparing, RecoveryState: RecoveryNormal}
	states := []ProcessState{ProcessStarting, ProcessWaitingReadiness, ProcessReady}
	for _, state := range states {
		next := p
		next.State = state
		if state == ProcessReady {
			next.PID, next.PGID, next.StartIdentity, next.ReadinessEvidenceID = 42, 42, "boot:1", testID("020")
		}
		if err := ValidateProcessTransition(p, next, nil); err != nil {
			t.Fatalf("process %s: %v", state, err)
		}
		p = next
	}
	p.State = ProcessTerminationRequested
	if err := ValidateProcessTransition(ProcessInstance{SchemaVersion: ContractVersion, ProcessInstanceID: testID("003"), OwnerInstanceID: testID("004"), ModelID: "model-a", ConfigFingerprint: "sha256:test", State: ProcessReady, PID: 42, PGID: 42, StartIdentity: "boot:1", ReadinessEvidenceID: testID("020"), RecoveryState: RecoveryNormal}, p, nil); err != nil {
		t.Fatalf("termination request: %v", err)
	}
	terminating := p
	terminating.State = ProcessTerminating
	if err := ValidateProcessTransition(p, terminating, nil); err != nil {
		t.Fatalf("termination progress: %v", err)
	}
	verified := terminating
	verified.State = ProcessTerminatedVerified
	verified.TerminationEvidenceID = testID("021")
	ev := &TerminalEvidence{SchemaVersion: ContractVersion, EvidenceID: testID("021"), SubjectKind: SubjectProcess, SubjectID: testID("003"), Source: "test-observer", Type: EvidenceProcessTerminationVerified, ObservedAt: contractNow, Result: EvidenceSuccess, CorrelationID: testID("003"), Guarantee: GuaranteeProcess, ProcessAbsent: true, ProcessIdentityVerified: true}
	if err := ValidateProcessTransition(terminating, verified, ev); err != nil {
		t.Fatalf("verified process termination: %v", err)
	}
}

func TestLateEventsCannotReactivateTerminalInference(t *testing.T) {
	current := inference(testID("101"), TransportCompleted, SemanticSucceeded)
	proposed := current
	proposed.Semantic = SemanticFailed
	if err := ValidateInferenceTransition(current, proposed, nil); err == nil {
		t.Fatal("late event changed terminal inference")
	}
}

func TestTransitionIdempotence(t *testing.T) {
	e := execution(ExecutionActive)
	if err := ValidateExecutionTransition(e, e, nil, nil); err != nil {
		t.Fatalf("execution idempotence: %v", err)
	}
	i := inference(testID("101"), TransportStarted, SemanticNotEvaluated)
	if err := ValidateInferenceTransition(i, i, nil); err != nil {
		t.Fatalf("inference idempotence: %v", err)
	}
}

func TestOpenAIWithoutExecutionUsesEphemeralExecution(t *testing.T) {
	e := NewEphemeralExecution(testID("001"), "model-a", contractNow)
	if err := ValidateExecution(e); err != nil {
		t.Fatalf("ephemeral execution: %v", err)
	}
	l := lease(FIFOActive, ReleaseNotRequested)
	l.Ownership = LeaseEphemeralOwnership
	i := inference(testID("101"), TransportNotStarted, SemanticNotEvaluated)
	i.ProcessAssignment = ProcessPending
	i.ProcessInstanceID = ""
	if err := ValidateInferenceOwnership(i, e, l); err != nil {
		t.Fatalf("ephemeral ownership: %v", err)
	}
}

func TestRetriesUseDistinctRequestIDs(t *testing.T) {
	first := inference(testID("101"), TransportFailed, SemanticFailed)
	retry := first
	retry.RequestID = testID("102")
	retry.Transport = TransportNotStarted
	retry.Semantic = SemanticNotEvaluated
	if err := ValidateRetry(first, retry); err != nil {
		t.Fatalf("new retry identity: %v", err)
	}
	retry.RequestID = first.RequestID
	if err := ValidateRetry(first, retry); err == nil {
		t.Fatal("retry reused request identity")
	}
}

func TestParentChildReferences(t *testing.T) {
	parent := execution(ExecutionActive)
	child := execution(ExecutionCreated)
	child.ExecutionID = testID("005")
	child.ParentExecutionID = parent.ExecutionID
	if err := ValidateParentChild(parent, child); err != nil {
		t.Fatalf("parent child: %v", err)
	}
	parent.State = ExecutionCompleted
	if err := ValidateParentChild(parent, child); err == nil {
		t.Fatal("terminal parent accepted child")
	}
}

func TestJSONVersionAndUnknownState(t *testing.T) {
	original := execution(ExecutionWaitingModel)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Execution
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecution(decoded); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	decoded.State = ExecutionState("FUTURE_STATE")
	if err := ValidateExecution(decoded); err == nil {
		t.Fatal("unknown state accepted")
	}
	var typed *ValidationError
	if !errors.As(ValidateExecution(decoded), &typed) || typed.Code != ErrCodeInvalidState {
		t.Fatal("unknown state did not return typed validation error")
	}
}

func TestSingleModelResidence(t *testing.T) {
	a := lease(FIFOActive, ReleaseNotRequested)
	if err := ValidateSingleModelResidence([]Lease{a}); err != nil {
		t.Fatal(err)
	}
	b := a
	b.LeaseID = testID("006")
	b.ExecutionID = testID("007")
	b.ModelID = "model-b"
	if err := ValidateSingleModelResidence([]Lease{a, b}); err == nil {
		t.Fatal("two resident models accepted")
	}
	b.ModelID = "model-a"
	if err := ValidateSingleModelResidence([]Lease{a, b}); err == nil {
		t.Fatal("two active residences accepted")
	}
}
