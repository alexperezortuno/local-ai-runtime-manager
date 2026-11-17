package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/scheduler"
)

// scriptedCapacityEvaluator returns a scripted capacity evaluation.
type scriptedCapacityEvaluator struct {
	mu      int
	verdict capacity.Verdict
	status  capacity.EvaluationStatus
	cause   string
	err     error
	inputs  []capacity.AdmissionInput
	block   chan struct{}
}

func (e *scriptedCapacityEvaluator) Evaluate(ctx context.Context, input capacity.AdmissionInput) (capacity.AdmissionEvaluation, error) {
	e.mu++
	e.inputs = append(e.inputs, input)
	if e.block != nil {
		select {
		case <-e.block:
		case <-ctx.Done():
			return capacity.AdmissionEvaluation{Verdict: capacity.Unknown, Status: capacity.EvaluationCancelled, Cause: ctx.Err().Error()}, ctx.Err()
		}
	}
	return capacity.AdmissionEvaluation{
		EvaluationID: "evaluation-1",
		ModelID:      input.ModelID,
		RequestID:    input.RequestID,
		LeaseID:      input.LeaseID,
		Operation:    input.Operation,
		Verdict:      e.verdict,
		Status:       e.status,
		Cause:        e.cause,
		Source:       []string{"fixture"},
		Justification: []string{
			"fixture",
		},
		EvaluatedAt: time.Now().UTC(),
	}, e.err
}

// The adapter preserves FIT, INSUFFICIENT and UNKNOWN.
func TestAdapterPreservesVerdicts(t *testing.T) {
	for _, expected := range []capacity.Verdict{capacity.Fit, capacity.Insufficient, capacity.Unknown} {
		source := &scriptedCapacityEvaluator{verdict: expected, status: capacity.EvaluationCompleted}
		evaluator := New(source)
		evidence, err := evaluator.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: scheduler.AdmissionOperationNewLoad})
		if err != nil {
			t.Fatal(err)
		}
		switch expected {
		case capacity.Fit:
			if evidence.Verdict != scheduler.AdmissionFit {
				t.Fatalf("verdict = %s, want FIT", evidence.Verdict)
			}
		case capacity.Insufficient:
			if evidence.Verdict != scheduler.AdmissionInsufficient {
				t.Fatalf("verdict = %s, want INSUFFICIENT", evidence.Verdict)
			}
		default:
			if evidence.Verdict != scheduler.AdmissionUnknown {
				t.Fatalf("verdict = %s, want UNKNOWN", evidence.Verdict)
			}
		}
	}
}

// The adapter preserves the evidence, the cause and the timestamps.
func TestAdapterPreservesEvidenceAndCause(t *testing.T) {
	source := &scriptedCapacityEvaluator{verdict: capacity.Unknown, status: capacity.EvaluationError, cause: "planner failed"}
	evaluator := New(source)
	evidence, err := evaluator.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: scheduler.AdmissionOperationNewLoad})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Cause != "planner failed" {
		t.Fatalf("cause = %q, want %q", evidence.Cause, "planner failed")
	}
	if evidence.EvaluationID != "evaluation-1" {
		t.Fatalf("evaluation id = %q", evidence.EvaluationID)
	}
	if evidence.EvaluatedAt.IsZero() {
		t.Fatal("the evaluation timestamp was lost")
	}
	if len(evidence.Justification) == 0 || len(evidence.Source) == 0 {
		t.Fatalf("evidence = %+v", evidence)
	}
}

// The adapter correlates the request, the lease and the model.
func TestAdapterCorrelatesRequestAndLease(t *testing.T) {
	source := &scriptedCapacityEvaluator{verdict: capacity.Fit, status: capacity.EvaluationCompleted}
	evaluator := New(source)
	if _, err := evaluator.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationNewLoad}); err != nil {
		t.Fatal(err)
	}
	if len(source.inputs) != 1 {
		t.Fatalf("inputs = %d, want 1", len(source.inputs))
	}
	input := source.inputs[0]
	if input.ModelID != "model-1" || input.RequestID != "request-1" || input.LeaseID != "lease-1" {
		t.Fatalf("input = %+v", input)
	}
	if input.Operation != capacity.NewLoad {
		t.Fatalf("operation = %s, want NEW_LOAD", input.Operation)
	}
}

// The adapter maps the resident reuse operation and forwards the identity.
func TestAdapterMapsResidentReuse(t *testing.T) {
	source := &scriptedCapacityEvaluator{verdict: capacity.Fit, status: capacity.EvaluationCompleted}
	evaluator := New(source)
	request := scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationResidentReuse, Resident: &scheduler.AdmissionResident{ModelID: "model-1", PID: 42, ProcessInstanceID: "p1", StartIdentity: "start-1", RuntimeState: "READY", OwnershipStatus: "OWNED_VERIFIED"}}
	if _, err := evaluator.Evaluate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	input := source.inputs[0]
	if input.Operation != capacity.ResidentReuse {
		t.Fatalf("operation = %s, want RESIDENT_REUSE", input.Operation)
	}
	if input.ObservedResident == nil || input.ObservedResident.ProcessInstanceID != "p1" || input.ObservedResident.StartIdentity != "start-1" {
		t.Fatalf("resident = %+v", input.ObservedResident)
	}
}

// The adapter propagates the context.
func TestAdapterPropagatesContext(t *testing.T) {
	type key struct{}
	source := &scriptedCapacityEvaluator{verdict: capacity.Fit, status: capacity.EvaluationCompleted}
	evaluator := New(source)
	ctx := context.WithValue(context.Background(), key{}, "caller")
	if _, err := evaluator.Evaluate(ctx, scheduler.AdmissionRequest{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: scheduler.AdmissionOperationNewLoad}); err != nil {
		t.Fatal(err)
	}
}

// A capacity failure is reported as an error, not as a verdict.
func TestAdapterReportsEvaluatorFailureAsError(t *testing.T) {
	source := &scriptedCapacityEvaluator{verdict: capacity.Unknown, status: capacity.EvaluationError, cause: "planner failed", err: errors.New("planner failed")}
	evaluator := New(source)
	evidence, err := evaluator.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: scheduler.AdmissionOperationNewLoad})
	if err == nil {
		t.Fatal("the evaluator failure was not reported as an error")
	}
	if evidence.Verdict != scheduler.AdmissionUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN", evidence.Verdict)
	}
}

// The context provider serves the caller context and forgets it afterwards.
func TestContextProviderTracksAndForgets(t *testing.T) {
	provider := NewContextProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider.Track("lease-1", ctx, time.Now().Add(time.Minute))
	served, deadline, ok := provider.AdmissionContext("lease-1")
	if !ok || served == nil || deadline.IsZero() {
		t.Fatalf("context = %v deadline = %v ok = %v", served, deadline, ok)
	}
	provider.Forget("lease-1")
	if _, _, ok := provider.AdmissionContext("lease-1"); ok {
		t.Fatal("the provider retained a forgotten context")
	}
}

// A missing capacity evaluator is reported as unavailable, never as FIT.
func TestAdapterWithoutSourceIsUnavailable(t *testing.T) {
	evaluator := New(nil)
	evidence, err := evaluator.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: scheduler.AdmissionOperationNewLoad})
	if !errors.Is(err, capacity.ErrAdmissionUnavailable) {
		t.Fatalf("err = %v, want ErrAdmissionUnavailable", err)
	}
	if evidence.Verdict != scheduler.AdmissionUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN", evidence.Verdict)
	}
}
