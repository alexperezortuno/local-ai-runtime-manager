package capacity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/memory"
)

type admissionFixturePlanner struct {
	mu      sync.Mutex
	results []Result
	calls   int
	block   <-chan struct{}
}

func (p *admissionFixturePlanner) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *admissionFixturePlanner) Plan(ctx context.Context, _ string) Result {
	p.mu.Lock()
	p.calls++
	result := Result{Verdict: Unknown}
	if len(p.results) > 0 {
		result = p.results[0]
		p.results = p.results[1:]
	}
	block := p.block
	p.mu.Unlock()
	if block != nil {
		<-block // deliberately non-cooperative: the evaluator must keep it bounded
	}
	return result
}

func admissionInput(operation AdmissionOperation) AdmissionInput {
	return AdmissionInput{ModelID: "model", RequestID: "request", LeaseID: "lease", Operation: operation}
}

func TestADMISSION0103EvaluatorVerdictsAndEvidence(t *testing.T) {
	now := time.Now().UTC()
	available := uint64(100)
	planner := &admissionFixturePlanner{results: []Result{
		{Verdict: Fit, Snapshot: memory.Snapshot{Status: memory.Available, MeasuredAt: &now, Source: "fixture", System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available, Source: "system"}}, Estimate: Estimate{RequiredBytes: 80}, Capacity: Capacity{SystemAvailableBytes: &available}, Justification: []string{"fixture fit"}},
		{Verdict: Insufficient, Snapshot: memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Status: memory.Available}}, Justification: []string{"fixture insufficient"}},
		{Verdict: Unknown, Snapshot: memory.Snapshot{Status: memory.Unavailable}, Justification: []string{"fixture unavailable"}},
	}}
	evaluator, err := NewAdmissionEvaluator(planner, EvaluatorOptions{EvaluationTimeout: time.Second, MaxConcurrent: 1, MaxPending: 2, MaxSnapshotAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer evaluator.Close()
	for _, expected := range []Verdict{Fit, Insufficient, Unknown} {
		got, err := evaluator.Evaluate(context.Background(), admissionInput(NewLoad))
		if err != nil || got.Verdict != expected || got.Status != EvaluationCompleted {
			t.Fatalf("evaluation = %+v, err=%v", got, err)
		}
		if got.EvaluationID == "" || got.ModelID != "model" || got.RequestID != "request" || got.Operation != NewLoad {
			t.Fatalf("missing evidence = %+v", got)
		}
	}
}

func TestADMISSION04ResidentReuseUsesIdentityWithoutPlanning(t *testing.T) {
	planner := &admissionFixturePlanner{results: []Result{{Verdict: Unknown}}}
	evaluator, err := NewAdmissionEvaluator(planner, EvaluatorOptions{MaxSnapshotAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer evaluator.Close()
	input := admissionInput(ResidentReuse)
	input.ObservedResident = &ResidentIdentity{ModelID: "model", PID: 42, ProcessInstanceID: "p1", StartIdentity: "start-1", RuntimeState: "READY", OwnershipStatus: "MANAGED"}
	got, err := evaluator.Evaluate(context.Background(), input)
	if err != nil || got.Verdict != Fit || got.Resident == nil || planner.calls != 0 {
		t.Fatalf("resident evaluation = %+v, calls=%d, err=%v", got, planner.calls, err)
	}
}

func TestADMISSION15StalePartialAndLateResultsDoNotFit(t *testing.T) {
	old := time.Now().Add(-time.Minute)
	planner := &admissionFixturePlanner{results: []Result{
		{Verdict: Fit, Snapshot: memory.Snapshot{Status: memory.Partial, MeasuredAt: &old}},
	}}
	evaluator, err := NewAdmissionEvaluator(planner, EvaluatorOptions{MaxSnapshotAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluator.Evaluate(context.Background(), admissionInput(NewLoad))
	if err != nil || got.Verdict != Unknown {
		t.Fatalf("stale/partial evaluation = %+v, err=%v", got, err)
	}
	evaluator.Close()

	release := make(chan struct{})
	latePlanner := &admissionFixturePlanner{block: release}
	evaluator, err = NewAdmissionEvaluator(latePlanner, EvaluatorOptions{EvaluationTimeout: 10 * time.Millisecond, MaxPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AdmissionEvaluation, 1)
	go func() {
		result, _ := evaluator.Evaluate(ctx, admissionInput(NewLoad))
		done <- result
	}()
	time.Sleep(time.Millisecond)
	cancel()
	result := <-done
	if result.Status != EvaluationCancelled || result.Verdict == Fit {
		t.Fatalf("cancelled evaluation = %+v", result)
	}
	close(release)
	evaluator.Close()
}

func TestADMISSION16EvaluatorSaturationIsBounded(t *testing.T) {
	release := make(chan struct{})
	planner := &admissionFixturePlanner{block: release}
	evaluator, err := NewAdmissionEvaluator(planner, EvaluatorOptions{MaxConcurrent: 1, MaxPending: 1, EvaluationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer evaluator.Close()
	first := make(chan struct{})
	go func() {
		_, _ = evaluator.Evaluate(context.Background(), admissionInput(NewLoad))
		close(first)
	}()
	deadline := time.Now().Add(time.Second)
	for planner.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go func() { _, _ = evaluator.Evaluate(context.Background(), admissionInput(NewLoad)) }()
	for len(evaluator.jobs) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got, err := evaluator.Evaluate(context.Background(), admissionInput(NewLoad))
	if !errors.Is(err, ErrAdmissionUnavailable) || got.Verdict != Unknown || got.Status != EvaluationError || got.Cause != "evaluator saturated" {
		t.Fatalf("saturation = %+v, err=%v", got, err)
	}
	close(release)
	<-first
}

type admissionPolicyFixture struct {
	mu      sync.Mutex
	results []AdmissionEvaluation
}

func (p *admissionPolicyFixture) Evaluate(context.Context, AdmissionInput) (AdmissionEvaluation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.results) == 0 {
		return AdmissionEvaluation{Verdict: Unknown, Status: EvaluationCompleted}, nil
	}
	result := p.results[0]
	p.results = p.results[1:]
	return result, nil
}

type recoveryFixture bool

func (r recoveryFixture) AdmissionBlocked() bool { return bool(r) }

func TestADMISSION060910PolicyWaitReevaluatesAndCancels(t *testing.T) {
	fixture := &admissionPolicyFixture{results: []AdmissionEvaluation{{Verdict: Insufficient}, {Verdict: Fit}}}
	policy, err := NewAdmissionPolicy(fixture, PolicyOptions{WaitTimeout: time.Second, ReevaluationInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := policy.Admit(context.Background(), admissionInput(NewLoad))
	if err != nil || !decision.Authorized || decision.Action != PolicyContinue || len(decision.Evaluations) != 2 {
		t.Fatalf("wait decision = %+v, err=%v", decision, err)
	}

	fixture = &admissionPolicyFixture{results: []AdmissionEvaluation{{Verdict: Insufficient}}}
	policy, _ = NewAdmissionPolicy(fixture, PolicyOptions{WaitTimeout: time.Millisecond, ReevaluationInterval: time.Second})
	decision, err = policy.Admit(context.Background(), admissionInput(NewLoad))
	if !errors.Is(err, ErrAdmissionTimeout) || decision.Authorized || decision.Action != PolicyTimeout {
		t.Fatalf("timeout decision = %+v, err=%v", decision, err)
	}

	fixture = &admissionPolicyFixture{results: []AdmissionEvaluation{{Verdict: Insufficient}}}
	policy, _ = NewAdmissionPolicy(fixture, PolicyOptions{WaitTimeout: time.Second, ReevaluationInterval: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()
	decision, err = policy.Admit(ctx, admissionInput(NewLoad))
	if !errors.Is(err, ErrAdmissionCancelled) || decision.Authorized {
		t.Fatalf("cancel decision = %+v, err=%v", decision, err)
	}
}

func TestADMISSION13RecoveryPrecedesFitAndUnknownAllow(t *testing.T) {
	fixture := &admissionPolicyFixture{results: []AdmissionEvaluation{{Verdict: Fit}}}
	policy, err := NewAdmissionPolicy(fixture, PolicyOptions{Recovery: recoveryFixture(true)})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := policy.Admit(context.Background(), admissionInput(NewLoad))
	if !errors.Is(err, ErrAdmissionRecovery) || decision.Authorized || decision.Action != PolicyBlocked {
		t.Fatalf("recovery decision = %+v, err=%v", decision, err)
	}

	fixture = &admissionPolicyFixture{results: []AdmissionEvaluation{{Verdict: Unknown}}}
	policy, _ = NewAdmissionPolicy(fixture, PolicyOptions{OnUnknown: "allow"})
	decision, err = policy.Admit(context.Background(), admissionInput(NewLoad))
	if err != nil || !decision.Authorized || decision.Evaluation.Verdict != Unknown {
		t.Fatalf("unknown decision = %+v, err=%v", decision, err)
	}
}
