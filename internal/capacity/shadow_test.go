package capacity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/memory"
)

type shadowPlanner struct {
	result Result
	err    bool
}

type slowShadowPlanner struct{}

func (slowShadowPlanner) Plan(context.Context, string) Result {
	time.Sleep(50 * time.Millisecond)
	return Result{Verdict: Fit}
}

func (p shadowPlanner) Plan(context.Context, string) Result {
	if p.err {
		panic(errors.New("fixture planner error"))
	}
	return p.result
}

func TestSHADOW0103VerdictsAndTraceability(t *testing.T) {
	available := uint64(100)
	result := Result{Verdict: Fit, Snapshot: memory.Snapshot{Source: "fixture", System: memory.SystemMemory{AvailableBytes: &available, Source: "fixture-system"}}, Estimate: Estimate{RawBytes: 80, RequiredBytes: 90, SystemRequiredBytes: 90, SystemMarginPercent: 10, GPUMarginPercent: 20}, Capacity: Capacity{SystemAvailableBytes: &available}, Justification: []string{"fixture"}}
	shadow := NewShadowWithOptions(shadowPlanner{result: result}, ShadowOptions{Enabled: true, EvaluationTimeout: time.Second, MaxRecent: 10, QueueSize: 10})
	defer shadow.Close()
	shadow.ObserveAdmission("request-1", "model-1", OperationNewLoad)
	evaluation := waitEvaluation(t, shadow)
	if evaluation.RequestID != "request-1" || evaluation.ModelID != "model-1" || evaluation.Operation != OperationNewLoad || evaluation.Verdict != Fit || evaluation.MetricsSource != "fixture,fixture-system" || evaluation.ObservationState != ObservationEvaluated {
		t.Fatalf("evaluation = %+v", evaluation)
	}
	if evaluation.RequiredBytes != 90 || evaluation.SystemAvailableBytes == nil || *evaluation.SystemAvailableBytes != 100 || evaluation.SystemMarginPercent != 10 || len(evaluation.Justification) != 1 {
		t.Fatalf("evaluation evidence = %+v", evaluation)
	}
}

func TestSHADOW0207InsufficientUnknownErrorAndStale(t *testing.T) {
	available := uint64(1)
	insufficient := Result{Verdict: Insufficient, Snapshot: memory.Snapshot{Source: "fixture"}, Estimate: Estimate{RawBytes: 10, RequiredBytes: 11}, Justification: []string{"not enough"}}
	unknown := Result{Verdict: Unknown, Snapshot: memory.Snapshot{Status: memory.Stale}, Justification: []string{"stale"}}
	shadow := NewShadowWithOptions(shadowPlanner{result: insufficient}, ShadowOptions{Enabled: true, MaxRecent: 10})
	defer shadow.Close()
	shadow.ObserveAdmission("request-2", "model-2", OperationNewLoad)
	if got := waitEvaluation(t, shadow); got.Verdict != Insufficient {
		t.Fatalf("insufficient evaluation = %+v", got)
	}
	shadow.planner = shadowPlanner{result: unknown}
	shadow.ObserveAdmission("request-3", "model-3", OperationQueued)
	if got := waitEvaluationAt(t, shadow, 2); got.Verdict != Unknown || got.Operation != OperationQueued {
		t.Fatalf("stale evaluation = %+v", got)
	}
	shadow.planner = shadowPlanner{err: true}
	shadow.ObserveAdmission("request-4", "model-4", OperationNewLoad)
	got := waitEvaluationAt(t, shadow, 3)
	if got.Verdict != Unknown || got.ObservationState != ObservationError || got.Operation != OperationNewLoad {
		t.Fatalf("error evaluation = %+v", got)
	}
	_ = available
}

func TestSHADOW04ResidentReuseSkipsPlanner(t *testing.T) {
	shadow := NewShadowWithOptions(shadowPlanner{err: true}, ShadowOptions{Enabled: true, MaxRecent: 10})
	defer shadow.Close()
	shadow.ObserveAdmission("request-reuse", "model", OperationResidentReuse)
	evaluation := waitEvaluation(t, shadow)
	if evaluation.Verdict != Fit || evaluation.ObservationState != ObservationEvaluated || evaluation.MetricsSource != "resident-model" {
		t.Fatalf("resident evaluation = %+v", evaluation)
	}
}

func TestSHADOW07TimeoutIsUnknown(t *testing.T) {
	shadow := NewShadowWithOptions(slowShadowPlanner{}, ShadowOptions{Enabled: true, EvaluationTimeout: time.Millisecond, MaxRecent: 10})
	defer shadow.Close()
	shadow.ObserveAdmission("request-timeout", "model", OperationNewLoad)
	evaluation := waitEvaluation(t, shadow)
	if evaluation.Verdict != Unknown || evaluation.ObservationState != ObservationError {
		t.Fatalf("timeout evaluation = %+v", evaluation)
	}
}

func TestSHADOW08ConcurrentObservations(t *testing.T) {
	shadow := NewShadowWithOptions(shadowPlanner{result: Result{Verdict: Fit}}, ShadowOptions{Enabled: true, MaxRecent: 64, QueueSize: 64})
	defer shadow.Close()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shadow.ObserveAdmission("request", "model", OperationNewLoad)
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(shadow.Evaluations()) != 32 {
		time.Sleep(time.Millisecond)
	}
	if got := len(shadow.Evaluations()); got != 32 {
		t.Fatalf("concurrent evaluations = %d", got)
	}
}

func waitEvaluation(t *testing.T, shadow *Shadow) Evaluation {
	return waitEvaluationAt(t, shadow, 1)
}

func waitEvaluationAt(t *testing.T, shadow *Shadow, count int) Evaluation {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if evaluations := shadow.Evaluations(); len(evaluations) >= count {
			return evaluations[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for shadow evaluation")
	return Evaluation{}
}
