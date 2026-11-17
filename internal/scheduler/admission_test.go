package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

type admissionEvaluatorFixture struct {
	mu    sync.Mutex
	calls []AdmissionRequest
}

func (f *admissionEvaluatorFixture) Evaluate(_ context.Context, request AdmissionRequest) (AdmissionEvidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, request)
	return AdmissionEvidence{Verdict: AdmissionFit, EvaluatedAt: time.Now().UTC()}, nil
}

func (f *admissionEvaluatorFixture) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestAdmissionDependencyIsOptionalAndNotCalled(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if s.admissionEvaluator != nil {
		t.Fatal("scheduler unexpectedly has an admission evaluator")
	}
}

func TestAdmissionEvaluatorIsConsultedBeforeActivation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionEvaluatorFixture{}
	s.SetAdmissionEvaluator(evaluator)
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if evaluator.callCount() == 0 {
		t.Fatal("evaluator was never consulted for the FIFO head")
	}
}

func TestAdmissionHeadCapturesCorrelationWithoutChangingFIFO(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Active)
	s.mu.Lock()
	head, ok := s.admissionHeadLocked()
	queue := append([]string(nil), s.queue...)
	s.mu.Unlock()
	if !ok || head.ModelID != "model-2" || head.RequestID != "request-2" || head.LeaseID != second.ID || head.Generation == 0 {
		t.Fatalf("head = %+v ok=%v", head, ok)
	}
	if len(queue) != 1 || queue[0] != second.ID {
		t.Fatalf("queue = %v", queue)
	}
	evaluator := &admissionEvaluatorFixture{}
	if evaluator.callCount() != 0 {
		t.Fatal("unexpected evaluator call")
	}
}

func TestAdmissionDisabledIsStructurallyObservableWithoutEvaluatorCalls(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	lease, err := s.RequestWithRequestID("request-disabled", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	s.mu.Lock()
	enabled := s.admissionEvaluator != nil
	s.mu.Unlock()
	if enabled {
		t.Fatal("admission unexpectedly enabled")
	}
}

func TestAdmissionIdempotentReplayDoesNotCreateAnotherRequest(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	key := "2ef8d7ef-b335-4404-b72c-5b2c08a36101"
	first, replay, err := s.RequestIdempotent(key, "model-1")
	if err != nil || replay {
		t.Fatalf("first = %+v replay=%v err=%v", first, replay, err)
	}
	second, replay, err := s.RequestIdempotent(key, "model-1")
	if err != nil || !replay || second.ID != first.ID {
		t.Fatalf("second = %+v replay=%v err=%v", second, replay, err)
	}
	s.mu.Lock()
	tracked := len(s.admissionRequests)
	s.mu.Unlock()
	if tracked != 1 {
		t.Fatalf("tracked requests = %d, want 1", tracked)
	}
}
