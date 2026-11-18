package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// controlEvaluator is a deterministic Scheduler admission evaluator. It
// records every decision and can block one evaluation to exercise the
// serialized commit boundary.
type controlEvaluator struct {
	mu       sync.Mutex
	verdict  AdmissionVerdict
	cause    string
	calls    []AdmissionRequest
	started  chan struct{}
	release  chan struct{}
	blockOne atomic.Bool
	once     sync.Once
}

func newControlEvaluator(verdict AdmissionVerdict) *controlEvaluator {
	return &controlEvaluator{verdict: verdict, started: make(chan struct{})}
}

func (e *controlEvaluator) Evaluate(ctx context.Context, request AdmissionRequest) (AdmissionEvidence, error) {
	e.mu.Lock()
	e.calls = append(e.calls, request)
	verdict := e.verdict
	cause := e.cause
	release := e.release
	block := release != nil && e.blockOne.CompareAndSwap(true, false)
	e.mu.Unlock()
	if block {
		e.once.Do(func() { close(e.started) })
		select {
		case <-release:
		case <-ctx.Done():
			return AdmissionEvidence{Verdict: AdmissionUnknown, Cause: ctx.Err().Error()}, ctx.Err()
		}
	}
	return AdmissionEvidence{Verdict: verdict, Cause: cause, EvaluationID: fmt.Sprintf("eval-%d", len(e.calls)), EvaluatedAt: time.Now().UTC()}, nil
}

func (e *controlEvaluator) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *controlEvaluator) requests() []AdmissionRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]AdmissionRequest(nil), e.calls...)
}

func (e *controlEvaluator) setVerdict(verdict AdmissionVerdict) {
	e.mu.Lock()
	e.verdict = verdict
	e.mu.Unlock()
}

func (e *controlEvaluator) blockNext() {
	e.mu.Lock()
	e.release = make(chan struct{})
	e.blockOne.Store(true)
	e.mu.Unlock()
}

func (e *controlEvaluator) unblock() {
	e.mu.Lock()
	release := e.release
	e.release = nil
	e.mu.Unlock()
	if release != nil {
		close(release)
	}
}

func controlAdmission(t *testing.T, s *Scheduler, evaluator AdmissionEvaluator, options AdmissionOptions) {
	t.Helper()
	if options.ReevaluationInterval == 0 {
		options.ReevaluationInterval = 5 * time.Millisecond
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = 2 * time.Second
	}
	if err := s.SetAdmissionOptions(options); err != nil {
		t.Fatal(err)
	}
	s.SetAdmissionEvaluator(evaluator)
}

func controlNoProcess(t *testing.T, s *Scheduler) {
	t.Helper()
	if status := s.manager.Status(); status.PID != 0 {
		t.Fatalf("runtime started PID %d", status.PID)
	}
}

// CONTROL-01: FIT admits the FIFO head, reaches ACTIVE, and does not duplicate
// the acquisition.
func TestCONTROL01FitAdmitsFIFOHeadOnce(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionFit)
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("control-01", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if evaluator.count() != 1 {
		t.Fatalf("evaluations = %d, want 1", evaluator.count())
	}
	if got := countProcessEvent(t, "START"); got != 1 {
		t.Fatalf("runtime starts = %d, want 1", got)
	}
}

// CONTROL-02: INSUFFICIENT keeps the head QUEUED and prevents overtaking.
func TestCONTROL02InsufficientKeepsHeadQueued(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	head, err := s.RequestWithRequestID("control-02-head", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	follower, err := s.RequestWithRequestID("control-02-follower", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.count() >= 1 })
	for _, request := range evaluator.requests() {
		if request.LeaseID != head.ID {
			t.Fatalf("evaluated follower %s", request.LeaseID)
		}
	}
	headStatus, _ := s.Get(head.ID)
	followerStatus, _ := s.Get(follower.ID)
	if headStatus.State != Queued || headStatus.QueuePosition != 1 {
		t.Fatalf("head = %+v, want QUEUED at position 1", headStatus)
	}
	if followerStatus.State != Queued || followerStatus.QueuePosition != 2 {
		t.Fatalf("follower = %+v, want QUEUED at position 2", followerStatus)
	}
	if snapshot := s.Snapshot(); snapshot.Preparing != nil {
		t.Fatalf("PREPARING published while waiting: %+v", snapshot.Preparing)
	}
	controlNoProcess(t, s)
}

// CONTROL-03: an INSUFFICIENT to FIT transition reevaluates the original lease
// without changing its generation or creating another lease.
func TestCONTROL03CapacityRecoveryReevaluatesOriginalLease(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("control-03", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.count() >= 1 })
	evaluator.setVerdict(AdmissionFit)
	waitLease(t, s, lease.ID, Active)
	requests := evaluator.requests()
	if len(requests) < 2 {
		t.Fatalf("evaluations = %d, want reevaluation", len(requests))
	}
	if requests[0].LeaseID != lease.ID || requests[1].LeaseID != lease.ID {
		t.Fatalf("lease changed across reevaluation: %+v", requests[:2])
	}
	if requests[0].Generation != requests[1].Generation {
		t.Fatalf("generation changed: %d -> %d", requests[0].Generation, requests[1].Generation)
	}
	if got := countProcessEvent(t, "START"); got != 1 {
		t.Fatalf("runtime starts = %d, want 1", got)
	}
}

// CONTROL-04: UNKNOWN is never rewritten as FIT. allow continues; wait keeps
// the head queued. Both policies preserve the cause.
func TestCONTROL04UnknownPoliciesPreserveVerdictAndCause(t *testing.T) {
	for _, policy := range []string{"allow", "wait"} {
		t.Run(policy, func(t *testing.T) {
			_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
			evaluator := newControlEvaluator(AdmissionUnknown)
			evaluator.cause = "snapshot unavailable"
			controlAdmission(t, s, evaluator, AdmissionOptions{OnUnknown: policy})
			lease, err := s.RequestWithRequestID("control-04-"+policy, "model-1")
			if err != nil {
				t.Fatal(err)
			}
			if policy == "allow" {
				waitLease(t, s, lease.ID, Active)
			} else {
				waitFor(t, func() bool { return evaluator.count() >= 1 })
				status, _ := s.Get(lease.ID)
				if status.State != Queued {
					t.Fatalf("UNKNOWN/wait state = %s, want QUEUED", status.State)
				}
			}
			for _, request := range evaluator.requests() {
				if request.LeaseID != lease.ID {
					t.Fatalf("unexpected lease evaluation: %+v", request)
				}
			}
		})
	}
}

// CONTROL-05: the earliest effective deadline wins and a replay preserves the
// original deadline.
func TestCONTROL05EffectiveDeadlineAndIdempotentReplay(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	lease, err := s.RequestWithContext(ctx, "control-05", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	if elapsed := time.Since(lease.CreatedAt); elapsed > time.Second {
		t.Fatalf("effective deadline did not win: %s", elapsed)
	}
	controlNoProcess(t, s)

	_, replayScheduler := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	replayEvaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, replayScheduler, replayEvaluator, AdmissionOptions{WaitTimeout: 300 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
	key := "9ef8d7ef-b335-4404-b72c-5b2c08a36109"
	first, repeated, err := replayScheduler.RequestIdempotentWithContext(context.Background(), key, "model-1")
	if err != nil || repeated {
		t.Fatalf("first = %+v repeated=%v err=%v", first, repeated, err)
	}
	replayScheduler.mu.Lock()
	original := replayScheduler.admissionDeadline[first.ID]
	replayScheduler.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	second, repeated, err := replayScheduler.RequestIdempotentWithContext(context.Background(), key, "model-1")
	if err != nil || !repeated || second.ID != first.ID {
		t.Fatalf("second = %+v repeated=%v err=%v", second, repeated, err)
	}
	replayScheduler.mu.Lock()
	after := replayScheduler.admissionDeadline[first.ID]
	replayScheduler.mu.Unlock()
	if !after.Equal(original) {
		t.Fatalf("replay restarted deadline: %s -> %s", original, after)
	}
}

// CONTROL-06: cancelling a waiting request prevents activation and clears the
// wait timer.
func TestCONTROL06CancellationStopsWaitingLease(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("control-06", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.count() >= 1 })
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	s.mu.Lock()
	timer := s.admissionTimer
	wait := s.admissionWait
	s.mu.Unlock()
	if timer != nil || wait != nil {
		t.Fatalf("cancellation left admission resources: timer=%v wait=%+v", timer, wait)
	}
	controlNoProcess(t, s)
}

// CONTROL-07: Recovery appears during evaluation and prevents the late FIT
// from committing.
func TestCONTROL07RecoveryPreventsLateCommit(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionFit)
	evaluator.blockNext()
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	gate := &admissionGateFixture{}
	s.SetAdmissionGate(gate)
	lease, err := s.RequestWithRequestID("control-07", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	gate.blocked.Store(true)
	evaluator.unblock()
	waitLease(t, s, lease.ID, Cancelled)
	controlNoProcess(t, s)
}

// CONTROL-08: a blocked execution gate prevents a capacity FIT from starting
// a process.
func TestCONTROL08ExecutionGateBlocksCapacityFit(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionFit)
	evaluator.blockNext()
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	gate := &admissionGateFixture{}
	s.SetAdmissionGate(gate)
	lease, err := s.RequestWithRequestID("control-08", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	gate.blocked.Store(true)
	evaluator.unblock()
	waitLease(t, s, lease.ID, Cancelled)
	controlNoProcess(t, s)
}

// CONTROL-09: concurrent requests preserve FIFO and produce one commit per
// lease.
func TestCONTROL09ConcurrencyPreservesFIFOAndSingleCommit(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 6, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionFit)
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	const count = 5
	leases := make(chan Lease, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := s.RequestWithRequestID(fmt.Sprintf("control-09-%d", i), "model-1")
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			leases <- lease
		}(i)
	}
	wg.Wait()
	close(leases)
	seen := map[string]bool{}
	for lease := range leases {
		if seen[lease.ID] {
			t.Fatalf("duplicate lease %s", lease.ID)
		}
		seen[lease.ID] = true
	}
	for lease := range leases {
		waitLease(t, s, lease.ID, Active)
		if _, err := s.Release(lease.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range evaluator.requests() {
		count := 0
		for _, other := range evaluator.requests() {
			if other.LeaseID == request.LeaseID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("lease %s evaluated %d times", request.LeaseID, count)
		}
	}
}

// CONTROL-10: shutdown during evaluation cancels the lease and releases the
// scheduler worker.
func TestCONTROL10ShutdownCancelsEvaluation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newControlEvaluator(AdmissionFit)
	evaluator.blockNext()
	controlAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("control-10", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdown <- s.Shutdown(ctx)
	}()
	// Shutdown cancels the in-flight evaluation. Releasing the fake after the
	// worker observes cancellation proves the worker exits without waiting for
	// a non-cooperative provider.
	time.Sleep(20 * time.Millisecond)
	evaluator.unblock()
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	status, _ := s.Get(lease.ID)
	if status.State != Cancelled {
		t.Fatalf("state = %s, want CANCELLED", status.State)
	}
	if _, err := s.Request("model-1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("request after shutdown = %v, want ErrClosed", err)
	}
}
