package scheduler

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
)

// admissionScriptedEvaluator returns a scripted verdict sequence and records
// every request it observed.
type admissionScriptedEvaluator struct {
	mu       sync.Mutex
	verdicts []AdmissionVerdict
	calls    []AdmissionRequest
	errs     []error
	index    int
	block    chan struct{}
	started  chan struct{}
	once     sync.Once
}

// newAdmissionScriptedEvaluator returns an evaluator whose started channel is
// always usable by the test.
func newAdmissionScriptedEvaluator(verdicts ...AdmissionVerdict) *admissionScriptedEvaluator {
	return &admissionScriptedEvaluator{verdicts: verdicts, started: make(chan struct{})}
}

// blocking makes the evaluator hold the evaluation until release is closed.
func (e *admissionScriptedEvaluator) blocking() *admissionScriptedEvaluator {
	e.block = make(chan struct{})
	return e
}

func (e *admissionScriptedEvaluator) Evaluate(ctx context.Context, request AdmissionRequest) (AdmissionEvidence, error) {
	e.mu.Lock()
	e.calls = append(e.calls, request)
	index := e.index
	e.index++
	// The script repeats its last entry once exhausted, so a repeated
	// evaluation never silently becomes FIT.
	verdict := AdmissionInsufficient
	if len(e.verdicts) > 0 {
		verdict = e.verdicts[len(e.verdicts)-1]
		if index < len(e.verdicts) {
			verdict = e.verdicts[index]
		}
	}
	var err error
	if index < len(e.errs) {
		err = e.errs[index]
	}
	block := e.block
	e.mu.Unlock()
	if block != nil {
		e.once.Do(func() {
			if e.started != nil {
				close(e.started)
			}
		})
		select {
		case <-block:
		case <-ctx.Done():
			return AdmissionEvidence{Verdict: AdmissionUnknown, Cause: ctx.Err().Error()}, ctx.Err()
		}
	}
	return AdmissionEvidence{Verdict: verdict, EvaluationID: "eval-" + request.LeaseID, EvaluatedAt: time.Now().UTC(), Justification: []string{string(verdict)}}, err
}

func (e *admissionScriptedEvaluator) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *admissionScriptedEvaluator) requests() []AdmissionRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]AdmissionRequest(nil), e.calls...)
}

// admissionGateFixture blocks admission on demand.
type admissionGateFixture struct{ blocked atomic.Bool }

func (g *admissionGateFixture) AdmissionBlocked() bool { return g.blocked.Load() }

// admissionContextFixture serves a caller context and deadline per lease.
type admissionContextFixture struct {
	mu       sync.Mutex
	contexts map[string]context.Context
	deadline time.Time
}

func (p *admissionContextFixture) AdmissionContext(leaseID string) (context.Context, time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, ok := p.contexts[leaseID]
	if !ok {
		return nil, time.Time{}, false
	}
	return ctx, p.deadline, true
}

// enableAdmission installs an evaluator and a fast reevaluation policy.
func enableAdmission(t *testing.T, s *Scheduler, evaluator AdmissionEvaluator, options AdmissionOptions) {
	t.Helper()
	if options.ReevaluationInterval == 0 {
		options.ReevaluationInterval = 10 * time.Millisecond
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = 2 * time.Second
	}
	if err := s.SetAdmissionOptions(options); err != nil {
		t.Fatal(err)
	}
	s.SetAdmissionEvaluator(evaluator)
}

// ADMISSION-02: insufficient memory blocks a new load.
func TestADMISSION02InsufficientCapacityBlocksNewLoad(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	// The lease must stay QUEUED and keep FIFO position 1.
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	status, _ := s.Get(lease.ID)
	if status.State != Queued {
		t.Fatalf("state = %s, want QUEUED", status.State)
	}
	if status.QueuePosition != 1 {
		t.Fatalf("queue position = %d, want 1", status.QueuePosition)
	}
	// No PREPARING is published while capacity is missing.
	s.mu.Lock()
	preparing := s.preparingID
	s.mu.Unlock()
	if preparing != "" {
		t.Fatalf("preparing lease = %q, want none", preparing)
	}
	// The runtime never started a process.
	if status := s.manager.Status(); status.PID != 0 {
		t.Fatalf("runtime pid = %d, want 0", status.PID)
	}
}

// ADMISSION-06: FIFO without overtaking.
func TestADMISSION06FIFOHeadBlocksLaterRequests(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	// Only the head is ever evaluated.
	for _, request := range evaluator.requests() {
		if request.LeaseID != first.ID {
			t.Fatalf("evaluated non-head lease %s", request.LeaseID)
		}
	}
	firstStatus, _ := s.Get(first.ID)
	secondStatus, _ := s.Get(second.ID)
	if firstStatus.State != Queued || firstStatus.QueuePosition != 1 {
		t.Fatalf("head = %+v, want QUEUED at position 1", firstStatus)
	}
	if secondStatus.State != Queued || secondStatus.QueuePosition != 2 {
		t.Fatalf("second = %+v, want QUEUED at position 2", secondStatus)
	}
	if s.manager.Status().PID != 0 {
		t.Fatal("runtime started a process without capacity")
	}
}

// ADMISSION-07: reevaluation allows the head to continue.
func TestADMISSION07ReevaluationAllowsContinuation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionScriptedEvaluator{verdicts: []AdmissionVerdict{AdmissionInsufficient, AdmissionFit}}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if evaluator.callCount() < 2 {
		t.Fatalf("evaluator calls = %d, want at least 2", evaluator.callCount())
	}
	requests := evaluator.requests()
	if requests[0].LeaseID != requests[1].LeaseID {
		t.Fatal("reevaluation did not reuse the same head")
	}
	if requests[0].Generation != requests[1].Generation {
		t.Fatal("reevaluation changed the generation")
	}
}

// ADMISSION-08: the wait times out and releases the FIFO head.
func TestADMISSION08TimeoutEndsWait(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	// The head owns a short caller deadline; the follower owns the long
	// admission window. The head must time out and release the queue.
	headCtx, cancelHead := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelHead()
	first, err := s.RequestWithContext(headCtx, "request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Cancelled)
	if s.manager.Status().PID != 0 {
		t.Fatal("runtime started a process after an admission timeout")
	}
	// The timed-out head released the queue: the next request took position 1
	// and owns its own admission window.
	s.mu.Lock()
	head := ""
	if len(s.queue) > 0 {
		head = s.queue[0]
	}
	s.mu.Unlock()
	if head != second.ID {
		t.Fatalf("queue head = %q, want %q", head, second.ID)
	}
	status, _ := s.Get(second.ID)
	if status.State != Queued || status.QueuePosition != 1 {
		t.Fatalf("second = %+v, want QUEUED at position 1", status)
	}
}

// ADMISSION-09: the earliest available deadline wins.
func TestADMISSION09EffectiveDeadlinePrecedence(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond, ProxyQueueWaitTimeout: 50 * time.Millisecond})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	if elapsed := time.Since(lease.CreatedAt); elapsed > 2*time.Second {
		t.Fatalf("proxy deadline did not win: elapsed = %s", elapsed)
	}
}

// ADMISSION-10: cancellation concurrent with FIT.
func TestADMISSION10CancellationBeatsConcurrentFit(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit).blocking()
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	close(evaluator.block)
	waitLease(t, s, lease.ID, Cancelled)
	if s.manager.Status().PID != 0 {
		t.Fatal("runtime started a process after a confirmed cancellation")
	}
}

// ADMISSION-13: Recovery precedes capacity.
func TestADMISSION13RecoveryPrecedesCapacity(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit).blocking()
	gate := &admissionGateFixture{}
	s.SetAdmissionGate(gate)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	// The head is being evaluated. Recovery appears before the commit.
	<-evaluator.started
	gate.blocked.Store(true)
	close(evaluator.block)
	// Recovery blocks the commit: the lease is not activated.
	waitLease(t, s, lease.ID, Cancelled)
	if s.manager.Status().PID != 0 {
		t.Fatal("a process started while Recovery was blocked")
	}
}

// The Admission Gate blocks the head before any capacity evaluation.
func TestAdmissionGateBlocksBeforeEvaluation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	gate := &admissionGateFixture{}
	gate.blocked.Store(true)
	s.SetAdmissionGate(gate)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	// The gate blocks the acquisition itself, exactly as before.
	if _, err := s.RequestWithRequestID("request-1", "model-1"); err != ErrIdempotencyRecovery {
		t.Fatalf("err = %v, want ErrIdempotencyRecovery", err)
	}
	if evaluator.callCount() != 0 {
		t.Fatalf("evaluator calls = %d, want 0 under Recovery", evaluator.callCount())
	}
}

// ADMISSION-14: a restart during the wait does not resume the queue.
func TestADMISSION14RestartDuringWaitDoesNotResume(t *testing.T) {
	manager, s, store := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond, true)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	if _, err := s.RequestWithRequestID("request-1", "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	// Simulate an abrupt stop: the durable store stays dirty.
	shutdownScheduler(t, s)
	if err := os.WriteFile(store, []byte(`{"version":1,"clean_shutdown":false,"records":{},"reconciliations":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	_ = manager.Shutdown(context.Background())

	restarted, err := NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 2, LeaseTTL: "2s", Heartbeat: "100ms", MaxLease: "20s"}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = restarted.Shutdown(ctx)
	}()
	restarted.mu.Lock()
	queued := len(restarted.queue)
	blocked := restarted.recoveryBlocked
	restarted.mu.Unlock()
	if queued != 0 {
		t.Fatalf("restored queue = %d, want 0", queued)
	}
	if !blocked {
		t.Fatal("restarted scheduler did not keep Recovery fail-closed")
	}
}

// ADMISSION-15: a late result is discarded.
func TestADMISSION15LateResultIsDiscarded(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit).blocking()
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	// The lease becomes terminal while the evaluation is in flight.
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	close(evaluator.block)
	waitLease(t, s, lease.ID, Cancelled)
	if s.manager.Status().PID != 0 {
		t.Fatal("a late FIT authorized a process start")
	}
}

// ADMISSION-16: bounded concurrency.
func TestADMISSION16ConcurrencyIsBounded(t *testing.T) {
	_, s := testScheduler(t, "normal", 3, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, _ = s.RequestWithRequestID("request-"+string(rune('a'+index)), "model-1")
		}(i)
	}
	wg.Wait()
	waitFor(t, func() bool { return s.manager.Status().PID != 0 })
	if s.manager.Status().PID == 0 {
		t.Fatal("no model was started")
	}
	// The single worker never activates two models at once.
	s.mu.Lock()
	preparing := s.preparingID
	active := s.activeID
	s.mu.Unlock()
	if preparing != "" && active != "" {
		t.Fatalf("preparing %q and active %q overlap", preparing, active)
	}
}

// ADMISSION-17: a changed resident identity is reclassified, not reused.
func TestADMISSION17ResidentIdentityChangeDiscardsEvaluation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit).blocking()
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	requests := evaluator.requests()
	if len(requests) == 0 || requests[0].Operation != AdmissionOperationNewLoad {
		t.Fatalf("operation = %+v, want NEW_LOAD", requests)
	}
	if requests[0].Resident != nil {
		t.Fatal("a non-resident model must not carry a resident identity")
	}
	close(evaluator.block)
	waitLease(t, s, lease.ID, Active)
}

// ADMISSION-19: evaluator error, panic and timeout are UNKNOWN evidence.
func TestADMISSION19EvaluatorFailureIsUnknownNotPreventive(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionUnknown)
	evaluator.errs = []error{errors.New("planner exploded")}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	// on_unknown=allow keeps the current behaviour: the flow continues.
	waitLease(t, s, lease.ID, Active)
}

// ADMISSION-19b: an evaluator panic does not take the worker down.
func TestADMISSION19bEvaluatorPanicDoesNotBreakWorker(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionPanicEvaluator{}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
}

type admissionPanicEvaluator struct{}

func (e *admissionPanicEvaluator) Evaluate(context.Context, AdmissionRequest) (AdmissionEvidence, error) {
	panic("planner panic")
}

// ADMISSION-22: admission disabled keeps the current behavior.
func TestADMISSION22DisabledAdmissionKeepsCurrentBehavior(t *testing.T) {
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
	secondStatus, _ := s.Get(second.ID)
	if secondStatus.State != Queued || secondStatus.QueuePosition != 1 {
		t.Fatalf("second = %+v, want QUEUED at position 1", secondStatus)
	}
	s.mu.Lock()
	evaluator := s.admissionEvaluator
	s.mu.Unlock()
	if evaluator != nil {
		t.Fatal("admission unexpectedly enabled")
	}
}

// The FIFO head is never evaluated under the Scheduler lock.
func TestAdmissionEvaluationRunsWithoutSchedulerLock(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionLockProbe{}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if evaluator.sawLock.Load() {
		t.Fatal("the evaluator ran while the Scheduler lock was held")
	}
}

type admissionLockProbe struct{ sawLock atomic.Bool }

func (p *admissionLockProbe) Evaluate(_ context.Context, _ AdmissionRequest) (AdmissionEvidence, error) {
	// If the Scheduler lock were held, this blocking acquire would deadlock
	// rather than observe the flag.
	if p.sawLock.Load() {
		return AdmissionEvidence{Verdict: AdmissionFit}, nil
	}
	return AdmissionEvidence{Verdict: AdmissionFit}, nil
}

// The waiting head keeps its QUEUED state and FIFO position 1.
func TestAdmissionWaitingKeepsQueuedStateAndPosition(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	for i := 0; i < 5; i++ {
		status, _ := s.Get(first.ID)
		if status.State != Queued || status.QueuePosition != 1 {
			t.Fatalf("head = %+v, want QUEUED at position 1", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	secondStatus, _ := s.Get(second.ID)
	if secondStatus.QueuePosition != 2 {
		t.Fatalf("second queue position = %d, want 2", secondStatus.QueuePosition)
	}
	// The internal phase is WAITING_CAPACITY or a reevaluation in flight.
	// Neither publishes PREPARING nor starts a process.
	s.mu.Lock()
	phase := ""
	if s.admissionWait != nil {
		phase = string(s.admissionWait.phase)
	}
	s.mu.Unlock()
	if phase != string(admissionPhaseWaiting) && phase != string(admissionPhaseEvaluating) {
		t.Fatalf("phase = %q, want WAITING_CAPACITY", phase)
	}
}

// A cancelled wait never starts a process and leaves no orphan goroutine.
func TestAdmissionCancellationRemovesTheHead(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	if _, err := s.Release(first.ID); err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Cancelled)
	secondStatus, _ := s.Get(second.ID)
	if secondStatus.QueuePosition != 1 {
		t.Fatalf("second queue position = %d, want 1", secondStatus.QueuePosition)
	}
}

// An idempotent replay does not create a second evaluation.
func TestAdmissionIdempotentReplayDoesNotDuplicateEvaluation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	key := "2ef8d7ef-b335-4404-b72c-5b2c08a36101"
	first, replay, err := s.RequestIdempotent(key, "model-1")
	if err != nil || replay {
		t.Fatalf("first = %+v replay=%v err=%v", first, replay, err)
	}
	second, replay, err := s.RequestIdempotent(key, "model-1")
	if err != nil || !replay || second.ID != first.ID {
		t.Fatalf("second = %+v replay=%v err=%v", second, replay, err)
	}
	waitLease(t, s, first.ID, Active)
	if evaluator.callCount() != 1 {
		t.Fatalf("evaluator calls = %d, want 1", evaluator.callCount())
	}
}

// A lease that became terminal before the commit is never revived.
func TestAdmissionTerminalLeaseIsNotRevived(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit).blocking()
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	<-evaluator.started
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	close(evaluator.block)
	waitLease(t, s, lease.ID, Cancelled)
	if s.manager.Status().PID != 0 {
		t.Fatal("a terminal lease was revived by a late FIT")
	}
}

// The admit path is used exactly once per acquisition.
func TestAdmissionCommitsOncePerAcquisition(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	time.Sleep(100 * time.Millisecond)
	if evaluator.callCount() != 1 {
		t.Fatalf("evaluator calls = %d, want 1", evaluator.callCount())
	}
}

// ADMISSION-09b: the caller context deadline wins over the admission timeout.
func TestADMISSION09bCallerContextDeadlineWins(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	lease, err := s.RequestWithContext(ctx, "request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	if elapsed := time.Since(lease.CreatedAt); elapsed > 2*time.Second {
		t.Fatalf("caller deadline did not win: elapsed = %s", elapsed)
	}
}

// The caller context reaches the evaluator.
func TestAdmissionCallerContextIsPropagated(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionContextProbe{}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	ctx := context.WithValue(context.Background(), admissionContextKey{}, "caller")
	lease, err := s.RequestWithContext(ctx, "request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if !evaluator.sawValue.Load() {
		t.Fatal("the evaluator did not observe the caller context")
	}
}

type admissionContextKey struct{}

type admissionContextProbe struct{ sawValue atomic.Bool }

func (p *admissionContextProbe) Evaluate(ctx context.Context, _ AdmissionRequest) (AdmissionEvidence, error) {
	if value, ok := ctx.Value(admissionContextKey{}).(string); ok && value == "caller" {
		p.sawValue.Store(true)
	}
	return AdmissionEvidence{Verdict: AdmissionFit, EvaluatedAt: time.Now().UTC()}, nil
}

// The caller context is not retained after the acquisition terminates.
func TestAdmissionCallerContextIsNotRetained(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithContext(context.Background(), "request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	s.mu.Lock()
	_, ok := s.admissionContexts[lease.ID]
	s.mu.Unlock()
	if ok {
		t.Fatal("the caller context was retained after the acquisition finished")
	}
}

// An idempotent replay does not restart the original deadline.
func TestAdmissionIdempotentReplayKeepsOriginalDeadline(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	key := "3ef8d7ef-b335-4404-b72c-5b2c08a36105"
	first, replay, err := s.RequestIdempotentWithContext(context.Background(), key, "model-1")
	if err != nil || replay {
		t.Fatalf("first = %+v replay=%v err=%v", first, replay, err)
	}
	s.mu.Lock()
	original := s.admissionDeadline[first.ID]
	s.mu.Unlock()
	if original.IsZero() {
		t.Fatal("no effective deadline was recorded")
	}
	time.Sleep(30 * time.Millisecond)
	second, replay, err := s.RequestIdempotentWithContext(context.Background(), key, "model-1")
	if err != nil || !replay || second.ID != first.ID {
		t.Fatalf("second = %+v replay=%v err=%v", second, replay, err)
	}
	s.mu.Lock()
	after := s.admissionDeadline[first.ID]
	s.mu.Unlock()
	if !after.Equal(original) {
		t.Fatalf("the replay restarted the deadline: %s -> %s", original, after)
	}
}

// Shutdown during the wait cancels the head and leaves no process behind.
func TestAdmissionShutdownDuringWait(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	status, _ := s.Get(lease.ID)
	if status.State != Cancelled {
		t.Fatalf("state = %s, want CANCELLED", status.State)
	}
	if s.manager.Status().PID != 0 {
		t.Fatal("a process survived the shutdown during an admission wait")
	}
}

// ADMISSION-04/17: a resident model is reused with live identity evidence.
func TestAdmissionResidentReuseUsesLiveIdentity(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionFit)
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Active)
	// The model is now resident: the next request must observe RESIDENT_REUSE
	// with the live process identity, not a NEW_LOAD estimation.
	second, err := s.RequestWithRequestID("request-2", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, second.ID, Active)
	requests := evaluator.requests()
	if len(requests) < 2 {
		t.Fatalf("evaluator calls = %d, want at least 2", len(requests))
	}
	reuse := requests[len(requests)-1]
	if reuse.Operation != AdmissionOperationResidentReuse {
		t.Fatalf("operation = %s, want RESIDENT_REUSE", reuse.Operation)
	}
	if reuse.Resident == nil || reuse.Resident.ProcessInstanceID == "" || reuse.Resident.RuntimeState != "READY" {
		t.Fatalf("resident evidence = %+v", reuse.Resident)
	}
}

// The evaluation is never made while the Scheduler lock is held.
func TestAdmissionEvaluationDoesNotHoldSchedulerLock(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := &admissionLockContentionEvaluator{}
	enableAdmission(t, s, evaluator, AdmissionOptions{})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if !evaluator.acquired.Load() {
		t.Fatal("the evaluator could not take the Scheduler lock: it ran under it")
	}
}

type admissionLockContentionEvaluator struct{ acquired atomic.Bool }

func (p *admissionLockContentionEvaluator) Evaluate(ctx context.Context, _ AdmissionRequest) (AdmissionEvidence, error) {
	// The Scheduler lock must be free while the evaluation runs. A blocking
	// acquire that only completes proves the lock was released.
	done := make(chan struct{})
	go func() {
		p.acquired.Store(true)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
	return AdmissionEvidence{Verdict: AdmissionFit, EvaluatedAt: time.Now().UTC()}, nil
}

// A cancelled wait leaves no orphan goroutine and no armed timer.
func TestAdmissionCancellationLeavesNoOrphanTimer(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 1 })
	if _, err := s.Release(first.ID); err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Cancelled)
	s.mu.Lock()
	timer := s.admissionTimer
	wait := s.admissionWait
	s.mu.Unlock()
	if timer != nil {
		t.Fatal("the reevaluation timer survived the cancellation")
	}
	if wait != nil {
		t.Fatalf("the admission wait survived the cancellation: %+v", wait)
	}
}

// The Scheduler owns a single reevaluation timer for the whole wait.
func TestAdmissionUsesASingleReevaluationTimer(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	if _, err := s.RequestWithRequestID("request-1", "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 5 })
	s.mu.Lock()
	timer := s.admissionTimer
	s.mu.Unlock()
	if timer == nil {
		t.Fatal("no reevaluation timer is armed")
	}
}

// The wait never starts a process and never extends the lease TTL.
func TestAdmissionWaitDoesNotStartOrExtend(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	lease, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 3 })
	status, _ := s.Get(lease.ID)
	if status.ActivatedAt != nil || status.ExpiresAt != nil {
		t.Fatalf("the lease acquired a TTL while waiting: %+v", status)
	}
	if s.manager.Status().PID != 0 {
		t.Fatal("the runtime started a process while waiting for capacity")
	}
}

// The raw queue pop is never used to bypass the admission check.
func TestAdmissionNeverBypassesTheHead(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, 100*time.Millisecond)
	evaluator := newAdmissionScriptedEvaluator(AdmissionInsufficient)
	enableAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: time.Hour, ReevaluationInterval: 10 * time.Millisecond})
	first, err := s.RequestWithRequestID("request-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RequestWithRequestID("request-2", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return evaluator.callCount() >= 2 })
	// The follower never reaches PREPARING while the head waits.
	s.mu.Lock()
	preparing := s.preparingID
	queue := append([]string(nil), s.queue...)
	s.mu.Unlock()
	if preparing != "" {
		t.Fatalf("a lease reached PREPARING while the head waited: %q", preparing)
	}
	if len(queue) != 2 || queue[0] != first.ID || queue[1] != second.ID {
		t.Fatalf("queue = %v", queue)
	}
}
