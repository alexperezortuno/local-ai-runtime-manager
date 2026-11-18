package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/scheduler"
)

// controlHTTPEvaluator is a deterministic Scheduler admission evaluator used
// by the operational HTTP controls. It never starts a model itself.
type controlHTTPEvaluator struct {
	mu      sync.Mutex
	verdict scheduler.AdmissionVerdict
	cause   string
	calls   int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newControlHTTPEvaluator(verdict scheduler.AdmissionVerdict) *controlHTTPEvaluator {
	return &controlHTTPEvaluator{verdict: verdict, started: make(chan struct{})}
}

func (e *controlHTTPEvaluator) Evaluate(ctx context.Context, request scheduler.AdmissionRequest) (scheduler.AdmissionEvidence, error) {
	e.mu.Lock()
	e.calls++
	verdict := e.verdict
	cause := e.cause
	release := e.release
	e.mu.Unlock()
	if release != nil {
		e.once.Do(func() { close(e.started) })
		select {
		case <-release:
		case <-ctx.Done():
			return scheduler.AdmissionEvidence{Verdict: scheduler.AdmissionUnknown, Cause: ctx.Err().Error()}, ctx.Err()
		}
	}
	return scheduler.AdmissionEvidence{Verdict: verdict, Cause: cause, EvaluationID: "control-" + request.LeaseID, EvaluatedAt: time.Now().UTC()}, nil
}

func (e *controlHTTPEvaluator) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *controlHTTPEvaluator) blockNext() {
	e.mu.Lock()
	e.release = make(chan struct{})
	e.mu.Unlock()
}

func (e *controlHTTPEvaluator) unblock() {
	e.mu.Lock()
	release := e.release
	e.release = nil
	e.mu.Unlock()
	if release != nil {
		close(release)
	}
}

func enableControlHTTPAdmission(t *testing.T, sched *scheduler.Scheduler, evaluator scheduler.AdmissionEvaluator, options scheduler.AdmissionOptions) {
	t.Helper()
	if options.ReevaluationInterval == 0 {
		options.ReevaluationInterval = 5 * time.Millisecond
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = 2 * time.Second
	}
	if err := sched.SetAdmissionOptions(options); err != nil {
		t.Fatal(err)
	}
	sched.SetAdmissionEvaluator(evaluator)
}

// CONTROL-05: the proxy queue deadline is the effective deadline, and a
// timeout while queued does not cancel an active inference.
func TestCONTROL05HTTPProxyDeadlineDoesNotCancelActiveInference(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newControlHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableControlHTTPAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "50ms"}))
	defer server.Close()

	started := time.Now()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504: %s", response.StatusCode, body)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("proxy deadline did not win: %s", elapsed)
	}
	if manager.Status().PID != 0 {
		t.Fatal("a model started without capacity")
	}
}

// CONTROL-05: HTTP context cancellation interrupts the queue wait and leaves
// no active lease or process.
func TestCONTROL05HTTPContextCancellationInterruptsWait(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newControlHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableControlHTTPAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		response, err := server.Client().Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		_ = err
		close(done)
	}()
	waitForAPI(t, func() bool { return evaluator.count() >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request did not return")
	}
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 0 })
	if manager.Status().PID != 0 {
		t.Fatal("a model started after cancellation")
	}
}

// CONTROL-05: an idempotent HTTP replay returns the original lease and does
// not restart its deadline.
func TestCONTROL05HTTPIdempotentReplayKeepsDeadline(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newControlHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableControlHTTPAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 200 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	key := "7ef8d7ef-b335-4404-b72c-5b2c08a36107"
	post := func() *http.Response {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/leases", strings.NewReader(`{"model":"model-1"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := post()
	var firstStatus scheduler.LeaseStatus
	if err := json.NewDecoder(first.Body).Decode(&firstStatus); err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	waitForAPI(t, func() bool { return evaluator.count() >= 1 })
	replay := post()
	var replayStatus scheduler.LeaseStatus
	if err := json.NewDecoder(replay.Body).Decode(&replayStatus); err != nil {
		t.Fatal(err)
	}
	_ = replay.Body.Close()
	if replayStatus.ID != firstStatus.ID {
		t.Fatalf("replay created another lease: %s != %s", replayStatus.ID, firstStatus.ID)
	}
	if evaluator.count() != 1 {
		t.Fatalf("evaluations = %d, want 1", evaluator.count())
	}
	waitForAPI(t, func() bool {
		lease, err := sched.Get(firstStatus.ID)
		return err == nil && lease.State == scheduler.Cancelled
	})
}

// CONTROL-07: Recovery appearing while the HTTP evaluator is in flight wins
// over the pending FIT.
func TestCONTROL07HTTPRecoveryDuringEvaluationPreventsStart(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newControlHTTPEvaluator(scheduler.AdmissionFit)
	evaluator.blockNext()
	enableControlHTTPAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	done := make(chan int, 1)
	go func() {
		done <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	}()
	<-evaluator.started
	sched.SetAdmissionGate(blockedAdmissionGate{})
	evaluator.unblock()
	select {
	case code := <-done:
		if code == http.StatusOK {
			t.Fatal("request succeeded despite Recovery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not return after Recovery")
	}
	if manager.Status().PID != 0 {
		t.Fatal("a model started despite Recovery")
	}
}

// CONTROL-10: shutting down LARM while an HTTP request waits cancels the
// request, clears queued work, and leaves no process.
func TestCONTROL10HTTPShutdownCancelsWaitingRequest(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newControlHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableControlHTTPAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	done := make(chan int, 1)
	go func() {
		done <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	}()
	waitForAPI(t, func() bool { return evaluator.count() >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code == http.StatusOK {
			t.Fatal("request succeeded after shutdown")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request did not return after shutdown")
	}
	if manager.Status().PID != 0 {
		t.Fatal("a process survived shutdown")
	}
}
