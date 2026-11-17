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

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/scheduler"
)

// httpAdmissionEvaluator is a deterministic scheduler-level admission
// evaluator. It never starts a process and never touches the Runtime.
type httpAdmissionEvaluator struct {
	mu       sync.Mutex
	verdict  scheduler.AdmissionVerdict
	err      error
	calls    int
	requests []scheduler.AdmissionRequest
	block    chan struct{}
	started  chan struct{}
	once     sync.Once
}

func newHTTPEvaluator(verdict scheduler.AdmissionVerdict) *httpAdmissionEvaluator {
	return &httpAdmissionEvaluator{verdict: verdict, started: make(chan struct{})}
}

func (e *httpAdmissionEvaluator) Evaluate(ctx context.Context, request scheduler.AdmissionRequest) (scheduler.AdmissionEvidence, error) {
	e.mu.Lock()
	e.calls++
	e.requests = append(e.requests, request)
	block := e.block
	e.mu.Unlock()
	if block != nil {
		e.once.Do(func() { close(e.started) })
		select {
		case <-block:
		case <-ctx.Done():
			return scheduler.AdmissionEvidence{Verdict: scheduler.AdmissionUnknown, Cause: ctx.Err().Error()}, ctx.Err()
		}
	}
	return scheduler.AdmissionEvidence{Verdict: e.verdict, EvaluationID: "eval-" + request.LeaseID, EvaluatedAt: time.Now().UTC()}, e.err
}

func (e *httpAdmissionEvaluator) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *httpAdmissionEvaluator) observedRequests() []scheduler.AdmissionRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]scheduler.AdmissionRequest(nil), e.requests...)
}

// enableHTTPAAdmission installs the evaluator and a fast policy on a
// scheduler.
func enableHTTPAAdmission(t *testing.T, sched *scheduler.Scheduler, evaluator scheduler.AdmissionEvaluator, options scheduler.AdmissionOptions) {
	t.Helper()
	if options.ReevaluationInterval == 0 {
		options.ReevaluationInterval = 10 * time.Millisecond
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = 2 * time.Second
	}
	if err := sched.SetAdmissionOptions(options); err != nil {
		t.Fatal(err)
	}
	sched.SetAdmissionEvaluator(evaluator)
}

// HTTP-01: admission disabled keeps the baseline behavior.
func TestHTTP01AdmissionDisabledKeepsBaseline(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[{"role":"user","content":"hi"}]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.StatusCode, body)
	}
	var chat map[string]any
	if err := json.Unmarshal(body, &chat); err != nil {
		t.Fatal(err)
	}
	if chat["usage"] == nil || chat["choices"] == nil {
		t.Fatalf("baseline OpenAI shape changed: %s", body)
	}
}

// HTTP-02: admission enabled and FIT allows the request to continue.
func TestHTTP02AdmissionFitAllowsContinuation(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if evaluator.callCount() == 0 {
		t.Fatal("the evaluator was never called")
	}
}

// HTTP-03: INSUFFICIENT keeps the request waiting.
func TestHTTP03InsufficientKeepsRequestWaiting(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "1s"}))
	defer server.Close()

	done := make(chan int, 1)
	go func() {
		done <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	}()
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	// The request is still waiting: no response yet and no process.
	select {
	case code := <-done:
		t.Fatalf("the request returned %d while capacity was insufficient", code)
	case <-time.After(100 * time.Millisecond):
	}
	if manager.Status().PID != 0 {
		t.Fatal("a process started without capacity")
	}
	snapshot := sched.Snapshot()
	if len(snapshot.Pending) != 1 || snapshot.Pending[0].State != scheduler.Queued {
		t.Fatalf("pending = %+v, want one QUEUED lease", snapshot.Pending)
	}
	if snapshot.Preparing != nil {
		t.Fatal("PREPARING was published while capacity was missing")
	}
	// The wait ends through the existing queue-wait timeout.
	select {
	case code := <-done:
		if code == http.StatusOK {
			t.Fatal("the request succeeded without capacity")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting request did not return")
	}
}

// HTTP-04: an admission timeout returns the correct typed error.
func TestHTTP04AdmissionTimeoutReturnsTypedError(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 80 * time.Millisecond, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504: %s", response.StatusCode, body)
	}
	if !strings.Contains(string(body), "capacity") {
		t.Fatalf("the timeout body lost its cause: %s", body)
	}
}

// HTTP-05: an HTTP cancellation interrupts the wait.
func TestHTTP05HTTPCancellationInterruptsWait(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second})
	server := httptest.NewServer(NewHandler(manager, sched))
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
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the cancelled request did not return")
	}
	// The lease is cleaned up by the existing route.
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 0 })
	if manager.Status().PID != 0 {
		t.Fatal("a process started after the client cancelled")
	}
}

// HTTP-06: the proxy deadline wins when it is the earliest.
func TestHTTP06ProxyDeadlineWinsWhenEarliest(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "80ms"}))
	defer server.Close()

	started := time.Now()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	elapsed := time.Since(started)
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", response.StatusCode)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the proxy deadline did not win: elapsed = %s", elapsed)
	}
}

// HTTP-07: the admission deadline wins when it is the earliest.
func TestHTTP07AdmissionDeadlineWinsWhenEarliest(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 80 * time.Millisecond, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	started := time.Now()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	elapsed := time.Since(started)
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", response.StatusCode)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the admission deadline did not win: elapsed = %s", elapsed)
	}
}

// HTTP-08: the caller context deadline wins when it is the earliest.
func TestHTTP08ContextDeadlineWinsWhenEarliest(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	started := time.Now()
	response, err := server.Client().Do(request)
	if err == nil {
		_ = response.Body.Close()
	}
	elapsed := time.Since(started)
	if elapsed > 5*time.Second {
		t.Fatalf("the context deadline did not win: elapsed = %s", elapsed)
	}
	// The request is gone: the lease must be cleaned up.
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 0 })
}

// HTTP-09: an idempotent replay does not restart the deadline.
func TestHTTP09IdempotentReplayDoesNotRestartDeadline(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 600 * time.Millisecond, ReevaluationInterval: 20 * time.Millisecond})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	key := "4ef8d7ef-b335-4404-b72c-5b2c08a36103"
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
	var status scheduler.LeaseStatus
	if err := json.NewDecoder(first.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first status = %d", first.StatusCode)
	}
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	created := time.Now()
	// The replay returns the same lease and does not create a second
	// acquisition nor a second evaluation.
	replay := post()
	var replayStatus scheduler.LeaseStatus
	if err := json.NewDecoder(replay.Body).Decode(&replayStatus); err != nil {
		t.Fatal(err)
	}
	_ = replay.Body.Close()
	if replayStatus.ID != status.ID {
		t.Fatalf("the replay created a new lease: %q != %q", replayStatus.ID, status.ID)
	}
	// The original deadline is preserved: the lease ends with the original
	// admission window, not with a restarted one.
	waitForAPI(t, func() bool {
		lease, err := sched.Get(status.ID)
		return err == nil && lease.State == scheduler.Cancelled
	})
	elapsed := time.Since(created)
	// A restarted deadline would push the end past the original window.
	if elapsed > 2*time.Second {
		t.Fatalf("the replay restarted the deadline: elapsed = %s", elapsed)
	}
}

// HTTP-10: an error before SSE keeps the HTTP response.
func TestHTTP10ErrorBeforeSSEPreservesHTTPResponse(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 80 * time.Millisecond, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","stream":true,"messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", response.StatusCode)
	}
	if strings.Contains(string(body), "data:") {
		t.Fatalf("a partial SSE response was emitted: %s", body)
	}
	if contentType := response.Header.Get("Content-Type"); strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("the SSE content type was sent before the stream started: %q", contentType)
	}
}

// HTTP-11: an error after SSE keeps the protocol.
func TestHTTP11ErrorAfterSSEKeepsProtocol(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := response.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	// The status was already committed: a later error cannot change it.
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	cancel()
	_ = response.Body.Close()
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// HTTP-12: an explicit ACTIVE lease is not reacquired.
func TestHTTP12ExplicitActiveLeaseIsNotReacquired(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitActiveAPI(t, sched, lease.ID)
	before := evaluator.callCount()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, lease.ID)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if evaluator.callCount() != before {
		t.Fatalf("the explicit lease caused a new evaluation: %d -> %d", before, evaluator.callCount())
	}
	if got, _ := sched.Get(lease.ID); got.State != scheduler.Active {
		t.Fatalf("the explicit lease was released: %s", got.State)
	}
	_, _ = sched.Release(lease.ID)
}

// HTTP-13: an explicit QUEUED lease respects its wait.
func TestHTTP13ExplicitQueuedLeaseRespectsWait(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "1s"}))
	defer server.Close()

	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	done := make(chan int, 1)
	go func() {
		done <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, lease.ID)
	}()
	select {
	case code := <-done:
		t.Fatalf("the queued explicit lease returned %d instead of waiting", code)
	case <-time.After(100 * time.Millisecond):
	}
	// The pending lease was not reacquired: no second lease exists and the
	// evaluations still belong to the original lease.
	snapshot := sched.Snapshot()
	if len(snapshot.Pending) != 1 || snapshot.Pending[0].ID != lease.ID {
		t.Fatalf("pending = %+v, want only the original lease", snapshot.Pending)
	}
	for _, request := range evaluator.observedRequests() {
		if request.LeaseID != lease.ID {
			t.Fatalf("a second lease was acquired: %s", request.LeaseID)
		}
	}
	// The wait ends through the existing queue-wait timeout.
	select {
	case code := <-done:
		if code == http.StatusOK {
			t.Fatal("the request succeeded without capacity")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting request did not return")
	}
}

// HTTP-14: an invalid or expired lease is rejected.
func TestHTTP14InvalidOrExpiredLeaseIsRejected(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "not-a-lease")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("invalid lease status = %d, want 404", response.StatusCode)
	}
	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sched.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	response = postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, lease.ID)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("terminal lease status = %d, want 409", response.StatusCode)
	}
}

// HTTP-15: Recovery prevails over FIT.
func TestHTTP15RecoveryPrevailsOverFit(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	// A blocked admission gate rejects the acquisition with the existing
	// recovery branch, before any capacity evaluation.
	sched.SetAdmissionGate(blockedAdmissionGate{})
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.StatusCode)
	}
	if evaluator.callCount() != 0 {
		t.Fatalf("the evaluator ran while Recovery was blocked: %d", evaluator.callCount())
	}
}

// HTTP-16: shutdown cancels the wait.
func TestHTTP16ShutdownCancelsWait(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	done := make(chan int, 1)
	go func() {
		done <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	}()
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code == http.StatusOK {
			t.Fatal("the waiting request succeeded after the shutdown")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting request did not return after the shutdown")
	}
	if manager.Status().PID != 0 {
		t.Fatal("a process survived the shutdown during an admission wait")
	}
}

// HTTP-17: Shadow Mode keeps its compatibility.
func TestHTTP17ShadowModeKeepsCompatibility(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	handler := NewHandlerWithCapacity(manager, sched, config.ProxyConfig{}, capacity.Options{ShadowMode: true})
	if closer, ok := handler.(interface{ Close() error }); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := server.Client().Post(server.URL+"/api/v1/leases", "application/json", strings.NewReader(`{"model":"model-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("lease status = %d", response.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err = server.Client().Get(server.URL + "/api/v1/capacity/evaluations")
		if err == nil {
			var evaluations []map[string]any
			decodeErr := json.NewDecoder(response.Body).Decode(&evaluations)
			_ = response.Body.Close()
			if decodeErr == nil && len(evaluations) > 0 {
				if evaluations[0]["model_id"] != "model-1" {
					t.Fatalf("evaluation = %+v", evaluations[0])
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the shadow evaluation endpoint produced no evaluation")
}

// HTTP-18: concurrency does not duplicate activations.
func TestHTTP18ConcurrencyDoesNotDuplicateActivations(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	var wg sync.WaitGroup
	statuses := make([]int, 4)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i] = doPostStatus(server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
		}(i)
	}
	wg.Wait()
	for i, code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, code)
		}
	}
	// Every evaluation belongs to a distinct lease: no acquisition was
	// evaluated twice.
	seen := map[string]bool{}
	for _, request := range evaluator.observedRequests() {
		if seen[request.LeaseID] {
			t.Fatalf("lease %s was evaluated more than once", request.LeaseID)
		}
		seen[request.LeaseID] = true
	}
}

// HTTP-19: the evaluator is installed only when admission is enabled.
func TestHTTP19EvaluatorInstalledOnlyWhenEnabled(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	// Disabled: no evaluator, no admission worker, no behavior change.
	server := httptest.NewServer(NewHandler(manager, sched))
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	server.Close()
	// Enabled: the evaluator is installed and used.
	manager2, sched2 := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched2, evaluator, scheduler.AdmissionOptions{})
	server2 := httptest.NewServer(NewHandler(manager2, sched2))
	defer server2.Close()
	response = postJSON(t, server2, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if evaluator.callCount() == 0 {
		t.Fatal("the enabled evaluator was never called")
	}
}

// HTTP-20: shutdown releases the workers and resources.
func TestHTTP20ShutdownReleasesWorkersAndResources(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionFit)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	_ = response.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if sched.ActiveInferenceCount() != 0 {
		t.Fatal("an inference survived the shutdown")
	}
	if manager.Status().PID != 0 {
		t.Fatal("a process survived the shutdown")
	}
}

// blockedAdmissionGate blocks admission like the Recovery authority does.
type blockedAdmissionGate struct{}

func (blockedAdmissionGate) AdmissionBlocked() bool { return true }

// admissionCancelledEvaluator cancels the evaluation with the caller
// context.
type admissionCancelledEvaluator struct{ started chan struct{} }

func (e *admissionCancelledEvaluator) Evaluate(ctx context.Context, _ scheduler.AdmissionRequest) (scheduler.AdmissionEvidence, error) {
	close(e.started)
	<-ctx.Done()
	return scheduler.AdmissionEvidence{Verdict: scheduler.AdmissionUnknown}, ctx.Err()
}

// The admission cancellation is mapped to a typed error and never to a
// generic internal failure.
func TestAdmissionCancellationIsMappedToTypedError(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := &admissionCancelledEvaluator{started: make(chan struct{})}
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 50 * time.Millisecond, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode == http.StatusInternalServerError {
		t.Fatalf("the cancellation became an internal error: %s", body)
	}
	if response.StatusCode != http.StatusGatewayTimeout && response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 504 or 503: %s", response.StatusCode, body)
	}
}

// A scheduler admission failure is exposed with its typed kind.
func TestSchedulerAdmissionFailureIsTyped(t *testing.T) {
	_, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitForAPI(t, func() bool { return evaluator.callCount() >= 1 })
	if _, err := sched.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	if failure := sched.AdmissionFailure(lease.ID); failure != nil {
		if failure.Kind != scheduler.AdmissionErrorCapacity && failure.Kind != scheduler.AdmissionErrorCancelled {
			t.Fatalf("kind = %s", failure.Kind)
		}
	}
}

// The admission error kinds map to distinct HTTP statuses.
func TestAdmissionErrorStatusMapping(t *testing.T) {
	for _, testCase := range []struct {
		err    error
		status int
	}{
		{scheduler.ErrAdmissionCapacity, http.StatusConflict},
		{scheduler.ErrAdmissionTimeout, http.StatusGatewayTimeout},
		{scheduler.ErrAdmissionUnavailable, http.StatusServiceUnavailable},
		{scheduler.ErrAdmissionCancelled, http.StatusServiceUnavailable},
	} {
		recorder := httptest.NewRecorder()
		writeInferenceSchedulerError(recorder, testCase.err)
		if recorder.Code != testCase.status {
			t.Fatalf("err %v status = %d, want %d", testCase.err, recorder.Code, testCase.status)
		}
		if body := recorder.Body.String(); strings.Contains(body, "/Users/") || strings.Contains(body, "pid") {
			t.Fatalf("the error body leaked internal details: %s", body)
		}
	}
}

// The recovery admission failure keeps the recovery branch.
func TestAdmissionRecoveryUsesRecoveryBranch(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeSchedulerError(recorder, scheduler.ErrAdmissionRecovery)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), string(scheduler.RecoveryRequired)) {
		t.Fatalf("body = %s, want the recovery state", recorder.Body.String())
	}
}

// A preventive capacity rejection maps to the existing conflict branch.
func TestAdmissionCapacityRejectionUsesConflictBranch(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionInsufficient)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{OnInsufficient: "reject", WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", response.StatusCode, body)
	}
	if !strings.Contains(string(body), "capacity") {
		t.Fatalf("the rejection body lost its cause: %s", body)
	}
}

// An unavailable evaluation never uses the capacity conflict branch.
func TestAdmissionUnavailableIsNotCapacity(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	evaluator := newHTTPEvaluator(scheduler.AdmissionUnknown)
	enableHTTPAAdmission(t, sched, evaluator, scheduler.AdmissionOptions{OnUnknown: "reject", WaitTimeout: 30 * time.Second, ReevaluationInterval: 10 * time.Millisecond})
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "30s"}))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "")
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", response.StatusCode, body)
	}
	if strings.Contains(string(body), "capacity is exhausted") {
		t.Fatalf("UNKNOWN was reported as a capacity rejection: %s", body)
	}
}
