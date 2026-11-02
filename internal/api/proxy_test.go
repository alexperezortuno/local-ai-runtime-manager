package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/scheduler"
)

func TestOpenAIModelsChatAndEmbeddings(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	var models struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&models); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || models.Object != "list" || len(models.Data) != 2 || models.Data[0].ID != "model-1" {
		t.Fatalf("models response: status=%d value=%+v", response.StatusCode, models)
	}

	response = postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[{"role":"user","content":"hi"}]}`, "")
	var chat map[string]any
	if err := json.NewDecoder(response.Body).Decode(&chat); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	choices := chat["choices"].([]any)
	choice := choices[0].(map[string]any)
	if response.StatusCode != http.StatusOK || chat["usage"] == nil || choice["finish_reason"] != "tool_calls" || choice["message"].(map[string]any)["tool_calls"] == nil {
		t.Fatalf("chat response lost compatible fields: status=%d body=%v", response.StatusCode, chat)
	}

	response = postJSON(t, server, "/v1/embeddings", `{"model":"model-1","input":"hello"}`, "")
	var embeddings map[string]any
	if err := json.NewDecoder(response.Body).Decode(&embeddings); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || embeddings["data"] == nil || embeddings["usage"] == nil {
		t.Fatalf("embedding response: status=%d body=%v", response.StatusCode, embeddings)
	}
	if sched.ActiveInferenceCount() != 0 {
		t.Fatalf("automatic lease left inference pinned: %d", sched.ActiveInferenceCount())
	}
	waitForAPI(t, func() bool { return manager.Status().PID == 0 })
}

func TestChatStreamingSSEPreserved(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "")
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") || !strings.Contains(text, `"content":"one"`) || !strings.Contains(text, "data: [DONE]\n\n") {
		t.Fatalf("SSE response: status=%d headers=%v body=%q", response.StatusCode, response.Header, text)
	}
}

func TestOpenAIUnknownModelAndExternalLeaseValidation(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"absent","messages":[]}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown model status = %d", response.StatusCode)
	}
	response = postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, "not-a-lease")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("invalid external lease status = %d", response.StatusCode)
	}

	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitActiveAPI(t, sched, lease.ID)
	response = postJSON(t, server, "/v1/chat/completions", `{"model":"model-2","messages":[]}`, lease.ID)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("mismatched model status = %d", response.StatusCode)
	}
	response = postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[]}`, lease.ID)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("valid external lease status = %d", response.StatusCode)
	}
	if got, _ := sched.Get(lease.ID); got.State != scheduler.Active {
		t.Fatalf("proxy released external lease: %s", got.State)
	}
	_, _ = sched.Release(lease.ID)
}

func TestQueuedClientCancellationRemovesAutomaticLease(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	hold, _ := sched.Request("model-1")
	waitActiveAPI(t, sched, hold.ID)
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-2","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		response, _ := server.Client().Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		close(done)
	}()
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled queued request did not return")
	}
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 0 })
	_, _ = sched.Release(hold.ID)
}

func TestStreamingDisconnectAndBackendErrors(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	lease, _ := sched.Request("model-1")
	waitActiveAPI(t, sched, lease.ID)
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","stream":true}`))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := response.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = response.Body.Close()
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
	_, _ = sched.Release(lease.ID)

	t.Setenv("LARM_API_BACKEND_MODE", "error")
	manager2, sched2 := apiTestScheduler(t)
	server2 := httptest.NewServer(NewHandler(manager2, sched2))
	defer server2.Close()
	response = postJSON(t, server2, "/v1/chat/completions", `{"model":"model-1"}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("backend error status was not preserved: %d", response.StatusCode)
	}
}

func TestInferenceTimeoutAndLeaseExpiryDoNotInterruptAdmittedWork(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "slow")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{InferenceTimeout: "50ms"}))
	defer server.Close()
	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1"}`, "")
	_ = response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("inference timeout status = %d", response.StatusCode)
	}

	manager2, sched2 := apiTestSchedulerWithTTL(t, "120ms")
	server2 := httptest.NewServer(NewHandler(manager2, sched2))
	defer server2.Close()
	lease, _ := sched2.Request("model-1")
	waitActiveAPI(t, sched2, lease.ID)
	response = postJSON(t, server2, "/v1/chat/completions", `{"model":"model-1"}`, lease.ID)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("inference admitted before expiry did not finish: %d", response.StatusCode)
	}
	waitLeaseStateAPI(t, sched2, lease.ID, scheduler.Expired)
	waitForAPI(t, func() bool { return manager2.Status().PID == 0 })
}

func TestModelSwitchWaitsForInferenceAndConcurrentCallers(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "slow")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	firstDone := make(chan int, 1)
	go func() {
		firstDone <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1"}`, "")
	}()
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 1 })
	secondDone := make(chan int, 1)
	go func() {
		secondDone <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-2"}`, "")
	}()
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 1 })
	if manager.Status().ActiveModel != "model-1" || manager.Status().PID == 0 {
		t.Fatal("runtime changed models while first inference was admitted")
	}
	if code := <-firstDone; code != http.StatusOK {
		t.Fatalf("first inference status = %d", code)
	}
	if code := <-secondDone; code != http.StatusOK {
		t.Fatalf("second inference status = %d", code)
	}
	if manager.Status().ActiveModel != "model-2" && manager.Status().PID != 0 {
		t.Fatalf("second model was not activated after inference: %+v", manager.Status())
	}
}

func TestReleasedExternalLeaseKeepsBackendUntilAdmittedInferenceEnds(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "slow")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	lease, _ := sched.Request("model-1")
	waitActiveAPI(t, sched, lease.ID)
	result := make(chan int, 1)
	go func() { result <- doPostStatus(server, "/v1/chat/completions", `{"model":"model-1"}`, lease.ID) }()
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 1 })
	if _, err := sched.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	next, err := sched.Request("model-2")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if manager.Status().ActiveModel != "model-1" || manager.Status().PID == 0 || sched.ActiveInferenceCount() != 1 {
		t.Fatalf("backend stopped before admitted inference ended: runtime=%+v inferences=%d", manager.Status(), sched.ActiveInferenceCount())
	}
	if code := <-result; code != http.StatusOK {
		t.Fatalf("inference status = %d", code)
	}
	waitActiveAPI(t, sched, next.ID)
	if manager.Status().ActiveModel != "model-2" {
		t.Fatalf("next model not activated after inference: %+v", manager.Status())
	}
	_, _ = sched.Release(next.ID)
}

func TestShutdownCancelsAndWaitsForActiveInference(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "slow")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	lease, _ := sched.Request("model-1")
	waitActiveAPI(t, sched, lease.ID)
	done := make(chan struct{})
	go func() {
		_ = doPostStatus(server, "/v1/chat/completions", `{"model":"model-1"}`, lease.ID)
		close(done)
	}()
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inference did not finish cancellation during shutdown")
	}
	if sched.ActiveInferenceCount() != 0 || manager.Status().PID != 0 {
		t.Fatalf("shutdown returned with inference or process active: count=%d runtime=%+v", sched.ActiveInferenceCount(), manager.Status())
	}
}

func postJSON(t *testing.T, server *httptest.Server, path, body, leaseID string) *http.Response {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if leaseID != "" {
		request.Header.Set("X-LARM-Lease-ID", leaseID)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func doPostStatus(server *httptest.Server, path, body, leaseID string) int {
	request, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if leaseID != "" {
		request.Header.Set("X-LARM-Lease-ID", leaseID)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	return response.StatusCode
}

func waitForAPI(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}

func waitActiveAPI(t *testing.T, sched *scheduler.Scheduler, id string) {
	waitLeaseStateAPI(t, sched, id, scheduler.Active)
}

func waitLeaseStateAPI(t *testing.T, sched *scheduler.Scheduler, id string, state scheduler.LeaseState) {
	t.Helper()
	waitForAPI(t, func() bool {
		lease, err := sched.Get(id)
		return err == nil && lease.State == state
	})
}
