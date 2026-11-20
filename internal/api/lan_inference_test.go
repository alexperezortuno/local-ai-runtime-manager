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
)

const lanTestKey = "lan-secret-key"

// lanTestServer starts the real LAN router over TLS. It returns the client,
// the base URL and the shared proxy owner so the tests can assert the lease
// contract on the very same Scheduler the LAN requests use.
func lanTestServer(t *testing.T, keys *LANAPIKeys, allow []string) (*httptest.Server, *http.Client, *Handler) {
	t.Helper()
	manager, sched := apiTestScheduler(t)
	handler, ok := NewHandler(manager, sched).(*Handler)
	if !ok {
		t.Fatal("local handler is not a *Handler")
	}
	if keys == nil {
		keys = NewLANAPIKeysFromValues(lanTestKey)
	}
	if allow == nil {
		allow = []string{"127.0.0.0/8"}
	}
	lan := newLANHandler(config.LANConfig{Access: config.LANAccessConfig{Allow: allow}}, keys, handler)
	server := httptest.NewTLSServer(lan)
	client := server.Client()
	client.Timeout = 30 * time.Second
	t.Cleanup(server.Close)
	return server, client, handler
}

func lanRequest(t *testing.T, client *http.Client, method, url, token, body string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestLANInferenceModelsAuthorized(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodGet, server.URL+"/v1/models", lanTestKey, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("models status = %d", response.StatusCode)
	}
	var payload struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Object != "list" || len(payload.Data) != 2 || payload.Data[0].ID != "model-1" {
		t.Fatalf("models payload = %+v", payload)
	}
}

func TestLANInferenceChatAndEmbeddingsAuthorized(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","messages":[{"role":"user","content":"hi"}]}`)
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d body = %s", response.StatusCode, body)
	}
	var chat map[string]any
	if err := json.Unmarshal(body, &chat); err != nil {
		t.Fatal(err)
	}
	if chat["choices"] == nil || chat["usage"] == nil {
		t.Fatalf("chat payload lost compatible fields: %s", body)
	}

	response = lanRequest(t, client, http.MethodPost, server.URL+"/v1/embeddings", lanTestKey, `{"model":"model-1","input":"hello"}`)
	body, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("embeddings status = %d body = %s", response.StatusCode, body)
	}
	var embeddings map[string]any
	if err := json.Unmarshal(body, &embeddings); err != nil {
		t.Fatal(err)
	}
	if embeddings["data"] == nil || embeddings["usage"] == nil {
		t.Fatalf("embeddings payload = %s", body)
	}
}

func TestLANInferenceRequiresAuthentication(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	for _, test := range []struct {
		name, method, path, token, body string
	}{
		{"models without token", http.MethodGet, "/v1/models", "", ""},
		{"models with invalid token", http.MethodGet, "/v1/models", "wrong-key", ""},
		{"chat without token", http.MethodPost, "/v1/chat/completions", "", `{"model":"model-1"}`},
		{"chat with invalid token", http.MethodPost, "/v1/chat/completions", "wrong-key", `{"model":"model-1"}`},
		{"embeddings without token", http.MethodPost, "/v1/embeddings", "", `{"model":"model-1"}`},
		{"embeddings with invalid token", http.MethodPost, "/v1/embeddings", "wrong-key", `{"model":"model-1"}`},
		{"health without token", http.MethodGet, "/api/v1/lan/health", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := lanRequest(t, client, test.method, server.URL+test.path, test.token, test.body)
			defer response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", response.StatusCode)
			}
			if response.Header.Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q", response.Header.Get("WWW-Authenticate"))
			}
		})
	}
}

// TestLANInferenceRejectsQueryParameterToken proves a query string never
// authenticates.
func TestLANInferenceRejectsQueryParameterToken(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodGet, server.URL+"/v1/models?api_key="+lanTestKey, "", "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.StatusCode)
	}
}

// TestLANInferenceRejectsAddressOutsideAllowlist proves the CIDR gate is
// enforced before any lease is created.
func TestLANInferenceRejectsAddressOutsideAllowlist(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, []string{"10.99.0.0/24"})
	response := lanRequest(t, client, http.MethodGet, server.URL+"/v1/models", lanTestKey, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", response.StatusCode)
	}
	if handler.scheduler.ActiveInferenceCount() != 0 || len(handler.scheduler.Snapshot().Pending) != 0 {
		t.Fatal("a denied LAN request created scheduler state")
	}
}

// TestLANClientSuppliedLeaseIsIgnored proves a LAN client cannot use an
// explicit lease ID or a request identity. Leases and request identities are
// only granted on the local listener, so the LAN request always takes the
// automatic lease path with a generated request identity.
func TestLANClientSuppliedLeaseIsIgnored(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+lanTestKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-LARM-Lease-ID", "lease-does-not-exist")
	request.Header.Set("X-LARM-Request-ID", "3f2504e0-4f89-41d3-9a0c-0305e82c3301")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if got := response.Header.Get("X-LARM-Request-ID"); got == "" || got == "3f2504e0-4f89-41d3-9a0c-0305e82c3301" {
		t.Fatalf("the LAN request identity was not generated by LARM: %q", got)
	}
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
}

// TestLANAdministrativeRoutesAreDenied proves the administrative surface
// stays unreachable from the LAN listener.
func TestLANAdministrativeRoutesAreDenied(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	for _, path := range []string{
		"/api/v1/status",
		"/api/v1/models",
		"/api/v1/processes",
		"/api/v1/leases",
		"/api/v1/recovery/status",
		"/api/v1/recovery/inferences",
		"/api/v1/recovery/leases",
		"/api/v1/recovery/audit",
		"/api/v1/capacity",
		"/api/v1/capacity/evaluations",
		"/api/v1/capacity/model-1",
		"/api/v1/admission/metrics",
		"/api/v1/admission/events",
		"/api/v1/models/model-1/start",
		"/api/v1/models/model-1/stop",
		"/api/v1/inferences",
		"/api/v1/queue",
		"/health",
		"/api/v1/lan/health/",
		"/v1/models/",
		"/v1/chat/completions/",
		"/v1/responses/",
		"/api/v1/status/",
	} {
		t.Run(path, func(t *testing.T) {
			response := lanRequest(t, client, http.MethodGet, server.URL+path, lanTestKey, "")
			defer response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("GET %s status = %d, want 404", path, response.StatusCode)
			}
			response = lanRequest(t, client, http.MethodPost, server.URL+path, lanTestKey, `{"model":"model-1"}`)
			defer response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("POST %s status = %d, want 404", path, response.StatusCode)
			}
		})
	}
}

// TestLANAlternativeMethodsAreDenied proves the administrative routes stay
// unreachable through another verb, a prefix or an equivalent path.
func TestLANAlternativeMethodsAreDenied(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	for _, test := range []struct {
		method, path string
	}{
		{http.MethodDelete, "/api/v1/leases/lease-1"},
		{http.MethodPut, "/api/v1/leases/lease-1"},
		{http.MethodPatch, "/api/v1/models/model-1/start"},
		{http.MethodHead, "/api/v1/status"},
		{http.MethodOptions, "/api/v1/status"},
		{http.MethodPost, "/api/v1/leases"},
		{http.MethodGet, "/v1/responses"},
		{http.MethodDelete, "/v1/responses"},
		{http.MethodGet, "/v1/chat/completions"},
		{http.MethodGet, "/v1/embeddings"},
		{http.MethodPost, "/v1/models"},
		{http.MethodPost, "/api/v1/lan/health"},
		{http.MethodGet, "//v1/models"},
		{http.MethodGet, "/v1//models"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			response := lanRequest(t, client, test.method, server.URL+test.path, lanTestKey, "")
			defer response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", response.StatusCode)
			}
		})
	}
}

func TestLANUnknownModelIsNotFound(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"absent","messages":[]}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.StatusCode)
	}
}

func TestLANStreamingSSEIsPreserved(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","stream":true,"messages":[]}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("Content-Type = %q", response.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, `"content":"one"`) || !strings.Contains(text, "data: [DONE]\n\n") {
		t.Fatalf("SSE body = %q", text)
	}
}

// TestLANStreamingDeliversProgressively proves the SSE events are not
// buffered until the backend finishes.
func TestLANStreamingDeliversProgressively(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","stream":true}`)
	defer response.Body.Close()
	buf := make([]byte, 128)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := response.Body.Read(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if n > 0 && strings.Contains(string(buf[:n]), `"content":"one"`) {
			return
		}
	}
	t.Fatal("the first SSE event never arrived while the backend was still running")
}

func TestLANClientDisconnectReleasesLease(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	server, client, handler := lanTestServer(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1","stream":true}`))
	request.Header.Set("Authorization", "Bearer "+lanTestKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := response.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = response.Body.Close()
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
	if len(handler.scheduler.Snapshot().Pending) != 0 {
		t.Fatal("a disconnected LAN request left a queued lease")
	}
}

// TestLANQueuedCancellationRemovesAutomaticLease exercises the cancellation
// during QUEUED on the LAN transport.
func TestLANQueuedCancellationRemovesAutomaticLease(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	hold, err := handler.scheduler.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitActiveAPI(t, handler.scheduler, hold.ID)
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-2","messages":[]}`))
	request.Header.Set("Authorization", "Bearer "+lanTestKey)
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		response, _ := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		close(done)
	}()
	waitForAPI(t, func() bool { return len(handler.scheduler.Snapshot().Pending) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the cancelled queued LAN request did not return")
	}
	waitForAPI(t, func() bool { return len(handler.scheduler.Snapshot().Pending) == 0 })
	_, _ = handler.scheduler.Release(hold.ID)
}

// TestLANBackendErrorIsPropagated proves the backend status is preserved.
func TestLANBackendErrorIsPropagated(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "error")
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1"}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 502/429 passthrough", response.StatusCode)
	}
}

// TestLANReusesTheSameSchedulerAndLeaseContract proves a LAN request uses the
// very same Scheduler, lease and registry as a local request.
func TestLANReusesTheSameSchedulerAndLeaseContract(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","messages":[]}`)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	// The automatic lease was released by the proxy and the inference is
	// recorded in the very same registry the local listener uses.
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
	inferences := handler.inferences.List()
	if len(inferences) == 0 {
		t.Fatal("the LAN request left no inference record")
	}
	for _, item := range inferences {
		if item.ModelID != "model-1" {
			t.Fatalf("inference model = %q", item.ModelID)
		}
	}
}

// TestLANConcurrentRequestsShareTheScheduler exercises concurrent LAN
// requests against the single FIFO Scheduler. The Scheduler keeps its own
// queue bound, so a burst larger than the queue is answered with the
// Scheduler conflict and never with a second queue.
func TestLANConcurrentRequestsShareTheScheduler(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	var wait sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","messages":[]}`)
			_ = response.Body.Close()
			statuses[index] = response.StatusCode
		}(i)
	}
	wait.Wait()
	accepted := 0
	for index, status := range statuses {
		switch status {
		case http.StatusOK:
			accepted++
		case http.StatusTooManyRequests:
			// The Scheduler queue bound rejected the request. It is the
			// single Scheduler of the local listener, not a second one.
		default:
			t.Fatalf("concurrent request %d status = %d", index, status)
		}
	}
	if accepted == 0 {
		t.Fatal("no concurrent LAN request was accepted")
	}
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
}

// TestLANDoesNotExposeLlamaServerDirectly proves the LAN listener never
// proxies to the llama-server port and that the backend stays unreachable
// from the LAN surface.
func TestLANDoesNotExposeLlamaServerDirectly(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	for _, path := range []string{"/health", "/v1/chat/completions/../health", "/api/v1/lan/health/../../health"} {
		response := lanRequest(t, client, http.MethodGet, server.URL+path, lanTestKey, "")
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(response.Body)
			if strings.Contains(string(body), "\"status\"") && strings.Contains(string(body), "ok") {
				t.Fatalf("the LAN listener exposed the llama-server surface at %s: %s", path, body)
			}
		}
	}
}

// TestLANUsesTLS proves the LAN listener requires TLS.
func TestLANUsesTLS(t *testing.T) {
	server, _, _ := lanTestServer(t, nil, nil)
	if !strings.HasPrefix(server.URL, "https://") {
		t.Fatalf("LAN URL = %q", server.URL)
	}
	plain := &http.Client{Timeout: 5 * time.Second}
	response, err := plain.Get(server.URL + "/api/v1/lan/health")
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("a plaintext request was accepted by the LAN listener")
	}
}

// TestLANDoesNotForwardTheBearerKeyToTheBackend proves the LAN credential
// stays at LARM and never reaches llama-server.
func TestLANDoesNotForwardTheBearerKeyToTheBackend(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/chat/completions", lanTestKey, `{"model":"model-1","messages":[]}`)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		BackendAuthorization string `json:"larm_backend_authorization"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.BackendAuthorization != "" {
		t.Fatalf("the LAN credential was forwarded to the backend: %q", payload.BackendAuthorization)
	}
	if strings.Contains(string(body), lanTestKey) {
		t.Fatalf("the LAN credential leaked into the backend response: %s", body)
	}
}

// TestLANLocalListenerRemainsCompatible proves the local listener keeps
// serving its own routes while the LAN listener is active.
func TestLANLocalListenerRemainsCompatible(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	handler, ok := NewHandler(manager, sched).(*Handler)
	if !ok {
		t.Fatal("local handler is not a *Handler")
	}
	local := httptest.NewServer(handler)
	defer local.Close()
	lan := httptest.NewTLSServer(newLANHandler(config.LANConfig{Access: config.LANAccessConfig{Allow: []string{"127.0.0.0/8"}}}, NewLANAPIKeysFromValues(lanTestKey), handler))
	defer lan.Close()

	response, err := local.Client().Get(local.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("local models status = %d", response.StatusCode)
	}
	response, err = local.Client().Get(local.URL + "/api/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("local status = %d", response.StatusCode)
	}
	lanResponse := lanRequest(t, lan.Client(), http.MethodGet, lan.URL+"/v1/models", lanTestKey, "")
	_ = lanResponse.Body.Close()
	if lanResponse.StatusCode != http.StatusOK {
		t.Fatalf("lan models status = %d", lanResponse.StatusCode)
	}
	lanResponse = lanRequest(t, lan.Client(), http.MethodGet, lan.URL+"/api/v1/status", lanTestKey, "")
	_ = lanResponse.Body.Close()
	if lanResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("lan status = %d, want 404", lanResponse.StatusCode)
	}
}

// TestLANShutdownIsClean proves both listeners close without leaking the
// backend or the scheduler.
func TestLANShutdownIsClean(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	handler, ok := NewHandler(manager, sched).(*Handler)
	if !ok {
		t.Fatal("local handler is not a *Handler")
	}
	lan := httptest.NewTLSServer(newLANHandler(config.LANConfig{Access: config.LANAccessConfig{Allow: []string{"127.0.0.0/8"}}}, NewLANAPIKeysFromValues(lanTestKey), handler))
	response, err := lan.Client().Get(lan.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	lan.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatalf("scheduler shutdown: %v", err)
	}
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("manager shutdown: %v", err)
	}
}

// TestLANHealthStillWorks proves the LARM-008.1 health route is preserved.
func TestLANHealthStillWorks(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodGet, server.URL+"/api/v1/lan/health", lanTestKey, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Fatalf("body = %s", body)
	}
}

// TestLANListenerWithoutProxyKeepsOnlyTheHealthRoute preserves the
// LARM-008.1 contract for a LAN handler built without the local proxy.
func TestLANListenerWithoutProxyKeepsOnlyTheHealthRoute(t *testing.T) {
	lan := newLANHandler(config.LANConfig{Access: config.LANAccessConfig{Allow: []string{"127.0.0.0/8"}}}, NewLANAPIKeysFromValues(lanTestKey), nil)
	server := httptest.NewTLSServer(lan)
	defer server.Close()
	response := lanRequest(t, server.Client(), http.MethodGet, server.URL+"/api/v1/lan/health", lanTestKey, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", response.StatusCode)
	}
	response = lanRequest(t, server.Client(), http.MethodGet, server.URL+"/v1/models", lanTestKey, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("models status = %d, want 404", response.StatusCode)
	}
}

// TestLANCredentialIsNotLogged proves the credential never appears in the
// response body of a rejected request.
func TestLANCredentialIsNotLogged(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodGet, server.URL+"/v1/models", "wrong-key", "")
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if strings.Contains(string(body), "wrong-key") || strings.Contains(string(body), lanTestKey) {
		t.Fatalf("the credential leaked into the response body: %s", body)
	}
}
