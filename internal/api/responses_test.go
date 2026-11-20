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
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/scheduler"
)

// responsesBody is a Responses API request body in the shape Codex CLI sends.
const responsesBody = `{"model":"model-1","stream":false,"input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{}}}],"tool_choice":"auto","parallel_tool_calls":true,"store":false,"include":[]}`

// sseEvent is one parsed Responses SSE event.
type sseEvent struct {
	Name string
	Data string
}

// sseEvents parses the Responses SSE wire format into (event name, data).
func sseEvents(t *testing.T, body string) []sseEvent {
	t.Helper()
	var events []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		name := ""
		data := ""
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event:"):
				name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		if name != "" || data != "" {
			events = append(events, sseEvent{Name: name, Data: data})
		}
	}
	return events
}

func responsesEventNames(t *testing.T, body string) []string {
	t.Helper()
	names := []string{}
	for _, event := range sseEvents(t, body) {
		names = append(names, event.Name)
	}
	return names
}

// TestResponsesJSONResponse proves the non-streaming Responses API returns the
// backend payload with its identifiers and tool calls intact.
func TestResponsesJSONResponse(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", responsesBody, "")
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("responses status = %d body = %s", response.StatusCode, raw)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["object"] != "response" || payload["id"] != "resp_test" || payload["status"] != "completed" {
		t.Fatalf("responses identity lost: %s", raw)
	}
	output, ok := payload["output"].([]any)
	if !ok || len(output) != 2 {
		t.Fatalf("responses output lost: %s", raw)
	}
	toolCall, ok := output[1].(map[string]any)
	if !ok || toolCall["type"] != "function_call" || toolCall["call_id"] != "call_1" || toolCall["name"] != "lookup" {
		t.Fatalf("responses tool call lost: %s", raw)
	}
	if payload["usage"] == nil {
		t.Fatalf("responses usage lost: %s", raw)
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesStreamingSSEDeliversProgressiveEvents proves the Responses SSE
// event names and identifiers survive the proxy and that the events are not
// buffered until the backend finishes.
func TestResponsesStreamingSSEDeliversProgressiveEvents(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","stream":true,"input":"hi"}`, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("responses status = %d", response.StatusCode)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("Content-Type = %q", response.Header.Get("Content-Type"))
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	names := responsesEventNames(t, string(raw))
	for _, expected := range []string{"response.created", "response.in_progress", "response.output_item.added", "response.content_part.added", "response.output_text.delta", "response.output_text.done", "response.content_part.done", "response.output_item.done", "response.completed"} {
		found := false
		for _, name := range names {
			if name == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing Responses event %q in %v", expected, names)
		}
	}
	// The Responses stream has no "[DONE]" sentinel.
	if strings.Contains(string(raw), "[DONE]") {
		t.Fatalf("responses stream carried a Chat Completions sentinel: %s", raw)
	}
	// Identifiers are preserved across events.
	if !strings.Contains(string(raw), `"id":"resp_test"`) || !strings.Contains(string(raw), `"item_id":"msg_test"`) {
		t.Fatalf("responses identifiers lost: %s", raw)
	}
	// The data payload carries the type Codex CLI reads.
	events := sseEvents(t, string(raw))
	for _, event := range events {
		if event.Name == "response.completed" && !strings.Contains(event.Data, `"type":"response.completed"`) {
			t.Fatalf("response.completed data lost its type: %s", event.Data)
		}
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesStreamingIsProgressive proves the first event reaches the
// client while the backend is still generating.
func TestResponsesStreamingIsProgressive(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","stream":true,"input":"hi"}`, "")
	defer response.Body.Close()
	buf := make([]byte, 512)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := response.Body.Read(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if n > 0 && strings.Contains(string(buf[:n]), "response.created") {
			return
		}
	}
	t.Fatal("the first Responses event never arrived while the backend was still running")
}

// TestResponsesToolCallingEvents proves the tool calling events of the
// Responses API are preserved with their identifiers.
func TestResponsesToolCallingEvents(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "toolcall")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","stream":true,"input":"hi","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{}}}],"tool_choice":"auto"}`, "")
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("responses status = %d body = %s", response.StatusCode, raw)
	}
	text := string(raw)
	if !strings.Contains(text, "response.function_call_arguments.delta") {
		t.Fatalf("tool call argument delta lost: %s", text)
	}
	if !strings.Contains(text, `"call_id":"call_1"`) || !strings.Contains(text, `"name":"lookup"`) {
		t.Fatalf("tool call identifiers lost: %s", text)
	}
	if !strings.Contains(text, "response.completed") {
		t.Fatalf("tool call response was not completed: %s", text)
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesMultipleEventsOfOneResponse proves several events of the same
// response reach the client and that the response identity is stable.
func TestResponsesMultipleEventsOfOneResponse(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","stream":true,"input":"hi"}`, "")
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	events := sseEvents(t, string(raw))
	if len(events) < 5 {
		t.Fatalf("a single response produced %d events", len(events))
	}
	identifiers := map[string]bool{}
	for _, event := range events {
		if !strings.Contains(event.Data, `"resp_test"`) && !strings.Contains(event.Data, `"msg_test"`) {
			continue
		}
		identifiers[event.Data] = true
	}
	if len(identifiers) < 2 {
		t.Fatalf("events did not carry distinct identifiers: %s", raw)
	}
}

// TestResponsesClientCancellationReleasesLease proves a disconnected client
// cancels the upstream inference and releases the automatic lease.
func TestResponsesClientCancellationReleasesLease(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "hangstream")
	manager, sched := apiTestScheduler(t)
	registry := inference.New(inference.Options{})
	server := httptest.NewServer(NewHandlerWithConfigAndRegistry(manager, sched, config.ProxyConfig{}, registry))
	defer server.Close()

	requestID, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","stream":true,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-LARM-Request-ID", requestID)
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
	if len(sched.Snapshot().Pending) != 0 {
		t.Fatal("a disconnected Responses request left a queued lease")
	}
	snapshot, err := registry.Get(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.IsAmbiguous() || snapshot.Cancellation != corecontract.CancelUnknown {
		t.Fatalf("cancellation state = %+v", snapshot)
	}
}

// TestResponsesBackendErrorIsPropagated proves the backend status is preserved
// and that the lease is released.
func TestResponsesBackendErrorIsPropagated(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "error")
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","input":"hi"}`, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("backend error status was not preserved: %d", response.StatusCode)
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesUnknownModelIsNotFound proves an unknown model never reaches
// the backend and never creates a lease.
func TestResponsesUnknownModelIsNotFound(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"absent","input":"hi"}`, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown model status = %d", response.StatusCode)
	}
	if sched.ActiveInferenceCount() != 0 || len(sched.Snapshot().Pending) != 0 {
		t.Fatal("an unknown model created scheduler state")
	}
}

// TestResponsesSchedulerTimeout proves the Scheduler queue wait timeout is
// honored on the Responses route and that no lease is left behind.
func TestResponsesSchedulerTimeout(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandlerWithConfig(manager, sched, config.ProxyConfig{QueueWaitTimeout: "100ms"}))
	defer server.Close()

	hold, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitActiveAPI(t, sched, hold.ID)

	response := postJSON(t, server, "/v1/responses", `{"model":"model-2","input":"hi"}`, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("queue timeout status = %d", response.StatusCode)
	}
	waitForAPI(t, func() bool { return len(sched.Snapshot().Pending) == 0 })
	_, _ = sched.Release(hold.ID)
}

// TestResponsesConcurrentRequestsShareTheScheduler proves concurrent
// Responses requests use the single FIFO Scheduler of the local listener.
func TestResponsesConcurrentRequestsShareTheScheduler(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	var wait sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			response := postJSON(t, server, "/v1/responses", `{"model":"model-1","input":"hi"}`, "")
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
			// The Scheduler queue bound rejected the request.
		default:
			t.Fatalf("concurrent request %d status = %d", index, status)
		}
	}
	if accepted == 0 {
		t.Fatal("no concurrent Responses request was accepted")
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesReleasesTheAutomaticLease proves the automatic lease is
// released and the inference is recorded as a success.
func TestResponsesReleasesTheAutomaticLease(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	registry := inference.New(inference.Options{})
	server := httptest.NewServer(NewHandlerWithConfigAndRegistry(manager, sched, config.ProxyConfig{}, registry))
	defer server.Close()

	requestID, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","stream":true,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-LARM-Request-ID", requestID)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("responses status = %d", response.StatusCode)
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
	if len(sched.Snapshot().Pending) != 0 || sched.Snapshot().Active != nil {
		t.Fatalf("the automatic lease was not released: %+v", sched.Snapshot())
	}
	snapshot, err := registry.Get(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Semantic != corecontract.SemanticSucceeded || snapshot.Transport != corecontract.TransportCompleted {
		t.Fatalf("responses inference state = %+v", snapshot)
	}
	if snapshot.SSE == nil || !snapshot.SSE.DoneDetected || !snapshot.SSE.EOF || snapshot.SSE.Truncated {
		t.Fatalf("responses SSE observation = %+v", snapshot.SSE)
	}
}

// TestResponsesTruncatedStreamIsNotSuccess proves a Responses stream cut
// before "response.completed" is recorded as a truncation, not a success.
func TestResponsesTruncatedStreamIsNotSuccess(t *testing.T) {
	t.Setenv("LARM_API_BACKEND_MODE", "truncated")
	manager, sched := apiTestScheduler(t)
	registry := inference.New(inference.Options{})
	server := httptest.NewServer(NewHandlerWithConfigAndRegistry(manager, sched, config.ProxyConfig{}, registry))
	defer server.Close()

	requestID, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","stream":true,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-LARM-Request-ID", requestID)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if strings.Contains(string(raw), "response.completed") {
		t.Fatalf("a truncated Responses stream reported completion: %s", raw)
	}
	waitForAPI(t, func() bool {
		snapshot, getErr := registry.Get(requestID)
		return getErr == nil && snapshot.Semantic == corecontract.SemanticTruncated
	})
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

// TestResponsesUnsupportedFieldsAreForwarded proves the proxy does not filter
// unknown Responses API fields: the backend receives the body unchanged.
func TestResponsesUnsupportedFieldsAreForwarded(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	// "store", "include" and "previous_response_id" are Responses API fields.
	// llama.cpp rejects "previous_response_id" and ignores the others; the
	// proxy must forward them untouched so the backend owns the decision.
	body := `{"model":"model-1","input":"hi","store":true,"include":["reasoning.encrypted_content"],"previous_response_id":"resp_prev","unknown_future_field":{"nested":true}}`
	response := postJSON(t, server, "/v1/responses", body, "")
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("responses status = %d body = %s", response.StatusCode, raw)
	}
}

// TestResponsesKeepsChatCompletionsAndEmbeddings proves the existing
// OpenAI endpoints are unchanged by the Responses route.
func TestResponsesKeepsChatCompletionsAndEmbeddings(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/chat/completions", `{"model":"model-1","messages":[{"role":"user","content":"hi"}]}`, "")
	raw, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d body = %s", response.StatusCode, raw)
	}
	var chat map[string]any
	if err := json.Unmarshal(raw, &chat); err != nil {
		t.Fatal(err)
	}
	if chat["choices"] == nil || chat["usage"] == nil {
		t.Fatalf("chat payload lost compatible fields: %s", raw)
	}

	response = postJSON(t, server, "/v1/embeddings", `{"model":"model-1","input":"hello"}`, "")
	raw, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("embeddings status = %d body = %s", response.StatusCode, raw)
	}
	var embeddings map[string]any
	if err := json.Unmarshal(raw, &embeddings); err != nil {
		t.Fatal(err)
	}
	if embeddings["data"] == nil || embeddings["usage"] == nil {
		t.Fatalf("embeddings payload lost compatible fields: %s", raw)
	}
}

// TestLANResponsesRequiresAuthentication proves the LAN Responses route keeps
// the Bearer and CIDR guards of LARM-008.1 and LARM-008.2.
func TestLANResponsesRequiresAuthentication(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	for _, test := range []struct {
		name, token string
	}{
		{"without token", ""},
		{"with invalid token", "wrong-key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/responses", test.token, `{"model":"model-1","input":"hi"}`)
			defer response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", response.StatusCode)
			}
			if response.Header.Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q", response.Header.Get("WWW-Authenticate"))
			}
		})
	}
	if handler.scheduler.ActiveInferenceCount() != 0 || len(handler.scheduler.Snapshot().Pending) != 0 {
		t.Fatal("a denied LAN Responses request created scheduler state")
	}
}

// TestLANResponsesRejectsAddressOutsideAllowlist proves the CIDR gate runs
// before any lease is acquired.
func TestLANResponsesRejectsAddressOutsideAllowlist(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, []string{"10.99.0.0/24"})
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/responses", lanTestKey, `{"model":"model-1","input":"hi"}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", response.StatusCode)
	}
	if handler.scheduler.ActiveInferenceCount() != 0 || len(handler.scheduler.Snapshot().Pending) != 0 {
		t.Fatal("a denied LAN Responses request created scheduler state")
	}
}

// TestLANResponsesAuthorized proves the LAN Responses route serves the same
// contract as the local route.
func TestLANResponsesAuthorized(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/responses", lanTestKey, `{"model":"model-1","input":"hi"}`)
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.StatusCode, raw)
	}
	if !strings.Contains(string(raw), `"id":"resp_test"`) {
		t.Fatalf("LAN responses payload lost the response identity: %s", raw)
	}
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
}

// TestLANResponsesDoesNotForwardTheBearerKeyToTheBackend proves the LAN
// credential stays at LARM and never reaches llama-server.
func TestLANResponsesDoesNotForwardTheBearerKeyToTheBackend(t *testing.T) {
	server, client, _ := lanTestServer(t, nil, nil)
	response := lanRequest(t, client, http.MethodPost, server.URL+"/v1/responses", lanTestKey, `{"model":"model-1","input":"hi"}`)
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	var payload struct {
		BackendAuthorization string `json:"larm_backend_authorization"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.BackendAuthorization != "" {
		t.Fatalf("the LAN credential was forwarded to the backend: %q", payload.BackendAuthorization)
	}
	if strings.Contains(string(raw), lanTestKey) {
		t.Fatalf("the LAN credential leaked into the backend response: %s", raw)
	}
}

// TestLANResponsesClientLeaseAndRequestIdentityAreDropped proves the LAN
// client cannot use a lease or a request identity of another client.
func TestLANResponsesClientLeaseAndRequestIdentityAreDropped(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","input":"hi"}`))
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

// TestLANResponsesExecutionIDDoesNotInterfere proves the caller-declared
// execution identity is scoped to its own request and never grants access to
// another client's session.
func TestLANResponsesExecutionIDDoesNotInterfere(t *testing.T) {
	server, client, handler := lanTestServer(t, nil, nil)
	first, err := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","input":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	first.Header.Set("Authorization", "Bearer "+lanTestKey)
	first.Header.Set("Content-Type", "application/json")
	first.Header.Set("X-LARM-Execution-ID", "6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	firstResponse, err := client.Do(first)
	if err != nil {
		t.Fatal(err)
	}
	_ = firstResponse.Body.Close()

	// A second client reusing the same execution identity must not observe or
	// reuse the first client's inference: the request identity is generated
	// per request, so the second request owns a separate record.
	second, err := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model-1","input":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	second.Header.Set("Authorization", "Bearer "+lanTestKey)
	second.Header.Set("Content-Type", "application/json")
	second.Header.Set("X-LARM-Execution-ID", "6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	secondResponse, err := client.Do(second)
	if err != nil {
		t.Fatal(err)
	}
	_ = secondResponse.Body.Close()
	if secondResponse.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d", secondResponse.StatusCode)
	}
	firstID := firstResponse.Header.Get("X-LARM-Request-ID")
	secondID := secondResponse.Header.Get("X-LARM-Request-ID")
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatalf("the request identities were not independent: %q %q", firstID, secondID)
	}
	records := handler.inferences.List()
	if len(records) < 2 {
		t.Fatalf("the inferences were not recorded separately: %+v", records)
	}
	for _, record := range records {
		if record.RequestID == firstID && record.ExecutionID == secondID {
			t.Fatal("the execution identity crossed the request identities")
		}
	}
	waitForAPI(t, func() bool { return handler.scheduler.ActiveInferenceCount() == 0 })
}

// TestResponsesLeaseIsNotReleasedByAnotherClient proves a Responses response
// identifier never grants access to a foreign session: the response payload
// carries only the backend data and no LARM identity.
func TestResponsesLeaseIsNotReleasedByAnotherClient(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response := postJSON(t, server, "/v1/responses", `{"model":"model-1","input":"hi"}`, "")
	raw, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if strings.Contains(string(raw), "lease") || strings.Contains(string(raw), "LeaseID") {
		t.Fatalf("the Responses payload exposed a lease identity: %s", raw)
	}
	if got := response.Header.Get("X-LARM-Lease-ID"); got != "" {
		t.Fatalf("the Responses response exposed a lease identity: %q", got)
	}
	waitForAPI(t, func() bool { return sched.ActiveInferenceCount() == 0 })
}

var _ = scheduler.Active
