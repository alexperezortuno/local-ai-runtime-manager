package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

func TestMain(m *testing.M) {
	if os.Getenv("LARM_API_CHILD") == "1" {
		port := ""
		for i, arg := range os.Args {
			if arg == "--port" && i+1 < len(os.Args) {
				port = os.Args[i+1]
			}
		}
		server := &http.Server{Addr: "127.0.0.1:" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			switch os.Getenv("LARM_API_BACKEND_MODE") {
			case "error":
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":{"message":"simulated"}}`)
				return
			case "slow":
				select {
				case <-time.After(300 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
			}
			var request struct {
				Stream bool `json:"stream"`
			}
			_ = json.NewDecoder(r.Body).Decode(&request)
			if r.URL.Path == "/v1/embeddings" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"model":"test","usage":{"prompt_tokens":2,"total_tokens":2}}`)
				return
			}
			if r.URL.Path != "/v1/chat/completions" {
				http.NotFound(w, r)
				return
			}
			if request.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
				flusher, _ := w.(http.Flusher)
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n")
				flusher.Flush()
				if os.Getenv("LARM_API_BACKEND_MODE") == "hangstream" {
					<-r.Context().Done()
					return
				}
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\ndata: [DONE]\n\n")
				flusher.Flush()
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok","tool_calls":[{"id":"call_1","type":"function"}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
		})}
		go func() { _ = server.ListenAndServe() }()
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		<-signals
		_ = server.Close()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestLeaseHTTPFlowAndLegacyStatus(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	httpServer := httptest.NewServer(NewHandler(manager, sched))
	defer httpServer.Close()
	client := httpServer.Client()
	post := func(path, body string) *http.Response {
		t.Helper()
		response, err := client.Post(httpServer.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	firstResponse := post("/api/v1/leases", `{"model":"model-1"}`)
	if firstResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("POST lease status = %d", firstResponse.StatusCode)
	}
	var first scheduler.LeaseStatus
	if err := json.NewDecoder(firstResponse.Body).Decode(&first); err != nil {
		t.Fatal(err)
	}
	_ = firstResponse.Body.Close()
	if first.ID == "" {
		t.Fatal("missing lease_id")
	}
	waitHTTPLease(t, client, httpServer.URL, first.ID, scheduler.Active)

	secondResponse := post("/api/v1/leases", `{"model":"model-2"}`)
	if secondResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("second POST status = %d", secondResponse.StatusCode)
	}
	var second scheduler.LeaseStatus
	if err := json.NewDecoder(secondResponse.Body).Decode(&second); err != nil {
		t.Fatal(err)
	}
	_ = secondResponse.Body.Close()
	queueResponse, err := client.Get(httpServer.URL + "/api/v1/queue")
	if err != nil {
		t.Fatal(err)
	}
	var queue scheduler.Snapshot
	if err := json.NewDecoder(queueResponse.Body).Decode(&queue); err != nil {
		t.Fatal(err)
	}
	_ = queueResponse.Body.Close()
	if queue.Active == nil || queue.Active.ID != first.ID || len(queue.Pending) != 1 || queue.Pending[0].ID != second.ID {
		t.Fatalf("queue response = %+v", queue)
	}

	statusResponse, err := client.Get(httpServer.URL + "/api/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]json.RawMessage
	if err := json.NewDecoder(statusResponse.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	_ = statusResponse.Body.Close()
	if status["active_model"] == nil || status["scheduler"] == nil || status["queued_requests"] == nil || status["active_inferences"] == nil || status["completed_requests"] == nil || status["failed_requests"] == nil {
		t.Fatalf("status lost legacy/scheduler fields: %v", status)
	}

	manualResponse, err := client.Post(httpServer.URL+"/api/v1/models/model-2/start", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = manualResponse.Body.Close()
	if manualResponse.StatusCode != http.StatusConflict {
		t.Fatalf("manual start while lease active = %d", manualResponse.StatusCode)
	}

	releaseRequest, _ := http.NewRequest(http.MethodDelete, httpServer.URL+"/api/v1/leases/"+first.ID, nil)
	releaseResponse, err := client.Do(releaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = releaseResponse.Body.Close()
	if releaseResponse.StatusCode != http.StatusOK {
		t.Fatalf("DELETE lease status = %d", releaseResponse.StatusCode)
	}
	waitHTTPLease(t, client, httpServer.URL, second.ID, scheduler.Active)

	bad := post("/api/v1/leases", `{"model":"missing"}`)
	_ = bad.Body.Close()
	if bad.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown model status = %d", bad.StatusCode)
	}
}

func apiTestScheduler(t *testing.T) (*runtime.Manager, *scheduler.Scheduler) {
	return apiTestSchedulerWithTTL(t, "5s")
}

func apiTestSchedulerWithTTL(t *testing.T, ttl string) (*runtime.Manager, *scheduler.Scheduler) {
	t.Helper()
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	models := make(map[string]config.ModelConfig)
	for i := 1; i <= 2; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		models[fmt.Sprintf("model-%d", i)] = config.ModelConfig{Path: modelPath, Port: port, Context: 1024, Batch: 128, UBatch: 32, GPULayers: "all", Parallel: 1, Temperature: 0.2, TopP: 0.9}
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_API_CHILD", "1")
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: "2s", ShutdownTimeout: "1s"}, Models: models})
	ttlDuration, err := time.ParseDuration(ttl)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := ttlDuration / 5
	if heartbeat <= 0 {
		heartbeat = time.Millisecond
	}
	sched := scheduler.New(manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: ttl, Heartbeat: heartbeat.String(), MaxLease: "30s"})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = sched.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return manager, sched
}

func waitHTTPLease(t *testing.T, client *http.Client, base, id string, state scheduler.LeaseState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(base + "/api/v1/leases/" + id)
		if err == nil {
			var lease scheduler.LeaseStatus
			decodeErr := json.NewDecoder(response.Body).Decode(&lease)
			_ = response.Body.Close()
			if decodeErr == nil && lease.State == state {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lease %s did not reach %s", id, state)
}
