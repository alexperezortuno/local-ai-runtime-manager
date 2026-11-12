package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
)

func TestMain(m *testing.M) {
	if os.Getenv("LARM_TEST_CHILD") == "1" {
		runChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runChild() {
	mode := os.Getenv("LARM_TEST_MODE")
	port := os.Getenv("LARM_TEST_PORT")
	for i, arg := range os.Args {
		if arg == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
		}
	}
	if mode == "die" {
		time.Sleep(250 * time.Millisecond)
		os.Exit(7)
	}
	if mode == "never" {
		select {}
	}
	if mode == "delay" {
		time.Sleep(250 * time.Millisecond)
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		http.NotFound(w, r)
	})}
	go func() { _ = server.ListenAndServe() }()
	if mode == "crash" {
		time.Sleep(400 * time.Millisecond)
		os.Exit(7)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	if mode != "ignore" {
		_ = server.Close()
	}
	if mode == "ignore" {
		select {}
	}
}

func TestModelNotFound(t *testing.T) {
	m := testManager(t, "normal", 1, 100*time.Millisecond, 100*time.Millisecond)
	if err := m.Start(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Start missing model error = %v", err)
	}
}

func TestStartReadyIdempotentAndStop(t *testing.T) {
	m := testManager(t, "delay", 1, time.Second, 300*time.Millisecond)
	ctx := context.Background()
	if err := m.Start(ctx, "model-1"); err != nil {
		t.Fatal(err)
	}
	first := m.Status()
	if first.State != Ready || first.PID == 0 || first.ActiveModel != "model-1" {
		t.Fatalf("unexpected ready status: %+v", first)
	}
	if err := m.Start(ctx, "model-1"); err != nil {
		t.Fatalf("duplicate ready Start: %v", err)
	}
	if m.Status().PID != first.PID {
		t.Fatal("idempotent start replaced process")
	}
	if err := m.Stop(ctx, "model-1"); err != nil {
		t.Fatal(err)
	}
	if got := m.Status().State; got != Stopped {
		t.Fatalf("state after stop = %s", got)
	}
}

func TestReadinessTimeoutTerminatesChild(t *testing.T) {
	m := testManager(t, "never", 1, 150*time.Millisecond, 100*time.Millisecond)
	if err := m.Start(context.Background(), "model-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	if s := m.Status(); s.State != Failed || s.PID != 0 {
		t.Fatalf("unexpected status after timeout: %+v", s)
	}
}

func TestUnexpectedExitMarksFailed(t *testing.T) {
	m := testManager(t, "crash", 1, time.Second, 100*time.Millisecond)
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for m.Status().State != Failed && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.Status().State != Failed {
		t.Fatalf("unexpected exit did not mark FAILED: %+v", m.Status())
	}
}

func TestStopForcesProcessAfterTimeout(t *testing.T) {
	m := testManager(t, "ignore", 1, time.Second, 120*time.Millisecond)
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := m.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 100*time.Millisecond || m.Status().State != Stopped {
		t.Fatalf("process was not force-stopped after timeout: %v %+v", time.Since(start), m.Status())
	}
}

func TestSecondModelConflicts(t *testing.T) {
	m := testManager(t, "normal", 2, time.Second, 100*time.Millisecond)
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background(), "model-2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("second model Start error = %v", err)
	}
}

func TestConfiguredCapacityAllowsMultipleResidentModels(t *testing.T) {
	m := testManager(t, "normal", 2, time.Second, 100*time.Millisecond)
	m.SetMaxActiveModels(2)
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background(), "model-2"); err != nil {
		t.Fatal(err)
	}
	models := m.Models()
	if models["model-1"].PID == 0 || models["model-2"].PID == 0 || models["model-1"].ProcessInstanceID == models["model-2"].ProcessInstanceID {
		t.Fatalf("resident models were not independent: %+v", models)
	}
	if err := m.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(context.Background(), "model-2"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentStartsAreSerialized(t *testing.T) {
	m := testManager(t, "delay", 2, time.Second, 100*time.Millisecond)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"model-1", "model-2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			results <- m.Start(context.Background(), id)
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrConflict) {
			conflicted++
		} else {
			t.Fatalf("unexpected concurrent Start error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("successes=%d conflicts=%d", succeeded, conflicted)
	}
}

func TestCancelDuringStartingCleansUp(t *testing.T) {
	m := testManager(t, "never", 1, 5*time.Second, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.Start(ctx, "model-1") }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
	if s := m.Status(); s.PID != 0 || s.State != Failed {
		t.Fatalf("child was not reaped after cancellation: %+v", s)
	}
}

func TestShutdownStopsActiveModel(t *testing.T) {
	m := testManager(t, "normal", 1, time.Second, 100*time.Millisecond)
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if s := m.Status(); s.State != Stopped || s.PID != 0 {
		t.Fatalf("shutdown left a process active: %+v", s)
	}
}

func TestModelStatesAndMultipleConfiguredModels(t *testing.T) {
	m := testManager(t, "normal", 2, time.Second, 100*time.Millisecond)
	if len(m.Models()) != 2 {
		t.Fatalf("model list has %d entries", len(m.Models()))
	}
}

func testManager(t *testing.T, mode string, modelCount int, startup, shutdown time.Duration) *Manager {
	t.Helper()
	dir := t.TempDir()
	ports := make([]int, modelCount)
	listeners := make([]net.Listener, 0, modelCount)
	for i := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	port := ports[0]
	modelPath := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_TEST_CHILD", "1")
	t.Setenv("LARM_TEST_MODE", mode)
	t.Setenv("LARM_TEST_PORT", fmt.Sprint(port))
	configured := make(map[string]config.ModelConfig, modelCount)
	for i := 0; i < modelCount; i++ {
		id := fmt.Sprintf("model-%d", i+1)
		configured[id] = config.ModelConfig{Path: modelPath, Port: ports[i], Context: 4096, Batch: 512, UBatch: 128, GPULayers: "all", Parallel: 1, Temperature: 0.2, TopP: 0.9, TopK: 40}
	}
	cfg := config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: startup.String(), ShutdownTimeout: shutdown.String()}, Models: configured}
	m := NewManager(cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	return m
}
