package scheduler

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
)

func TestKeepAliveReusesProcessInstanceAfterRelease(t *testing.T) {
	m, s := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	first := m.Models()["model-1"]
	if first.PID == 0 || first.ProcessInstanceID == "" {
		t.Fatalf("model was not ready: %+v", first)
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == first.PID })
	second, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, second.ID, Active)
	if got := m.Models()["model-1"]; got.PID != first.PID || got.ProcessInstanceID != first.ProcessInstanceID {
		t.Fatalf("keep_alive restarted resident process: first=%+v second=%+v", first, got)
	}
}

func TestTimeAliveExpiresOnlyAfterLeaseRelease(t *testing.T) {
	m, s := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {TimeAlive: "80ms"}})
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	pid := m.Models()["model-1"].PID
	time.Sleep(40 * time.Millisecond)
	if m.Models()["model-1"].PID != pid {
		t.Fatal("time_alive expired while the lease was active")
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
}

func TestPersistentCapacityIsStrict(t *testing.T) {
	m, s := residencyScheduler(t, 1, map[string]config.ModelConfig{
		"model-1": {KeepAlive: true},
		"model-2": {},
	})
	first, _ := s.Request("model-1")
	waitLease(t, s, first.ID, Active)
	_, _ = s.Release(first.ID)
	second, err := s.Request("model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, second.ID, Failed)
	if m.Models()["model-1"].PID == 0 {
		t.Fatal("persistent model was evicted for capacity")
	}
}

func residencyScheduler(t *testing.T, capacity int, policies map[string]config.ModelConfig) (*runtime.Manager, *Scheduler) {
	t.Helper()
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_SCHEDULER_CHILD", "1")
	t.Setenv("LARM_SCHEDULER_MODE", "normal")
	modelPath := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	configured := make(map[string]config.ModelConfig, len(policies))
	for id, policy := range policies {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		policy.Path, policy.Port = modelPath, port
		policy.Context, policy.Batch, policy.UBatch, policy.GPULayers, policy.Parallel = 4096, 512, 128, "all", 1
		policy.Temperature, policy.TopP, policy.TopK = 0.2, 0.9, 40
		configured[id] = policy
	}
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: "1s", ShutdownTimeout: "1s"}, Models: configured})
	scheduler, err := NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxActiveModels: capacity, MaxQueueSize: 8, LeaseTTL: "1s", Heartbeat: "100ms", MaxLease: "10s"}, filepath.Join(dir, "leases.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = scheduler.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return manager, scheduler
}
