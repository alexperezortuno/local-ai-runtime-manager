package recovery_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/inference"
	recoveryservice "local-ai-runtime/internal/recovery"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

func TestOpenRecoversOnlyAnEmptyDirtySchedulerStore(t *testing.T) {
	dir := t.TempDir()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: "/does/not/exist"}, Models: map[string]config.ModelConfig{
		"model-1": {Path: filepath.Join(dir, "model.gguf"), Port: 12345},
	}}, journal)
	store := filepath.Join(dir, "scheduler.json")
	if err := os.WriteFile(store, []byte(`{"version":1,"clean_shutdown":false,"records":{},"reconciliations":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	sched, err := scheduler.NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 2, LeaseTTL: "1m", Heartbeat: "1s", MaxLease: "2m"}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = sched.Shutdown(context.Background())
		_ = manager.Shutdown(context.Background())
	}()
	sched.SetReleaseProtector(registry)
	sched.SetAdmissionGate(registry)
	if _, err := recoveryservice.Open(registry, sched, filepath.Join(dir, "recovery.json"), manager); err != nil {
		t.Fatal(err)
	}
	diagnostics := sched.RecoveryDiagnostics()
	if diagnostics.RecoveryBlocked || !diagnostics.CleanShutdown {
		t.Fatalf("safe empty-store recovery did not complete: %+v", diagnostics)
	}
	data, err := os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	var persisted struct {
		CleanShutdown bool `json:"clean_shutdown"`
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if !persisted.CleanShutdown {
		t.Fatal("scheduler recovery was not durably recorded")
	}
	lease, err := sched.Request("model-1")
	if err != nil || lease.ID == "" {
		t.Fatalf("lease after recovery = %+v, err=%v", lease, err)
	}
}
