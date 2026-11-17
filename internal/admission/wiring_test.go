package admission

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

// wiringPlanSource is a deterministic read-only planner.
type wiringPlanSource struct {
	calls atomic.Int64
}

func (p *wiringPlanSource) Plan(context.Context, string) capacity.Result {
	p.calls.Add(1)
	return capacity.Result{Verdict: capacity.Fit, ModelID: "model-1"}
}

func wiringScheduler(t *testing.T) *scheduler.Scheduler {
	t.Helper()
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	manager := runtime.NewManager(config.Config{
		Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: "fixture", StartupTimeout: "2s", ShutdownTimeout: "1s"},
		Models:  map[string]config.ModelConfig{"model-1": {Path: modelPath, Port: 18099, Context: 1024, Batch: 128, UBatch: 32, GPULayers: "all", Parallel: 1, Temperature: 0.2, TopP: 0.9}},
	})
	sched := scheduler.New(manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: "5s", Heartbeat: "1s", MaxLease: "30s"})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = sched.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return sched
}

// The wiring installs the evaluator and the policy on the Scheduler.
func TestInstallInstallsEvaluatorAndPolicy(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &wiringPlanSource{}
	wiring, err := Install(sched, planner, scheduler.AdmissionOptions{OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: time.Second, ReevaluationInterval: 10 * time.Millisecond}, capacity.DefaultEvaluatorOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer wiring.Close()
	if wiring.adapter == nil || wiring.provider == nil || wiring.evaluator == nil {
		t.Fatal("the wiring did not build its dependencies")
	}
	// The evaluator is installed and usable.
	if _, err := wiring.adapter.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationNewLoad}); err != nil {
		t.Fatal(err)
	}
	if planner.calls.Load() == 0 {
		t.Fatal("the planner was never consulted")
	}
}

// The wiring rejects an invalid policy without touching the Scheduler.
func TestInstallRejectsInvalidPolicy(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &wiringPlanSource{}
	if _, err := Install(sched, planner, scheduler.AdmissionOptions{OnInsufficient: "invalid"}, capacity.DefaultEvaluatorOptions()); err == nil {
		t.Fatal("an invalid policy was accepted")
	}
}

// The wiring requires a planner and a Scheduler.
func TestInstallRequiresDependencies(t *testing.T) {
	sched := wiringScheduler(t)
	if _, err := Install(nil, &wiringPlanSource{}, scheduler.AdmissionOptions{}, capacity.DefaultEvaluatorOptions()); err == nil {
		t.Fatal("a nil Scheduler was accepted")
	}
	if _, err := Install(sched, nil, scheduler.AdmissionOptions{}, capacity.DefaultEvaluatorOptions()); err == nil {
		t.Fatal("a nil planner was accepted")
	}
}

// Closing the wiring releases the evaluator workers and is idempotent.
func TestWiringCloseReleasesWorkers(t *testing.T) {
	sched := wiringScheduler(t)
	wiring, err := Install(sched, &wiringPlanSource{}, scheduler.AdmissionOptions{}, capacity.DefaultEvaluatorOptions())
	if err != nil {
		t.Fatal(err)
	}
	wiring.Close()
	wiring.Close()
	// A closed evaluator is unavailable, never a FIT.
	evidence, err := wiring.adapter.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationNewLoad})
	if err == nil {
		t.Fatal("a closed evaluator still answered")
	}
	if evidence.Verdict == scheduler.AdmissionFit {
		t.Fatal("a closed evaluator reported FIT")
	}
}

// A closed wiring is safe to close again.
func TestWiringCloseOnNilIsSafe(t *testing.T) {
	var wiring *Wiring
	if err := wiring.Close(); err != nil {
		t.Fatal(err)
	}
}

// The configuration decides whether an evaluator is installed at all.
func TestInstallFromConfigRespectsEnabled(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &wiringPlanSource{}
	// Disabled: nothing is installed and no planner is consulted.
	wiring, err := InstallFromConfig(sched, nil, config.AdmissionConfig{Enabled: false, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "5m", ReevaluationInterval: "2s", MaxSnapshotAge: "6s"})
	if err != nil {
		t.Fatal(err)
	}
	if wiring != nil {
		t.Fatal("a disabled admission installed a wiring")
	}
	if planner.calls.Load() != 0 {
		t.Fatal("a disabled admission consulted the planner")
	}
	// Enabled: the evaluator is installed and the planner is consulted.
	wiring, err = InstallFromConfig(sched, planner, config.AdmissionConfig{Enabled: true, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "5m", ReevaluationInterval: "2s", MaxSnapshotAge: "6s"})
	if err != nil {
		t.Fatal(err)
	}
	if wiring == nil {
		t.Fatal("an enabled admission installed no wiring")
	}
	defer wiring.Close()
	if _, err := wiring.adapter.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationNewLoad}); err != nil {
		t.Fatal(err)
	}
	if planner.calls.Load() == 0 {
		t.Fatal("the planner was never consulted")
	}
}

// The wiring is installed exactly once: a second Install replaces the
// evaluator without leaving a duplicate worker behind.
func TestInstallIsNotDuplicated(t *testing.T) {
	sched := wiringScheduler(t)
	first, err := Install(sched, &wiringPlanSource{}, scheduler.AdmissionOptions{}, capacity.DefaultEvaluatorOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Install(sched, &wiringPlanSource{}, scheduler.AdmissionOptions{}, capacity.DefaultEvaluatorOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.evaluator == second.evaluator {
		t.Fatal("the second install reused the first evaluator")
	}
	// The first evaluator is closed with its wiring and reports unavailable.
	if _, err := first.adapter.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: scheduler.AdmissionOperationNewLoad}); err == nil {
		t.Fatal("the superseded evaluator still answered")
	}
}
