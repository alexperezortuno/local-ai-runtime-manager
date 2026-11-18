package admission

import (
	"context"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/memory"
	"local-ai-runtime/internal/scheduler"
)

// controlPlanner is a deterministic capacity planner controlled by tests. It
// preserves the three admission verdicts without touching real memory or a
// model file.
type controlPlanner struct {
	mu      sync.Mutex
	verdict capacity.Verdict
	cause   string
	calls   int
}

func (p *controlPlanner) Plan(context.Context, string) capacity.Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	available := uint64(1024)
	now := time.Now().UTC()
	return capacity.Result{
		Verdict:       p.verdict,
		Snapshot:      capacitySnapshot(available, now),
		Estimate:      capacity.Estimate{RequiredBytes: 100},
		Capacity:      capacity.Capacity{SystemAvailableBytes: &available},
		Justification: []string{"controlled integration fixture"},
	}
}

func capacitySnapshot(available uint64, now time.Time) memory.Snapshot {
	return memory.Snapshot{Status: memory.Available, MeasuredAt: &now, Source: "control-fixture", System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available, Source: "control-fixture"}}
}

// TestControlEnvironmentEnforcesExplicitly proves that the controlled
// integration environment has enforcement installed and the default remains
// disabled.
func TestControlEnvironmentEnforcesExplicitly(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &controlPlanner{verdict: capacity.Fit}
	wiring, err := InstallFromConfig(sched, planner, config.AdmissionConfig{Enabled: true, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "2s", ReevaluationInterval: "5ms", MaxSnapshotAge: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	defer wiring.Close()
	if wiring.evaluator == nil || wiring.adapter == nil {
		t.Fatal("enabled admission did not install the controlled evaluator")
	}
	evidence, err := wiring.adapter.Evaluate(context.Background(), scheduler.AdmissionRequest{ModelID: "model-1", RequestID: "control", LeaseID: "control-lease", Operation: scheduler.AdmissionOperationNewLoad})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Verdict != scheduler.AdmissionFit {
		t.Fatalf("verdict = %s, want FIT", evidence.Verdict)
	}
	if planner.calls != 1 {
		t.Fatalf("planner calls = %d, want 1", planner.calls)
	}

	disabledScheduler := wiringScheduler(t)
	disabled, err := InstallFromConfig(disabledScheduler, nil, config.AdmissionConfig{Enabled: false, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "5m", ReevaluationInterval: "2s", MaxSnapshotAge: "6s"})
	if err != nil {
		t.Fatal(err)
	}
	if disabled != nil {
		t.Fatal("disabled admission installed a wiring")
	}
}
