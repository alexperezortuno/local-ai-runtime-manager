package scheduler

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	runtime "local-ai-runtime/internal/runtime"
)

func TestKeepAliveReusesProcessInstanceAfterRelease(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
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

func TestPreloadKeepAliveStartsWithoutLease(t *testing.T) {
	m, s, journal := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].State == runtime.Ready })
	status := m.Models()["model-1"]
	if status.PID == 0 || status.ProcessInstanceID == "" || status.Residency != "READY_PERSISTENT" {
		t.Fatalf("keep_alive was not preloaded: %+v", status)
	}
	if got := s.Leases(); len(got) != 0 {
		t.Fatalf("preload created leases: %+v", got)
	}
	if len(journal.ProcessInstances()) != 1 {
		t.Fatalf("preload created unexpected process instances: %+v", journal.ProcessInstances())
	}
}

func TestPreloadTimeAliveStartsTimerAndDoesNotReload(t *testing.T) {
	m, s, journal := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {TimeAlive: "80ms"}})
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].State == runtime.Ready })
	first := m.Models()["model-1"]
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
	count := len(journal.ProcessInstances())
	if got := s.ModelsSnapshot()["model-1"].PreloadState; got != PreloadExpired {
		t.Fatalf("time_alive preload state = %q, want %q", got, PreloadExpired)
	}
	if first.ProcessInstanceID == "" || count != 1 {
		t.Fatalf("unexpected timed preload evidence: first=%+v instances=%d", first, count)
	}
	waitFor(t, func() bool { return len(journal.ProcessInstances()) == count && m.Models()["model-1"].PID == 0 })
}

func TestPreloadSkipsModelsWithoutResidency(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {}})
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.ModelsSnapshot()["model-1"].PreloadState == PreloadNotRequired })
	if status := m.Models()["model-1"]; status.PID != 0 {
		t.Fatalf("on-demand model was preloaded: %+v", status)
	}
}

func TestPreloadKeepAliveWinsAndCapacityIsStrict(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{
		"a-persistent": {KeepAlive: true, TimeAlive: "80ms"},
		"b-temporary":  {TimeAlive: "80ms"},
	})
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		models := s.ModelsSnapshot()
		return models["a-persistent"].State == runtime.Ready && models["b-temporary"].PreloadState == PreloadSkippedCapacity
	})
	if status := m.Models()["a-persistent"]; status.Residency != "READY_PERSISTENT" {
		t.Fatalf("keep_alive did not win policy precedence: %+v", status)
	}
	if status := m.Models()["b-temporary"]; status.PID != 0 {
		t.Fatalf("capacity was exceeded by preload: %+v", status)
	}
}

func TestPreloadAndSameModelAcquisitionShareOneInstance(t *testing.T) {
	m, s, journal := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if got := m.Models()["model-1"]; got.PID == 0 || len(journal.ProcessInstances()) != 1 {
		t.Fatalf("preload/acquisition did not share one process: status=%+v instances=%d", got, len(journal.ProcessInstances()))
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
}

func TestPreloadBlockedByRecoveryDoesNotStartProcess(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	s.SetAdmissionGate(blockedAdmissionGate{})
	if !errors.Is(s.Preload(), ErrIdempotencyRecovery) {
		t.Fatal("preload was admitted while recovery was blocked")
	}
	waitFor(t, func() bool { return s.ModelsSnapshot()["model-1"].PreloadState == PreloadBlockedRecovery })
	if status := m.Models()["model-1"]; status.PID != 0 {
		t.Fatalf("recovery-blocked preload started a process: %+v", status)
	}
}

type blockedAdmissionGate struct{}

func (blockedAdmissionGate) AdmissionBlocked() bool { return true }

func TestAdministrativeStopDuringPreloadSuppressesReload(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	t.Setenv("LARM_SCHEDULER_MODE", "delay")
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.preloadingID == "model-1"
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.ManualStop(ctx, "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
	if got := s.ModelsSnapshot()["model-1"].PreloadState; got != PreloadAdminStopped {
		t.Fatalf("stop during preload state = %q, want %q", got, PreloadAdminStopped)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
}

func TestShutdownDuringPreloadCancelsAndLeavesNoProcess(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	t.Setenv("LARM_SCHEDULER_MODE", "delay")
	if err := s.Preload(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.preloadingID == "model-1"
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if status := m.Models()["model-1"]; status.PID != 0 {
		t.Fatalf("shutdown left preload process: %+v", status)
	}
}

func TestTimeAliveExpiresOnlyAfterLeaseRelease(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {TimeAlive: "80ms"}})
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
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{
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
	failed := waitLease(t, s, second.ID, Failed)
	if !strings.Contains(failed.Error, "model capacity exhausted: 1/1 resident slots occupied") {
		t.Fatalf("capacity error = %q", failed.Error)
	}
	if m.Models()["model-1"].PID == 0 {
		t.Fatal("persistent model was evicted for capacity")
	}
}

func TestAdministrativeStopDownloadsPersistentModelAndAllowsReload(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	first := m.Models()["model-1"]
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == first.PID })
	if err := s.ManualStop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
	if err := s.ManualStop(context.Background(), "model-1"); !errors.Is(err, ErrAlreadyStopped) {
		t.Fatalf("second stop = %v", err)
	}
	second, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, second.ID, Active)
	if got := m.Models()["model-1"]; got.ProcessInstanceID == first.ProcessInstanceID {
		t.Fatalf("reload reused stopped process: first=%+v second=%+v", first, got)
	}
}

func TestAdministrativeStopRejectsProtectedInference(t *testing.T) {
	_, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	releaseInference, err := s.BeginInference(lease.ID, "model-1", func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseInference()
	if err := s.ManualStop(context.Background(), "model-1"); !errors.Is(err, ErrInferenceProtected) {
		t.Fatalf("protected stop = %v", err)
	}
}

func TestResidenceExpiryAndAcquisitionRaceKeepsRuntimeAndSchedulerConsistent(t *testing.T) {
	m, s, _ := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {TimeAlive: "30ms"}})
	first, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Active)
	if _, err := s.Release(first.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })

	const callers = 6
	start := make(chan struct{})
	leases := make([]Lease, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range leases {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			leases[i], errs[i] = s.Request("model-1")
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	completed := 0
	for completed < callers {
		var active Lease
		waitFor(t, func() bool {
			snapshot := s.Snapshot()
			if snapshot.Active == nil {
				return false
			}
			active = *snapshot.Active
			return true
		})
		if _, err := s.Release(active.ID); err != nil {
			t.Fatalf("release of FIFO lease %s failed: %v", active.ID, err)
		}
		completed++
	}
	for i, lease := range leases {
		if got := waitLease(t, s, lease.ID, Completed); got.State != Completed {
			t.Fatalf("lease %d did not complete: %+v", i, got)
		}
	}
	waitFor(t, func() bool {
		status := m.Models()["model-1"]
		return status.PID == 0 && m.Status().State == runtime.Stopped
	})
	s.mu.Lock()
	timers, due := len(s.residenceTimers), len(s.residenceDue)
	s.mu.Unlock()
	if timers != 0 || due != 0 {
		t.Fatalf("orphaned residence state: timers=%d due=%d", timers, due)
	}
}

func TestStopAndNewRequestSerializeAcrossStopping(t *testing.T) {
	m, s, journal := residencyScheduler(t, 1, map[string]config.ModelConfig{"model-1": {KeepAlive: true}})
	// The child and journal event make the lifecycle boundary deterministic without a production hook.
	t.Setenv("LARM_SCHEDULER_MODE", "ignore")
	if err := s.ManualStart(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	first := m.Models()["model-1"]
	stopResult := make(chan error, 1)
	go func() { stopResult <- s.ManualStop(context.Background(), "model-1") }()
	select {
	case instance := <-journal.terminationRequested:
		if instance.ModelID != "model-1" {
			t.Fatalf("termination event model = %q", instance.ModelID)
		}
		s.mu.Lock()
		busy := s.manualBusy
		s.mu.Unlock()
		if !busy {
			t.Fatal("manual stop was not busy at the published stopping boundary")
		}
		if status := m.Status(); status.State != runtime.Stopping {
			t.Fatalf("runtime state at stopping boundary = %s, want %s", status.State, runtime.Stopping)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("termination boundary was not published before timeout")
	}
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := <-stopResult; err != nil && !errors.Is(err, runtime.ErrConflict) {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	second := m.Models()["model-1"]
	if second.PID == 0 || second.ProcessInstanceID == "" {
		t.Fatalf("request did not obtain a verified process: %+v", second)
	}
	if second.ProcessInstanceID == first.ProcessInstanceID {
		t.Fatalf("request reused process across STOPPING: first=%+v second=%+v", first, second)
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ManualStop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Models()["model-1"].PID == 0 })
}

type residencyJournal struct {
	mu                   sync.Mutex
	owner                string
	instances            map[string]corecontract.ProcessInstance
	terminationRequested chan corecontract.ProcessInstance
	terminationOnce      sync.Once
}

func (j *residencyJournal) PersistProcess(instance corecontract.ProcessInstance) error {
	j.mu.Lock()
	j.instances[instance.ProcessInstanceID] = instance
	j.mu.Unlock()
	if instance.State == corecontract.ProcessTerminationRequested {
		j.terminationOnce.Do(func() { j.terminationRequested <- instance })
	}
	return nil
}

func (j *residencyJournal) ProcessInstances() []corecontract.ProcessInstance {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]corecontract.ProcessInstance, 0, len(j.instances))
	for _, instance := range j.instances {
		result = append(result, instance)
	}
	return result
}

func (j *residencyJournal) OwnerInstanceID() string { return j.owner }

func residencyScheduler(t *testing.T, capacity int, policies map[string]config.ModelConfig) (*runtime.Manager, *Scheduler, *residencyJournal) {
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
	owner, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	journal := &residencyJournal{owner: owner, instances: make(map[string]corecontract.ProcessInstance), terminationRequested: make(chan corecontract.ProcessInstance, 1)}
	manager := runtime.NewManager(config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: "1s", ShutdownTimeout: "1s"}, Models: configured}, journal)
	scheduler, err := NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxActiveModels: capacity, MaxQueueSize: 8, LeaseTTL: "10s", Heartbeat: "1s", MaxLease: "1m"}, filepath.Join(dir, "leases.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = scheduler.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return manager, scheduler, journal
}
