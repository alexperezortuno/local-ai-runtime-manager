package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
)

func TestMain(m *testing.M) {
	if os.Getenv("LARM_SCHEDULER_CHILD") == "1" {
		port := ""
		for i, arg := range os.Args {
			if arg == "--port" && i+1 < len(os.Args) {
				port = os.Args[i+1]
			}
		}
		switch os.Getenv("LARM_SCHEDULER_MODE") {
		case "die":
			time.Sleep(80 * time.Millisecond)
			os.Exit(9)
		case "crash":
			serveFakeServer(port)
			time.Sleep(300 * time.Millisecond)
			logProcessEvent("STOP", port)
			os.Exit(9)
		case "delay":
			time.Sleep(250 * time.Millisecond)
			startFakeServer(port)
			os.Exit(0)
		default:
			startFakeServer(port)
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

func startFakeServer(port string) {
	serveFakeServer(port)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	logProcessEvent("STOP", port)
}

func serveFakeServer(port string) {
	logProcessEvent("START", port)
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	})}
	go func() { _ = server.ListenAndServe() }()
}

func logProcessEvent(event, port string) {
	path := os.Getenv("LARM_SCHEDULER_EVENTS")
	if path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%s %s\n", event, port)
			_ = f.Close()
		}
	}
}

func TestLeaseActivationQueueSwitchAndModelReuse(t *testing.T) {
	m, s := testScheduler(t, "normal", 2, time.Second, time.Second, 10, time.Second, time.Second)
	first, _ := s.Request("model-1")
	waitLease(t, s, first.ID, Active)
	firstPID := m.Status().PID
	second, _ := s.Request("model-2")
	third, _ := s.Request("model-1")
	if got, _ := s.Get(second.ID); got.QueuePosition != 1 {
		t.Fatalf("position of second = %d", got.QueuePosition)
	}
	if got, _ := s.Get(third.ID); got.QueuePosition != 2 {
		t.Fatalf("position of third = %d", got.QueuePosition)
	}
	_, _ = s.Release(first.ID)
	waitLease(t, s, second.ID, Active)
	if m.Status().ActiveModel != "model-2" || m.Status().PID == firstPID {
		t.Fatal("model switch did not replace process")
	}
	secondPID := m.Status().PID
	_, _ = s.Release(second.ID)
	waitLease(t, s, third.ID, Active)
	if m.Status().PID == secondPID {
		t.Fatal("switch back did not replace process")
	}
	_, _ = s.Release(third.ID)
	waitFor(t, func() bool { return m.Status().PID == 0 })
}

func TestQueuedSameModelReusesResidentProcess(t *testing.T) {
	m, s := testScheduler(t, "normal", 1, time.Second, time.Second, 10, time.Second, time.Second)
	if err := s.ManualStart(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	pid := m.Status().PID
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if m.Status().PID != pid {
		t.Fatal("same model was restarted instead of reused")
	}
	if err := s.ManualStop(context.Background(), "model-1"); err == nil {
		t.Fatal("manual stop bypassed active lease")
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Status().PID == 0 })
}

func TestQueueLimitAndPendingCancellation(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 1, time.Second, time.Second)
	active, _ := s.Request("model-1")
	waitLease(t, s, active.ID, Active)
	pending, _ := s.Request("model-1")
	if _, err := s.Request("model-1"); err != ErrQueueFull {
		t.Fatalf("full queue error = %v", err)
	}
	if _, err := s.Release(pending.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(pending.ID); got.State != Cancelled {
		t.Fatalf("cancelled state = %s", got.State)
	}
	_, _ = s.Release(active.ID)
}

func TestCancelDuringModelStartupCleansUp(t *testing.T) {
	m, s := testScheduler(t, "delay", 1, 2*time.Second, time.Second, 2, 5*time.Second, time.Second)
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Preparing)
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	waitFor(t, func() bool { return m.Status().PID == 0 })
}

func TestReleaseIsIdempotentAndHeartbeatRenews(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, 400*time.Millisecond, 30*time.Millisecond)
	lease, _ := s.Request("model-1")
	waitLease(t, s, lease.ID, Active)
	oldExpiry, _ := s.Get(lease.ID)
	time.Sleep(60 * time.Millisecond)
	if _, err := s.Heartbeat(lease.ID); err != nil {
		t.Fatal(err)
	}
	renewed, _ := s.Get(lease.ID)
	if !renewed.ExpiresAt.After(*oldExpiry.ExpiresAt) {
		t.Fatal("heartbeat did not extend TTL")
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatalf("repeat release: %v", err)
	}
}

func TestLeaseExpiresAndLateHeartbeatRejected(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, 120*time.Millisecond, 30*time.Millisecond)
	lease, _ := s.Request("model-1")
	waitLease(t, s, lease.ID, Active)
	waitLease(t, s, lease.ID, Expired)
	if _, err := s.Heartbeat(lease.ID); err != ErrExpired {
		t.Fatalf("late heartbeat error = %v", err)
	}
}

func TestHeartbeatCannotExtendPastMaximumLeaseDuration(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, 120*time.Millisecond, 30*time.Millisecond)
	s.maxLease = 300 * time.Millisecond
	lease, _ := s.Request("model-1")
	waitLease(t, s, lease.ID, Active)
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := s.Heartbeat(lease.ID); err == ErrExpired {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		time.Sleep(35 * time.Millisecond)
	}
	waitLease(t, s, lease.ID, Expired)
	got, _ := s.Get(lease.ID)
	if got.ActivatedAt == nil || time.Since(*got.ActivatedAt) < 200*time.Millisecond || time.Since(*got.ActivatedAt) > 500*time.Millisecond {
		t.Fatalf("lease expiration ignored max duration: %+v", got)
	}
}

func TestFailedStartDoesNotBlockAndUnexpectedExitFailsLease(t *testing.T) {
	_, s := testScheduler(t, "die", 1, time.Second, time.Second, 3, time.Second, time.Second)
	failed, _ := s.Request("model-1")
	next, _ := s.Request("model-1")
	waitLease(t, s, failed.ID, Failed)
	waitLease(t, s, next.ID, Failed)

	_, s2 := testScheduler(t, "crash", 1, time.Second, time.Second, 3, time.Second, time.Second)
	crashing, _ := s2.Request("model-1")
	waitLease(t, s2, crashing.ID, Active)
	waitLease(t, s2, crashing.ID, Failed)
}

func TestConcurrentRequestsAndShutdownCancelPending(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 32, 5*time.Second, time.Second)
	const count = 12
	leases := make([]Lease, count)
	var wg sync.WaitGroup
	for i := range leases {
		wg.Add(1)
		go func(i int) { defer wg.Done(); leases[i], _ = s.Request("model-1") }(i)
	}
	wg.Wait()
	for _, l := range leases {
		if l.ID == "" {
			t.Fatal("concurrent request failed")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, l := range leases {
		got, err := s.Get(l.ID)
		if err != nil || got.State != Cancelled {
			t.Fatalf("shutdown lease %s: %+v, %v", l.ID, got, err)
		}
	}
}

func TestManualOperationsCannotBypassQueueAndAreSerialized(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, time.Second)
	if err := s.ManualStart(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	lease, _ := s.Request("model-1")
	if err := s.ManualStop(context.Background(), "model-1"); err == nil {
		t.Fatal("manual stop bypassed queued lease")
	}
	waitLease(t, s, lease.ID, Active)
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
}

func TestThreeRequestsRemainFIFO(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 5, time.Second, time.Second)
	first, _ := s.Request("model-1")
	waitLease(t, s, first.ID, Active)
	second, _ := s.Request("model-2")
	third, _ := s.Request("model-1")
	_, _ = s.Release(first.ID)
	waitLease(t, s, second.ID, Active)
	_, _ = s.Release(second.ID)
	waitLease(t, s, third.ID, Active)
	_, _ = s.Release(third.ID)
}

func TestNeverRunsTwoModelProcessesAtOnce(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 3, time.Second, time.Second)
	first, _ := s.Request("model-1")
	waitLease(t, s, first.ID, Active)
	second, _ := s.Request("model-2")
	_, _ = s.Release(first.ID)
	waitLease(t, s, second.ID, Active)
	_, _ = s.Release(second.ID)
	waitFor(t, func() bool { return len(readEvents(t)) >= 4 })
	events := readEvents(t)
	active, maximum, starts := 0, 0, 0
	for _, event := range events {
		if strings.HasPrefix(event, "START ") {
			active++
			starts++
			if active > maximum {
				maximum = active
			}
		}
		if strings.HasPrefix(event, "STOP ") {
			active--
		}
	}
	if maximum > 1 || starts < 2 || active != 0 {
		t.Fatalf("overlapping or unreaped processes: max=%d starts=%d active=%d events=%v", maximum, starts, active, events)
	}
}

func testScheduler(t *testing.T, mode string, modelsCount int, startup, shutdown time.Duration, queueSize int, ttl, heartbeat time.Duration) (*runtime.Manager, *Scheduler) {
	t.Helper()
	dir := t.TempDir()
	ports := make([]int, modelsCount)
	for i := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[i] = listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
	}
	modelPath := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_SCHEDULER_CHILD", "1")
	t.Setenv("LARM_SCHEDULER_MODE", mode)
	eventsPath := filepath.Join(dir, "process-events.log")
	t.Setenv("LARM_SCHEDULER_EVENTS", eventsPath)
	configured := make(map[string]config.ModelConfig, modelsCount)
	for i := range ports {
		configured[fmt.Sprintf("model-%d", i+1)] = config.ModelConfig{Path: modelPath, Port: ports[i], Context: 4096, Batch: 512, UBatch: 128, GPULayers: "all", Parallel: 1, Temperature: 0.2, TopP: 0.9, TopK: 40}
	}
	cfg := config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: startup.String(), ShutdownTimeout: shutdown.String()}, Models: configured}
	manager := runtime.NewManager(cfg)
	sched := New(manager, config.SchedulerConfig{MaxQueueSize: queueSize, LeaseTTL: ttl.String(), Heartbeat: heartbeat.String(), MaxLease: (10 * ttl).String()})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sched.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return manager, sched
}

func readEvents(t *testing.T) []string {
	t.Helper()
	// Test-only scheduler helpers keep the process log alongside the manager fixture.
	data, err := os.ReadFile(os.Getenv("LARM_SCHEDULER_EVENTS"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(data) == 0 {
		return nil
	}
	return strings.FieldsFunc(strings.TrimSpace(string(data)), func(r rune) bool { return r == '\n' })
}

func waitLease(t *testing.T, s *Scheduler, id string, state LeaseState) LeaseStatus {
	t.Helper()
	var got LeaseStatus
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		got, _ = s.Get(id)
		if got.State == state {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lease %s state = %s, want %s; runtime = %+v", id, got.State, state, s.manager.Status())
	return got
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
