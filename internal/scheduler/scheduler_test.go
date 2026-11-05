package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func TestIdempotentAcquisitionReplaysWithoutChangingFIFOOrTTL(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond)
	hold, err := s.Request("model-2")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, hold.ID, Active)
	first, replay, err := s.RequestIdempotent("2ef8d7ef-b335-4404-b72c-5b2c08a36101", "model-1")
	if err != nil || replay || first.State != Queued {
		t.Fatalf("first acquisition = %+v replay=%v err=%v", first, replay, err)
	}
	second, err := s.Request("model-2")
	if err != nil {
		t.Fatal(err)
	}
	retried, replay, err := s.RequestIdempotent("2ef8d7ef-b335-4404-b72c-5b2c08a36101", "model-1")
	if err != nil || !replay || retried.ID != first.ID || retried.QueuePosition != first.QueuePosition {
		t.Fatalf("queued replay = %+v replay=%v err=%v", retried, replay, err)
	}
	if got, _ := s.Get(second.ID); got.QueuePosition != 2 {
		t.Fatalf("expected second request behind first in FIFO: %+v", got)
	}
	if _, err := s.Release(hold.ID); err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, first.ID, Active)
	activeBefore, _ := s.Get(first.ID)
	time.Sleep(30 * time.Millisecond)
	activeAfter, replay, err := s.RequestIdempotent("2ef8d7ef-b335-4404-b72c-5b2c08a36101", "model-1")
	if err != nil || !replay || activeAfter.ID != first.ID || !activeAfter.ExpiresAt.Equal(*activeBefore.ExpiresAt) {
		t.Fatalf("active replay renewed/changed lease: before=%+v after=%+v replay=%v err=%v", activeBefore, activeAfter, replay, err)
	}
	if got, _ := s.Get(second.ID); got.QueuePosition != 1 {
		t.Fatalf("retry altered following FIFO position: %+v", got)
	}
	if _, _, err := s.RequestIdempotent("2ef8d7ef-b335-4404-b72c-5b2c08a36101", "model-2"); err != ErrIdempotencyConflict {
		t.Fatalf("different model error = %v", err)
	}
	_, _ = s.Release(first.ID)
	if got, replay, err := s.RequestIdempotent("2ef8d7ef-b335-4404-b72c-5b2c08a36101", "model-1"); err != ErrIdempotencyTerminal || !replay || got.State != Completed {
		t.Fatalf("terminal replay = %+v replay=%v err=%v", got, replay, err)
	}
}

func TestConcurrentSameIdempotencyKeyCreatesOneQueueEntry(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
	const callers = 32
	results := make([]LeaseStatus, callers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _, _ = s.RequestIdempotent("3ef8d7ef-b335-4404-b72c-5b2c08a36102", "model-1")
		}(i)
	}
	wg.Wait()
	for _, got := range results {
		if got.ID == "" || got.ID != results[0].ID {
			t.Fatalf("concurrent requests produced different leases: %+v", results)
		}
	}
	if snapshot := s.Snapshot(); len(snapshot.Pending) > 1 || (snapshot.Active != nil && snapshot.Active.ID != results[0].ID) {
		t.Fatalf("duplicate operation reached scheduler: %+v", snapshot)
	}
}

func TestDifferentIdempotencyKeysKeepAcceptanceFIFO(t *testing.T) {
	_, s := testScheduler(t, "normal", 2, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
	active, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, active.ID, Active)
	first, _, err := s.RequestIdempotent("aef8d7ef-b335-4404-b72c-5b2c08a36109", "model-2")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := s.RequestIdempotent("bef8d7ef-b335-4404-b72c-5b2c08a36110", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.QueuePosition != 1 || second.QueuePosition != 2 {
		t.Fatalf("distinct keys did not keep FIFO acceptance order: first=%+v second=%+v", first, second)
	}
}

func TestIdempotencyStoreRestartRequiresRecoveryAndNeverRecreates(t *testing.T) {
	_, s, store := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond, true)
	key := "4ef8d7ef-b335-4404-b72c-5b2c08a36103"
	created, _, err := s.RequestIdempotent(key, "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, created.ID, Active)
	manager := s.manager
	setIdempotencyPath(s, "")
	restarted, err := NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: "2s", Heartbeat: "100ms", MaxLease: "20s"}, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownScheduler(t, restarted) })
	replayed, replay, err := restarted.RequestIdempotent(key, "model-1")
	if err != ErrIdempotencyRecovery || !replay || replayed.ID != created.ID || replayed.State != RecoveryRequired {
		t.Fatalf("restart replay = %+v replay=%v err=%v", replayed, replay, err)
	}
	if snapshot := restarted.Snapshot(); snapshot.Active != nil || len(snapshot.Pending) != 0 || snapshot.Preparing != nil {
		t.Fatalf("restart reactivated historical lease: %+v", snapshot)
	}
	if _, err := restarted.Request("model-1"); err != ErrIdempotencyRecovery {
		t.Fatalf("legacy acquisition was not blocked during recovery: %v", err)
	}
	if _, _, err := restarted.RequestIdempotent("f2f8d7ef-b335-4404-b72c-5b2c08a36116", "model-1"); err != ErrIdempotencyRecovery {
		t.Fatalf("new keyed acquisition was not blocked during recovery: %v", err)
	}
}

func TestIdempotencyQueuedReservationAfterRestartIsRecoveryRequired(t *testing.T) {
	_, s, store := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond, true)
	hold, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, hold.ID, Active)
	key := "cef8d7ef-b335-4404-b72c-5b2c08a36111"
	queued, _, err := s.RequestIdempotent(key, "model-1")
	if err != nil || queued.State != Queued {
		t.Fatalf("queued reservation = %+v err=%v", queued, err)
	}
	setIdempotencyPath(s, "")
	restarted, err := NewWithIdempotencyStore(s.manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: "2s", Heartbeat: "100ms", MaxLease: "20s"}, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownScheduler(t, restarted) })
	got, replay, err := restarted.RequestIdempotent(key, "model-1")
	if err != ErrIdempotencyRecovery || !replay || got.ID != queued.ID || got.State != RecoveryRequired {
		t.Fatalf("queued crash recovery = %+v replay=%v err=%v", got, replay, err)
	}
}

func TestIdempotencyStoreWriteFailureDoesNotAcceptOrReserve(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
	setIdempotencyPath(s, t.TempDir())
	key := "def8d7ef-b335-4404-b72c-5b2c08a36112"
	if _, _, err := s.RequestIdempotent(key, "model-1"); err == nil {
		t.Fatal("expected durable reservation write failure")
	}
	if snapshot := s.Snapshot(); snapshot.Active != nil || snapshot.Preparing != nil || len(snapshot.Pending) != 0 {
		t.Fatalf("failed persistence accepted an operation: %+v", snapshot)
	}
	setIdempotencyPath(s, "")
	if _, replay, err := s.RequestIdempotent(key, "model-1"); err != nil || replay {
		t.Fatalf("failed reservation left a key behind: replay=%v err=%v", replay, err)
	}
}

func TestReleaseDoesNotPublishWhenDurableSyncFails(t *testing.T) {
	_, s, _ := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond, true)
	lease, _, err := s.RequestIdempotent("def8d7ef-b335-4404-b72c-5b2c08a36112", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	setIdempotencyPath(s, t.TempDir()) // rename cannot replace a directory
	if _, err := s.Release(lease.ID); err == nil {
		t.Fatal("release succeeded without durable evidence")
	}
	if got, _ := s.Get(lease.ID); got.State != Active {
		t.Fatalf("failed release changed in-memory state: %+v", got)
	}
}

func TestIdempotencyTerminalTombstoneSurvivesRestart(t *testing.T) {
	_, s, store := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond, true)
	key := "e1f8d7ef-b335-4404-b72c-5b2c08a36114"
	created, _, err := s.RequestIdempotent(key, "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, created.ID, Active)
	if _, err := s.Release(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store); err != nil {
		t.Fatalf("terminal tombstone store was cleaned: %v", err)
	}
	shutdownScheduler(t, s)
	restarted, err := NewWithIdempotencyStore(s.manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: "2s", Heartbeat: "100ms", MaxLease: "20s"}, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownScheduler(t, restarted) })
	got, replay, err := restarted.RequestIdempotent(key, "model-1")
	if err != ErrIdempotencyTerminal || !replay || got.ID != created.ID || got.State != Completed {
		t.Fatalf("terminal tombstone replay = %+v replay=%v err=%v", got, replay, err)
	}
	if _, replay, err := restarted.RequestIdempotent("e2f8d7ef-b335-4404-b72c-5b2c08a36115", "model-1"); err != nil || replay {
		t.Fatalf("clean restart blocked a new key: replay=%v err=%v", replay, err)
	}
}

func TestUnkeyedActivityIsMarkedDirtyAndBlocksAfterCrash(t *testing.T) {
	_, s, store := testSchedulerWithStore(t, "normal", 1, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond, true)
	if _, err := s.Request("model-1"); err != nil {
		t.Fatal(err)
	}
	setIdempotencyPath(s, "")
	restarted, err := NewWithIdempotencyStore(s.manager, config.SchedulerConfig{MaxQueueSize: 5, LeaseTTL: "2s", Heartbeat: "100ms", MaxLease: "20s"}, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownScheduler(t, restarted) })
	if _, err := restarted.Request("model-1"); err != ErrIdempotencyRecovery {
		t.Fatalf("legacy acquisition after unkeyed crash = %v", err)
	}
	if err := restarted.ManualStart(context.Background(), "model-1"); err != ErrIdempotencyRecovery {
		t.Fatalf("manual start after unkeyed crash = %v", err)
	}
}

func TestIdempotencyPreparingCancellationExpirationFailureAndShutdown(t *testing.T) {
	t.Run("preparing replay", func(t *testing.T) {
		_, s := testScheduler(t, "delay", 1, time.Second, time.Second, 5, 2*time.Second, 100*time.Millisecond)
		key := "5ef8d7ef-b335-4404-b72c-5b2c08a36104"
		first, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, first.ID, Preparing)
		replay, repeated, err := s.RequestIdempotent(key, "model-1")
		if err != nil || !repeated || replay.ID != first.ID || replay.State != Preparing {
			t.Fatalf("preparing replay = %+v repeated=%v err=%v", replay, repeated, err)
		}
		waitLease(t, s, first.ID, Active)
		if starts := countProcessEvent(t, "START"); starts > 1 {
			t.Fatalf("retry started another backend: START count=%d", starts)
		}
	})

	t.Run("cancelled queued operation", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
		active, _ := s.Request("model-1")
		waitLease(t, s, active.ID, Active)
		key := "6ef8d7ef-b335-4404-b72c-5b2c08a36105"
		queued, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Release(queued.ID); err != nil {
			t.Fatal(err)
		}
		got, replay, err := s.RequestIdempotent(key, "model-1")
		if err != ErrIdempotencyTerminal || !replay || got.State != Cancelled {
			t.Fatalf("cancelled replay = %+v replay=%v err=%v", got, replay, err)
		}
	})

	t.Run("cancel races retry", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
		hold, _ := s.Request("model-1")
		waitLease(t, s, hold.ID, Active)
		key := "a1f8d7ef-b335-4404-b72c-5b2c08a36113"
		queued, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, _ = s.Release(queued.ID) }()
		var replay LeaseStatus
		var replayErr error
		go func() { defer wg.Done(); <-start; replay, _, replayErr = s.RequestIdempotent(key, "model-1") }()
		close(start)
		wg.Wait()
		if replay.ID != queued.ID || (replay.State != Queued && replay.State != Cancelled) || (replayErr != nil && replayErr != ErrIdempotencyTerminal) {
			t.Fatalf("cancel/retry race returned another result: %+v err=%v", replay, replayErr)
		}
		got, repeated, err := s.RequestIdempotent(key, "model-1")
		if err != ErrIdempotencyTerminal || !repeated || got.ID != queued.ID || got.State != Cancelled {
			t.Fatalf("post-race retry = %+v repeated=%v err=%v", got, repeated, err)
		}
	})

	t.Run("expired operation", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, 80*time.Millisecond, 20*time.Millisecond)
		key := "7ef8d7ef-b335-4404-b72c-5b2c08a36106"
		first, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, first.ID, Expired)
		got, replay, err := s.RequestIdempotent(key, "model-1")
		if err != ErrIdempotencyTerminal || !replay || got.State != Expired {
			t.Fatalf("expired replay = %+v replay=%v err=%v", got, replay, err)
		}
	})

	t.Run("failed operation", func(t *testing.T) {
		_, s := testScheduler(t, "die", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
		key := "8ef8d7ef-b335-4404-b72c-5b2c08a36107"
		first, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, first.ID, Failed)
		got, replay, err := s.RequestIdempotent(key, "model-1")
		if err != ErrIdempotencyTerminal || !replay || got.State != Failed {
			t.Fatalf("failed replay = %+v replay=%v err=%v", got, replay, err)
		}
	})

	t.Run("shutdown operation", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 5, time.Second, 100*time.Millisecond)
		key := "9ef8d7ef-b335-4404-b72c-5b2c08a36108"
		first, _, err := s.RequestIdempotent(key, "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, first.ID, Active)
		endInference, err := s.BeginInference(first.ID, first.Model, func() {})
		if err != nil {
			t.Fatal(err)
		}
		shutdown := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			shutdown <- s.Shutdown(ctx)
		}()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				break
			}
			time.Sleep(time.Millisecond)
		}
		got, replay, err := s.RequestIdempotent(key, "model-1")
		if err != ErrIdempotencyTerminal || !replay || got.ID != first.ID || got.State != Cancelled {
			t.Fatalf("shutdown replay = %+v replay=%v err=%v", got, replay, err)
		}
		endInference()
		if err := <-shutdown; err != nil {
			t.Fatal(err)
		}
	})
}

func countProcessEvent(t *testing.T, event string) int {
	t.Helper()
	count := 0
	for _, line := range readEvents(t) {
		if strings.HasPrefix(line, event+" ") {
			count++
		}
	}
	return count
}

func shutdownScheduler(t *testing.T, s *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
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

type testReleaseProtector struct{ protected atomic.Bool }

func (p *testReleaseProtector) Protected(string, string, string) (bool, string) {
	if p.protected.Load() {
		return true, "fixture protection"
	}
	return false, ""
}

func TestProtectedLeaseCannotReleaseOrExpire(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, 60*time.Millisecond, 10*time.Millisecond)
	lease, err := s.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	protector := &testReleaseProtector{}
	protector.protected.Store(true)
	s.SetReleaseProtector(protector)
	time.Sleep(120 * time.Millisecond)
	if got, _ := s.Get(lease.ID); got.State != Active {
		t.Fatalf("protected lease expired: %+v", got)
	}
	if _, err := s.Release(lease.ID); !errors.Is(err, ErrInferenceProtected) {
		t.Fatalf("protected release error = %v", err)
	}
	protector.protected.Store(false)
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
}

func testScheduler(t *testing.T, mode string, modelsCount int, startup, shutdown time.Duration, queueSize int, ttl, heartbeat time.Duration) (*runtime.Manager, *Scheduler) {
	m, s, _ := testSchedulerWithStore(t, mode, modelsCount, startup, shutdown, queueSize, ttl, heartbeat, false)
	return m, s
}

func testSchedulerWithStore(t *testing.T, mode string, modelsCount int, startup, shutdown time.Duration, queueSize int, ttl, heartbeat time.Duration, persistent bool) (*runtime.Manager, *Scheduler, string) {
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
	schedulerCfg := config.SchedulerConfig{MaxQueueSize: queueSize, LeaseTTL: ttl.String(), Heartbeat: heartbeat.String(), MaxLease: (10 * ttl).String()}
	store := ""
	var sched *Scheduler
	if persistent {
		store = filepath.Join(dir, "idempotency.json")
		sched, err = NewWithIdempotencyStore(manager, schedulerCfg, store)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		sched = New(manager, schedulerCfg)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sched.Shutdown(ctx)
		_ = manager.Shutdown(ctx)
	})
	return manager, sched, store
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

func setIdempotencyPath(s *Scheduler, path string) {
	s.mu.Lock()
	s.idempotencyPath = path
	s.mu.Unlock()
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
