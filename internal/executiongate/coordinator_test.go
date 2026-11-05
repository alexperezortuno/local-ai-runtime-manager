package executiongate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"syscall"
)

func testID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", n) }

func openTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func createTestExecution(t *testing.T, c *Coordinator, id, parent string) {
	t.Helper()
	if _, err := c.CreateExecution(context.Background(), CreateExecutionInput{ExecutionID: id, ParentExecutionID: parent, Initiator: "test-wrapper"}); err != nil {
		t.Fatal(err)
	}
}

func acquireTest(t *testing.T, c *Coordinator, server, session, id string, model ...string) {
	t.Helper()
	got, err := c.AcquireSession(context.Background(), server, session, id)
	if err != nil || got != Acquired {
		t.Fatalf("acquire = %q, %v; want ACQUIRED", got, err)
	}
	modelID := "probe"
	if len(model) > 0 {
		modelID = model[0]
	}
	n, _ := strconv.ParseInt(id[len(id)-12:], 16, 64)
	if err := c.BindLease(context.Background(), id, BindLeaseInput{ModelID: modelID, IdempotencyKey: testID(10000 + int(n)), LeaseID: testID(20000 + int(n)), LeaseState: "ACTIVE"}); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireExclusiveAndIndependentScopes(t *testing.T) {
	c := openTestCoordinator(t)
	for i := 1; i <= 3; i++ {
		createTestExecution(t, c, testID(i), "")
	}
	var wg sync.WaitGroup
	results := make(chan AcquireResult, 2)
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			result, err := c.AcquireSession(context.Background(), "opencode-A", "session-A", id)
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
			results <- result
		}(testID(i))
	}
	wg.Wait()
	close(results)
	acquired, busy := 0, 0
	for result := range results {
		if result == Acquired {
			acquired++
		}
		if result == Busy {
			busy++
		}
	}
	if acquired != 1 || busy != 1 {
		t.Fatalf("same scope results: acquired=%d busy=%d", acquired, busy)
	}
	acquireTest(t, c, "opencode-A", "session-B", testID(3))
	// The same Session ID at another server scope is independent.
	createTestExecution(t, c, testID(4), "")
	acquireTest(t, c, "opencode-B", "session-A", testID(4))
}

func TestRequestsSSEFinalizeAndCleanup(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(10), "")
	acquireTest(t, c, "server", "session", testID(10))
	if _, err := c.CreateRequest(context.Background(), testID(20), testID(10), "probe", true); err != nil {
		t.Fatal(err)
	}
	if err := c.StartRequest(context.Background(), testID(10), testID(20)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(10), testID(20), RequestEvent{HeadersReceived: true, BodyStarted: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(context.Background(), testID(10), ExecutionCompleted, "execution_succeeded"); err != ErrActive {
		t.Fatalf("headers must not permit release, got %v", err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(10), testID(20), RequestEvent{SSEDone: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(context.Background(), testID(10), ExecutionCompleted, "execution_succeeded"); err != ErrActive {
		t.Fatalf("SSE marker without EOF must remain active, got %v", err)
	}
	request, err := c.ObserveRequest(context.Background(), testID(10), testID(20), RequestEvent{EOF: true})
	if err != nil || request.State != RequestCompleted {
		t.Fatalf("EOF completion = %#v, %v", request, err)
	}
	if err := c.Finalize(context.Background(), testID(10), ExecutionCompleted, "execution_succeeded"); err != nil {
		t.Fatal(err)
	}
	if result, err := c.AcquireSession(context.Background(), "server", "session", testID(11)); result != AcquireError || err != ErrNotFound {
		t.Fatalf("unregistered acquire = %q, %v", result, err)
	}
	if err := c.Cleanup(context.Background(), testID(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(10), testID(20), RequestEvent{EOF: true}); err != ErrNotFound {
		t.Fatalf("late event = %v, want not found", err)
	}
	snap, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Counts.PendingCleanup != 0 || snap.Counts.DeletedExecutions != 1 || snap.Counts.OwnedSessions != 0 || snap.Counts.ActiveRequests != 0 {
		t.Fatalf("unexpected cleanup metrics: %+v", snap.Counts)
	}
}

func TestLeaseBindingIsImmutableAndRequestsUseItsModelAndLease(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(12), "")
	acquireTest(t, c, "server", "session", testID(12), "model-a")
	snapshot, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner := snapshot.Executions[testID(12)]
	if owner.ModelID != "model-a" || owner.IdempotencyKey == "" || owner.LeaseID == "" {
		t.Fatalf("lease association missing: %+v", owner)
	}
	if _, err := c.CreateRequest(context.Background(), testID(13), testID(12), "model-b", false); err != ErrConflict {
		t.Fatalf("wrong model request = %v", err)
	}
	request, err := c.CreateRequest(context.Background(), testID(13), testID(12), "model-a", false)
	if err != nil || request.LeaseID != owner.LeaseID {
		t.Fatalf("request did not inherit explicit owner lease: %+v, %v", request, err)
	}
	if err := c.BindLease(context.Background(), testID(12), BindLeaseInput{ModelID: "model-a", IdempotencyKey: testID(99), LeaseID: testID(98), LeaseState: "ACTIVE"}); err != ErrConflict {
		t.Fatalf("rebinding execution to another lease = %v", err)
	}
	createTestExecution(t, c, testID(14), "")
	acquire, err := c.AcquireSession(context.Background(), "server", "other-session", testID(14))
	if err != nil || acquire != Acquired {
		t.Fatalf("second session acquire = %s, %v", acquire, err)
	}
	if err := c.BindLease(context.Background(), testID(14), BindLeaseInput{ModelID: "model-a", IdempotencyKey: owner.IdempotencyKey, LeaseID: testID(97), LeaseState: "ACTIVE"}); err != ErrConflict {
		t.Fatalf("reusing another execution's acquisition key = %v", err)
	}
	if err := c.BindLease(context.Background(), testID(14), BindLeaseInput{ModelID: "model-a", IdempotencyKey: testID(96), LeaseID: owner.LeaseID, LeaseState: "ACTIVE"}); err != ErrConflict {
		t.Fatalf("reusing another execution's lease ID = %v", err)
	}
}

func TestBeginFinalizeStopsNewRequestsAndWaitsForSSEEOF(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(15), "")
	acquireTest(t, c, "server", "finalizing", testID(15), "model-a")
	if _, err := c.CreateRequest(context.Background(), testID(16), testID(15), "model-a", true); err != nil {
		t.Fatal(err)
	}
	if err := c.StartRequest(context.Background(), testID(15), testID(16)); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginFinalize(context.Background(), testID(15), ExecutionCancelled, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateRequest(context.Background(), testID(17), testID(15), "model-a", false); err != ErrNotOwner {
		t.Fatalf("new request after finalization began = %v", err)
	}
	if err := c.Finalize(context.Background(), testID(15), ExecutionCancelled, "cancelled"); err != ErrActive {
		t.Fatalf("SSE lease released before EOF = %v", err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(15), testID(16), RequestEvent{SSEDone: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(15), testID(16), RequestEvent{EOF: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(context.Background(), testID(15), ExecutionCancelled, "cancelled"); err != nil {
		t.Fatal(err)
	}
}

func TestRestartMarksAmbiguousLeaseAcquisitionUnknownWithoutReleasingSession(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(18), "")
	if result, err := c.AcquireSession(context.Background(), "server", "acquisition-unknown", testID(18)); err != nil || result != Acquired {
		t.Fatalf("session acquire = %s, %v", result, err)
	}
	if err := c.BeginLeaseAcquisition(context.Background(), testID(18), LeaseIntentInput{ModelID: "probe", IdempotencyKey: testID(19)}); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(c.dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Executions[testID(18)].State != ExecutionUnknown || snapshot.Owners[scopeKey("server", "acquisition-unknown")] != testID(18) {
		t.Fatalf("restart lost conservative ownership: execution=%+v owners=%+v", snapshot.Executions[testID(18)], snapshot.Owners)
	}
}

func TestRejectedBeforeAcquireCanBeCleaned(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(19), "")
	createTestExecution(t, c, testID(20), "")
	acquireTest(t, c, "server", "busy", testID(19))
	if result, err := c.AcquireSession(context.Background(), "server", "busy", testID(20)); err != nil || result != Busy {
		t.Fatalf("second acquire = %q %v, want BUSY", result, err)
	}
	if err := c.Finalize(context.Background(), testID(20), ExecutionFailed, "rejected_before_prompt"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background(), testID(20)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.Snapshot(context.Background())
	if err != nil || snapshot.Counts.OwnedSessions != 1 || snapshot.Counts.DeletedExecutions != 1 {
		t.Fatalf("rejected execution cleanup: %+v, %v", snapshot.Counts, err)
	}
}

func TestSSEWithoutDoneFailsAndCancelNeedsConfirmation(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(30), "")
	acquireTest(t, c, "server", "sse", testID(30))
	_, _ = c.CreateRequest(context.Background(), testID(31), testID(30), "probe", true)
	_ = c.StartRequest(context.Background(), testID(30), testID(31))
	r, err := c.ObserveRequest(context.Background(), testID(30), testID(31), RequestEvent{HeadersReceived: true, EOF: true})
	if err != nil || r.State != RequestFailed {
		t.Fatalf("truncated SSE = %#v %v", r, err)
	}
	if err := c.Finalize(context.Background(), testID(30), ExecutionFailed, "sse_truncated"); err != nil {
		t.Fatal(err)
	}
	createTestExecution(t, c, testID(32), "")
	acquireTest(t, c, "server", "cancel", testID(32))
	_, _ = c.CreateRequest(context.Background(), testID(33), testID(32), "probe", false)
	_ = c.StartRequest(context.Background(), testID(32), testID(33))
	if err := c.Finalize(context.Background(), testID(32), ExecutionCancelled, "cancel"); err != ErrActive {
		t.Fatalf("unconfirmed cancel release = %v", err)
	}
	r, err = c.ObserveRequest(context.Background(), testID(32), testID(33), RequestEvent{CancelConfirmed: true})
	if err != nil || r.State != RequestCancelled {
		t.Fatalf("confirmed cancel = %#v %v", r, err)
	}
	if err := c.Finalize(context.Background(), testID(32), ExecutionCancelled, "cancel_confirmed"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPErrorWaitsForBodyEOF(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(34), "")
	acquireTest(t, c, "server", "http-error", testID(34))
	if _, err := c.CreateRequest(context.Background(), testID(35), testID(34), "probe", false); err != nil {
		t.Fatal(err)
	}
	if err := c.StartRequest(context.Background(), testID(34), testID(35)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(34), testID(35), RequestEvent{HTTPStatus: 503}); err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(context.Background(), testID(34), ExecutionFailed, "http_503"); err != ErrActive {
		t.Fatalf("headers released before body EOF: %v", err)
	}
	r, err := c.ObserveRequest(context.Background(), testID(34), testID(35), RequestEvent{EOF: true})
	if err != nil || r.State != RequestFailed {
		t.Fatalf("503 EOF = %#v %v", r, err)
	}
}

func TestWrongOwnerAndLateEventCannotAffectNextExecution(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(40), "")
	createTestExecution(t, c, testID(41), "")
	acquireTest(t, c, "server", "shared", testID(40))
	if err := c.Finalize(context.Background(), testID(41), ExecutionCompleted, "wrong_owner"); err != ErrConflict {
		t.Fatalf("wrong-owner release = %v", err)
	}
	if got, err := c.AcquireSession(context.Background(), "server", "shared", testID(41)); err != nil || got != Busy {
		t.Fatalf("owner replaced: %q %v", got, err)
	}
	if err := c.Finalize(context.Background(), testID(40), ExecutionCompleted, "no_requests"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background(), testID(40)); err != nil {
		t.Fatal(err)
	}
	acquireTest(t, c, "server", "shared", testID(41))
	if err := c.Finalize(context.Background(), testID(40), ExecutionCompleted, "duplicate_release"); err != ErrNotFound {
		t.Fatalf("repeated release = %v", err)
	}
	if _, err := c.ObserveRequest(context.Background(), testID(40), testID(42), RequestEvent{EOF: true}); err != ErrNotFound {
		t.Fatalf("late event mutated next owner: %v", err)
	}
	s, _ := c.Snapshot(context.Background())
	if s.Owners[scopeKey("server", "shared")] != testID(41) {
		t.Fatalf("owner changed by late event: %v", s.Owners)
	}
}

func TestCancellationAndUnknownSurviveCoordinatorRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gate")
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	createTestExecution(t, c, testID(50), "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := c.AcquireSession(ctx, "server", "cancel", testID(50)); result != Cancelled || err != ErrCancelled {
		t.Fatalf("cancelled acquire = %q %v", result, err)
	}
	createTestExecution(t, c, testID(51), "")
	acquireTest(t, c, "server", "unknown", testID(51))
	if err := c.SetExecutionState(context.Background(), testID(51), ExecutionUnknown); err != nil {
		t.Fatal(err)
	}
	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	createTestExecution(t, c2, testID(52), "")
	if result, err := c2.AcquireSession(context.Background(), "server", "unknown", testID(52)); err != nil || result != Busy {
		t.Fatalf("restart released unknown owner: %q %v", result, err)
	}
	if err := c2.SetExecutionState(context.Background(), testID(51), ExecutionClientDisconnected); err != ErrRecoveryRequired {
		t.Fatalf("unknown state was downgraded to releasable: %v", err)
	}
	if err := c2.Finalize(context.Background(), testID(51), ExecutionFailed, "no_reconciliation"); err != ErrRecoveryRequired {
		t.Fatalf("unknown execution released without reconciliation: %v", err)
	}
}

func TestMissingJournalWithExistingLockFailsClosed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gate")
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	createTestExecution(t, c, testID(53), "")
	if err := os.Remove(filepath.Join(dir, "registry.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("missing journal was treated as an empty coordinator")
	}
}

func TestCanceledWhileWaitingForCoordinatorFileLock(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(60), "")
	lock, err := os.OpenFile(filepath.Join(c.dir, "coordinator.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := c.AcquireSession(ctx, "server", "waiting", testID(60))
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	_ = lock.Close()
	if result != Cancelled || err != ErrCancelled {
		t.Fatalf("waiting cancellation = %q %v", result, err)
	}
}

func TestCleanupWaitsForBackgroundChild(t *testing.T) {
	c := openTestCoordinator(t)
	createTestExecution(t, c, testID(70), "")
	createTestExecution(t, c, testID(71), testID(70))
	acquireTest(t, c, "server", "parent", testID(70))
	acquireTest(t, c, "server", "child", testID(71))
	if err := c.Finalize(context.Background(), testID(70), ExecutionCompleted, "parent_finished"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background(), testID(70)); err != ErrActive {
		t.Fatalf("parent cleanup with active child = %v", err)
	}
	if err := c.Finalize(context.Background(), testID(71), ExecutionCompleted, "child_finished"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background(), testID(71)); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background(), testID(70)); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentSessionsKeepRequestsIndependent(t *testing.T) {
	c := openTestCoordinator(t)
	const count = 12
	start := make(chan struct{})
	ready := make(chan error, count)
	finish := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		executionID, requestID := testID(100+i), testID(200+i)
		wg.Add(1)
		go func(i int, executionID, requestID string) {
			defer wg.Done()
			<-start
			if _, err := c.CreateExecution(context.Background(), CreateExecutionInput{ExecutionID: executionID, Initiator: "stress-wrapper"}); err != nil {
				ready <- err
				return
			}
			sessionID, modelID := fmt.Sprintf("session-%d", i), fmt.Sprintf("model-%d", i)
			result, err := c.AcquireSession(context.Background(), "stress-server", sessionID, executionID)
			if err != nil || result != Acquired {
				ready <- fmt.Errorf("acquire %d: %s %v", i, result, err)
				return
			}
			if err := c.BindLease(context.Background(), executionID, BindLeaseInput{ModelID: modelID, IdempotencyKey: testID(30000 + i), LeaseID: testID(40000 + i), LeaseState: "ACTIVE"}); err != nil {
				ready <- err
				return
			}
			if _, err := c.CreateRequest(context.Background(), requestID, executionID, modelID, false); err != nil {
				ready <- err
				return
			}
			if err := c.StartRequest(context.Background(), executionID, requestID); err != nil {
				ready <- err
				return
			}
			ready <- nil
			<-finish
			if _, err := c.ObserveRequest(context.Background(), executionID, requestID, RequestEvent{HeadersReceived: true, BodyStarted: true, EOF: true}); err != nil {
				t.Errorf("finish %d: %v", i, err)
				return
			}
			if err := c.Finalize(context.Background(), executionID, ExecutionCompleted, "synthetic_eof"); err != nil {
				t.Errorf("finalize %d: %v", i, err)
				return
			}
			if err := c.Cleanup(context.Background(), executionID); err != nil {
				t.Errorf("cleanup %d: %v", i, err)
			}
		}(i, executionID, requestID)
	}
	close(start)
	for i := 0; i < count; i++ {
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counts.OwnedSessions != count || snapshot.Counts.ActiveRequests != count {
		t.Fatalf("concurrent registry counts: %+v", snapshot.Counts)
	}
	close(finish)
	wg.Wait()
	snapshot, err = c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counts.OwnedSessions != 0 || snapshot.Counts.ActiveRequests != 0 || snapshot.Counts.DeletedExecutions != count {
		t.Fatalf("terminal registry counts: %+v", snapshot.Counts)
	}
}

func TestHTTPHandlerAndRegistry(t *testing.T) {
	c := openTestCoordinator(t)
	srv := httptest.NewServer(NewHTTPHandler(c))
	defer srv.Close()
	// The handler is exercised here for its serving contract; state transitions
	// remain covered through the same public methods above.
	r, err := httpGet(srv.URL + "/v1/registry")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var snap Snapshot
	if err := json.NewDecoder(r.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != 200 || snap.Counts.OwnedSessions != 0 {
		t.Fatalf("snapshot response = %d %+v", r.StatusCode, snap.Counts)
	}
}

func httpGet(url string) (*http.Response, error) { return http.Get(url) }

func TestCoordinatorProcessHelper(t *testing.T) {
	if os.Getenv("LARM_GATE_HELPER") != "1" {
		return
	}
	dir, id, name := os.Getenv("LARM_GATE_DIR"), os.Getenv("LARM_GATE_ID"), os.Getenv("LARM_GATE_NAME")
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	start := filepath.Join(dir, "start")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(start); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper start barrier timed out")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := c.AcquireSession(context.Background(), "shared-server", "same-session", id)
	if err != nil {
		t.Fatal(err)
	}
	if result == Acquired {
		if err := c.BindLease(context.Background(), id, BindLeaseInput{ModelID: "process-probe", IdempotencyKey: testID(50000), LeaseID: testID(50001), LeaseState: "ACTIVE"}); err != nil {
			t.Fatal(err)
		}
		requestID := os.Getenv("LARM_GATE_REQUEST_ID")
		if _, err := c.CreateRequest(context.Background(), requestID, id, "process-probe", false); err != nil {
			t.Fatal(err)
		}
		if err := c.StartRequest(context.Background(), id, requestID); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := json.Marshal(map[string]string{"result": string(result)})
	if err := os.WriteFile(filepath.Join(dir, name+".result"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestTwoProcessesShareAtomicOwner(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gate")
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	createTestExecution(t, c, testID(80), "")
	createTestExecution(t, c, testID(81), "")
	cmds := make([]*exec.Cmd, 2)
	for i, id := range []string{testID(80), testID(81)} {
		name := strconv.Itoa(i)
		requestID := testID(280 + i)
		cmd := exec.Command(os.Args[0], "-test.run=^TestCoordinatorProcessHelper$")
		cmd.Env = append(os.Environ(), "LARM_GATE_HELPER=1", "LARM_GATE_DIR="+dir, "LARM_GATE_ID="+id, "LARM_GATE_REQUEST_ID="+requestID, "LARM_GATE_NAME="+name)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds[i] = cmd
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, a := os.Stat(filepath.Join(dir, "0.ready"))
		_, b := os.Stat(filepath.Join(dir, "1.ready"))
		if a == nil && b == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("process helpers did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(dir, "start"), []byte("go"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("helper process: %v", err)
		}
	}
	var acquired, busy int
	for i := 0; i < 2; i++ {
		data, err := os.ReadFile(filepath.Join(dir, strconv.Itoa(i)+".result"))
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]string
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result["result"] == string(Acquired) {
			acquired++
		}
		if result["result"] == string(Busy) {
			busy++
		}
	}
	if acquired != 1 || busy != 1 {
		t.Fatalf("cross-process results: acquired=%d busy=%d", acquired, busy)
	}
	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := c2.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counts.ActiveRequests != 1 || snapshot.Counts.OwnedSessions != 1 {
		t.Fatalf("wrapper exit lost active ownership: %+v", snapshot.Counts)
	}
	createTestExecution(t, c2, testID(82), "")
	if result, err := c2.AcquireSession(context.Background(), "shared-server", "same-session", testID(82)); err != nil || result != Busy {
		t.Fatalf("wrapper exit silently released its session: %q %v", result, err)
	}
}
