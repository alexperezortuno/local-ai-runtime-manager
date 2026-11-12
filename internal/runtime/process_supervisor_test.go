package runtime

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
)

type recordingProcessJournal struct {
	mu         sync.Mutex
	owner      string
	instances  []corecontract.ProcessInstance
	states     []corecontract.ProcessState
	blockState corecontract.ProcessState
	entered    chan struct{}
	release    chan struct{}
	gateOnce   sync.Once
}

func (j *recordingProcessJournal) OwnerInstanceID() string { return j.owner }
func (j *recordingProcessJournal) ProcessInstances() []corecontract.ProcessInstance {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := append([]corecontract.ProcessInstance(nil), j.instances...)
	return result
}
func (j *recordingProcessJournal) PersistProcess(instance corecontract.ProcessInstance) error {
	if err := corecontract.ValidateProcessInstance(instance); err != nil {
		return err
	}
	if instance.State == j.blockState && j.entered != nil && j.release != nil {
		j.gateOnce.Do(func() { close(j.entered) })
		<-j.release
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.states = append(j.states, instance.State)
	for i := range j.instances {
		if j.instances[i].ProcessInstanceID == instance.ProcessInstanceID {
			j.instances[i] = instance
			return nil
		}
	}
	j.instances = append(j.instances, instance)
	return nil
}

func TestStopAfterObservedExitReconcilesWithoutSignal(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID(), blockState: corecontract.ProcessTerminationRequested, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-journal.release:
		default:
			close(journal.release)
		}
	})
	m := supervisorTestManager(t, journal, "normal")
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p == nil {
		t.Fatal("manager did not retain the started process")
	}

	// Hold the observer while the owned child exits. This prevents the watch
	// goroutine from racing the Stop request past the observed Wait result.
	p.observeMu.Lock()
	if err := syscall.Kill(p.identity.PID, syscall.SIGTERM); err != nil {
		p.observeMu.Unlock()
		t.Fatal(err)
	}
	<-p.done
	result := make(chan error, 1)
	go func() { result <- m.Stop(context.Background(), "model-1") }()
	select {
	case <-journal.entered:
	case <-time.After(time.Second):
		p.observeMu.Unlock()
		t.Fatal("Stop did not reach the stop-intent barrier")
	}
	close(journal.release)
	p.observeMu.Unlock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if status := m.Status(); status.State != Stopped || status.PID != 0 {
		t.Fatalf("observed exit was not reconciled: %+v", status)
	}
	instance := journalProcess(t, journal)
	if instance.State != corecontract.ProcessTerminatedVerified || instance.TerminationStatus != "TERMINATED_VERIFIED" || instance.WaitError != "exited_zero" || len(instance.TerminationSignals) != 0 {
		t.Fatalf("reconciliation evidence = %+v", instance)
	}
}

func TestStopAfterWatchObservedExitReconcilesFinishedInstance(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "normal")
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if err := syscall.Kill(p.identity.PID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-p.done
	deadline := time.After(time.Second)
	for {
		status := m.Status()
		if status.State == Failed && status.PID == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("watch did not publish the observed exit: %+v", status)
		case <-time.After(time.Millisecond):
		}
	}
	if err := m.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if status := m.Status(); status.State != Stopped || status.PID != 0 {
		t.Fatalf("finished instance was not reconciled: %+v", status)
	}
	instance := journalProcess(t, journal)
	if instance.State != corecontract.ProcessTerminatedVerified || instance.WaitError != "exited_zero" || len(instance.TerminationSignals) != 0 {
		t.Fatalf("finished-instance evidence = %+v", instance)
	}
}

func TestConcurrentStopsShareOneTermination(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID(), blockState: corecontract.ProcessTerminating, entered: make(chan struct{}), release: make(chan struct{})}
	m := supervisorTestManager(t, journal, "ignore")
	m.config.ShutdownTimeout = (120 * time.Millisecond).String()
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- m.Stop(context.Background(), "model-1") }()
	select {
	case <-journal.entered:
	case <-time.After(time.Second):
		t.Fatal("first Stop did not reach the termination barrier")
	}
	second := make(chan error, 1)
	go func() { second <- m.Stop(context.Background(), "model-1") }()
	close(journal.release)
	if err := <-first; err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	instance := journalProcess(t, journal)
	if len(instance.TerminationSignals) != 2 || instance.TerminationSignals[0] != "SIGTERM" || instance.TerminationSignals[1] != "SIGKILL" {
		t.Fatalf("concurrent stop signalled more than once: %+v", instance)
	}
}

func TestAmbiguousWaitCannotVerifyTermination(t *testing.T) {
	p := &process{done: make(chan struct{}), wait: processWaitResult{errorClass: "wait_error"}}
	close(p.done)
	if err := p.terminationObserved(time.Millisecond); err == nil {
		t.Fatal("ambiguous Wait result was accepted as verified termination")
	}
}

func TestProcessIdentityIsDurableBeforeReady(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "normal")
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	status := m.Status()
	if status.ProcessInstanceID == "" || status.PID == 0 || status.PGID == 0 || status.ProcessIdentityStatus != "VERIFIED" || status.OwnershipStatus != "OWNED_VERIFIED" {
		t.Fatalf("unverified ready status: %+v", status)
	}
	journal.mu.Lock()
	states := append([]corecontract.ProcessState(nil), journal.states...)
	instance := journal.instances[0]
	journal.mu.Unlock()
	if len(states) < 4 || states[0] != corecontract.ProcessPreparing || states[len(states)-1] != corecontract.ProcessReady {
		t.Fatalf("durable lifecycle before ready = %v", states)
	}
	if instance.StartIdentity == "" || instance.OSProcessStartIdentity == "" || instance.Executable == "" || instance.StartedAt.IsZero() {
		t.Fatalf("missing process identity evidence: %+v", instance)
	}
}

func TestEverySpawnGetsNewProcessInstanceID(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "normal")
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	first := m.Status().ProcessInstanceID
	if err := m.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	second := m.Status().ProcessInstanceID
	if first == "" || second == "" || first == second {
		t.Fatalf("spawn IDs were not unique: %q %q", first, second)
	}
}

func TestRestartDoesNotAdoptPersistedProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	portListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := portListener.Addr().(*net.TCPAddr).Port
	_ = portListener.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_TEST_CHILD", "1")
	t.Setenv("LARM_TEST_MODE", "normal")
	t.Setenv("LARM_TEST_PORT", fmt.Sprint(port))
	cfg := config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: time.Second.String(), ShutdownTimeout: time.Second.String()}, Models: map[string]config.ModelConfig{"model-1": {Path: path, Port: port, Context: 1, Batch: 1, UBatch: 1, GPULayers: "all", Parallel: 1}}}
	journal := &recordingProcessJournal{owner: testOwnerID()}
	first := NewManager(cfg, journal)
	if err := first.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	second := NewManager(cfg, journal)
	if blocked, _ := second.RecoveryRequired(); !blocked {
		t.Fatal("restart did not enter recovery for a persisted live process")
	}
	if instances := second.ProcessInstances(); len(instances) != 1 || instances[0].OwnershipStatus == "OWNED_VERIFIED" {
		t.Fatalf("restart adopted or lost survivor classification: %+v", instances)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := first.Stop(ctx, "model-1"); err != nil {
		t.Fatal(err)
	}
}

func TestProcessIdentitySurvivesJournalReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.json")
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	m := supervisorTestManager(t, journal, "normal")
	if err := m.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	reopened, err := inference.OpenJournal(inference.JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	instances := reopened.ProcessInstances()
	if len(instances) != 1 || instances[0].ProcessInstanceID != m.Status().ProcessInstanceID || instances[0].StartIdentity == "" {
		t.Fatalf("reopened journal lost process identity: %+v", instances)
	}
}

func supervisorTestManager(t *testing.T, journal ProcessJournal, mode string) *Manager {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_TEST_CHILD", "1")
	t.Setenv("LARM_TEST_MODE", mode)
	t.Setenv("LARM_TEST_PORT", fmt.Sprint(port))
	cfg := config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: time.Second.String(), ShutdownTimeout: time.Second.String()}, Models: map[string]config.ModelConfig{"model-1": {Path: path, Port: port, Context: 1, Batch: 1, UBatch: 1, GPULayers: "all", Parallel: 1}}}
	m := NewManager(cfg, journal)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	return m
}

func testOwnerID() string {
	id, _ := corecontract.NewID()
	return id
}
