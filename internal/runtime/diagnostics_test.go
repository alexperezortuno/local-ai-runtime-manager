package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
)

func TestStartupDiagnosticsSpawnFailure(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "normal")
	m.config.Binary = filepath.Join(t.TempDir(), "missing llama-server")

	if err := m.Start(context.Background(), "model-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	instance := journalProcess(t, journal)
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticSpawnError {
		t.Fatalf("diagnostic = %q, want SPAWN_ERROR", instance.StartupDiagnostic)
	}
	if instance.StartupError == "" || strings.Contains(instance.StartupError, "missing llama-server") {
		t.Fatalf("startup error was not sanitized: %q", instance.StartupError)
	}
}

func TestStartupDiagnosticsCancellation(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "never")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.Start(ctx, "model-1") }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	instance := journalProcess(t, journal)
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticStartupCancelled || !instance.SignalRequestedByLARM {
		t.Fatalf("cancellation diagnostic = %+v", instance)
	}
	if instance.TerminatedAt.IsZero() || instance.WaitError == "" {
		t.Fatalf("missing cancellation wait result: %+v", instance)
	}
}

func TestStartupDiagnosticsReadinessTimeout(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "never")
	m.config.StartupTimeout = (100 * time.Millisecond).String()
	if err := m.Start(context.Background(), "model-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	instance := journalProcess(t, journal)
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticReadinessTimeout {
		t.Fatalf("diagnostic = %q, want READINESS_TIMEOUT", instance.StartupDiagnostic)
	}
	if instance.StartupCancellationReason != "startup_timeout" || instance.WaitError == "" {
		t.Fatalf("timeout metadata = %+v", instance)
	}
}

func TestStartupDiagnosticsBackendExitAndWaitResult(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "die")
	if err := m.Start(context.Background(), "model-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	instance := journalProcess(t, journal)
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticBackendExited {
		t.Fatalf("diagnostic = %q, want BACKEND_EXITED", instance.StartupDiagnostic)
	}
	if instance.ExitCode == nil || *instance.ExitCode != 7 || instance.WaitError != "nonzero_exit" || instance.TerminatedAt.IsZero() {
		t.Fatalf("wait result = %+v", instance)
	}
	if instance.ProcessInstanceID == "" || instance.PID == 0 {
		t.Fatalf("wait result lost process association: %+v", instance)
	}
}

func TestStartupDiagnosticsIdentityFailureDoesNotSignal(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "normal")
	id, _ := corecontract.NewID()
	instance := corecontract.ProcessInstance{SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: id, OwnerInstanceID: journal.owner, ModelID: "model-1", ConfigFingerprint: "sha256:test", State: corecontract.ProcessPreparing, RecoveryState: corecontract.RecoveryNormal}
	if err := journal.PersistProcess(instance); err != nil {
		t.Fatal(err)
	}
	m.processes[id] = instance
	m.markIdentityUnknown("model-1", instance, errors.New("secret prompt /private/path"))
	got := journalProcess(t, journal)
	if got.StartupDiagnostic != corecontract.StartupDiagnosticIdentityUnverified || got.SignalRequestedByLARM || got.TerminationSignals != nil {
		t.Fatalf("identity failure metadata = %+v", got)
	}
	if got.StartupError != "startup_error" || strings.Contains(got.StartupError, "secret") {
		t.Fatalf("identity error was not sanitized: %q", got.StartupError)
	}
}

func TestStartupDiagnosticsCancellationAndExitRaceUsesOneWait(t *testing.T) {
	journal := &recordingProcessJournal{owner: testOwnerID()}
	m := supervisorTestManager(t, journal, "die")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.Start(ctx, "model-1") }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	instance := journalProcess(t, journal)
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticStartupCancelled && instance.StartupDiagnostic != corecontract.StartupDiagnosticBackendExited {
		t.Fatalf("race diagnostic = %q", instance.StartupDiagnostic)
	}
	if instance.TerminatedAt.IsZero() || instance.WaitError == "" {
		t.Fatalf("race lost wait result: %+v", instance)
	}
}

func TestHistoricalJournalWithoutStartupDiagnostics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime.json")
	id, _ := corecontract.NewID()
	processID, _ := corecontract.NewID()
	legacy := map[string]any{
		"version":          1,
		"larm_instance_id": id,
		"clean_shutdown":   true,
		"updated_at":       time.Now().UTC(),
		"records":          []any{},
		"processes": []corecontract.ProcessInstance{{
			SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: processID, OwnerInstanceID: id,
			ModelID: "model-1", ConfigFingerprint: "sha256:legacy", State: corecontract.ProcessFailed,
			RecoveryState: corecontract.RecoveryNormal,
		}},
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	j, err := inference.OpenJournal(inference.JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	instances := j.ProcessInstances()
	if len(instances) != 1 || instances[0].StartupDiagnostic != "" {
		t.Fatalf("legacy journal was reinterpreted: %+v", instances)
	}
}

func TestStartupDiagnosticSanitization(t *testing.T) {
	for _, value := range []string{"prompt=secret", "Bearer token", "/private/inference/output"} {
		if got := sanitizeStartupError(errors.New(value)); got != "startup_error" || strings.Contains(got, value) {
			t.Fatalf("sanitize(%q) = %q", value, got)
		}
	}
}

func TestStartupDiagnosticContextClassification(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	startupCtx, cancelStartup := context.WithCancel(requestCtx)
	defer cancelStartup()
	if category, _ := startupDiagnosticForContext(requestCtx, context.Background(), startupCtx); category != corecontract.StartupDiagnosticStartupCancelled {
		t.Fatalf("request cancellation category = %q", category)
	}

	timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelTimeout()
	<-timeoutCtx.Done()
	if category, _ := startupDiagnosticForContext(context.Background(), context.Background(), timeoutCtx); category != corecontract.StartupDiagnosticReadinessTimeout {
		t.Fatalf("startup deadline category = %q", category)
	}
	if category, _ := startupDiagnosticForContext(requestCtx, context.Background(), timeoutCtx); category != corecontract.StartupDiagnosticUnknown {
		t.Fatalf("simultaneous cancellation/deadline category = %q", category)
	}
}

func journalProcess(t *testing.T, journal *recordingProcessJournal) corecontract.ProcessInstance {
	t.Helper()
	instances := journal.ProcessInstances()
	if len(instances) != 1 {
		t.Fatalf("journal has %d process instances: %+v", len(instances), instances)
	}
	return instances[0]
}
