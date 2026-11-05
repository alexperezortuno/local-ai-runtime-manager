package recovery_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/recovery"
)

// TestRealSIGKILLCrashRecovery is intentionally separate from the normal test
// suite. It builds an larmtest binary, kills only its verified PID, and then
// reconstructs the same temporary stores. Set LARM_REAL_CRASH=1 to run it.
func TestRealSIGKILLCrashRecovery(t *testing.T) {
	if os.Getenv("LARM_REAL_CRASH") != "1" {
		t.Skip("real SIGKILL validation is opt-in; run LARM_REAL_CRASH=1 go test ./internal/recovery -run TestRealSIGKILLCrashRecovery -count=1 -v")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("this validation is macOS-only")
	}
	fixtureBinary, backendBinary := buildCrashBinaries(t)
	cases := []struct {
		name   string
		target string
	}{
		{"CRASH-01-before-prepared", "BEFORE_PREPARED"},
		{"CRASH-02-after-prepared", "PREPARED"},
		{"CRASH-03-after-reconciling", "RECONCILING"},
		{"CRASH-04-after-runtime", "RUNTIME_COMMITTED"},
		{"CRASH-05-after-registry", "REGISTRY_COMMITTED"},
		{"CRASH-06-after-scheduler", "SCHEDULER_COMMITTED"},
		{"CRASH-07-after-audit-pending", "AUDIT_PENDING"},
		{"CRASH-08-after-complete-before-response", "COMPLETE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runRealCrashCase(t, fixtureBinary, backendBinary, tc.target) })
	}
	t.Run("SURVIVOR-process-is-not-adopted", func(t *testing.T) { runSurvivorCase(t, fixtureBinary, backendBinary) })
}

type crashMetadata struct {
	LARMProcessID int    `json:"larm_process_id"`
	BackendPID    int    `json:"backend_pid"`
	BackendBinary string `json:"backend_binary"`
	Journal       string `json:"journal"`
	Socket        string `json:"socket"`
	Capability    string `json:"capability"`
	IncidentID    string `json:"incident_id"`
	PlanID        string `json:"plan_id"`
	RequestID     string `json:"request_id"`
	LeaseID       string `json:"lease_id"`
	ProcessID     string `json:"process_instance_id"`
	Target        string `json:"target"`
}

type crashResult struct {
	BeforeAdmissionBlocked bool                             `json:"before_admission_blocked"`
	Operation              recovery.ReconciliationOperation `json:"operation"`
	Inference              inference.Snapshot               `json:"inference"`
	Lease                  map[string]any                   `json:"lease"`
	OldProcess             corecontract.ProcessInstance     `json:"old_process"`
	NewProcessInstanceID   string                           `json:"new_process_instance_id"`
	CleanShutdown          bool                             `json:"clean_shutdown"`
	RuntimeStartAfterRetry bool                             `json:"runtime_start_after_retry"`
	SurvivorNotAdopted     bool                             `json:"survivor_not_adopted"`
	SurvivorState          string                           `json:"survivor_state"`
}

func runRealCrashCase(t *testing.T, fixtureBinary, backendBinary, target string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("/private/tmp", "larm-crash-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	metaPath := filepath.Join(dir, "metadata.json")
	marker := filepath.Join(dir, "phase.marker")
	release := filepath.Join(dir, "release.marker")
	// Darwin's AF_UNIX path limit is short; the directory is already unique.
	socket := filepath.Join(socketDir, "s")
	env := []string{
		"LARM_REAL_CRASH_MODE=run", "LARM_REAL_CRASH_DIR=" + dir,
		"LARM_REAL_CRASH_SOCKET=" + socket, "LARM_REAL_CRASH_MARKER=" + marker,
		"LARM_REAL_CRASH_RELEASE=" + release, "LARM_REAL_CRASH_BACKEND=" + backendBinary,
		"LARM_REAL_CRASH_TARGET=" + target, "LARM_REAL_CRASH_META=" + metaPath,
	}
	cmd := exec.Command(fixtureBinary)
	cmd.Env = append(os.Environ(), env...)
	var output strings.Builder
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	meta := waitJSON[crashMetadata](t, metaPath, cmd, &output)
	if meta.Target != target || meta.Journal == "" || !under(dir, meta.Journal) || !under(dir, meta.Capability) || !under(socketDir, meta.Socket) {
		t.Fatalf("fixture isolation metadata invalid: %+v", meta)
	}
	if meta.Socket == "/tmp/larm.sock" || meta.Socket == "/var/run/larm/admin.sock" {
		t.Fatalf("fixture used an operational socket: %s", meta.Socket)
	}
	if !verifyProcessIdentity(t, cmd.Process.Pid, fixtureBinary) {
		t.Fatalf("fixture PID %d is not the expected LARM process", cmd.Process.Pid)
	}
	backendVerified := !processExists(meta.BackendPID) || verifyProcessIdentity(t, meta.BackendPID, backendBinary)
	if meta.LARMProcessID != cmd.Process.Pid || !backendVerified {
		t.Fatalf("fixture process ownership could not be verified: metadata=%+v", meta)
	}
	waitForFile(t, marker, 15*time.Second)
	if cmd.ProcessState != nil {
		t.Fatal("fixture terminated before the crash boundary")
	}
	if !verifyProcessIdentity(t, cmd.Process.Pid, fixtureBinary) {
		t.Fatal("fixture PID identity changed before SIGKILL")
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("fixture exited normally after SIGKILL")
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("fixture was not terminated by SIGKILL: %v", cmd.ProcessState)
	}
	if err := cleanupOwnedProcess(meta.BackendPID, backendBinary); err != nil {
		t.Fatal(err)
	}
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: meta.Journal})
	if err != nil {
		t.Fatal(err)
	}
	if journal.Meta().CleanShutdown {
		t.Fatal("SIGKILL unexpectedly produced a clean journal")
	}
	var preserved bool
	for _, snapshot := range journal.Records() {
		if snapshot.RequestID == meta.RequestID {
			preserved = snapshot.Semantic == corecontract.SemanticUnknown && snapshot.ProcessInstanceID == meta.ProcessID
		}
	}
	if !preserved {
		t.Fatal("UNKNOWN history or process correlation was lost before restart")
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	restartSocket := filepath.Join(socketDir, "r")
	ready := filepath.Join(dir, "restart.ready")
	resultPath := filepath.Join(dir, "result.json")
	restart := exec.Command(fixtureBinary)
	restart.Env = append(os.Environ(), "LARM_REAL_CRASH_MODE=restart", "LARM_REAL_CRASH_DIR="+dir, "LARM_REAL_CRASH_SOCKET="+restartSocket, "LARM_REAL_CRASH_BACKEND="+backendBinary, "LARM_REAL_CRASH_READY="+ready, "LARM_REAL_CRASH_RESULT="+resultPath)
	var restartOutput strings.Builder
	restart.Stdout, restart.Stderr = &restartOutput, &restartOutput
	if err := restart.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, ready, 15*time.Second)
	result := waitJSON[crashResult](t, resultPath, restart, &restartOutput)
	if err := restart.Wait(); err != nil {
		t.Fatalf("restart fixture failed: %v\n%s", err, restartOutput.String())
	}
	if target != "COMPLETE" && !result.BeforeAdmissionBlocked || result.Operation.Phase != recovery.PhaseComplete || result.Inference.Semantic != corecontract.SemanticUnknown || result.Inference.PhysicalReconciliationState != "RECONCILED" || result.Inference.ProcessInstanceID != meta.ProcessID {
		t.Fatalf("unsafe or incomplete reconstruction result: %+v", result)
	}
	if result.Lease["state"] != "COMPLETED" || !result.RuntimeStartAfterRetry || result.NewProcessInstanceID == "" || result.NewProcessInstanceID == meta.ProcessID || !result.CleanShutdown {
		t.Fatalf("post-recovery lifecycle result: %+v", result)
	}
	if result.OldProcess.State != corecontract.ProcessTerminatedVerified || result.OldProcess.WaitError == "" || result.OldProcess.TerminationEvidenceID == "" || result.OldProcess.DescendantStatus != "VERIFIED_OBSERVED_SCOPE" {
		t.Fatalf("physical termination evidence was not retained: %+v", result.OldProcess)
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale fixture socket remained after external cleanup: %v", err)
	}
	if _, err := os.Stat(restartSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart socket residue: %v", err)
	}
	if strings.Contains(output.String()+restartOutput.String(), "GGUF") {
		t.Fatal("real GGUF execution appeared in crash fixture output")
	}
}

func runSurvivorCase(t *testing.T, fixtureBinary, backendBinary string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("/private/tmp", "larm-crash-")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(socketDir, 0700)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	metaPath, marker := filepath.Join(dir, "metadata.json"), filepath.Join(dir, "phase.marker")
	cmd := exec.Command(fixtureBinary)
	cmd.Env = append(os.Environ(), "LARM_REAL_CRASH_MODE=run", "LARM_REAL_CRASH_SURVIVOR=1", "LARM_REAL_CRASH_DIR="+dir, "LARM_REAL_CRASH_SOCKET="+filepath.Join(socketDir, "s"), "LARM_REAL_CRASH_MARKER="+marker, "LARM_REAL_CRASH_RELEASE="+filepath.Join(dir, "release"), "LARM_REAL_CRASH_BACKEND="+backendBinary, "LARM_REAL_CRASH_TARGET=SURVIVOR", "LARM_REAL_CRASH_META="+metaPath)
	var output strings.Builder
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	meta := waitJSON[crashMetadata](t, metaPath, cmd, &output)
	waitForFile(t, marker, 15*time.Second)
	if !verifyProcessIdentity(t, cmd.Process.Pid, fixtureBinary) || !verifyProcessIdentity(t, meta.BackendPID, backendBinary) {
		t.Fatalf("survivor identities were not verified: %+v", meta)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("survivor fixture exited normally after SIGKILL")
	}
	status, _ := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("survivor fixture was not SIGKILLed: %v", cmd.ProcessState)
	}
	if !processExists(meta.BackendPID) || !verifyProcessIdentity(t, meta.BackendPID, backendBinary) {
		t.Fatal("backend survivor was not observable with verified identity")
	}
	restart := exec.Command(fixtureBinary)
	resultPath, ready := filepath.Join(dir, "result.json"), filepath.Join(dir, "restart.ready")
	restart.Env = append(os.Environ(), "LARM_REAL_CRASH_MODE=restart", "LARM_REAL_CRASH_DIR="+dir, "LARM_REAL_CRASH_SOCKET="+filepath.Join(socketDir, "r"), "LARM_REAL_CRASH_BACKEND="+backendBinary, "LARM_REAL_CRASH_READY="+ready, "LARM_REAL_CRASH_RESULT="+resultPath)
	var restartOutput strings.Builder
	restart.Stdout, restart.Stderr = &restartOutput, &restartOutput
	if err := restart.Start(); err != nil {
		t.Fatal(err)
	}
	result := waitJSON[crashResult](t, resultPath, restart, &restartOutput)
	if err := restart.Wait(); err != nil {
		t.Fatalf("survivor restart failed: %v\n%s", err, restartOutput.String())
	}
	if !result.BeforeAdmissionBlocked || !result.SurvivorNotAdopted || result.RuntimeStartAfterRetry {
		t.Fatalf("survivor was adopted or admitted: %+v", result)
	}
	if err := cleanupOwnedProcess(meta.BackendPID, backendBinary); err != nil {
		t.Fatal(err)
	}
}

func buildCrashBinaries(t *testing.T) (string, string) {
	t.Helper()
	root := moduleRoot(t)
	dir := t.TempDir()
	fixture := filepath.Join(dir, "larm-fixture")
	backend := filepath.Join(dir, "backend-fixture")
	build := func(output, pkg, tags string) {
		args := []string{"build", "-o", output}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		args = append(args, pkg)
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		if data, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, data)
		}
	}
	build(fixture, "./cmd/larm", "larmtest")
	build(backend, "./cmd/larm-test-backend", "")
	return fixture, backend
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitJSON[T any](t *testing.T, path string, cmd *exec.Cmd, output *strings.Builder) T {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastJSONErr error
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var value T
			if err := json.Unmarshal(data, &value); err == nil {
				return value
			} else {
				lastJSONErr = err
			}
		}
		if processExited(cmd.Process.Pid) {
			// The fixture publishes its result immediately before exiting. Re-read
			// once after observing exit so a rename racing this poll is not reported
			// as a missing result.
			data, readErr := os.ReadFile(path)
			if readErr == nil {
				var value T
				if err := json.Unmarshal(data, &value); err == nil {
					return value
				} else {
					lastJSONErr = err
				}
			}
			fixtureError, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "fixture.error"))
			waitErr := cmd.Wait()
			t.Fatalf("fixture exited before writing %s\nwait: %v\nread: %v\njson: %v\ndata: %q\n%s\nfixture: %s", path, waitErr, readErr, lastJSONErr, data, output.String(), fixtureError)
		}
		if cmd.ProcessState != nil {
			t.Fatalf("fixture exited before writing %s: %v\n%s", path, cmd.ProcessState, output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	t.Fatalf("timed out waiting for JSON %s", path)
	var zero T
	return zero
}

func verifyProcessIdentity(t *testing.T, pid int, expected string) bool {
	t.Helper()
	if pid <= 0 {
		return false
	}
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	data, err := cmd.Output()
	if err != nil {
		return false
	}
	line := strings.TrimSpace(string(data))
	return strings.Contains(line, expected)
}

func cleanupOwnedProcess(pid int, expected string) error {
	if pid <= 0 || !processExists(pid) {
		return nil
	}
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	data, err := cmd.Output()
	if err != nil || !strings.Contains(string(data), expected) {
		return fmt.Errorf("refusing cleanup signal for unverified backend PID %d", pid)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && processExists(pid) {
		time.Sleep(20 * time.Millisecond)
	}
	if processExists(pid) {
		return fmt.Errorf("backend PID %d did not terminate after verified SIGTERM", pid)
	}
	return nil
}

func processExists(pid int) bool { return syscall.Kill(pid, 0) == nil }

func processExited(pid int) bool {
	data, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
	return err != nil || strings.HasPrefix(strings.TrimSpace(string(data)), "Z")
}

func under(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
