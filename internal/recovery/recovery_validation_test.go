package recovery_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"local-ai-runtime/internal/adminauth"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/recovery"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

func TestMain(m *testing.M) {
	if os.Getenv("LARM_VALIDATION_CHILD") == "1" {
		runValidationChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runValidationChild() {
	port := ""
	for i, arg := range os.Args {
		if arg == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
		}
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		http.NotFound(w, r)
	})}
	go func() { _ = server.ListenAndServe() }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	_ = server.Close()
}

// TestPositiveRecoveryValidation runs the positive path with a real child,
// real Wait observation, durable journals, the Unix socket and a signed
// capability. It deliberately uses FailTransport rather than MarkUnknown:
// MarkUnknown is the contract path that forgets process correlation.
func TestPositiveRecoveryValidation(t *testing.T) {
	f := newValidationFixture(t)
	lease, err := f.scheduler.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := waitLeaseActive(t, f.scheduler, lease.ID); err != nil {
		t.Fatal(err)
	}
	leaseStatus, err := f.scheduler.Get(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	process, ok := f.manager.CurrentProcess()
	if !ok {
		t.Fatal("runtime did not expose a ready owned process")
	}
	requestID, executionID := newValidationID(t), newValidationID(t)
	if _, err := f.registry.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: lease.ID, ModelID: "model-1"}); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.AssignProcess(requestID, process, f.port); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.StartTransport(requestID); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.FailTransport(requestID, corecontract.ErrorTransport); err != nil {
		t.Fatal(err)
	}
	unknown, err := f.registry.Get(requestID)
	if err != nil || unknown.Semantic != corecontract.SemanticUnknown || unknown.ProcessInstanceID != process.ProcessInstanceID {
		t.Fatalf("legitimate UNKNOWN state = %+v, err=%v", unknown, err)
	}

	if err := f.manager.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	terminated, ok := f.manager.ProcessInstance(process.ProcessInstanceID)
	if !ok || terminated.State != corecontract.ProcessTerminatedVerified || terminated.WaitError == "" || terminated.TerminationEvidenceID == "" || terminated.DescendantStatus != "VERIFIED_OBSERVED_SCOPE" {
		t.Fatalf("termination evidence = %+v, ok=%v", terminated, ok)
	}
	if status := f.manager.Status(); status.PID != 0 {
		t.Fatalf("runtime still owns child after Stop: %+v", status)
	}

	service, err := recovery.Open(f.registry, f.scheduler, f.sidecar, f.manager)
	if err != nil {
		t.Fatal(err)
	}
	incident, plan := validationPlan(t, service, requestID, lease.ID)
	if plan.SafetyClassification != recovery.SafeToResolve {
		t.Fatalf("positive plan = %+v; incident=%+v", plan, incident)
	}

	server, capabilityPath, privateKey := f.adminServer(t, service)
	defer server.Close()
	if _, err := adminauth.Issue(f.socket, privateKey, capabilityPath, incident.IncidentID, plan.PlanID, recovery.RecoveryReconcileAction, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := adminauth.Execute(f.socket, capabilityPath); err != nil {
		t.Fatal(err)
	}
	// The capability consumption is replayed, but the same operation key is
	// idempotent and returns its durable COMPLETE result without new effects.
	if _, err := adminauth.Execute(f.socket, capabilityPath); err != nil {
		t.Fatal(err)
	}

	got, err := f.registry.Get(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Semantic != corecontract.SemanticUnknown || got.PhysicalReconciliationState != "RECONCILED" || got.RecoveryState != corecontract.RecoveryResolved {
		t.Fatalf("reconciliation changed UNKNOWN history: %+v", got)
	}
	if got.LeaseID != lease.ID || leaseStatus.ID != lease.ID {
		t.Fatal("lease correlation was not retained")
	}
	leaseAfter, err := f.scheduler.Get(lease.ID)
	if err != nil || leaseAfter.State != scheduler.Completed {
		t.Fatalf("lease after reconciliation = %+v, err=%v", leaseAfter, err)
	}
	operation, err := service.Operation(incident.IncidentID, plan.PlanID)
	if err != nil || operation.Phase != recovery.PhaseComplete {
		t.Fatalf("operation = %+v, err=%v", operation, err)
	}

	if err := f.manager.Start(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	newProcess := f.manager.Status()
	if newProcess.State != runtime.Ready || newProcess.PID == 0 || newProcess.ProcessInstanceID == process.ProcessInstanceID {
		t.Fatalf("post-recovery Start = %+v; old=%s", newProcess, process.ProcessInstanceID)
	}
	if err := f.manager.Stop(context.Background(), "model-1"); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.MarkCleanShutdown(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.scheduler.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Reopen every durable authority and retry the same capability. The
	// operation and scheduler reconciliation record must make this a no-op.
	restartedJournal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(f.dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	restartedRegistry, err := inference.Open(inference.Options{Journal: restartedJournal})
	if err != nil {
		t.Fatal(err)
	}
	restartedManager := runtime.NewManager(f.cfg, restartedJournal)
	restartedScheduler, err := scheduler.NewWithIdempotencyStore(restartedManager, f.schedulerConfig, filepath.Join(f.dir, "scheduler.json"))
	if err != nil {
		t.Fatal(err)
	}
	restartedScheduler.SetReleaseProtector(restartedRegistry)
	restartedScheduler.SetAdmissionGate(restartedRegistry)
	restartedService, err := recovery.Open(restartedRegistry, restartedScheduler, f.sidecar, restartedManager)
	if err != nil {
		t.Fatal(err)
	}
	restartedServer, _, _ := f.adminServer(t, restartedService)
	if _, err := adminauth.Execute(f.socket, capabilityPath); err != nil {
		t.Fatal(err)
	}
	restartedOperation, err := restartedService.Operation(incident.IncidentID, plan.PlanID)
	if err != nil || restartedOperation.Phase != recovery.PhaseComplete {
		t.Fatalf("post-restart operation = %+v, err=%v", restartedOperation, err)
	}
	if err := restartedServer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restartedScheduler.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restartedManager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(f.socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("admin socket residue: %v", err)
	}
}

func TestTwoLeasesSharingOneProcessRemainBlocked(t *testing.T) {
	dir := t.TempDir()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	owner := registry.RecoveryStatus().LARMInstanceID
	processID := newValidationID(t)
	ready := corecontract.ProcessInstance{SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: processID, OwnerInstanceID: owner, LARMInstanceID: owner, ModelID: "model-1", PID: 1, PGID: 1, StartIdentity: "fixture-start", OSProcessStartIdentity: "fixture-start", ConfigFingerprint: "sha256:fixture", State: corecontract.ProcessReady, ProcessIdentityStatus: "VERIFIED", OwnershipStatus: "OWNED_VERIFIED", TerminationStatus: "NOT_REQUESTED", DescendantStatus: "SUPERVISED_GROUP", ReadinessEvidenceID: newValidationID(t), RecoveryState: corecontract.RecoveryNormal, UpdatedAt: time.Now().UTC()}
	for range 2 {
		requestID, executionID, leaseID := newValidationID(t), newValidationID(t), newValidationID(t)
		if _, err := registry.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model-1"}); err != nil {
			t.Fatal(err)
		}
		if err := registry.AdmissionAccepted(requestID); err != nil {
			t.Fatal(err)
		}
		if err := registry.AssignProcess(requestID, ready, 1); err != nil {
			t.Fatal(err)
		}
		if err := registry.FailTransport(requestID, corecontract.ErrorTransport); err != nil {
			t.Fatal(err)
		}
	}
	terminated := ready
	terminated.State = corecontract.ProcessTerminatedVerified
	terminated.TerminationStatus = "TERMINATED_VERIFIED"
	terminated.OwnershipStatus = "OWNED_VERIFIED"
	terminated.TerminatedAt = time.Now().UTC()
	terminated.WaitError = "exited_zero"
	terminated.DescendantStatus = "VERIFIED_OBSERVED_SCOPE"
	evidence := corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: newValidationID(t), SubjectKind: corecontract.SubjectProcess, SubjectID: processID, ProcessInstanceID: processID, Source: "fixture-supervisor", Type: corecontract.EvidenceProcessTerminationVerified, ObservedAt: time.Now().UTC(), Result: corecontract.EvidenceSuccess, CorrelationID: processID, Guarantee: corecontract.GuaranteeProcess, ProcessAbsent: true, ProcessIdentityVerified: true}
	terminated.TerminationEvidenceID = evidence.EvidenceID
	terminated.Evidence = []corecontract.TerminalEvidence{evidence}
	service, err := recovery.Open(registry, nil, filepath.Join(dir, "recovery.json"), staticProcessProvider{process: terminated})
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := service.Incidents()
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, incident := range incidents {
		for _, reason := range incident.BlockingReasons {
			if strings.Contains(reason, "multiple leases") {
				seen = true
				plan, err := service.Plan(incident.IncidentID)
				if err != nil {
					t.Fatal(err)
				}
				if plan.SafetyClassification == recovery.SafeToResolve {
					t.Fatalf("shared-process topology was incorrectly eligible: %+v", plan)
				}
			}
		}
	}
	if !seen {
		t.Fatalf("shared-process contradiction was not recorded: %+v", incidents)
	}
}

type staticProcessProvider struct{ process corecontract.ProcessInstance }

func (p staticProcessProvider) ProcessInstances() []corecontract.ProcessInstance {
	return []corecontract.ProcessInstance{p.process}
}

type validationFixture struct {
	t               *testing.T
	dir             string
	port            int
	registry        *inference.Registry
	manager         *runtime.Manager
	scheduler       *scheduler.Scheduler
	cfg             config.Config
	schedulerConfig config.SchedulerConfig
	sidecar         string
	socket          string
	publicKey       string
	privateKey      string
}

func newValidationFixture(t *testing.T) *validationFixture {
	t.Helper()
	dir := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	modelPath := filepath.Join(dir, "fixture.gguf")
	if err := os.WriteFile(modelPath, []byte("fixture; never executed as GGUF"), 0600); err != nil {
		t.Fatal(err)
	}
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LARM_VALIDATION_CHILD", "1")
	cfg := config.Config{Runtime: config.RuntimeConfig{Backend: "llamacpp", Binary: binary, StartupTimeout: time.Second.String(), ShutdownTimeout: time.Second.String()}, Models: map[string]config.ModelConfig{"model-1": {Path: modelPath, Port: port, Context: 1, Batch: 1, UBatch: 1, GPULayers: "all", Parallel: 1}}}
	manager := runtime.NewManager(cfg, journal)
	store := filepath.Join(dir, "scheduler.json")
	sched, err := scheduler.NewWithIdempotencyStore(manager, config.SchedulerConfig{MaxQueueSize: 4, LeaseTTL: "10s", Heartbeat: "1s", MaxLease: "1m"}, store)
	if err != nil {
		t.Fatal(err)
	}
	sched.SetReleaseProtector(registry)
	sched.SetAdmissionGate(registry)
	socketDir, err := os.MkdirTemp("/tmp", "larm-v-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	t.Cleanup(func() {
		_ = sched.Shutdown(context.Background())
		_ = manager.Shutdown(context.Background())
	})
	return &validationFixture{t: t, dir: dir, port: port, registry: registry, manager: manager, scheduler: sched, cfg: cfg, schedulerConfig: config.SchedulerConfig{MaxQueueSize: 4, LeaseTTL: "10s", Heartbeat: "1s", MaxLease: "1m"}, sidecar: filepath.Join(dir, "recovery.json"), socket: filepath.Join(socketDir, "admin.sock")}
}

func waitLeaseActive(t *testing.T, sched *scheduler.Scheduler, id string) error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := sched.Get(id)
		if err != nil {
			return err
		}
		if status.State == scheduler.Active {
			return nil
		}
		if status.State == scheduler.Failed || status.State == scheduler.Cancelled {
			return fmt.Errorf("lease failed before activation: %+v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("lease did not become active")
}

func validationPlan(t *testing.T, service *recovery.Service, requestID, leaseID string) (recovery.Incident, recovery.RecoveryPlan) {
	t.Helper()
	incidents, err := service.Incidents()
	if err != nil {
		t.Fatal(err)
	}
	for _, incident := range incidents {
		if len(incident.RequestIDs) == 1 && incident.RequestIDs[0] == requestID && len(incident.LeaseIDs) == 1 && incident.LeaseIDs[0] == leaseID {
			plan, err := service.Plan(incident.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			return incident, plan
		}
	}
	t.Fatalf("correlated incident missing: %+v", incidents)
	return recovery.Incident{}, recovery.RecoveryPlan{}
}

func (f *validationFixture) adminServer(t *testing.T, service *recovery.Service) (*adminauth.Server, string, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicPath, privatePath := f.publicKey, f.privateKey
	if publicPath == "" {
		publicPath, privatePath = filepath.Join(f.dir, "public.json"), filepath.Join(f.dir, "private.json")
		writeValidationJSON(t, publicPath, map[string]any{"version": 1, "keys": map[string]string{"fixture": base64.RawStdEncoding.EncodeToString(public)}})
		writeValidationJSON(t, privatePath, map[string]any{"version": 1, "key_id": "fixture", "private_key": base64.RawStdEncoding.EncodeToString(private)})
		f.publicKey, f.privateKey = publicPath, privatePath
	}
	uid := os.Geteuid()
	server, err := adminauth.NewServer(config.AdminConfig{Enabled: true, SocketPath: f.socket, PublicKeyPath: publicPath, AllowedUID: &uid, Audience: "larm-admin", CapabilityTTL: "5m"}, f.registry.RecoveryStatus().LARMInstanceID, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	return server, filepath.Join(f.dir, "capability.json"), privatePath
}

func writeValidationJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func newValidationID(t *testing.T) string {
	t.Helper()
	id, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
