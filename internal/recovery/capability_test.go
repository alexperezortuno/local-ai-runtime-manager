package recovery

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
)

func capabilityFixture(t *testing.T) (*Service, Capability, CapabilityPolicy, ed25519.PrivateKey) {
	t.Helper()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(t.TempDir(), "inference.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := registry.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := registry.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkUnknown(requestID); err != nil {
		t.Fatal(err)
	}
	service, err := Open(registry, nil, filepath.Join(t.TempDir(), "operator.json"))
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := service.Incidents()
	if err != nil || len(incidents) == 0 {
		t.Fatalf("incidents = %#v, err=%v", incidents, err)
	}
	var chosen Incident
	for _, candidate := range incidents {
		if len(candidate.RequestIDs) > 0 {
			chosen = candidate
			break
		}
	}
	if chosen.IncidentID == "" {
		t.Fatalf("no request incident in %#v", incidents)
	}
	plan, err := service.Plan(chosen.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	plan.SafetyClassification = SafeToResolve
	plan.BlockingReasons = nil
	service.plans[plan.PlanID] = plan
	incident := service.incidents[chosen.IncidentID]
	incident.Status = StatusReadyForReview
	service.incidents[chosen.IncidentID] = incident
	if err := service.persistLocked(); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	policy := CapabilityPolicy{Issuer: CapabilityIssuer, Audience: "test-audience", InstanceID: registry.RecoveryStatus().LARMInstanceID, AllowedUID: 123, AllowedKeys: map[string]ed25519.PublicKey{"key-1": public}, AllowedActions: map[string]bool{RecoveryReconcileAction: true}}
	now := time.Now().UTC().Truncate(time.Millisecond)
	capability, err := service.PrepareCapability(CapabilityRequest{IncidentID: chosen.IncidentID, PlanID: plan.PlanID, AllowedOperation: RecoveryReconcileAction, TTL: time.Minute}, "uid:123", policy, now)
	if err != nil {
		t.Fatal(err)
	}
	capability, err = capability.Sign(private, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	return service, capability, policy, private
}

func TestCapabilityValidatesScopeAndSignature(t *testing.T) {
	service, capability, policy, private := capabilityFixture(t)
	result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC())
	if result.Decision != Authorized {
		t.Fatalf("decision = %+v", result)
	}
	capability.EvidenceDigest = "tampered"
	result = service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC())
	if result.Decision != InvalidSignature {
		t.Fatalf("tampered decision = %+v", result)
	}
	capability.EvidenceDigest = "different"
	capability, err := capability.Sign(private, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC()); result.Decision != StalePlan {
		t.Fatalf("stale plan decision = %+v", result)
	}
}

func TestCapabilityExpiryStaleAndOperator(t *testing.T) {
	service, capability, policy, _ := capabilityFixture(t)
	if result := service.ValidateCapability(capability, "uid:999", policy, time.Now().UTC()); result.Decision != InvalidScope {
		t.Fatalf("operator decision = %+v", result)
	}
	if result := service.ValidateCapability(capability, "uid:123", policy, capability.ExpiresAt); result.Decision != Expired {
		t.Fatalf("expiry decision = %+v", result)
	}
}

func TestCapabilityReservationIsDurableAndIdempotent(t *testing.T) {
	service, capability, policy, _ := capabilityFixture(t)
	first, err := service.ReserveCapability(capability, "uid:123", policy, time.Now().UTC())
	if err != nil || first.Decision != Authorized {
		t.Fatalf("first reservation = %+v, err=%v", first, err)
	}
	second, err := service.ReserveCapability(capability, "uid:123", policy, time.Now().UTC())
	if err != nil || second.Decision != Authorized {
		t.Fatalf("idempotent reservation = %+v, err=%v", second, err)
	}
	if _, err := service.ConsumeCapability(capability, "uid:123", policy, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if result, err := service.ConsumeCapability(capability, "uid:123", policy, time.Now().UTC()); err != nil || result.Decision != Replayed {
		t.Fatalf("consume replay = %+v, err=%v", result, err)
	}
	reopened, err := Open(service.registry, nil, service.path)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := reopened.ReserveCapability(capability, "uid:123", policy, time.Now().UTC()); err != nil || result.Decision != Authorized {
		t.Fatalf("restart reservation = %+v, err=%v", result, err)
	}
}

func TestCapabilityConcurrentReservationAndRevocation(t *testing.T) {
	service, capability, policy, _ := capabilityFixture(t)
	var group sync.WaitGroup
	results := make(chan CapabilityResult, 16)
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, _ := service.ReserveCapability(capability, "uid:123", policy, time.Now().UTC())
			results <- result
		}()
	}
	group.Wait()
	close(results)
	for result := range results {
		if result.Decision != Authorized {
			t.Fatalf("concurrent reservation = %+v", result)
		}
	}
	if err := service.RevokeNonce(capability.Nonce); err != nil {
		t.Fatal(err)
	}
	if result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC()); result.Decision != Replayed {
		t.Fatalf("revoked capability = %+v", result)
	}
}

func TestBlockedRecoveryCapabilityIsNotExecutableAndRejectionIsDurable(t *testing.T) {
	service, capability, policy, _ := capabilityFixture(t)
	incident, err := service.Incident(capability.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	plan := service.plans[capability.PlanID]
	plan.SafetyClassification = BlockedInconsistentState
	service.plans[plan.PlanID] = plan
	if err := service.persistLocked(); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()

	if _, err := service.PrepareCapability(CapabilityRequest{IncidentID: capability.IncidentID, PlanID: capability.PlanID, AllowedOperation: RecoveryReconcileAction, TTL: time.Minute}, "uid:123", policy, time.Now().UTC()); !errors.Is(err, errReconciliationIneligible) {
		t.Fatalf("blocked prepare error = %v", err)
	}
	if result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC()); result.Decision != Rejected {
		t.Fatalf("blocked validation = %+v", result)
	}
	if result, err := service.ReserveCapability(capability, "uid:123", policy, time.Now().UTC()); err != nil || result.Decision != Rejected {
		t.Fatalf("blocked reservation = %+v, err=%v", result, err)
	}
	service.mu.Lock()
	_, reserved := service.capabilities[capability.CapabilityID]
	service.mu.Unlock()
	if reserved {
		t.Fatal("blocked capability was inserted into reservations")
	}

	auth := Authorization{IncidentID: capability.IncidentID, PlanID: capability.PlanID, SnapshotVersion: capability.SnapshotVersion, EvidenceDigest: capability.EvidenceDigest, OperatorID: "uid:123", Authenticated: true, Mechanism: "unix-peer+ed25519", CapabilityID: capability.CapabilityID, Action: RecoveryReconcileAction}
	if err := service.Execute(auth); !errors.Is(err, errReconciliationIneligible) {
		t.Fatalf("blocked execution error = %v", err)
	}
	if _, err := service.Operation(capability.IncidentID, capability.PlanID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("blocked execution created operation: %v", err)
	}
	if err := service.Execute(auth); !errors.Is(err, errReconciliationIneligible) {
		t.Fatalf("duplicate blocked execution error = %v", err)
	}
	audit, err := service.Audit()
	if err != nil {
		t.Fatal(err)
	}
	var rejected *AuditEvent
	reservedEvents := 0
	for i := range audit {
		if audit[i].Action == "RECONCILIATION_REJECTED" {
			copy := audit[i]
			rejected = &copy
		}
		if audit[i].Action == "CAPABILITY_RESERVED" {
			reservedEvents++
		}
	}
	if rejected == nil {
		t.Fatalf("missing durable reconciliation rejection: %+v", audit)
	}
	if rejected.IncidentID != incident.IncidentID || rejected.PlanID != capability.PlanID || rejected.EvidenceDigest != capability.EvidenceDigest || rejected.CapabilityID != capability.CapabilityID || rejected.InstanceID != incident.LARMInstanceID || rejected.OperatorIdentity != "uid:123" || rejected.AuthorizationMechanism != "unix-peer+ed25519" || rejected.TechnicalClassification != BlockedInconsistentState {
		t.Fatalf("rejection audit correlation = %+v", *rejected)
	}
	if reservedEvents != 0 {
		t.Fatalf("blocked reservation audit = %d", reservedEvents)
	}
	reopened, err := Open(service.registry, nil, service.path)
	if err != nil {
		t.Fatal(err)
	}
	reopenedAudit, err := reopened.Audit()
	if err != nil {
		t.Fatal(err)
	}
	rejectedCount := 0
	for _, event := range reopenedAudit {
		if event.Action == "RECONCILIATION_REJECTED" {
			rejectedCount++
		}
	}
	if rejectedCount != 1 {
		t.Fatalf("durable rejection count = %d, audit = %+v", rejectedCount, reopenedAudit)
	}
}

func TestBlockedReconciliationAuditFailureFailsClosed(t *testing.T) {
	service, capability, _, _ := capabilityFixture(t)
	service.mu.Lock()
	plan := service.plans[capability.PlanID]
	plan.SafetyClassification = BlockedInconsistentState
	service.plans[plan.PlanID] = plan
	if err := service.persistLocked(); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()
	service.path = t.TempDir()
	err := service.Execute(Authorization{IncidentID: capability.IncidentID, PlanID: capability.PlanID, SnapshotVersion: capability.SnapshotVersion, EvidenceDigest: capability.EvidenceDigest, OperatorID: "uid:123", Authenticated: true, Mechanism: "unix-peer+ed25519", CapabilityID: capability.CapabilityID, Action: RecoveryReconcileAction})
	if !errors.Is(err, ErrAuditPersistence) {
		t.Fatalf("audit failure = %v", err)
	}
	service.mu.Lock()
	_, created := service.operations[reconciliationOperationKey(capability.IncidentID, capability.PlanID, capability.EvidenceDigest)]
	service.mu.Unlock()
	if created {
		t.Fatal("audit failure created operation")
	}
}

func TestModelStopCapabilityIsTargetedAndReplayProtected(t *testing.T) {
	service, _, oldPolicy, _ := capabilityFixture(t)
	public, generatedPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	policy := oldPolicy
	policy.AllowedKeys = map[string]ed25519.PublicKey{"key-stop": public}
	policy.AllowedActions = map[string]bool{ModelStopAction: true}
	capability, err := service.PrepareCapability(CapabilityRequest{AllowedOperation: ModelStopAction, ModelID: "model-1", TTL: time.Minute}, "uid:123", policy, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	capability, err = capability.Sign(generatedPrivate, "key-stop")
	if err != nil {
		t.Fatal(err)
	}
	if result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC()); result.Decision != Authorized {
		t.Fatalf("model stop authorization = %+v", result)
	}
	capability.ModelID = "model-2"
	if result := service.ValidateCapability(capability, "uid:123", policy, time.Now().UTC()); result.Decision != InvalidSignature {
		t.Fatalf("retargeted capability = %+v", result)
	}
}
