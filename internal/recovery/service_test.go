package recovery

import (
	"errors"
	"path/filepath"
	"testing"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
)

func TestUnknownInferenceCreatesDurableBlockedPlan(t *testing.T) {
	j, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(t.TempDir(), "inference.json")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := inference.Open(inference.Options{Journal: j})
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := r.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := r.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model"}); err != nil {
		t.Fatal(err)
	}
	if err := r.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkUnknown(requestID); err != nil {
		t.Fatal(err)
	}

	store := filepath.Join(t.TempDir(), "operator.json")
	s, err := Open(r, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := s.Incidents()
	if err != nil || len(incidents) == 0 {
		t.Fatalf("incidents = %#v, err=%v", incidents, err)
	}
	var incident Incident
	for _, candidate := range incidents {
		if candidate.RequestIDs != nil {
			incident = candidate
			break
		}
	}
	if incident.IncidentID == "" {
		t.Fatalf("request incident missing from %#v", incidents)
	}
	if incident.Status != StatusBlocked || incident.RequestIDs[0] != requestID || incident.ProcessAssignment != corecontract.ProcessUnverified {
		t.Fatalf("incident = %+v", incident)
	}
	plan, err := s.Plan(incident.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SafetyClassification == SafeToResolve || plan.EvidenceDigest == "" || plan.SnapshotVersion == "" {
		t.Fatalf("unsafe plan was not blocked: %+v", plan)
	}
	planAgain, err := s.Plan(incident.IncidentID)
	if err != nil || planAgain.PlanID != plan.PlanID || planAgain.EvidenceDigest != plan.EvidenceDigest {
		t.Fatalf("plan was not deterministic: %+v / %+v, %v", plan, planAgain, err)
	}
	if err := r.RequestCancellation(requestID); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkCancellationUnknown(requestID); err != nil {
		t.Fatal(err)
	}
	changedPlan, err := s.Plan(incident.IncidentID)
	if err != nil || changedPlan.PlanID == plan.PlanID {
		t.Fatalf("evidence change did not invalidate plan: %+v / %+v, %v", plan, changedPlan, err)
	}
	if err := s.Execute(Authorization{IncidentID: incident.IncidentID, PlanID: plan.PlanID, SnapshotVersion: plan.SnapshotVersion, EvidenceDigest: plan.EvidenceDigest}); !errors.Is(err, ErrPlanStale) {
		t.Fatalf("stale plan execution = %v", err)
	}
	if err := s.Execute(Authorization{IncidentID: incident.IncidentID, PlanID: changedPlan.PlanID, SnapshotVersion: changedPlan.SnapshotVersion, EvidenceDigest: changedPlan.EvidenceDigest}); !errors.Is(err, ErrAdministrativeAuthentication) {
		t.Fatalf("unauthenticated execution = %v", err)
	}
	s.path = t.TempDir() // a directory cannot receive the durable audit rename
	if err := s.Execute(Authorization{IncidentID: incident.IncidentID, PlanID: changedPlan.PlanID, SnapshotVersion: changedPlan.SnapshotVersion, EvidenceDigest: changedPlan.EvidenceDigest}); !errors.Is(err, ErrAuditPersistence) {
		t.Fatalf("audit failure did not block execution: %v", err)
	}

	restarted, err := Open(r, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := restarted.Audit()
	if err != nil || len(audit) < 4 {
		t.Fatalf("durable audit = %#v, err=%v", audit, err)
	}
}

func TestEvidenceValidatorRejectsUncorrelatedDeclarations(t *testing.T) {
	if err := ValidateEvidence(Evidence{Type: "DECLARATION", Source: "operator", Verification: EvidenceVerified}); err == nil {
		t.Fatal("uncorrelated evidence was accepted")
	}
}
