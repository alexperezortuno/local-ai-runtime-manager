package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAdmissionDecisionObservability records the current observable surface
// without adding a new metrics subsystem. FIT is observable through the
// evaluator and lease lifecycle; INSUFFICIENT and UNKNOWN remain observable
// through the evaluator and, when terminal, through the typed lease error.
func TestAdmissionDecisionObservability(t *testing.T) {
	t.Run("fit", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
		evaluator := newControlEvaluator(AdmissionFit)
		controlAdmission(t, s, evaluator, AdmissionOptions{})
		lease, err := s.RequestWithRequestID("observe-fit", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, lease.ID, Active)
		if evaluator.count() != 1 {
			t.Fatalf("FIT observations = %d, want 1", evaluator.count())
		}
	})

	t.Run("insufficient", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
		evaluator := newControlEvaluator(AdmissionInsufficient)
		controlAdmission(t, s, evaluator, AdmissionOptions{OnInsufficient: "reject"})
		lease, err := s.RequestWithRequestID("observe-insufficient", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, lease.ID, Cancelled)
		failure := s.AdmissionFailure(lease.ID)
		if failure == nil || failure.Kind != AdmissionErrorCapacity {
			t.Fatalf("failure = %+v, want capacity", failure)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
		evaluator := newControlEvaluator(AdmissionUnknown)
		evaluator.cause = "snapshot unavailable"
		controlAdmission(t, s, evaluator, AdmissionOptions{OnUnknown: "reject"})
		lease, err := s.RequestWithRequestID("observe-unknown", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, lease.ID, Cancelled)
		failure := s.AdmissionFailure(lease.ID)
		if failure == nil || failure.Kind != AdmissionErrorUnavailable {
			t.Fatalf("failure = %+v, want unavailable", failure)
		}
	})

	t.Run("timeout and cancellation", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
		evaluator := newControlEvaluator(AdmissionInsufficient)
		controlAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: 40 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
		lease, err := s.RequestWithRequestID("observe-timeout", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, lease.ID, Cancelled)
		failure := s.AdmissionFailure(lease.ID)
		if failure == nil || failure.Kind != AdmissionErrorTimeout {
			t.Fatalf("failure = %+v, want timeout", failure)
		}
	})

	t.Run("reevaluation", func(t *testing.T) {
		_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
		evaluator := newControlEvaluator(AdmissionInsufficient)
		controlAdmission(t, s, evaluator, AdmissionOptions{})
		lease, err := s.RequestWithRequestID("observe-reevaluation", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return evaluator.count() >= 2 })
		requests := evaluator.requests()
		if requests[0].LeaseID != lease.ID || requests[1].LeaseID != lease.ID {
			t.Fatalf("reevaluation observations = %+v", requests[:2])
		}
	})

	t.Run("cause remains in evaluator evidence", func(t *testing.T) {
		evaluator := newControlEvaluator(AdmissionUnknown)
		evaluator.cause = "snapshot unavailable"
		evidence, err := evaluator.Evaluate(context.Background(), AdmissionRequest{ModelID: "model-1", RequestID: "request-1", LeaseID: "lease-1", Operation: AdmissionOperationNewLoad})
		if err != nil {
			t.Fatal(err)
		}
		if evidence.Verdict != AdmissionUnknown || !strings.Contains(evidence.Cause, "snapshot unavailable") {
			t.Fatalf("evidence = %+v", evidence)
		}
	})
}
