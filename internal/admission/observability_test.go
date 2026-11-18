package admission

import (
	"testing"
	"time"

	"local-ai-runtime/internal/admission/observe"
	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/scheduler"
)

// TestWiringInstallsAdmissionRecorder verifies that the wiring installs the
// admission observability recorder on the Scheduler.
func TestWiringInstallsAdmissionRecorder(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &controlPlanner{verdict: capacity.Fit}
	wiring, err := InstallFromConfig(sched, planner, config.AdmissionConfig{Enabled: true, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "2s", ReevaluationInterval: "5ms", MaxSnapshotAge: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	defer wiring.Close()
	recorder := wiring.Recorder()
	if recorder == nil {
		t.Fatal("the wiring did not build a recorder")
	}
	if sched.AdmissionRecorder() != recorder {
		t.Fatal("the recorder was not installed on the scheduler")
	}
}

// TestDisabledAdmissionInstallsNoRecorder verifies that a disabled admission
// installs nothing: no recorder, no evaluator and no enforcement event.
func TestDisabledAdmissionInstallsNoRecorder(t *testing.T) {
	sched := wiringScheduler(t)
	wiring, err := InstallFromConfig(sched, &controlPlanner{verdict: capacity.Fit}, config.AdmissionConfig{Enabled: false, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "5m", ReevaluationInterval: "2s", MaxSnapshotAge: "6s"})
	if err != nil {
		t.Fatal(err)
	}
	if wiring != nil {
		t.Fatal("disabled admission installed a wiring")
	}
	if sched.AdmissionRecorder() != nil {
		t.Fatal("disabled admission installed a recorder")
	}
}

// TestEnforcementProducesAdmissionEvents verifies that the real evaluator and
// adapter produce the admission events through the Scheduler enforcement path.
func TestEnforcementProducesAdmissionEvents(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &controlPlanner{verdict: capacity.Insufficient}
	wiring, err := InstallFromConfig(sched, planner, config.AdmissionConfig{Enabled: true, OnInsufficient: "reject", OnUnknown: "allow", WaitTimeout: "2s", ReevaluationInterval: "5ms", MaxSnapshotAge: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	defer wiring.Close()
	recorder := wiring.Recorder()

	if _, err := sched.RequestWithRequestID("admission-observe", "model-1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if recorder.Metrics().Evaluations["INSUFFICIENT"] > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	metrics := recorder.Metrics()
	if metrics.Evaluations["INSUFFICIENT"] == 0 {
		t.Fatalf("no INSUFFICIENT evaluation was recorded: %+v", metrics)
	}
	if metrics.Admitted != 0 {
		t.Fatalf("a rejected request was counted as admitted: %+v", metrics)
	}
	var found bool
	for _, event := range recorder.Events() {
		if event.Type == observe.EventEvaluation && event.Verdict == "INSUFFICIENT" {
			found = true
			if event.Outcome != observe.OutcomeRejected {
				t.Fatalf("outcome = %q, want rejected", event.Outcome)
			}
		}
	}
	if !found {
		t.Fatalf("no INSUFFICIENT evaluation event was retained: %+v", recorder.Events())
	}
}

// TestUnknownAllowProducesPolicyAuthorizedEvent verifies the UNKNOWN + allow
// scenario through the real evaluator and adapter.
func TestUnknownAllowProducesPolicyAuthorizedEvent(t *testing.T) {
	sched := wiringScheduler(t)
	planner := &controlPlanner{verdict: capacity.Unknown}
	wiring, err := InstallFromConfig(sched, planner, config.AdmissionConfig{Enabled: true, OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: "2s", ReevaluationInterval: "5ms", MaxSnapshotAge: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	defer wiring.Close()
	recorder := wiring.Recorder()

	if _, err := sched.RequestWithRequestID("admission-observe-unknown", "model-1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if recorder.Metrics().UnknownAllowed > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	metrics := recorder.Metrics()
	if metrics.UnknownAllowed != 1 {
		t.Fatalf("unknown allowed = %d, want 1: %+v", metrics.UnknownAllowed, metrics)
	}
	if metrics.Evaluations["FIT"] != 0 {
		t.Fatalf("an UNKNOWN verdict was counted as FIT: %+v", metrics.Evaluations)
	}
	for _, event := range recorder.Events() {
		if event.Type == observe.EventContinuation {
			if event.Verdict != string(scheduler.AdmissionUnknown) {
				t.Fatalf("continuation verdict = %q, want UNKNOWN", event.Verdict)
			}
			if !event.PolicyAuthorized {
				t.Fatal("the continuation was not marked as policy authorized")
			}
			return
		}
	}
	t.Fatalf("no continuation event was retained: %+v", recorder.Events())
}
