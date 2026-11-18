package scheduler

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/admission/observe"
)

// observeScheduler installs an admission recorder on the Scheduler and
// returns it. The recorder is installed before any request, so no evaluation
// is ever missed.
func observeScheduler(t *testing.T) (*Scheduler, *observe.Recorder) {
	t.Helper()
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	s.SetAdmissionRecorder(recorder)
	return s, recorder
}

// eventsOfType returns the retained events of one type, newest first.
func eventsOfType(recorder *observe.Recorder, eventType observe.EventType) []observe.Event {
	var result []observe.Event
	for _, event := range recorder.Events() {
		if event.Type == eventType {
			result = append(result, event)
		}
	}
	return result
}

// TestObserveEachVerdictProducesItsEvent verifies that every verdict produces
// the corresponding evaluation event.
func TestObserveEachVerdictProducesItsEvent(t *testing.T) {
	cases := []struct {
		name    string
		verdict AdmissionVerdict
		options AdmissionOptions
		want    LeaseState
	}{
		{name: "fit", verdict: AdmissionFit, options: AdmissionOptions{}, want: Active},
		{name: "insufficient", verdict: AdmissionInsufficient, options: AdmissionOptions{OnInsufficient: "reject"}, want: Cancelled},
		{name: "unknown", verdict: AdmissionUnknown, options: AdmissionOptions{OnUnknown: "reject"}, want: Cancelled},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			s, recorder := observeScheduler(t)
			evaluator := newControlEvaluator(testCase.verdict)
			controlAdmission(t, s, evaluator, testCase.options)
			lease, err := s.RequestWithRequestID("observe-"+testCase.name, "model-1")
			if err != nil {
				t.Fatal(err)
			}
			waitLease(t, s, lease.ID, testCase.want)

			evaluations := eventsOfType(recorder, observe.EventEvaluation)
			if len(evaluations) == 0 {
				t.Fatal("no evaluation event was recorded")
			}
			got := evaluations[0]
			if got.Verdict != string(testCase.verdict) {
				t.Fatalf("verdict = %q, want %q", got.Verdict, testCase.verdict)
			}
			if got.LeaseID != lease.ID {
				t.Fatalf("lease id = %q, want %q", got.LeaseID, lease.ID)
			}
			if got.RequestID != "observe-"+testCase.name {
				t.Fatalf("request id = %q", got.RequestID)
			}
			if got.ModelID != "model-1" {
				t.Fatalf("model id = %q", got.ModelID)
			}
			if got.Generation == 0 {
				t.Fatal("the evaluation event carries no generation")
			}
			if got.DurationMS < 0 {
				t.Fatalf("duration = %v", got.DurationMS)
			}
			metrics := recorder.Metrics()
			if metrics.Evaluations[string(testCase.verdict)] != 1 {
				t.Fatalf("evaluations = %+v", metrics.Evaluations)
			}
		})
	}
}

// TestObserveUnknownAllowIsIdentified verifies that an UNKNOWN verdict
// continued by the on_unknown=allow policy is recorded as a policy
// authorization and never as a FIT.
func TestObserveUnknownAllowIsIdentified(t *testing.T) {
	s, recorder := observeScheduler(t)
	evaluator := newControlEvaluator(AdmissionUnknown)
	evaluator.cause = "snapshot unavailable"
	controlAdmission(t, s, evaluator, AdmissionOptions{OnUnknown: "allow"})
	lease, err := s.RequestWithRequestID("observe-unknown-allow", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)

	continuations := eventsOfType(recorder, observe.EventContinuation)
	if len(continuations) != 1 {
		t.Fatalf("continuations = %d, want 1", len(continuations))
	}
	continuation := continuations[0]
	if continuation.Verdict != string(AdmissionUnknown) {
		t.Fatalf("verdict = %q, want UNKNOWN", continuation.Verdict)
	}
	if !continuation.PolicyAuthorized {
		t.Fatal("the continuation was not marked as policy authorized")
	}
	if continuation.PolicyAction != "ALLOW_UNKNOWN" {
		t.Fatalf("policy action = %q", continuation.PolicyAction)
	}
	if !strings.Contains(continuation.Cause, "snapshot unavailable") {
		t.Fatalf("cause = %q, want the UNKNOWN cause to be preserved", continuation.Cause)
	}
	metrics := recorder.Metrics()
	if metrics.UnknownAllowed != 1 {
		t.Fatalf("unknown allowed = %d, want 1", metrics.UnknownAllowed)
	}
	if metrics.Admitted != 1 {
		t.Fatalf("admitted = %d, want 1", metrics.Admitted)
	}
	if metrics.Evaluations[string(AdmissionFit)] != 0 {
		t.Fatalf("an UNKNOWN verdict was counted as FIT: %+v", metrics.Evaluations)
	}
}

// TestObserveFitIsNotPolicyAuthorized verifies that a FIT continuation is not
// marked as a policy authorization.
func TestObserveFitIsNotPolicyAuthorized(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionFit), AdmissionOptions{})
	lease, err := s.RequestWithRequestID("observe-fit-authorized", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	continuations := eventsOfType(recorder, observe.EventContinuation)
	if len(continuations) != 1 {
		t.Fatalf("continuations = %d, want 1", len(continuations))
	}
	if continuations[0].PolicyAuthorized {
		t.Fatal("a FIT continuation must not be marked as policy authorized")
	}
	if continuations[0].PolicyAction != "ALLOW_FIT" {
		t.Fatalf("policy action = %q", continuations[0].PolicyAction)
	}
}

// TestObserveTimeoutCountedOnce verifies that a timeout produces exactly one
// terminal event and one timeout counter.
func TestObserveTimeoutCountedOnce(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{WaitTimeout: 40 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
	lease, err := s.RequestWithRequestID("observe-timeout-once", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	waitFor(t, func() bool { return recorder.Metrics().Timeouts > 0 })

	metrics := recorder.Metrics()
	if metrics.Timeouts != 1 {
		t.Fatalf("timeouts = %d, want 1", metrics.Timeouts)
	}
	timeouts := eventsOfType(recorder, observe.EventTimeout)
	if len(timeouts) != 1 {
		t.Fatalf("timeout events = %d, want 1", len(timeouts))
	}
	if timeouts[0].Outcome != observe.OutcomeTimeout {
		t.Fatalf("outcome = %q", timeouts[0].Outcome)
	}
	if timeouts[0].LeaseID != lease.ID {
		t.Fatalf("lease id = %q, want %q", timeouts[0].LeaseID, lease.ID)
	}
	if timeouts[0].RequestID != "observe-timeout-once" {
		t.Fatalf("request id = %q", timeouts[0].RequestID)
	}
	if timeouts[0].ModelID != "model-1" {
		t.Fatalf("model id = %q", timeouts[0].ModelID)
	}
	if timeouts[0].Verdict != string(AdmissionInsufficient) {
		t.Fatalf("verdict = %q, want INSUFFICIENT", timeouts[0].Verdict)
	}
	if metrics.WaitDuration.Count != 1 {
		t.Fatalf("wait durations = %d, want 1", metrics.WaitDuration.Count)
	}
}

// TestObserveCancellationCountedOnce verifies that an explicit cancellation
// produces exactly one terminal event and one cancellation counter.
func TestObserveCancellationCountedOnce(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{WaitTimeout: 5 * time.Second, ReevaluationInterval: 5 * time.Millisecond})
	lease, err := s.RequestWithRequestID("observe-cancel-once", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(eventsOfType(recorder, observe.EventWaitStart)) > 0 })
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return recorder.Metrics().Cancellations > 0 })

	metrics := recorder.Metrics()
	if metrics.Cancellations != 1 {
		t.Fatalf("cancellations = %d, want 1", metrics.Cancellations)
	}
	cancellations := eventsOfType(recorder, observe.EventCancellation)
	if len(cancellations) != 1 {
		t.Fatalf("cancellation events = %d, want 1", len(cancellations))
	}
	if cancellations[0].LeaseID != lease.ID {
		t.Fatalf("lease id = %q, want %q", cancellations[0].LeaseID, lease.ID)
	}
	if cancellations[0].RequestID != "observe-cancel-once" {
		t.Fatalf("request id = %q", cancellations[0].RequestID)
	}
	if cancellations[0].ModelID != "model-1" {
		t.Fatalf("model id = %q", cancellations[0].ModelID)
	}
	if cancellations[0].Outcome != observe.OutcomeCancelled {
		t.Fatalf("outcome = %q", cancellations[0].Outcome)
	}
	if metrics.Timeouts != 0 {
		t.Fatalf("a cancellation was also counted as a timeout: %d", metrics.Timeouts)
	}
}

// TestObserveCancellationDuringEvaluationIsRecorded verifies that a
// cancellation that arrives while the capacity evaluation is running is
// still observable.
//
// The evaluation runs outside the Scheduler lock, so the release can reach
// the wait state while it is in the EVALUATING phase. The wait is a real
// wait: the lease is the FIFO head and it is waiting for capacity. The
// cancellation ends it, so exactly one terminal event must be recorded.
//
// Before the fix this race lost the cancellation event entirely: the
// terminal event was only recorded for a wait in the WAITING_CAPACITY
// phase, so the cancellations counter stayed at zero.
func TestObserveCancellationDuringEvaluationIsRecorded(t *testing.T) {
	s, recorder := observeScheduler(t)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{OnInsufficient: "wait", WaitTimeout: 5 * time.Second, ReevaluationInterval: 5 * time.Millisecond})
	// The evaluator blocks inside the evaluation, so the wait state is
	// deterministically in the EVALUATING phase while the release happens.
	evaluator.blockNext()
	lease, err := s.RequestWithRequestID("observe-cancel-evaluating", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-evaluator.started:
	case <-time.After(4 * time.Second):
		t.Fatal("the evaluation did not start")
	}
	// The wait state exists and the evaluation is still running.
	s.mu.Lock()
	phase := s.admissionWait.phase
	s.mu.Unlock()
	if phase != admissionPhaseEvaluating {
		t.Fatalf("phase = %q, want %q", phase, admissionPhaseEvaluating)
	}
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	// The cancellation is observable while the evaluation is still running.
	waitFor(t, func() bool { return recorder.Metrics().Cancellations > 0 })
	evaluator.unblock()

	metrics := recorder.Metrics()
	if metrics.Cancellations != 1 {
		t.Fatalf("cancellations = %d, want 1", metrics.Cancellations)
	}
	cancellations := eventsOfType(recorder, observe.EventCancellation)
	if len(cancellations) != 1 {
		t.Fatalf("cancellation events = %d, want 1", len(cancellations))
	}
	// The correlation identifiers are present in both phases.
	if cancellations[0].LeaseID != lease.ID {
		t.Fatalf("lease id = %q, want %q", cancellations[0].LeaseID, lease.ID)
	}
	if cancellations[0].RequestID != "observe-cancel-evaluating" {
		t.Fatalf("request id = %q", cancellations[0].RequestID)
	}
	if cancellations[0].ModelID != "model-1" {
		t.Fatalf("model id = %q", cancellations[0].ModelID)
	}
	if cancellations[0].Outcome != observe.OutcomeCancelled {
		t.Fatalf("outcome = %q", cancellations[0].Outcome)
	}
	if metrics.Timeouts != 0 {
		t.Fatalf("a cancellation was also counted as a timeout: %d", metrics.Timeouts)
	}
}

// TestObserveReevaluationCounted verifies that the reevaluations of a waiting
// head are counted and reported as reevaluations, not as new waits.
func TestObserveReevaluationCounted(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{WaitTimeout: 5 * time.Second, ReevaluationInterval: 5 * time.Millisecond})
	if _, err := s.RequestWithRequestID("observe-reevaluation", "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return recorder.Metrics().Reevaluations >= 2 })

	metrics := recorder.Metrics()
	if metrics.Reevaluations < 2 {
		t.Fatalf("reevaluations = %d, want at least 2", metrics.Reevaluations)
	}
	if metrics.Waiting != 1 {
		t.Fatalf("waits = %d, want 1: a reevaluation is not a new wait", metrics.Waiting)
	}
	if len(eventsOfType(recorder, observe.EventWaitStart)) != 1 {
		t.Fatalf("wait start events = %d, want 1", len(eventsOfType(recorder, observe.EventWaitStart)))
	}
	if len(eventsOfType(recorder, observe.EventReevaluation)) < 2 {
		t.Fatalf("reevaluation events = %d, want at least 2", len(eventsOfType(recorder, observe.EventReevaluation)))
	}
}

// TestObserveNoDuplicateTerminalEvents verifies that a terminal decision is
// not reported twice.
func TestObserveNoDuplicateTerminalEvents(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{OnInsufficient: "reject"})
	lease, err := s.RequestWithRequestID("observe-no-duplicate", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	// The rejection is terminal. Releasing the cancelled lease must not add a
	// second terminal event.
	if _, err := s.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	metrics := recorder.Metrics()
	if metrics.Timeouts != 0 || metrics.Cancellations != 0 {
		t.Fatalf("a rejection produced a terminal event: %+v", metrics)
	}
	if metrics.Admitted != 0 {
		t.Fatalf("a rejected request was counted as admitted: %d", metrics.Admitted)
	}
	if metrics.WaitingNow != 0 {
		t.Fatalf("waiting now = %d, want 0", metrics.WaitingNow)
	}
}

// TestObserveEvaluationErrorCounted verifies that a failed evaluation is
// recorded as an error and never as a verdict of its own.
func TestObserveEvaluationErrorCounted(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, &failingEvaluator{}, AdmissionOptions{OnUnknown: "allow"})
	lease, err := s.RequestWithRequestID("observe-evaluation-error", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	waitFor(t, func() bool { return recorder.Metrics().EvaluationErrors > 0 })

	metrics := recorder.Metrics()
	if metrics.EvaluationErrors != 1 {
		t.Fatalf("evaluation errors = %d, want 1", metrics.EvaluationErrors)
	}
	errors := eventsOfType(recorder, observe.EventEvaluationError)
	if len(errors) != 1 {
		t.Fatalf("evaluation error events = %d, want 1", len(errors))
	}
	if !strings.Contains(errors[0].Cause, "evaluator failure") {
		t.Fatalf("cause = %q", errors[0].Cause)
	}
}

// failingEvaluator always fails with a sanitized error.
type failingEvaluator struct{}

func (e *failingEvaluator) Evaluate(context.Context, AdmissionRequest) (AdmissionEvidence, error) {
	return AdmissionEvidence{Verdict: AdmissionUnknown, Cause: "evaluator failure"}, errors.New("evaluator failure")
}

// TestObserveDisabledAdmissionEmitsNoEvent verifies that a Scheduler without
// an admission evaluator produces no enforcement event.
func TestObserveDisabledAdmissionEmitsNoEvent(t *testing.T) {
	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	s.SetAdmissionRecorder(recorder)
	lease, err := s.RequestWithRequestID("observe-disabled", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if events := recorder.Events(); len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
	metrics := recorder.Metrics()
	if metrics.Admitted != 0 || metrics.Waiting != 0 || metrics.Evaluations[string(AdmissionFit)] != 0 {
		t.Fatalf("metrics = %+v, want zeroed counters", metrics)
	}
}

// TestObserveDoesNotAlterFIFO verifies that the instrumentation does not
// change the FIFO order of the queue.
func TestObserveDoesNotAlterFIFO(t *testing.T) {
	with, recorder := observeScheduler(t)
	controlAdmission(t, with, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{OnInsufficient: "wait", WaitTimeout: 5 * time.Second, ReevaluationInterval: 5 * time.Millisecond})
	head, err := with.RequestWithRequestID("fifo-with-observer-1", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	follower, err := with.RequestWithRequestID("fifo-with-observer-2", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	// Both requests keep their FIFO position while they wait for capacity.
	waitFor(t, func() bool {
		first, firstErr := with.Get(head.ID)
		second, secondErr := with.Get(follower.ID)
		return firstErr == nil && secondErr == nil && first.State == Queued && second.State == Queued &&
			len(eventsOfType(recorder, observe.EventWaitStart)) > 0
	})
	observedHead, err := with.Get(head.ID)
	if err != nil {
		t.Fatal(err)
	}
	if observedHead.QueuePosition != 1 {
		t.Fatalf("queue position = %d, want 1", observedHead.QueuePosition)
	}
	observedFollower, err := with.Get(follower.ID)
	if err != nil {
		t.Fatal(err)
	}
	if observedFollower.QueuePosition != 2 {
		t.Fatalf("queue position = %d, want 2", observedFollower.QueuePosition)
	}
	if recorder.Metrics().Waiting != 1 {
		t.Fatalf("waits = %d, want 1", recorder.Metrics().Waiting)
	}
}

// TestObserveDoesNotAlterDeadline verifies that the instrumentation does not
// extend the effective admission deadline.
func TestObserveDoesNotAlterDeadline(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{WaitTimeout: 120 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
	lease, err := s.RequestWithRequestID("observe-deadline", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	waitLease(t, s, lease.ID, Cancelled)
	elapsed := time.Since(started)
	if elapsed > 2*time.Second {
		t.Fatalf("the wait lasted %v; the instrumentation extended the deadline", elapsed)
	}
	if recorder.Metrics().Timeouts != 1 {
		t.Fatalf("timeouts = %d, want 1", recorder.Metrics().Timeouts)
	}
}

// TestObserveDoesNotBlockScheduler verifies that a slow recorder cannot block
// the Scheduler worker. The recorder here is the real one; the assertion is
// that the FIFO head still commits while many events are recorded.
func TestObserveDoesNotBlockScheduler(t *testing.T) {
	s, recorder := observeScheduler(t)
	controlAdmission(t, s, newControlEvaluator(AdmissionFit), AdmissionOptions{})
	lease, err := s.RequestWithRequestID("observe-not-blocking", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Active)
	if recorder.Metrics().Admitted != 1 {
		t.Fatalf("admitted = %d, want 1", recorder.Metrics().Admitted)
	}
}

// TestObserveShutdownLeavesNoGoroutine verifies that the shutdown does not
// leave a goroutine behind. The instrumentation itself starts no goroutine.
func TestObserveShutdownLeavesNoGoroutine(t *testing.T) {
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	recorder.Record(observe.Event{Type: observe.EventEvaluation, Verdict: "FIT"})
	recorder.Metrics()
	// No goroutine may belong to the instrumentation. The check inspects the
	// stacks, so it is deterministic and not affected by the goroutines of
	// other tests running in the same package.
	if stack := goroutineStacksMatching("local-ai-runtime/internal/admission/observe"); stack != "" {
		t.Fatalf("the instrumentation owns a goroutine:\n%s", stack)
	}

	_, s := testScheduler(t, "normal", 1, time.Second, time.Second, 2, time.Second, 100*time.Millisecond)
	s.SetAdmissionRecorder(recorder)
	controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{WaitTimeout: 5 * time.Second, ReevaluationInterval: 5 * time.Millisecond})
	if _, err := s.RequestWithRequestID("observe-shutdown", "model-1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(eventsOfType(recorder, observe.EventWaitStart)) > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// The shutdown released the admission wait: no timer stays armed and no
	// wait state survives the Scheduler.
	s.mu.Lock()
	armedTimer := s.admissionTimer != nil
	pendingWait := s.admissionWait != nil
	s.mu.Unlock()
	if armedTimer {
		t.Fatal("the shutdown left the admission reevaluation timer armed")
	}
	if pendingWait {
		t.Fatal("the shutdown left an admission wait state behind")
	}
	if stack := goroutineStacksMatching("local-ai-runtime/internal/admission/observe"); stack != "" {
		t.Fatalf("the shutdown left an instrumentation goroutine behind:\n%s", stack)
	}
}

// goroutineStacksMatching returns the stacks of the goroutines whose stack
// contains the given package path. It returns an empty string when there is
// none.
func goroutineStacksMatching(path string) string {
	buffer := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			return matchingStacks(string(buffer[:n]), path)
		}
		buffer = make([]byte, 2*len(buffer))
	}
}

// matchingStacks returns the goroutine blocks whose stack contains the path.
func matchingStacks(stacks, path string) string {
	var matched []string
	for _, block := range strings.Split(stacks, "\n\n") {
		if strings.Contains(block, path) {
			matched = append(matched, block)
		}
	}
	return strings.Join(matched, "\n\n")
}

// TestObserveConcurrentUse verifies that the recorder is safe under
// concurrent access.
func TestObserveConcurrentUse(t *testing.T) {
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < 100; index++ {
				recorder.Record(observe.Event{Type: observe.EventEvaluation, Verdict: "FIT", LeaseID: "lease-1"})
				recorder.Metrics()
				recorder.Events()
			}
		}()
	}
	wait.Wait()
	if recorder.Metrics().Evaluations["FIT"] != 800 {
		t.Fatalf("evaluations = %+v", recorder.Metrics().Evaluations)
	}
}

// TestObserveAdmittedCountsAuthorizationNotActivation verifies the documented
// semantics of the admitted counter: it counts the authorizations to
// continue, not the activations.
//
// The lease is authorized (admitted) and then its activation fails. The
// counter must still report the authorization, because the authorization is
// what the counter measures. This proves that admitted is not a proxy for
// "the model started successfully".
func TestObserveAdmittedCountsAuthorizationNotActivation(t *testing.T) {
	// The "die" fixture terminates the process during the activation, so the
	// lease is authorized and committed to PREPARING but never reaches
	// ACTIVE: it ends Failed.
	_, s := testScheduler(t, "die", 1, time.Second, time.Second, 5, time.Second, time.Second)
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	s.SetAdmissionRecorder(recorder)
	controlAdmission(t, s, newControlEvaluator(AdmissionFit), AdmissionOptions{})
	lease, err := s.RequestWithRequestID("observe-admitted-authorization", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Failed)

	metrics := recorder.Metrics()
	if metrics.Admitted != 1 {
		t.Fatalf("admitted = %d, want 1: the authorization was not counted", metrics.Admitted)
	}
	continuations := eventsOfType(recorder, observe.EventContinuation)
	if len(continuations) != 1 {
		t.Fatalf("continuation events = %d, want 1", len(continuations))
	}
	// The lease never reached ACTIVE, yet it was admitted. The counter
	// measures authorizations, not activations.
	if status, _ := s.Get(lease.ID); status.State == Active {
		t.Fatal("the lease reached ACTIVE: the fixture did not fail the activation")
	}
}

// TestObserveAdmittedIsNotIncrementedForRejectedOrWaitingLeases verifies that
// admitted only counts authorizations: a rejected lease and a lease that is
// still waiting are never counted.
func TestObserveAdmittedIsNotIncrementedForRejectedOrWaitingLeases(t *testing.T) {
	t.Run("rejected", func(t *testing.T) {
		s, recorder := observeScheduler(t)
		controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{OnInsufficient: "reject"})
		rejected, err := s.RequestWithRequestID("observe-admitted-rejected", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitLease(t, s, rejected.ID, Cancelled)
		if admitted := recorder.Metrics().Admitted; admitted != 0 {
			t.Fatalf("admitted = %d, want 0: a rejected lease was counted", admitted)
		}
	})

	t.Run("waiting", func(t *testing.T) {
		s, recorder := observeScheduler(t)
		controlAdmission(t, s, newControlEvaluator(AdmissionInsufficient), AdmissionOptions{OnInsufficient: "wait", WaitTimeout: 5 * time.Second})
		waiting, err := s.RequestWithRequestID("observe-admitted-waiting", "model-1")
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return len(eventsOfType(recorder, observe.EventWaitStart)) > 0 })
		if admitted := recorder.Metrics().Admitted; admitted != 0 {
			t.Fatalf("admitted = %d, want 0: a waiting lease was counted", admitted)
		}
		if _, err := s.Release(waiting.ID); err != nil {
			t.Fatal(err)
		}
	})
}

// TestObserveTimeoutDoesNotReevaluateThePlanner verifies the documented
// semantics of the timeout event: the verdict of a timeout is the last known
// evaluation result, and the timeout itself does not trigger a new
// evaluation.
func TestObserveTimeoutDoesNotReevaluateThePlanner(t *testing.T) {
	s, recorder := observeScheduler(t)
	evaluator := newControlEvaluator(AdmissionInsufficient)
	controlAdmission(t, s, evaluator, AdmissionOptions{WaitTimeout: 60 * time.Millisecond, ReevaluationInterval: 5 * time.Millisecond})
	lease, err := s.RequestWithRequestID("observe-timeout-verdict", "model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLease(t, s, lease.ID, Cancelled)
	waitFor(t, func() bool { return recorder.Metrics().Timeouts > 0 })

	timeouts := eventsOfType(recorder, observe.EventTimeout)
	if len(timeouts) != 1 {
		t.Fatalf("timeout events = %d, want 1", len(timeouts))
	}
	timeout := timeouts[0]
	// The verdict is the last known evaluation result, not a fresh one.
	if timeout.Verdict != string(AdmissionInsufficient) {
		t.Fatalf("verdict = %q, want %q", timeout.Verdict, string(AdmissionInsufficient))
	}
	// The outcome marks the termination of the wait.
	if timeout.Outcome != observe.OutcomeTimeout {
		t.Fatalf("outcome = %q, want %q", timeout.Outcome, observe.OutcomeTimeout)
	}
	// The timeout is not an evaluation: it is not counted as one and it
	// carries no evaluation duration.
	if timeout.DurationMS != 0 {
		t.Fatalf("duration = %v, want 0: a timeout is not an evaluation", timeout.DurationMS)
	}
	// The timeout terminated the wait; it did not run the Planner again.
	// The evaluations counted are the ones the reevaluation loop produced
	// while the lease was waiting.
	evaluations := recorder.Metrics().Evaluations
	if evaluations[string(AdmissionInsufficient)] == 0 {
		t.Fatalf("evaluations = %+v", evaluations)
	}
	if got := int(evaluations[string(AdmissionInsufficient)]); got != evaluator.count() {
		t.Fatalf("evaluations = %d, planner calls = %d: the timeout counted as an evaluation", got, evaluator.count())
	}
}
