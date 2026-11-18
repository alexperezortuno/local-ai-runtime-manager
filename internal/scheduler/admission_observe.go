package scheduler

import (
	"time"

	"local-ai-runtime/internal/admission/observe"
)

// This file holds the admission observability helpers.
//
// The helpers only copy decisions that were already made. They never change a
// verdict, a priority, a deadline or a FIFO position, and they never block the
// Scheduler worker: every helper is a constant-time append into a bounded
// buffer.
//
// s.mu must be held on entry and on return, exactly like the enforcement
// functions they observe. The recorder itself has its own lock, so the
// Scheduler lock is never held while an unbounded operation runs.

// observeEvaluationLocked records the outcome of one capacity evaluation.
//
// The evaluation is counted once per evaluation. A verdict is never rewritten:
// an UNKNOWN verdict stays UNKNOWN even when the policy allowed it.
func (s *Scheduler) observeEvaluationLocked(recorder *observe.Recorder, request AdmissionRequest, evidence AdmissionEvidence, duration time.Duration, outcome observe.Outcome) {
	if recorder == nil {
		return
	}
	recorder.Record(observe.Event{
		Type:       observe.EventEvaluation,
		Timestamp:  time.Now().UTC(),
		LeaseID:    request.LeaseID,
		RequestID:  request.RequestID,
		ModelID:    request.ModelID,
		Generation: request.Generation,
		Verdict:    string(evidence.Verdict),
		Cause:      evidence.Cause,
		Operation:  request.Operation,
		Outcome:    outcome,
		DurationMS: float64(duration) / float64(time.Millisecond),
	})
}

// observeEvaluationErrorLocked records a failed evaluation.
//
// The failure is recorded as a cause. It never becomes a verdict: the
// verdict observed by the caller is preserved unchanged.
func (s *Scheduler) observeEvaluationErrorLocked(recorder *observe.Recorder, request AdmissionRequest, evidence AdmissionEvidence, err error, duration time.Duration) {
	if recorder == nil {
		return
	}
	cause := evidence.Cause
	if cause == "" && err != nil {
		cause = err.Error()
	}
	recorder.Record(observe.Event{
		Type:       observe.EventEvaluationError,
		Timestamp:  time.Now().UTC(),
		LeaseID:    request.LeaseID,
		RequestID:  request.RequestID,
		ModelID:    request.ModelID,
		Generation: request.Generation,
		Verdict:    string(evidence.Verdict),
		Cause:      cause,
		Operation:  request.Operation,
		Outcome:    observe.OutcomeError,
		DurationMS: float64(duration) / float64(time.Millisecond),
	})
}

// observeContinuationLocked records the authorization to continue.
//
// For an UNKNOWN verdict the continuation was authorized by the configured
// policy, not by a FIT result. The event keeps both facts: the verdict stays
// UNKNOWN and policy_authorized marks the policy as the authorizing authority.
//
// The event is recorded after the head was committed to PREPARING. It marks
// an authorization, not an activation: the lease reaches ACTIVE later, in
// the activation path, and that path can still fail. A lease counted here
// can end Failed or Cancelled.
func (s *Scheduler) observeContinuationLocked(recorder *observe.Recorder, request AdmissionRequest, evidence AdmissionEvidence, policyAction string) {
	if recorder == nil {
		return
	}
	recorder.Record(observe.Event{
		Type:         observe.EventContinuation,
		Timestamp:    time.Now().UTC(),
		LeaseID:      request.LeaseID,
		RequestID:    request.RequestID,
		ModelID:      request.ModelID,
		Generation:   request.Generation,
		Verdict:      string(evidence.Verdict),
		Cause:        evidence.Cause,
		Operation:    request.Operation,
		Outcome:      observe.OutcomeAdmitted,
		PolicyAction: policyAction,
		// PolicyAuthorized is only true when the verdict was not a FIT and
		// the policy nevertheless authorized the continuation.
		PolicyAuthorized: evidence.Verdict != AdmissionFit && policyAction != "",
	})
}

// observeWaitLocked records the beginning of a capacity wait or a
// reevaluation of a waiting head.
//
// A reevaluation never restarts the wait: the recorded wait duration is
// measured from the first wait of the lease and generation.
func (s *Scheduler) observeWaitLocked(recorder *observe.Recorder, head AdmissionRequest, evidence AdmissionEvidence, reevaluation bool) {
	if recorder == nil {
		return
	}
	eventType := observe.EventWaitStart
	if reevaluation {
		eventType = observe.EventReevaluation
	}
	recorder.Record(observe.Event{
		Type:       eventType,
		Timestamp:  time.Now().UTC(),
		LeaseID:    head.LeaseID,
		RequestID:  head.RequestID,
		ModelID:    head.ModelID,
		Generation: head.Generation,
		Verdict:    string(evidence.Verdict),
		Cause:      evidence.Cause,
		Operation:  head.Operation,
		Outcome:    observe.OutcomeWaiting,
	})
}

// observeTerminalLocked records the terminal outcome of a capacity wait.
//
// It is called exactly once per wait: the terminal paths clear the wait state
// before returning, so a later cancellation or timeout cannot record a second
// terminal event for the same wait.
//
// The verdict is the last known evaluation result of the lease. A terminal
// event is not an evaluation: recording one never consults the Planner and
// never changes the policy. For a timeout the verdict is the reason the
// lease was still waiting, and outcome=TIMEOUT marks the termination of the
// wait, not a new decision.
func (s *Scheduler) observeTerminalLocked(recorder *observe.Recorder, eventType observe.EventType, head AdmissionRequest, verdict string, cause string, waitingSince time.Time) {
	if recorder == nil {
		return
	}
	event := observe.Event{
		Type:       eventType,
		Timestamp:  time.Now().UTC(),
		LeaseID:    head.LeaseID,
		RequestID:  head.RequestID,
		ModelID:    head.ModelID,
		Generation: head.Generation,
		Verdict:    verdict,
		Cause:      cause,
		Operation:  head.Operation,
	}
	switch eventType {
	case observe.EventTimeout:
		event.Outcome = observe.OutcomeTimeout
	case observe.EventCancellation:
		event.Outcome = observe.OutcomeCancelled
	}
	recorder.Record(event)
	if !waitingSince.IsZero() {
		recorder.ObserveWaitDuration(time.Since(waitingSince))
	}
}
