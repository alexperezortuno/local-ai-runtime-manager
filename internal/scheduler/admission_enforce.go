package scheduler

import (
	"context"
	"errors"
	"log"
	"time"
)

// admissionOutcome is the internal result of one admission attempt.
type admissionOutcome int

const (
	// admissionProceed means the head was committed to PREPARING and the
	// caller must continue with the normal activation path.
	admissionProceed admissionOutcome = iota
	// admissionWaiting means the head keeps waiting; the lease stays QUEUED and
	// keeps its FIFO position.
	admissionWaiting
	// admissionReject means the head was terminated by the admission policy.
	admissionReject
)

// admitHeadLocked evaluates the logical FIFO head and, when both the
// evaluation and the final serialized check allow it, commits QUEUED ->
// PREPARING.
//
// The evaluation always runs without s.mu held. The lease stays QUEUED and
// keeps its FIFO position until the commit succeeds, so a later request can
// never overtake the head.
//
// It returns the committed lease ID and admissionProceed when the head was
// committed, or an empty ID with another outcome.
//
// s.mu is held on entry and on return.
func (s *Scheduler) admitHeadLocked() (string, admissionOutcome) {
	head, ok := s.admissionHeadLocked()
	if !ok {
		return "", admissionProceed
	}
	options := s.admissionOptions
	deadline := s.admissionDeadlineLocked(head)
	evaluator := s.admissionEvaluator
	provider := s.admissionContext

	switch s.admissionPreconditionLocked(head, deadline, options) {
	case admissionReject:
		return "", admissionReject
	case admissionWaiting:
		// The head is waiting for capacity; it must not be evaluated again
		// before the reevaluation interval elapsed.
		return "", admissionWaiting
	}

	// Capture an immutable copy of the request and the live resident identity
	// before releasing the lock.
	request := head
	request.Resident = s.admissionResidentLocked(head.ModelID)
	callerCtx, _, hasCaller := s.admissionContextForLocked(head.LeaseID)
	if !hasCaller {
		callerCtx = nil
	}
	s.admissionWait = &admissionWait{phase: admissionPhaseEvaluating, leaseID: head.LeaseID, generation: head.Generation, deadline: deadline}
	s.mu.Unlock()

	evidence, err := s.evaluateAdmission(evaluator, provider, callerCtx, request, deadline)

	s.mu.Lock()
	// The head may have changed while the evaluation ran. A late result must
	// never authorize an activation.
	if !s.admissionStillValidLocked(request) {
		s.admissionWait = nil
		s.stopAdmissionTimerLocked()
		s.notify()
		return "", admissionWaiting
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// The caller context expired. The wait resumes with the next
		// reevaluation; the deadline check will terminate it if it expired.
		s.admissionWait = nil
		s.stopAdmissionTimerLocked()
		s.notify()
		return "", admissionWaiting
	}
	if err != nil && !errors.Is(err, ErrAdmissionUnavailable) {
		// An evaluator failure is UNKNOWN evidence, not a preventive decision.
		log.Printf("admission evaluation for lease %s failed: %v", request.LeaseID, err)
	}
	switch evidence.Verdict {
	case AdmissionFit:
		// Continue to the final check.
	case AdmissionInsufficient:
		if options.OnInsufficient == "reject" {
			s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorCapacity, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation, EvaluationID: evidence.EvaluationID, Cause: ErrAdmissionCapacity})
			return "", admissionReject
		}
		return "", s.waitForAdmissionLocked(head, evidence, deadline, options)
	case AdmissionUnknown:
		switch options.OnUnknown {
		case "reject":
			s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorUnavailable, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation, EvaluationID: evidence.EvaluationID, Cause: ErrAdmissionUnavailable})
			return "", admissionReject
		case "wait":
			return "", s.waitForAdmissionLocked(head, evidence, deadline, options)
		default:
			// allow: continue to the final check, recording the risk.
		}
	default:
		return "", s.waitForAdmissionLocked(head, evidence, deadline, options)
	}

	// Final check, serialized under s.mu. Anything that fails discards the
	// evaluation and leaves the lease QUEUED. The head is reevaluated with a
	// fresh classification, never with the discarded FIT.
	if !s.admissionCommitAllowedLocked(request) {
		s.admissionWait = nil
		s.scheduleAdmissionWake(time.Now().Add(options.ReevaluationInterval))
		s.notify()
		return "", admissionWaiting
	}
	s.admissionWait = nil
	s.stopAdmissionTimerLocked()
	delete(s.admissionDeadline, request.LeaseID)
	if !s.commitHeadLocked(request.LeaseID) {
		return "", admissionWaiting
	}
	return request.LeaseID, admissionProceed
}

// admissionPreconditionLocked applies the checks that precede any evaluation:
// the effective deadline, Recovery, the Admission Gate and the reevaluation
// interval of an active wait.
func (s *Scheduler) admissionPreconditionLocked(head AdmissionRequest, deadline time.Time, options AdmissionOptions) admissionOutcome {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorTimeout, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation, Retryable: true})
		return admissionReject
	}
	if s.recoveryBlocked {
		s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorRecovery, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation})
		return admissionReject
	}
	if blocked, _ := s.manager.RecoveryRequired(); blocked {
		s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorRecovery, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation})
		return admissionReject
	}
	if s.admissionGate != nil && s.admissionGate.AdmissionBlocked() {
		s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorRecovery, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation})
		return admissionReject
	}
	wait := s.admissionWait
	if wait == nil || wait.phase != admissionPhaseWaiting {
		return admissionProceed
	}
	if wait.generation != head.Generation {
		s.admissionWait = nil
		s.stopAdmissionTimerLocked()
		return admissionProceed
	}
	if !wait.deadline.IsZero() && !time.Now().Before(wait.deadline) {
		s.failAdmissionHeadLocked(head, &AdmissionError{Kind: AdmissionErrorTimeout, RequestID: head.RequestID, LeaseID: head.LeaseID, ModelID: head.ModelID, Operation: head.Operation, Retryable: true})
		return admissionReject
	}
	if time.Now().Before(wait.nextEvaluation) {
		// The wake timer is already armed for this wait; it is never
		// rearmed per tick, so the wait creates no unbounded timers.
		return admissionWaiting
	}
	return admissionProceed
}

// evaluateAdmission runs the evaluator outside s.mu with a bounded context.
// A panic in the evaluator is contained here and reported as UNKNOWN evidence:
// it must never take the Scheduler worker down.
func (s *Scheduler) evaluateAdmission(evaluator AdmissionEvaluator, provider AdmissionContextProvider, callerCtx context.Context, request AdmissionRequest, deadline time.Time) (evidence AdmissionEvidence, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			evidence = AdmissionEvidence{Verdict: AdmissionUnknown, Cause: "admission evaluator panic", EvaluatedAt: time.Now().UTC()}
			err = nil
		}
	}()
	ctx := context.Background()
	if callerCtx != nil {
		ctx = callerCtx
	} else if provider != nil {
		if provided, providedDeadline, ok := provider.AdmissionContext(request.LeaseID); ok {
			ctx = provided
			if !providedDeadline.IsZero() && (deadline.IsZero() || providedDeadline.Before(deadline)) {
				deadline = providedDeadline
			}
		}
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	return evaluator.Evaluate(ctx, request)
}

// waitForAdmissionLocked records the internal WAITING_CAPACITY phase for the
// head and arranges the next reevaluation. The lease stays QUEUED, keeps its
// FIFO position and holds no runtime resource: no process, no reservation, no
// TTL extension and no heartbeat.
//
// s.mu must be held.
func (s *Scheduler) waitForAdmissionLocked(head AdmissionRequest, evidence AdmissionEvidence, deadline time.Time, options AdmissionOptions) admissionOutcome {
	next := time.Now().Add(options.ReevaluationInterval)
	if !deadline.IsZero() && deadline.Before(next) {
		next = deadline
	}
	s.admissionWait = &admissionWait{phase: admissionPhaseWaiting, leaseID: head.LeaseID, generation: head.Generation, deadline: deadline, nextEvaluation: next, reason: string(evidence.Verdict)}
	s.scheduleAdmissionWake(next)
	s.notify()
	return admissionWaiting
}

// scheduleAdmissionWake arms the single reevaluation timer. The Scheduler
// owns exactly one timer for the FIFO head: the wait never creates a
// goroutine per request nor a timer per tick.
//
// s.mu must be held.
func (s *Scheduler) scheduleAdmissionWake(at time.Time) {
	if s.admissionTimer != nil {
		s.admissionTimer.Stop()
	}
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	s.admissionTimer = time.AfterFunc(delay, func() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		s.notify()
	})
}

// stopAdmissionTimerLocked cancels the reevaluation timer.
//
// s.mu must be held.
func (s *Scheduler) stopAdmissionTimerLocked() {
	if s.admissionTimer != nil {
		s.admissionTimer.Stop()
		s.admissionTimer = nil
	}
}

// cancelAdmissionWaitLocked ends an admission wait for a lease that left the
// queue. A confirmed cancellation must prevent any later evaluation from
// authorizing an activation.
//
// s.mu must be held.
func (s *Scheduler) cancelAdmissionWaitLocked(leaseID string) {
	wait := s.admissionWait
	if wait == nil {
		return
	}
	if wait.leaseID != "" && wait.leaseID != leaseID {
		return
	}
	s.admissionWait = nil
	s.stopAdmissionTimerLocked()
	delete(s.admissionDeadline, leaseID)
}

// failAdmissionHeadLocked terminates a QUEUED head by the existing cleanup
// route and records the typed cause on the lease.
//
// s.mu must be held.
func (s *Scheduler) failAdmissionHeadLocked(head AdmissionRequest, admissionErr *AdmissionError) {
	lease := s.leases[head.LeaseID]
	if lease == nil || lease.State != Queued {
		s.admissionWait = nil
		s.stopAdmissionTimerLocked()
		s.notify()
		return
	}
	proposed := cloneLease(*lease)
	proposed.State = Cancelled
	proposed.Error = admissionFailurePrefix + string(admissionErr.Kind)
	if admissionErr.Cause != nil && !errors.Is(admissionErr.Cause, ErrAdmissionCapacity) && !errors.Is(admissionErr.Cause, ErrAdmissionUnavailable) && !errors.Is(admissionErr.Cause, ErrAdmissionRecovery) {
		proposed.Error = admissionFailurePrefix + string(admissionErr.Kind) + ": " + admissionErr.Cause.Error()
	}
	if err := s.persistLeaseChangeLocked(head.LeaseID, proposed); err != nil {
		s.recoveryBlocked = true
		log.Printf("persist admission cancellation: %v", err)
		s.notify()
		return
	}
	*lease = proposed
	s.queue.remove(head.LeaseID)
	s.admissionWait = nil
	s.stopAdmissionTimerLocked()
	delete(s.admissionDeadline, head.LeaseID)
	s.forgetAdmissionContextLocked(head.LeaseID)
	s.notify()
}

// commitHeadLocked removes the validated head from the FIFO and moves it to
// PREPARING. It returns false when the lease is no longer eligible.
//
// s.mu must be held.
func (s *Scheduler) commitHeadLocked(id string) bool {
	l := s.leases[id]
	if l == nil || l.State != Queued {
		return false
	}
	if len(s.queue) == 0 || s.queue[0] != id {
		return false
	}
	proposed := cloneLease(*l)
	proposed.State = Preparing
	if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
		s.recoveryBlocked = true
		log.Printf("persist lease preparation: %v", err)
		return false
	}
	l.State = proposed.State
	s.queue.remove(id)
	s.preparingID = id
	return true
}

// admissionCommitAllowedLocked runs the final serialized checks that must hold
// immediately before PREPARING. Any failure discards the evaluation.
//
// s.mu must be held.
func (s *Scheduler) admissionCommitAllowedLocked(request AdmissionRequest) bool {
	lease := s.leases[request.LeaseID]
	if lease == nil || lease.State != Queued {
		return false
	}
	if len(s.queue) == 0 || s.queue[0] != request.LeaseID {
		return false
	}
	state, ok := s.admissionRequests[request.LeaseID]
	if !ok || state.generation != request.Generation {
		return false
	}
	if s.closing || s.recoveryBlocked {
		return false
	}
	if blocked, _ := s.manager.RecoveryRequired(); blocked {
		return false
	}
	if s.admissionGate != nil && s.admissionGate.AdmissionBlocked() {
		return false
	}
	if deadline := s.admissionDeadlineLocked(request); !deadline.IsZero() && !time.Now().Before(deadline) {
		return false
	}
	// The operation must still describe the current residency. A changed
	// resident identity is reclassified, not silently reused.
	if request.Operation == AdmissionOperationResidentReuse {
		current := s.admissionResidentLocked(request.ModelID)
		if current == nil || request.Resident == nil || !sameResident(*current, *request.Resident) {
			return false
		}
	}
	return true
}

// admissionStillValidLocked rejects a stale evaluation: the head changed, the
// lease reached a terminal state, or a newer evaluation already started.
//
// s.mu must be held.
func (s *Scheduler) admissionStillValidLocked(request AdmissionRequest) bool {
	lease := s.leases[request.LeaseID]
	if lease == nil || lease.State != Queued {
		return false
	}
	if len(s.queue) == 0 || s.queue[0] != request.LeaseID {
		return false
	}
	state, ok := s.admissionRequests[request.LeaseID]
	if !ok || state.generation != request.Generation {
		return false
	}
	return true
}

// admissionResidentLocked returns the live runtime identity for a model, or
// nil when the model is not resident and managed.
//
// s.mu must be held.
func (s *Scheduler) admissionResidentLocked(model string) *AdmissionResident {
	instance, ready := s.manager.CurrentProcessForModel(model)
	if !ready || instance.ProcessInstanceID == "" {
		return nil
	}
	return &AdmissionResident{ModelID: model, PID: instance.PID, ProcessInstanceID: instance.ProcessInstanceID, StartIdentity: instance.StartIdentity, RuntimeState: string(instance.State), OwnershipStatus: instance.OwnershipStatus}
}

// sameResident compares two resident identities. PID alone is not identity.
func sameResident(current, observed AdmissionResident) bool {
	return current.ModelID == observed.ModelID && current.ProcessInstanceID == observed.ProcessInstanceID && current.StartIdentity == observed.StartIdentity && current.RuntimeState == observed.RuntimeState && current.OwnershipStatus == observed.OwnershipStatus
}

// admissionDeadlineLocked computes the effective admission deadline for a
// request. It is the earliest of the admission wait timeout, the proxy queue
// wait timeout and the caller deadline, measured from acceptance and never
// restarted by a reevaluation.
//
// s.mu must be held.
func (s *Scheduler) admissionDeadlineLocked(request AdmissionRequest) time.Time {
	if deadline, ok := s.admissionDeadline[request.LeaseID]; ok {
		return deadline
	}
	deadline := time.Now().Add(s.admissionOptions.WaitTimeout)
	if s.admissionOptions.ProxyQueueWaitTimeout > 0 {
		if proxyDeadline := time.Now().Add(s.admissionOptions.ProxyQueueWaitTimeout); proxyDeadline.Before(deadline) {
			deadline = proxyDeadline
		}
	}
	if provider := s.admissionContext; provider != nil {
		if _, provided, ok := provider.AdmissionContext(request.LeaseID); ok && !provided.IsZero() && provided.Before(deadline) {
			deadline = provided
		}
	}
	if _, provided, ok := s.admissionContextForLocked(request.LeaseID); ok && !provided.IsZero() && provided.Before(deadline) {
		deadline = provided
	}
	s.admissionDeadline[request.LeaseID] = deadline
	return deadline
}
