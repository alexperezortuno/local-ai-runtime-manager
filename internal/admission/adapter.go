// Package admission bridges the read-only capacity engine with the Scheduler
// enforcement boundary.
//
// It exists so that internal/capacity never imports internal/scheduler: the
// capacity engine stays read-only and cannot mutate leases, the FIFO, Runtime
// or Recovery, and the Scheduler keeps a single owner of its transitions.
package admission

import (
	"context"
	"errors"
	"sync"
	"time"

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/scheduler"
)

// Evaluator adapts a capacity evaluator to the Scheduler-owned interface.
//
// It preserves the verdict, the evidence, the cause and the timestamps. It
// never converts a verdict into an error: a verdict is a decision, an error
// is the absence of a decision.
type Evaluator struct {
	evaluator capacity.AdmissionEvaluator
}

// New returns a Scheduler-facing evaluator backed by a capacity evaluator.
func New(evaluator capacity.AdmissionEvaluator) *Evaluator {
	return &Evaluator{evaluator: evaluator}
}

// Evaluate projects a capacity evaluation onto the Scheduler contract.
func (e *Evaluator) Evaluate(ctx context.Context, request scheduler.AdmissionRequest) (scheduler.AdmissionEvidence, error) {
	if e == nil || e.evaluator == nil {
		return scheduler.AdmissionEvidence{Verdict: scheduler.AdmissionUnknown, Cause: capacity.ErrAdmissionUnavailable.Error()}, capacity.ErrAdmissionUnavailable
	}
	input := capacity.AdmissionInput{
		ModelID:   request.ModelID,
		RequestID: request.RequestID,
		LeaseID:   request.LeaseID,
		Operation: operationFor(request.Operation),
	}
	if request.Resident != nil {
		input.ObservedResident = &capacity.ResidentIdentity{
			ModelID:           request.Resident.ModelID,
			PID:               request.Resident.PID,
			ProcessInstanceID: request.Resident.ProcessInstanceID,
			StartIdentity:     request.Resident.StartIdentity,
			RuntimeState:      request.Resident.RuntimeState,
			OwnershipStatus:   request.Resident.OwnershipStatus,
		}
	}
	evaluation, err := e.evaluator.Evaluate(ctx, input)
	evidence := scheduler.AdmissionEvidence{
		Verdict:       verdictFor(evaluation.Verdict),
		EvaluationID:  evaluation.EvaluationID,
		Cause:         evaluation.Cause,
		Justification: evaluation.Justification,
		Source:        evaluation.Source,
		EvaluatedAt:   evaluation.EvaluatedAt,
	}
	if evaluation.Status == capacity.EvaluationTimeout {
		evidence.Cause = capacity.ErrAdmissionTimeout.Error()
	}
	if evaluation.Status == capacity.EvaluationCancelled {
		evidence.Cause = capacity.ErrAdmissionCancelled.Error()
	}
	return evidence, err
}

// operationFor maps the Scheduler operation onto the capacity operation.
// QUEUED is a Scheduler position, never a capacity operation.
func operationFor(operation string) capacity.AdmissionOperation {
	switch operation {
	case scheduler.AdmissionOperationResidentReuse:
		return capacity.ResidentReuse
	case scheduler.AdmissionOperationNewLoad:
		return capacity.NewLoad
	default:
		return capacity.NewLoad
	}
}

// verdictFor preserves FIT, INSUFFICIENT and UNKNOWN without reinterpretation.
func verdictFor(verdict capacity.Verdict) scheduler.AdmissionVerdict {
	switch verdict {
	case capacity.Fit:
		return scheduler.AdmissionFit
	case capacity.Insufficient:
		return scheduler.AdmissionInsufficient
	default:
		return scheduler.AdmissionUnknown
	}
}

// ContextProvider serves the caller context and deadline for an acquisition.
//
// It exists so the Scheduler can compute the effective admission deadline
// without changing any public HTTP, OpenAI or SSE contract. The proxy
// adapter installs it; until then the Scheduler falls back to the admission
// wait timeout alone.
type ContextProvider struct {
	mu        sync.Mutex
	contexts  map[string]context.Context
	deadlines map[string]time.Time
}

// NewContextProvider returns an empty provider.
func NewContextProvider() *ContextProvider {
	return &ContextProvider{contexts: map[string]context.Context{}, deadlines: map[string]time.Time{}}
}

// Track registers the caller context and its absolute deadline for a lease.
// The caller context must not be retained after the acquisition terminates.
func (p *ContextProvider) Track(leaseID string, ctx context.Context, deadline time.Time) {
	if p == nil || leaseID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.contexts[leaseID] = ctx
	p.deadlines[leaseID] = deadline
}

// Forget drops the retained context and deadline of a finished acquisition.
func (p *ContextProvider) Forget(leaseID string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.contexts, leaseID)
	delete(p.deadlines, leaseID)
}

// AdmissionContext implements scheduler.AdmissionContextProvider.
func (p *ContextProvider) AdmissionContext(leaseID string) (context.Context, time.Time, bool) {
	if p == nil {
		return nil, time.Time{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, ok := p.contexts[leaseID]
	if !ok || ctx == nil {
		return nil, time.Time{}, false
	}
	return ctx, p.deadlines[leaseID], true
}

var _ scheduler.AdmissionEvaluator = (*Evaluator)(nil)
var _ scheduler.AdmissionContextProvider = (*ContextProvider)(nil)

// ErrUnavailable is returned when no capacity evaluation could be obtained.
var ErrUnavailable = errors.New("admission evaluation unavailable")
