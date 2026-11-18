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

	"local-ai-runtime/internal/admission/observe"
	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
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

// Close releases the capacity evaluator behind this adapter. It is safe to
// call more than once. After Close the adapter reports the evaluation as
// unavailable, never as a FIT.
func (e *Evaluator) Close() error {
	if e == nil || e.evaluator == nil {
		return nil
	}
	if closer, ok := e.evaluator.(interface{ Close() }); ok {
		closer.Close()
	}
	return nil
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

// Wiring is the installed admission dependency set. It owns the resources
// that must be closed during shutdown.
type Wiring struct {
	evaluator *capacity.Evaluator
	adapter   *Evaluator
	provider  *ContextProvider
	recorder  *observe.Recorder
}

// Install builds the capacity admission evaluator and installs it into the
// Scheduler.
//
// It is the only place where the operational evaluator is constructed. When
// admission is disabled it installs nothing: the Scheduler keeps its previous
// behavior, no worker is started and no additional snapshot is taken.
//
// The evaluator is read-only: it never starts a model and never mutates the
// Scheduler. The returned Wiring must be closed during shutdown.
func Install(sched *scheduler.Scheduler, planner capacity.PlanSource, options scheduler.AdmissionOptions, evaluatorOptions capacity.EvaluatorOptions) (*Wiring, error) {
	if sched == nil {
		return nil, errors.New("admission wiring requires a scheduler")
	}
	if planner == nil {
		return nil, errors.New("admission wiring requires a capacity planner")
	}
	normalized, err := options.Normalize()
	if err != nil {
		return nil, err
	}
	evaluator, err := capacity.NewAdmissionEvaluator(planner, evaluatorOptions)
	if err != nil {
		return nil, err
	}
	adapter := New(evaluator)
	provider := NewContextProvider()
	// The recorder observes enforcement decisions. It never influences them:
	// the Scheduler keeps its own verdict, priority and deadline authority.
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	// The options and the evaluator are installed before the Scheduler can
	// accept a request, so no acquisition is ever evaluated without a policy.
	if err := sched.SetAdmissionOptions(normalized); err != nil {
		evaluator.Close()
		return nil, err
	}
	sched.SetAdmissionContextProvider(provider)
	sched.SetAdmissionRecorder(recorder)
	// A previous evaluator is released: the Scheduler never runs two
	// evaluators, and no worker is duplicated.
	if previous := sched.SetAdmissionEvaluator(adapter); previous != nil {
		if closer, ok := previous.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	return &Wiring{evaluator: evaluator, adapter: adapter, provider: provider, recorder: recorder}, nil
}

// InstallFromConfig builds and installs the capacity admission evaluator when
// the validated configuration enables it.
//
// With capacity.admission.enabled false it returns a nil wiring and installs
// nothing: the Scheduler keeps its previous behavior, no admission worker is
// started, no additional snapshot is taken and no HTTP response changes.
func InstallFromConfig(sched *scheduler.Scheduler, planner capacity.PlanSource, admission config.AdmissionConfig) (*Wiring, error) {
	if !admission.Enabled {
		return nil, nil
	}
	options := scheduler.AdmissionOptions{
		OnInsufficient:       admission.OnInsufficient,
		OnUnknown:            admission.OnUnknown,
		WaitTimeout:          admission.WaitDuration(),
		ReevaluationInterval: admission.ReevaluationDuration(),
	}
	// The proxy deadline is not a global option: the proxy contributes its own
	// queue_wait_timeout through the request context, so an administrative
	// acquisition without a waiting context is bounded by the admission
	// timeout alone, as the contract requires.
	evaluatorOptions := capacity.EvaluatorOptions{
		MaxSnapshotAge: admission.SnapshotAgeDuration(),
	}
	return Install(sched, planner, options, evaluatorOptions)
}

// Recorder returns the admission observability recorder of this wiring, or
// nil when the wiring does not carry one.
//
// The recorder is read-only for diagnostics: it exposes counters and the
// recent event ring buffer, never a decision.
func (w *Wiring) Recorder() *observe.Recorder {
	if w == nil {
		return nil
	}
	return w.recorder
}

// Close releases the admission workers. It is safe to call more than once.
func (w *Wiring) Close() error {
	if w == nil {
		return nil
	}
	if w.evaluator != nil {
		w.evaluator.Close()
	}
	return nil
}
