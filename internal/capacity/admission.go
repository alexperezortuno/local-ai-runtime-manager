package capacity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/memory"
)

// AdmissionOperation describes the work being observed, not a Scheduler
// transition. QUEUED remains a Scheduler lease state and is intentionally not
// accepted by the evaluator.
type AdmissionOperation string

const (
	NewLoad       AdmissionOperation = OperationNewLoad
	ResidentReuse AdmissionOperation = OperationResidentReuse
)

type ResidentIdentity struct {
	ModelID           string `json:"model_id"`
	PID               int    `json:"pid"`
	ProcessInstanceID string `json:"process_instance_id"`
	StartIdentity     string `json:"start_identity"`
	RuntimeState      string `json:"runtime_state"`
	OwnershipStatus   string `json:"ownership_status"`
}

type AdmissionInput struct {
	ModelID          string             `json:"model_id"`
	RequestID        string             `json:"request_id"`
	LeaseID          string             `json:"lease_id,omitempty"`
	Operation        AdmissionOperation `json:"operation"`
	ObservedResident *ResidentIdentity  `json:"observed_resident,omitempty"`
	EvaluationReason string             `json:"evaluation_reason,omitempty"`
}

type EvaluationStatus string

const (
	EvaluationCompleted EvaluationStatus = "completed"
	EvaluationError     EvaluationStatus = "error"
	EvaluationTimeout   EvaluationStatus = "timeout"
	EvaluationCancelled EvaluationStatus = "cancelled"
	EvaluationDiscarded EvaluationStatus = "discarded"
)

type SnapshotEvidence struct {
	State      memory.Availability `json:"state"`
	MeasuredAt time.Time           `json:"measured_at"`
	Age        time.Duration       `json:"age"`
	Source     string              `json:"source"`
}

type MemoryEvidence struct {
	RequiredBytes        uint64  `json:"required_bytes"`
	SystemRequiredBytes  uint64  `json:"system_required_bytes"`
	GPURequiredBytes     uint64  `json:"gpu_required_bytes"`
	SystemAvailableBytes *uint64 `json:"system_available_bytes"`
	GPUAvailableBytes    *uint64 `json:"gpu_available_bytes"`
}

type AdmissionEvaluation struct {
	EvaluationID  string             `json:"evaluation_id"`
	ModelID       string             `json:"model_id"`
	RequestID     string             `json:"request_id"`
	LeaseID       string             `json:"lease_id,omitempty"`
	Operation     AdmissionOperation `json:"operation"`
	Verdict       Verdict            `json:"verdict"`
	Snapshot      SnapshotEvidence   `json:"snapshot"`
	Memory        MemoryEvidence     `json:"memory"`
	Source        []string           `json:"source"`
	Justification []string           `json:"justification"`
	Resident      *ResidentIdentity  `json:"resident,omitempty"`
	Status        EvaluationStatus   `json:"status"`
	Cause         string             `json:"cause,omitempty"`
	EvaluatedAt   time.Time          `json:"evaluated_at"`
}

type AdmissionEvaluator interface {
	Evaluate(context.Context, AdmissionInput) (AdmissionEvaluation, error)
}

type EvaluatorOptions struct {
	EvaluationTimeout time.Duration
	MaxConcurrent     int
	MaxPending        int
	MaxSnapshotAge    time.Duration
}

func DefaultEvaluatorOptions() EvaluatorOptions {
	return EvaluatorOptions{EvaluationTimeout: 2 * time.Second, MaxConcurrent: 1, MaxPending: 128, MaxSnapshotAge: 6 * time.Second}
}

var (
	ErrInvalidAdmissionInput = errors.New("invalid admission input")
	ErrAdmissionCapacity     = errors.New("admission capacity insufficient")
	ErrAdmissionTimeout      = errors.New("admission wait timed out")
	ErrAdmissionUnavailable  = errors.New("admission evaluation unavailable")
	ErrAdmissionCancelled    = errors.New("admission cancelled")
	ErrAdmissionRecovery     = errors.New("admission requires recovery")
	ErrAdmissionRejected     = ErrAdmissionCapacity
)

type evaluationJob struct {
	ctx    context.Context
	input  AdmissionInput
	result chan AdmissionEvaluation
}

// Evaluator is a bounded, read-only adapter around the existing Planner. A
// non-cooperative planner can retain one fixed worker, but cannot create an
// unbounded goroutine or queue.
type Evaluator struct {
	planner PlanSource
	options EvaluatorOptions
	jobs    chan evaluationJob
	done    chan struct{}
	close   sync.Once
	mu      sync.RWMutex
	closed  bool
}

func NewAdmissionEvaluator(planner PlanSource, options EvaluatorOptions) (*Evaluator, error) {
	if planner == nil {
		return nil, errors.New("admission evaluator requires a capacity planner")
	}
	defaults := DefaultEvaluatorOptions()
	if options.EvaluationTimeout <= 0 {
		options.EvaluationTimeout = defaults.EvaluationTimeout
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = defaults.MaxConcurrent
	}
	if options.MaxPending < 0 {
		return nil, errors.New("admission evaluator max pending cannot be negative")
	}
	if options.MaxPending == 0 {
		options.MaxPending = defaults.MaxPending
	}
	if options.MaxSnapshotAge <= 0 {
		options.MaxSnapshotAge = defaults.MaxSnapshotAge
	}
	e := &Evaluator{planner: planner, options: options, jobs: make(chan evaluationJob, options.MaxPending), done: make(chan struct{})}
	for i := 0; i < options.MaxConcurrent; i++ {
		go e.worker()
	}
	return e, nil
}

func NewEvaluator(planner PlanSource, options EvaluatorOptions) (*Evaluator, error) {
	return NewAdmissionEvaluator(planner, options)
}

func (e *Evaluator) Evaluate(ctx context.Context, input AdmissionInput) (AdmissionEvaluation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateAdmissionInput(input); err != nil {
		return newEvaluation(input, EvaluationError, Unknown, err.Error()), err
	}
	if err := ctx.Err(); err != nil {
		return newEvaluation(input, statusForContext(err), Unknown, causeForContext(err)), nil
	}
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return newEvaluation(input, EvaluationDiscarded, Unknown, ErrAdmissionUnavailable.Error()), ErrAdmissionUnavailable
	}
	jobCtx, cancel := context.WithTimeout(ctx, e.options.EvaluationTimeout)
	defer cancel()
	job := evaluationJob{ctx: jobCtx, input: input, result: make(chan AdmissionEvaluation, 1)}
	select {
	case e.jobs <- job:
	case <-ctx.Done():
		return newEvaluation(input, statusForContext(ctx.Err()), Unknown, causeForContext(ctx.Err())), nil
	case <-e.done:
		return newEvaluation(input, EvaluationDiscarded, Unknown, ErrAdmissionUnavailable.Error()), ErrAdmissionUnavailable
	default:
		return newEvaluation(input, EvaluationError, Unknown, "evaluator saturated"), ErrAdmissionUnavailable
	}
	select {
	case result := <-job.result:
		if err := ctx.Err(); err != nil {
			return newEvaluation(input, statusForContext(err), Unknown, causeForContext(err)), nil
		}
		return result, nil
	case <-ctx.Done():
		return newEvaluation(input, statusForContext(ctx.Err()), Unknown, causeForContext(ctx.Err())), nil
	case <-e.done:
		return newEvaluation(input, EvaluationDiscarded, Unknown, ErrAdmissionUnavailable.Error()), ErrAdmissionUnavailable
	}
}

func (e *Evaluator) worker() {
	for {
		select {
		case job := <-e.jobs:
			job.result <- e.evaluate(job)
		case <-e.done:
			return
		}
	}
}

func (e *Evaluator) evaluate(job evaluationJob) AdmissionEvaluation {
	if err := job.ctx.Err(); err != nil {
		status := EvaluationCancelled
		if errors.Is(err, context.DeadlineExceeded) {
			status = EvaluationTimeout
		}
		return newEvaluation(job.input, status, Unknown, causeForContext(err))
	}
	evaluation := newEvaluation(job.input, EvaluationCompleted, Unknown, "")
	if job.input.Operation == ResidentReuse {
		resident := cloneResident(job.input.ObservedResident)
		evaluation.Resident = resident
		evaluation.Snapshot = SnapshotEvidence{State: memory.Unavailable, Source: "resident-runtime"}
		evaluation.Source = []string{"resident-runtime"}
		if resident == nil || !validResident(*resident, job.input.ModelID) {
			evaluation.Status = EvaluationError
			evaluation.Cause = "resident identity is not READY and managed"
			evaluation.Justification = []string{evaluation.Cause}
			return evaluation
		}
		evaluation.Verdict = Fit
		evaluation.Justification = []string{"proceso residente READY con identidad administrada; no se estimó una nueva carga"}
		if err := job.ctx.Err(); err != nil {
			evaluation.Status = EvaluationDiscarded
			evaluation.Verdict = Unknown
			evaluation.Cause = causeForContext(err)
		}
		return evaluation
	}

	result, panicked := safeAdmissionPlan(e.planner, job.ctx, job.input.ModelID)
	if err := job.ctx.Err(); err != nil {
		evaluation.Status = EvaluationDiscarded
		if errors.Is(err, context.DeadlineExceeded) {
			evaluation.Status = EvaluationTimeout
		}
		evaluation.Verdict = Unknown
		evaluation.Cause = causeForContext(err)
		evaluation.Justification = []string{"resultado tardío descartado; la evaluación no autoriza ejecución"}
		return evaluation
	}
	if panicked {
		evaluation.Status = EvaluationError
		evaluation.Cause = "capacity planner panic"
		evaluation.Justification = []string{"capacity planner failed; la evaluación no autoriza ejecución"}
		return evaluation
	}
	evaluation = evidenceFromResult(evaluation, result)
	measuredAt := result.Snapshot.MeasuredAt
	if measuredAt == nil {
		measuredAt = result.Snapshot.System.MeasuredAt
	}
	if measuredAt != nil {
		evaluation.Snapshot.MeasuredAt = *measuredAt
		evaluation.Snapshot.Age = time.Since(*measuredAt)
		if evaluation.Snapshot.Age < 0 {
			evaluation.Snapshot.Age = 0
		}
		if evaluation.Snapshot.Age > e.options.MaxSnapshotAge {
			evaluation.Verdict = Unknown
			evaluation.Justification = append(evaluation.Justification, "snapshot de memoria excede max_snapshot_age")
		}
	}
	if result.Snapshot.Status != "" && result.Snapshot.Status != memory.Available || result.Snapshot.System.Status != "" && result.Snapshot.System.Status != memory.Available {
		evaluation.Verdict = Unknown
		evaluation.Justification = append(evaluation.Justification, "snapshot de memoria no es AVAILABLE")
	}
	return evaluation
}

func (e *Evaluator) Close() {
	if e == nil {
		return
	}
	e.close.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.mu.Unlock()
		close(e.done)
	})
	// A non-cooperative planner is allowed to retain its bounded worker; Close
	// must not turn that provider limitation into an unbounded shutdown wait.
}

func validateAdmissionInput(input AdmissionInput) error {
	if strings.TrimSpace(input.ModelID) == "" || strings.TrimSpace(input.RequestID) == "" {
		return fmt.Errorf("%w: model_id and request_id are required", ErrInvalidAdmissionInput)
	}
	if input.Operation != NewLoad && input.Operation != ResidentReuse {
		return fmt.Errorf("%w: operation must be NEW_LOAD or RESIDENT_REUSE", ErrInvalidAdmissionInput)
	}
	return nil
}

func newEvaluation(input AdmissionInput, status EvaluationStatus, verdict Verdict, cause string) AdmissionEvaluation {
	id, err := corecontract.NewID()
	if err != nil {
		id = fmt.Sprintf("evaluation-%d", time.Now().UnixNano())
	}
	return AdmissionEvaluation{EvaluationID: id, ModelID: input.ModelID, RequestID: input.RequestID, LeaseID: input.LeaseID, Operation: input.Operation, Verdict: verdict, Status: status, Cause: cause, EvaluatedAt: time.Now().UTC()}
}

func evidenceFromResult(evaluation AdmissionEvaluation, result Result) AdmissionEvaluation {
	evaluation.Verdict = result.Verdict
	if evaluation.Verdict != Fit && evaluation.Verdict != Insufficient && evaluation.Verdict != Unknown {
		evaluation.Verdict = Unknown
	}
	snapshot := result.Snapshot
	evaluation.Snapshot = SnapshotEvidence{State: snapshot.Status, Source: firstNonEmpty(snapshot.Source, snapshot.System.Source, snapshot.GPUProvider.Source)}
	if snapshot.MeasuredAt != nil {
		evaluation.Snapshot.MeasuredAt = *snapshot.MeasuredAt
	}
	evaluation.Memory = MemoryEvidence{RequiredBytes: result.Estimate.RequiredBytes, SystemRequiredBytes: result.Estimate.SystemRequiredBytes, GPURequiredBytes: result.Estimate.GPURequiredBytes, SystemAvailableBytes: cloneUint64(result.Capacity.SystemAvailableBytes), GPUAvailableBytes: cloneUint64(result.Capacity.GPUFreeBytes)}
	evaluation.Source = uniqueStrings(snapshot.Source, snapshot.System.Source, snapshot.GPUProvider.Source, "capacity-planner")
	evaluation.Justification = safeJustification(result.Justification)
	return evaluation
}

func safeAdmissionPlan(planner PlanSource, ctx context.Context, modelID string) (result Result, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	return planner.Plan(ctx, modelID), false
}

func validResident(resident ResidentIdentity, modelID string) bool {
	return resident.ModelID == modelID && resident.PID > 0 && resident.ProcessInstanceID != "" && resident.StartIdentity != "" && resident.RuntimeState == "READY" && resident.OwnershipStatus != ""
}

func cloneResident(resident *ResidentIdentity) *ResidentIdentity {
	if resident == nil {
		return nil
	}
	copy := *resident
	return &copy
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func uniqueStrings(values ...string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func statusForContext(err error) EvaluationStatus {
	if errors.Is(err, context.DeadlineExceeded) {
		return EvaluationTimeout
	}
	return EvaluationCancelled
}

func causeForContext(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "capacity planner evaluation timed out"
	}
	return "capacity planner evaluation cancelled"
}

type PolicyAction string

const (
	PolicyContinue  PolicyAction = "CONTINUE"
	PolicyWait      PolicyAction = "WAIT"
	PolicyReject    PolicyAction = "REJECT"
	PolicyBlocked   PolicyAction = "BLOCKED_RECOVERY"
	PolicyCancelled PolicyAction = "CANCELLED"
	PolicyTimeout   PolicyAction = "TIMEOUT"
)

type AdmissionDecision struct {
	Authorized  bool                  `json:"authorized"`
	Action      PolicyAction          `json:"action"`
	Evaluation  AdmissionEvaluation   `json:"evaluation"`
	Evaluations []AdmissionEvaluation `json:"evaluations,omitempty"`
	Cause       string                `json:"cause,omitempty"`
}

type RecoveryGuard interface {
	AdmissionBlocked() bool
}

type PolicyOptions struct {
	OnInsufficient       string
	OnUnknown            string
	WaitTimeout          time.Duration
	ReevaluationInterval time.Duration
	Recovery             RecoveryGuard
	Wake                 <-chan struct{}
}

func DefaultPolicyOptions() PolicyOptions {
	return PolicyOptions{OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: 5 * time.Minute, ReevaluationInterval: 2 * time.Second}
}

type Policy struct {
	evaluator AdmissionEvaluator
	options   PolicyOptions
}

func NewAdmissionPolicy(evaluator AdmissionEvaluator, options PolicyOptions) (*Policy, error) {
	if evaluator == nil {
		return nil, errors.New("admission policy requires an evaluator")
	}
	defaults := DefaultPolicyOptions()
	if options.OnInsufficient == "" {
		options.OnInsufficient = defaults.OnInsufficient
	}
	if options.OnUnknown == "" {
		options.OnUnknown = defaults.OnUnknown
	}
	if options.WaitTimeout <= 0 {
		options.WaitTimeout = defaults.WaitTimeout
	}
	if options.ReevaluationInterval <= 0 {
		options.ReevaluationInterval = defaults.ReevaluationInterval
	}
	if (options.OnInsufficient != "wait" && options.OnInsufficient != "reject") || (options.OnUnknown != "allow" && options.OnUnknown != "wait" && options.OnUnknown != "reject") {
		return nil, errors.New("invalid admission policy enum")
	}
	return &Policy{evaluator: evaluator, options: options}, nil
}

func NewPolicy(evaluator AdmissionEvaluator, options PolicyOptions) (*Policy, error) {
	return NewAdmissionPolicy(evaluator, options)
}

func (p *Policy) Admit(ctx context.Context, input AdmissionInput) (AdmissionDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(ctx, p.options.WaitTimeout)
	defer cancel()
	decision := AdmissionDecision{Action: PolicyWait}
	for attempt := 0; ; attempt++ {
		if err := waitCtx.Err(); err != nil {
			return finishPolicy(decision, input, policyContextDecision(err))
		}
		if p.options.Recovery != nil && p.options.Recovery.AdmissionBlocked() {
			return finishPolicy(decision, input, AdmissionDecision{Action: PolicyBlocked, Cause: ErrAdmissionRecovery.Error()})
		}
		current := input
		if attempt > 0 {
			current.EvaluationReason = "retry"
		}
		evaluation, err := p.evaluator.Evaluate(waitCtx, current)
		if err != nil {
			// Evaluator failures are represented as UNKNOWN evidence where possible.
			decision.Cause = err.Error()
		}
		decision.Evaluation = evaluation
		decision.Evaluations = append(decision.Evaluations, evaluation)
		if err := waitCtx.Err(); err != nil {
			return finishPolicy(decision, input, policyContextDecision(err))
		}
		if p.options.Recovery != nil && p.options.Recovery.AdmissionBlocked() {
			return finishPolicy(decision, input, AdmissionDecision{Action: PolicyBlocked, Cause: ErrAdmissionRecovery.Error()})
		}
		switch evaluation.Verdict {
		case Fit:
			decision.Authorized, decision.Action = true, PolicyContinue
			return decision, nil
		case Insufficient:
			if p.options.OnInsufficient == "reject" {
				return finishPolicy(decision, input, AdmissionDecision{Action: PolicyReject, Cause: ErrAdmissionRejected.Error()})
			}
		case Unknown:
			switch p.options.OnUnknown {
			case "allow":
				decision.Authorized, decision.Action = true, PolicyContinue
				return decision, nil
			case "reject":
				return finishPolicy(decision, input, AdmissionDecision{Action: PolicyReject, Cause: ErrAdmissionRejected.Error()})
			}
		}
		if err := waitForReevaluation(waitCtx, p.options.ReevaluationInterval, p.options.Wake); err != nil {
			return finishPolicy(decision, input, policyContextDecision(err))
		}
	}
}

func finishPolicy(previous AdmissionDecision, input AdmissionInput, result AdmissionDecision) (AdmissionDecision, error) {
	if result.Evaluation.EvaluationID == "" {
		result.Evaluation = newEvaluation(input, EvaluationCancelled, Unknown, result.Cause)
	}
	if len(previous.Evaluations) > 0 {
		result.Evaluations = append(previous.Evaluations, result.Evaluations...)
		if result.Evaluation.EvaluationID == "" {
			result.Evaluation = previous.Evaluations[len(previous.Evaluations)-1]
		}
	}
	return result, policyError(result.Action)
}

func policyContextDecision(err error) AdmissionDecision {
	if errors.Is(err, context.DeadlineExceeded) {
		return AdmissionDecision{Action: PolicyTimeout, Cause: ErrAdmissionTimeout.Error()}
	}
	return AdmissionDecision{Action: PolicyCancelled, Cause: ErrAdmissionCancelled.Error()}
}

func policyError(action PolicyAction) error {
	switch action {
	case PolicyTimeout:
		return ErrAdmissionTimeout
	case PolicyCancelled:
		return ErrAdmissionCancelled
	case PolicyBlocked:
		return ErrAdmissionRecovery
	case PolicyReject:
		return ErrAdmissionRejected
	default:
		return nil
	}
}

func waitForReevaluation(ctx context.Context, interval time.Duration, wake <-chan struct{}) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case <-wake:
		return nil
	}
}
