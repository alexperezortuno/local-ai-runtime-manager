package capacity

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"local-ai-runtime/internal/corecontract"
)

// Admission operations describe what the scheduler observed at admission time.
// They are observations only; none of them is a scheduling decision.
const (
	OperationNewLoad       = "NEW_LOAD"
	OperationResidentReuse = "RESIDENT_REUSE"
	OperationQueued        = "QUEUED"
)

const (
	ObservationEvaluated = "EVALUATED"
	ObservationError     = "ERROR"
	ObservationDropped   = "DROPPED"
)

// PlanSource is the small interface needed by Shadow. *Planner implements it,
// and the interface keeps the observation path independently testable.
type PlanSource interface {
	Plan(context.Context, string) Result
}

type ShadowOptions struct {
	Enabled           bool
	EvaluationTimeout time.Duration
	MaxRecent         int
	QueueSize         int
}

func DefaultShadowOptions() ShadowOptions {
	return ShadowOptions{Enabled: true, EvaluationTimeout: 2 * time.Second, MaxRecent: 100, QueueSize: 128}
}

type Evaluation struct {
	EvaluationID         string    `json:"evaluation_id"`
	RequestID            string    `json:"request_id"`
	ModelID              string    `json:"model_id"`
	Operation            string    `json:"operation"`
	Verdict              Verdict   `json:"verdict"`
	Timestamp            time.Time `json:"timestamp"`
	MetricsSource        string    `json:"metrics_source"`
	ObservationState     string    `json:"observation_state"`
	SystemAvailableBytes *uint64   `json:"system_available_bytes,omitempty"`
	GPUFreeBytes         *uint64   `json:"gpu_free_bytes,omitempty"`
	EstimatedBytes       uint64    `json:"estimated_bytes"`
	RequiredBytes        uint64    `json:"required_bytes"`
	SystemRequiredBytes  uint64    `json:"system_required_bytes"`
	GPURequiredBytes     uint64    `json:"gpu_required_bytes"`
	SystemMarginPercent  float64   `json:"system_margin_percent"`
	GPUMarginPercent     float64   `json:"gpu_margin_percent"`
	Justification        []string  `json:"justification"`
}

type shadowJob struct {
	evaluationID string
	requestID    string
	modelID      string
	operation    string
}

// Shadow evaluates asynchronously so GGUF parsing and memory observation are
// never on the Scheduler's admission or activation path.
type Shadow struct {
	planner PlanSource
	enabled bool
	timeout time.Duration
	max     int
	jobs    chan shadowJob
	done    chan struct{}
	close   sync.Once
	mu      sync.Mutex
	recent  []Evaluation
}

func NewShadow(planner PlanSource) *Shadow {
	options := DefaultShadowOptions()
	return NewShadowWithOptions(planner, options)
}

func NewShadowWithOptions(planner PlanSource, options ShadowOptions) *Shadow {
	if options.EvaluationTimeout <= 0 {
		options.EvaluationTimeout = 2 * time.Second
	}
	if options.MaxRecent <= 0 {
		options.MaxRecent = 100
	}
	if options.QueueSize <= 0 {
		options.QueueSize = 128
	}
	s := &Shadow{planner: planner, enabled: options.Enabled, timeout: options.EvaluationTimeout, max: options.MaxRecent, jobs: make(chan shadowJob, options.QueueSize), done: make(chan struct{})}
	if options.Enabled {
		go s.worker()
	}
	return s
}

// ObserveAdmission satisfies the scheduler's intentionally string-only
// observer contract. It returns immediately, including when the queue is full.
func (s *Shadow) ObserveAdmission(requestID, modelID, operation string) {
	if s == nil || !s.enabled {
		return
	}
	evaluationID, err := corecontract.NewID()
	if err != nil {
		evaluationID = fmt.Sprintf("evaluation-%d", time.Now().UnixNano())
	}
	job := shadowJob{evaluationID: evaluationID, requestID: requestID, modelID: modelID, operation: normalizeOperation(operation)}
	select {
	case s.jobs <- job:
	default:
		s.record(Evaluation{EvaluationID: evaluationID, RequestID: requestID, ModelID: modelID, Operation: job.operation, Verdict: Unknown, Timestamp: time.Now().UTC(), ObservationState: ObservationDropped, Justification: []string{"cola de evaluación llena; la admisión no fue afectada"}})
	}
}

func (s *Shadow) worker() {
	for {
		select {
		case job := <-s.jobs:
			s.evaluate(job)
		case <-s.done:
			for {
				select {
				case job := <-s.jobs:
					s.evaluate(job)
				default:
					return
				}
			}
		}
	}
}

func (s *Shadow) evaluate(job shadowJob) {
	evaluation := Evaluation{EvaluationID: job.evaluationID, RequestID: job.requestID, ModelID: job.modelID, Operation: job.operation, Verdict: Unknown, Timestamp: time.Now().UTC(), ObservationState: ObservationEvaluated}
	if job.operation == OperationResidentReuse {
		evaluation.Verdict = Fit
		evaluation.MetricsSource = "resident-model"
		evaluation.Justification = []string{"modelo residente; no se estimó una nueva carga de memoria"}
		s.record(evaluation)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	result, panicked, timedOut := planWithTimeout(s.planner, ctx, job.modelID)
	if panicked {
		evaluation.ObservationState = ObservationError
		evaluation.Justification = []string{"capacity planner failed; la solicitud no fue afectada"}
		s.record(evaluation)
		return
	}
	if timedOut || ctx.Err() != nil {
		evaluation.ObservationState = ObservationError
		evaluation.Justification = []string{"capacity planner excedió el tiempo de observación; la solicitud no fue afectada"}
		s.record(evaluation)
		return
	}
	evaluation.Verdict = result.Verdict
	evaluation.Timestamp = time.Now().UTC()
	evaluation.MetricsSource = metricsSource(result)
	evaluation.SystemAvailableBytes = result.Capacity.SystemAvailableBytes
	evaluation.GPUFreeBytes = result.Capacity.GPUFreeBytes
	evaluation.EstimatedBytes = result.Estimate.RawBytes
	evaluation.RequiredBytes = result.Estimate.RequiredBytes
	evaluation.SystemRequiredBytes = result.Estimate.SystemRequiredBytes
	evaluation.GPURequiredBytes = result.Estimate.GPURequiredBytes
	evaluation.SystemMarginPercent = result.Estimate.SystemMarginPercent
	evaluation.GPUMarginPercent = result.Estimate.GPUMarginPercent
	evaluation.Justification = safeJustification(result.Justification)
	s.record(evaluation)
}

func planWithTimeout(planner PlanSource, ctx context.Context, modelID string) (Result, bool, bool) {
	type outcome struct {
		result   Result
		panicked bool
	}
	completed := make(chan outcome, 1)
	go func() {
		result, panicked := safePlan(planner, ctx, modelID)
		completed <- outcome{result: result, panicked: panicked}
	}()
	select {
	case result := <-completed:
		return result.result, result.panicked, false
	case <-ctx.Done():
		return Result{Verdict: Unknown}, false, true
	}
}

func safePlan(planner PlanSource, ctx context.Context, modelID string) (result Result, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	return planner.Plan(ctx, modelID), false
}

func metricsSource(result Result) string {
	sources := []string{result.Snapshot.Source, result.Snapshot.System.Source, result.Snapshot.GPUProvider.Source}
	seen := make(map[string]struct{}, len(sources))
	unique := make([]string, 0, len(sources))
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		unique = append(unique, source)
	}
	sort.Strings(unique)
	return strings.Join(unique, ",")
}

func safeJustification(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.HasPrefix(value, "archivo GGUF no disponible:") {
			result = append(result, "archivo GGUF no disponible")
			continue
		}
		result = append(result, value)
	}
	return result
}

func normalizeOperation(operation string) string {
	switch operation {
	case OperationNewLoad, OperationResidentReuse, OperationQueued:
		return operation
	default:
		return OperationNewLoad
	}
}

func (s *Shadow) record(evaluation Evaluation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recent) >= s.max {
		copy(s.recent, s.recent[1:])
		s.recent = s.recent[:s.max-1]
	}
	s.recent = append(s.recent, evaluation)
}

// Evaluations returns newest-first copies. It contains historical observations,
// not a claim about the current memory state.
func (s *Shadow) Evaluations() []Evaluation {
	if s == nil {
		return []Evaluation{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Evaluation, len(s.recent))
	for i := range s.recent {
		result[len(s.recent)-1-i] = cloneEvaluation(s.recent[i])
	}
	return result
}

func cloneEvaluation(e Evaluation) Evaluation {
	e.Justification = append([]string(nil), e.Justification...)
	if e.SystemAvailableBytes != nil {
		value := *e.SystemAvailableBytes
		e.SystemAvailableBytes = &value
	}
	if e.GPUFreeBytes != nil {
		value := *e.GPUFreeBytes
		e.GPUFreeBytes = &value
	}
	return e
}

func (s *Shadow) Close() {
	if s == nil || !s.enabled {
		return
	}
	s.close.Do(func() { close(s.done) })
}
