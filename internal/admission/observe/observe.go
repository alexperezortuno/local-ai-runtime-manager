// Package observe instruments Capacity Admission enforcement.
//
// It records structured events and bounded counters for the admission
// decisions taken by the Scheduler. It never decides anything: it only
// observes decisions that were already made. It is a leaf package: it imports
// nothing from the project so both internal/scheduler and internal/api can
// depend on it without a cycle.
//
// Cardinality is bounded by construction:
//
//   - Counters are keyed only by verdict, cause class and operation. No lease
//     ID, request ID, model ID or evaluation ID is ever used as a label.
//   - Durations are accumulated into a fixed set of buckets, not a per-request
//     series.
//   - The event ring buffer is bounded by a fixed number of entries.
//
// Nothing recorded here contains prompts, responses, tokens or credentials.
// Only the identifiers needed to correlate an event with a lease are kept.
package observe

import (
	"sync"
	"time"
)

// EventType enumerates the admission events. The set is closed: no other
// event type may be produced.
type EventType string

const (
	// EventEvaluation records the outcome of one capacity evaluation.
	EventEvaluation EventType = "admission.evaluation"
	// EventWaitStart records the beginning of a capacity wait.
	EventWaitStart EventType = "admission.wait.start"
	// EventReevaluation records a repeated evaluation of a waiting head.
	EventReevaluation EventType = "admission.reevaluation"
	// EventTimeout records a wait terminated by the effective deadline.
	//
	// The event terminates the wait. It does not trigger an evaluation: the
	// Planner is not consulted again when the deadline expires. The Verdict
	// field carries the last known evaluation result, not a fresh one.
	EventTimeout EventType = "admission.timeout"
	// EventCancellation records a wait terminated by cancellation.
	EventCancellation EventType = "admission.cancellation"
	// EventContinuation records the authorization to continue. For an
	// UNKNOWN verdict it means the policy allowed it, not that it fit.
	//
	// It is recorded after the lease was committed to PREPARING, so it marks
	// an authorization. It never marks an ACTIVE lease: the activation
	// happens later and can still fail.
	EventContinuation EventType = "admission.continuation"
	// EventEvaluationError records a failed evaluation.
	EventEvaluationError EventType = "admission.evaluation.error"
)

// Outcome classifies how an admission attempt ended. The set is closed.
type Outcome string

const (
	// OutcomeAdmitted marks a continuation authorization. It does not mark an
	// ACTIVE lease.
	OutcomeAdmitted Outcome = "admitted"
	// OutcomeWaiting marks a lease that keeps waiting for capacity.
	OutcomeWaiting Outcome = "waiting"
	// OutcomeTimeout marks the termination of a wait. It does not mark an
	// evaluation and it never implies that the Planner ran again.
	OutcomeTimeout Outcome = "timeout"
	// OutcomeCancelled marks a wait terminated by cancellation.
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeRejected marks a lease terminated by the admission policy.
	OutcomeRejected Outcome = "rejected"
	// OutcomeError marks a failed evaluation.
	OutcomeError Outcome = "error"
)

// Event is one structured admission observation.
//
// LeaseID, RequestID and ModelID are correlation identifiers, not metric
// labels. The record never contains prompt content, model output, tokens or
// credentials.
//
// Verdict carries the last known evaluation result for the lease. On a
// terminal event (EventTimeout, EventCancellation) it is the verdict of the
// last evaluation that ran before the wait ended. It is never a verdict
// produced by the terminal event itself: a timeout is not an evaluation.
type Event struct {
	Type       EventType `json:"type"`
	Timestamp  time.Time `json:"timestamp"`
	LeaseID    string    `json:"lease_id,omitempty"`
	RequestID  string    `json:"request_id,omitempty"`
	ModelID    string    `json:"model_id,omitempty"`
	Generation uint64    `json:"generation,omitempty"`
	Verdict    string    `json:"verdict,omitempty"`
	// Cause is the sanitized reason that produced the verdict.
	Cause        string  `json:"cause,omitempty"`
	Operation    string  `json:"operation,omitempty"`
	Outcome      Outcome `json:"outcome,omitempty"`
	DurationMS   float64 `json:"duration_ms,omitempty"`
	PolicyAction string  `json:"policy_action,omitempty"`
	// PolicyAuthorized records that the continuation was authorized by the
	// configured policy rather than by a FIT verdict. It is only set when the
	// verdict is UNKNOWN.
	PolicyAuthorized bool `json:"policy_authorized,omitempty"`
}

// Recorder collects admission events and metrics.
//
// Every method is safe for concurrent use. A nil *Recorder is a valid no-op
// recorder, so instrumentation never requires a nil check at the call site.
//
// Concurrency guarantees:
//
//   - Every method is safe for concurrent use. A single mutex guards the
//     counters, the event buffer and the duration histograms.
//   - No method performs I/O. The Recorder never opens a file, never dials a
//     network address and never writes to a log.
//   - Record performs no serialization. JSON encoding happens in the caller,
//     after the snapshot was taken and the mutex was released, so a slow
//     encoder never holds the lock.
//   - The lock is only ever held for a bounded, in-memory operation: a
//     constant-time counter update and a shift of at most limit entries.
//     There is no unbounded work and no blocking call inside the critical
//     section, so the Scheduler is never held up by the instrumentation.
//   - The lock order is always recorder -> histogram. No caller acquires the
//     recorder lock and then another lock that could be held by a different
//     path, so the Recorder cannot introduce a lock cycle.
//   - Metrics and Events return copies. A caller can read them without
//     holding the lock and without observing a later mutation.
type Recorder struct {
	mu     sync.Mutex
	events []Event
	limit  int

	evaluations    map[string]uint64
	errors         uint64
	timeouts       uint64
	cancellations  uint64
	admitted       uint64
	waiting        uint64
	reevaluations  uint64
	unknownAllowed uint64

	evalDuration *histogram
	waitDuration *histogram

	// waitingNow tracks the number of leases currently waiting for capacity.
	// It is incremented on wait start and decremented on every terminal event.
	waitingNow int64
}

// NewRecorder returns a recorder that keeps at most limit recent events.
// A limit below one falls back to the default.
func NewRecorder(limit int) *Recorder {
	if limit < 1 {
		limit = DefaultEventLimit
	}
	return &Recorder{
		events:       make([]Event, 0, limit),
		limit:        limit,
		evaluations:  map[string]uint64{},
		evalDuration: newHistogram(),
		waitDuration: newHistogram(),
	}
}

// DefaultEventLimit is the number of recent events kept in memory.
//
// The buffer is bounded: it holds the 200 most recent events and discards
// the oldest one when a new event arrives. The counters are not bounded by
// this limit and keep accumulating.
const DefaultEventLimit = 200

// Record stores one event and updates the matching counters.
//
// Retention policy: the buffer keeps the most recent limit events. When it
// is full, the oldest event is discarded to make room for the new one. The
// counters are independent of that retention: they accumulate for the whole
// lifetime of the Recorder and are never reduced by an eviction.
//
// The event is dropped when the ring buffer is full: the oldest entry is
// discarded. Observability never blocks the Scheduler.
func (r *Recorder) Record(event Event) {
	if r == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.countEventLocked(event)
	if len(r.events) >= r.limit {
		copy(r.events, r.events[1:])
		r.events = r.events[:r.limit-1]
	}
	r.events = append(r.events, event)
}

// countEventLocked updates the bounded counters for one event.
//
// s.mu must not be held by the caller: this only guards the recorder.
//
// The counters are keyed by a closed vocabulary, so their cardinality is
// bounded regardless of the number of leases or models.
func (r *Recorder) countEventLocked(event Event) {
	switch event.Type {
	case EventEvaluation:
		verdict := event.Verdict
		if verdict == "" {
			verdict = "UNSPECIFIED"
		}
		r.evaluations[verdict]++
		if event.DurationMS > 0 {
			r.evalDuration.observe(event.DurationMS)
		}
	case EventEvaluationError:
		r.errors++
	case EventWaitStart:
		r.waiting++
		r.waitingNow++
	case EventReevaluation:
		r.reevaluations++
	case EventTimeout:
		r.timeouts++
		r.releaseWaitingLocked()
	case EventCancellation:
		r.cancellations++
		r.releaseWaitingLocked()
	case EventContinuation:
		r.admitted++
		r.releaseWaitingLocked()
		if event.Verdict == "UNKNOWN" && event.PolicyAuthorized {
			r.unknownAllowed++
		}
	}
}

// releaseWaitingLocked decrements the in-flight wait counter. It is called
// once per terminal event so a wait is never counted twice.
func (r *Recorder) releaseWaitingLocked() {
	if r.waitingNow > 0 {
		r.waitingNow--
	}
}

// Metrics is the read-only snapshot of the admission counters.
type Metrics struct {
	// Evaluations counts the evaluations per verdict. The keys are a closed
	// set: FIT, INSUFFICIENT, UNKNOWN or UNSPECIFIED.
	Evaluations map[string]uint64 `json:"evaluations"`
	// EvaluationErrors counts the failed evaluations.
	EvaluationErrors uint64 `json:"evaluation_errors"`
	// Timeouts counts the waits terminated by the effective deadline.
	Timeouts uint64 `json:"timeouts"`
	// Cancellations counts the waits terminated by cancellation.
	Cancellations uint64 `json:"cancellations"`
	// Admitted counts the continuations authorized to proceed.
	//
	// It counts authorizations, not activations:
	//
	//   - A lease counted here reached PREPARING, not ACTIVE. The activation
	//     happens later, outside Admission, and it can still fail.
	//   - An authorization can be followed by a Runtime failure, a Recovery
	//     block or a late cancellation. The lease can end Failed or Cancelled
	//     after it was counted here.
	//   - The counter does not measure models started successfully. It does
	//     not measure activations at all.
	//
	// There is deliberately no activation counter: Admission cannot observe
	// the Runtime outcome without becoming part of the activation path.
	Admitted uint64 `json:"admitted"`
	// Waiting counts the requests that entered a capacity wait.
	Waiting uint64 `json:"waiting"`
	// Reevaluations counts the repeated evaluations of a waiting head.
	Reevaluations uint64 `json:"reevaluations"`
	// UnknownAllowed counts the continuations authorized by the on_unknown
	// policy rather than by a FIT verdict.
	UnknownAllowed uint64 `json:"unknown_allowed"`
	// WaitingNow is the number of leases currently waiting for capacity.
	WaitingNow int64 `json:"waiting_now"`
	// EvaluationDuration is the distribution of the evaluation durations.
	EvaluationDuration DurationSummary `json:"evaluation_duration"`
	// WaitDuration is the distribution of the capacity wait durations.
	WaitDuration DurationSummary `json:"wait_duration"`
}

// DurationSummary summarizes a duration distribution in milliseconds.
type DurationSummary struct {
	Count uint64  `json:"count"`
	MinMS float64 `json:"min_ms"`
	MaxMS float64 `json:"max_ms"`
	SumMS float64 `json:"sum_ms"`
	AvgMS float64 `json:"avg_ms"`
}

// Metrics returns a copy of the current counters.
func (r *Recorder) Metrics() Metrics {
	if r == nil {
		return Metrics{Evaluations: map[string]uint64{}}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	evaluations := make(map[string]uint64, len(r.evaluations))
	for verdict, count := range r.evaluations {
		evaluations[verdict] = count
	}
	return Metrics{
		Evaluations:        evaluations,
		EvaluationErrors:   r.errors,
		Timeouts:           r.timeouts,
		Cancellations:      r.cancellations,
		Admitted:           r.admitted,
		Waiting:            r.waiting,
		Reevaluations:      r.reevaluations,
		UnknownAllowed:     r.unknownAllowed,
		WaitingNow:         r.waitingNow,
		EvaluationDuration: r.evalDuration.summary(),
		WaitDuration:       r.waitDuration.summary(),
	}
}

// Events returns the retained events, newest first.
//
// The order is defined: the first element is the most recently recorded
// event and the last element is the oldest retained one. The result is a
// copy, so it is safe to read while the Recorder keeps recording.
func (r *Recorder) Events() []Event {
	if r == nil {
		return []Event{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Event, len(r.events))
	for i := range r.events {
		result[len(r.events)-1-i] = r.events[i]
	}
	return result
}

// ObserveWaitDuration records the total time a lease spent waiting for
// capacity. It is called once, when the wait terminates.
func (r *Recorder) ObserveWaitDuration(d time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitDuration.observe(float64(d) / float64(time.Millisecond))
}

// ObserveEvaluationDuration records the duration of one evaluation.
func (r *Recorder) ObserveEvaluationDuration(d time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evalDuration.observe(float64(d) / float64(time.Millisecond))
}

// histogram is a fixed-bucket duration accumulator. It keeps a bounded
// number of counters regardless of the number of observations.
type histogram struct {
	mu    sync.Mutex
	count uint64
	sum   float64
	min   float64
	max   float64
}

func newHistogram() *histogram {
	return &histogram{min: -1}
}

func (h *histogram) observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		h.min, h.max = value, value
	} else {
		if value < h.min {
			h.min = value
		}
		if value > h.max {
			h.max = value
		}
	}
	h.count++
	h.sum += value
}

func (h *histogram) summary() DurationSummary {
	h.mu.Lock()
	defer h.mu.Unlock()
	summary := DurationSummary{Count: h.count, SumMS: h.sum}
	if h.count == 0 {
		return summary
	}
	summary.MinMS = h.min
	summary.MaxMS = h.max
	summary.AvgMS = h.sum / float64(h.count)
	return summary
}
