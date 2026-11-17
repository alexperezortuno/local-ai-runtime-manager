package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AdmissionVerdict is the Scheduler-side projection of a capacity decision.
// It never hides the difference between a measurement, a preventive decision
// and a failure to measure.
type AdmissionVerdict string

const (
	// AdmissionFit allows the operation to continue to the final check. It is
	// not a reservation and does not guarantee that Runtime can start.
	AdmissionFit AdmissionVerdict = "FIT"
	// AdmissionInsufficient blocks a new load until capacity frees up or the
	// effective deadline expires.
	AdmissionInsufficient AdmissionVerdict = "INSUFFICIENT"
	// AdmissionUnknown means the capacity could not be established.
	AdmissionUnknown AdmissionVerdict = "UNKNOWN"
)

// AdmissionOperation describes the work being evaluated.
const (
	AdmissionOperationNewLoad       = "NEW_LOAD"
	AdmissionOperationResidentReuse = "RESIDENT_REUSE"
)

// AdmissionRequest is the immutable correlation captured for an admission
// evaluation. It does not grant authority to start a process.
type AdmissionRequest struct {
	ModelID    string
	RequestID  string
	LeaseID    string
	Operation  string
	Generation uint64
	// Resident carries the observed runtime identity for RESIDENT_REUSE. It is
	// a snapshot: the final check revalidates it against live Runtime state.
	Resident *AdmissionResident
}

// AdmissionResident is the Scheduler-owned view of a runtime process identity.
// PID alone is never a sufficient identity.
type AdmissionResident struct {
	ModelID           string
	PID               int
	ProcessInstanceID string
	StartIdentity     string
	RuntimeState      string
	OwnershipStatus   string
}

// AdmissionEvidence is the read-only result of one evaluation.
type AdmissionEvidence struct {
	Verdict       AdmissionVerdict
	EvaluationID  string
	Cause         string
	Justification []string
	Source        []string
	EvaluatedAt   time.Time
}

// AdmissionEvaluator is the consumer-owned boundary for capacity admission.
// Implementations must be read-only with respect to Scheduler, leases,
// Runtime, and Recovery. Evaluate must honor ctx and must never block longer
// than the deadline it is given.
type AdmissionEvaluator interface {
	Evaluate(context.Context, AdmissionRequest) (AdmissionEvidence, error)
}

// AdmissionOptions configures the Scheduler-owned admission policy. The
// Scheduler owns the wait state; the evaluator only reports a verdict.
type AdmissionOptions struct {
	// OnInsufficient is "wait" (default) or "reject".
	OnInsufficient string
	// OnUnknown is "allow" (default), "wait" or "reject".
	OnUnknown string
	// WaitTimeout bounds the total admission wait measured from acceptance.
	WaitTimeout time.Duration
	// ReevaluationInterval is the periodic reevaluation cadence.
	ReevaluationInterval time.Duration
	// ProxyQueueWaitTimeout participates in the effective deadline when the
	// caller supplied it. Zero means "no proxy deadline".
	ProxyQueueWaitTimeout time.Duration
}

// DefaultAdmissionOptions returns the conservative defaults required by the
// admission contract: wait on insufficient, allow on unknown.
func DefaultAdmissionOptions() AdmissionOptions {
	return AdmissionOptions{OnInsufficient: "wait", OnUnknown: "allow", WaitTimeout: 5 * time.Minute, ReevaluationInterval: 2 * time.Second}
}

// Normalize applies defaults and rejects unknown policy enums.
func (o AdmissionOptions) Normalize() (AdmissionOptions, error) {
	defaults := DefaultAdmissionOptions()
	if o.OnInsufficient == "" {
		o.OnInsufficient = defaults.OnInsufficient
	}
	if o.OnUnknown == "" {
		o.OnUnknown = defaults.OnUnknown
	}
	if o.WaitTimeout <= 0 {
		o.WaitTimeout = defaults.WaitTimeout
	}
	if o.ReevaluationInterval <= 0 {
		o.ReevaluationInterval = defaults.ReevaluationInterval
	}
	if o.OnInsufficient != "wait" && o.OnInsufficient != "reject" {
		return o, errors.New("admission on_insufficient must be wait or reject")
	}
	if o.OnUnknown != "allow" && o.OnUnknown != "wait" && o.OnUnknown != "reject" {
		return o, errors.New("admission on_unknown must be allow, wait or reject")
	}
	return o, nil
}

// AdmissionContextProvider supplies the caller's context and the effective
// deadline for an acquisition. Implementations must return a context that is
// not retained after the acquisition terminates.
type AdmissionContextProvider interface {
	// AdmissionContext returns the caller context and, when known, the
	// absolute deadline contributed by the caller (for example the proxy
	// queue wait timeout). A zero time means "no deadline". The boolean
	// reports whether a context is available at all.
	AdmissionContext(leaseID string) (context.Context, time.Time, bool)
}

// AdmissionErrorKind enumerates the terminal reasons of an admission wait.
type AdmissionErrorKind string

const (
	AdmissionErrorCapacity    AdmissionErrorKind = "capacity"
	AdmissionErrorTimeout     AdmissionErrorKind = "timeout"
	AdmissionErrorUnavailable AdmissionErrorKind = "unavailable"
	AdmissionErrorCancelled   AdmissionErrorKind = "cancelled"
	AdmissionErrorRecovery    AdmissionErrorKind = "recovery"
)

var (
	// ErrAdmissionCapacity is the preventive decision: capacity is insufficient.
	ErrAdmissionCapacity = errors.New("admission capacity insufficient")
	// ErrAdmissionTimeout means the effective deadline expired while waiting.
	ErrAdmissionTimeout = errors.New("admission wait timed out")
	// ErrAdmissionUnavailable means no evaluation could be obtained.
	ErrAdmissionUnavailable = errors.New("admission evaluation unavailable")
	// ErrAdmissionCancelled means the wait was cancelled before commit.
	ErrAdmissionCancelled = errors.New("admission cancelled")
	// ErrAdmissionRecovery means Recovery blocked the operation.
	ErrAdmissionRecovery = errors.New("admission requires recovery")
)

// AdmissionError carries the terminal reason of an admission decision without
// hiding the underlying cause.
type AdmissionError struct {
	Kind         AdmissionErrorKind
	RequestID    string
	LeaseID      string
	ModelID      string
	Operation    string
	EvaluationID string
	Cause        error
	Retryable    bool
}

// admissionFailurePrefix marks a persisted lease error as a typed admission
// failure. The HTTP layer maps it back to a typed error without parsing the
// human-readable message.
const admissionFailurePrefix = "admission: "

// AdmissionFailure reconstructs the typed admission error of a lease that
// failed admission. It returns nil when the lease did not fail for an
// admission reason.
//
// The persisted lease error carries the admission kind, so the HTTP layer can
// map a terminal admission decision without parsing free-form text.
func AdmissionFailure(err string) *AdmissionError {
	if !strings.HasPrefix(err, admissionFailurePrefix) {
		return nil
	}
	rest := strings.TrimPrefix(err, admissionFailurePrefix)
	kind, cause, _ := strings.Cut(rest, ": ")
	admissionErr := &AdmissionError{Kind: AdmissionErrorKind(strings.TrimSpace(kind))}
	switch admissionErr.Kind {
	case AdmissionErrorCapacity:
		admissionErr.Cause = ErrAdmissionCapacity
	case AdmissionErrorTimeout:
		admissionErr.Cause = ErrAdmissionTimeout
	case AdmissionErrorUnavailable:
		admissionErr.Cause = ErrAdmissionUnavailable
	case AdmissionErrorCancelled:
		admissionErr.Cause = ErrAdmissionCancelled
	case AdmissionErrorRecovery:
		admissionErr.Cause = ErrAdmissionRecovery
	default:
		return nil
	}
	if cause != "" {
		admissionErr.Cause = fmt.Errorf("%w: %s", admissionErr.Cause, cause)
	}
	return admissionErr
}

func (e *AdmissionError) Error() string {
	if e.Cause != nil {
		return string(e.Kind) + ": " + e.Cause.Error()
	}
	return string(e.Kind)
}

func (e *AdmissionError) Unwrap() error { return e.Cause }

// Is maps the typed kind onto the package sentinels so callers can use
// errors.Is without comparing messages.
func (e *AdmissionError) Is(target error) bool {
	switch e.Kind {
	case AdmissionErrorCapacity:
		return target == ErrAdmissionCapacity
	case AdmissionErrorTimeout:
		return target == ErrAdmissionTimeout
	case AdmissionErrorUnavailable:
		return target == ErrAdmissionUnavailable
	case AdmissionErrorCancelled:
		return target == ErrAdmissionCancelled
	case AdmissionErrorRecovery:
		return target == ErrAdmissionRecovery
	default:
		return false
	}
}

// admissionPhase is the internal Scheduler phase. It is deliberately not a
// LeaseState: the lease stays QUEUED and keeps its FIFO position while the
// Scheduler waits for capacity.
type admissionPhase string

const (
	admissionPhaseNone       admissionPhase = ""
	admissionPhaseWaiting    admissionPhase = "WAITING_CAPACITY"
	admissionPhaseEvaluating admissionPhase = "EVALUATING"
)

// admissionWait is the Scheduler-owned wait state for one FIFO head. It holds
// no runtime resource: no process, no reservation, no TTL, no heartbeat.
type admissionWait struct {
	phase          admissionPhase
	leaseID        string
	generation     uint64
	deadline       time.Time
	nextEvaluation time.Time
	reason         string
	lastError      error
}
