package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
)

var (
	ErrNotFound              = errors.New("lease or model not found")
	ErrQueueFull             = errors.New("scheduler queue is full")
	ErrConflict              = errors.New("operation conflicts with scheduler state")
	ErrAlreadyStopped        = errors.New("model is already downloaded")
	ErrLeaseActive           = errors.New("model has an active lease")
	ErrExpired               = errors.New("lease has expired")
	ErrClosed                = errors.New("scheduler is shutting down")
	ErrInferenceProtected    = errors.New("lease is protected by an active or ambiguous inference")
	ErrInvalidIdempotencyKey = errors.New("idempotency key must be a canonical UUID")
	ErrIdempotencyConflict   = errors.New("idempotency key was used with different acquisition parameters")
	ErrIdempotencyTerminal   = errors.New("idempotent acquisition is terminal")
	ErrIdempotencyRecovery   = errors.New("idempotent acquisition cannot be recovered after restart")
	ErrCapacity              = errors.New("model capacity exhausted")
)

type Scheduler struct {
	mu                 sync.Mutex
	manager            *runtime.Manager
	maxQueue           int
	ttl                time.Duration
	heartbeat          time.Duration
	maxLease           time.Duration
	leases             map[string]*Lease
	queue              leaseQueue
	activeID           string
	preparingID        string
	operationCancel    context.CancelFunc
	preloadQueue       []string
	preloadingID       string
	preloadCancel      context.CancelFunc
	preloadState       map[string]string
	preloadError       map[string]string
	preloadSuppressed  map[string]bool
	manual             []manualCommand
	manualBusy         bool
	manualModel        bool
	unloadWhenIdle     bool
	unloadModel        string
	residenceTimers    map[string]*time.Timer
	residenceDue       map[string]uint64
	residenceGen       map[string]uint64
	residenceDeadlines map[string]time.Time
	inferences         map[string]map[uint64]inferencePin
	nextInferenceID    uint64
	changedMu          sync.Mutex
	changed            chan struct{}
	closing            bool
	ctx                context.Context
	cancel             context.CancelFunc
	wake               chan struct{}
	done               chan struct{}
	closeOnce          sync.Once
	idempotencyPath    string
	idempotency        map[string]idempotencyRecord
	reconciliations    map[string]reconciliationRecord
	recoveryBlocked    bool
	cleanShutdown      bool
	releaseProtector   ReleaseProtector
	admissionGate      AdmissionGate
	// idempotencyFault is a package-local test seam. Production never sets it;
	// it lets persistence tests fail at an explicit durable-store boundary.
	idempotencyFault func(string) error
}

// ReleaseProtector is consulted while the scheduler lock is held. It must
// only inspect its own state and never call back into the scheduler.
type ReleaseProtector interface {
	Protected(leaseID, requestID, model string) (bool, string)
}

// AdmissionGate is a read-only Runtime Core guard. The scheduler remains the
// only component that owns queue and lease transitions.
type AdmissionGate interface {
	AdmissionBlocked() bool
}

type inferencePin struct {
	requestID string
	cancel    context.CancelFunc
}

type manualCommand struct {
	start bool
	id    string
	ctx   context.Context
	done  chan error
}

func New(manager *runtime.Manager, cfg config.SchedulerConfig) *Scheduler {
	s := newScheduler(manager, cfg)
	go s.worker()
	return s
}

// NewWithIdempotencyStore enables durable acquisition-key records. A process
// restart never restores leases or llama-server processes; unresolved records
// become RECOVERY_REQUIRED and continue reserving their keys.
func NewWithIdempotencyStore(manager *runtime.Manager, cfg config.SchedulerConfig, path string) (*Scheduler, error) {
	s := newScheduler(manager, cfg)
	if err := s.openIdempotencyStore(path); err != nil {
		s.cancel()
		return nil, err
	}
	go s.worker()
	return s, nil
}

func newScheduler(manager *runtime.Manager, cfg config.SchedulerConfig) *Scheduler {
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 100
	}
	if cfg.LeaseTTLDuration() <= 0 {
		cfg.LeaseTTL = "10m"
	}
	if cfg.HeartbeatDuration() <= 0 {
		cfg.Heartbeat = "30s"
	}
	if cfg.MaxLeaseDuration() <= 0 {
		cfg.MaxLease = "2h"
	}
	ctx, cancel := context.WithCancel(context.Background())
	if manager != nil {
		manager.SetMaxActiveModels(cfg.MaxActiveModels)
	}
	preloadState := make(map[string]string)
	if manager != nil {
		for id, model := range manager.Models() {
			if model.KeepAlive || policyDuration(model.TimeAlive) > 0 {
				preloadState[id] = PreloadNotStarted
			} else {
				preloadState[id] = PreloadNotRequired
			}
		}
	}
	s := &Scheduler{manager: manager, maxQueue: cfg.MaxQueueSize, ttl: cfg.LeaseTTLDuration(), heartbeat: cfg.HeartbeatDuration(), maxLease: cfg.MaxLeaseDuration(), leases: make(map[string]*Lease), inferences: make(map[string]map[uint64]inferencePin), residenceTimers: make(map[string]*time.Timer), residenceDue: make(map[string]uint64), residenceGen: make(map[string]uint64), residenceDeadlines: make(map[string]time.Time), preloadState: preloadState, preloadError: make(map[string]string), preloadSuppressed: make(map[string]bool), changed: make(chan struct{}), ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}), idempotency: make(map[string]idempotencyRecord), reconciliations: make(map[string]reconciliationRecord)}
	return s
}

func (s *Scheduler) Request(model string) (Lease, error) {
	if _, ok := s.manager.Models()[model]; !ok {
		return Lease{}, fmt.Errorf("%w: model %s", ErrNotFound, model)
	}
	id, err := newID()
	if err != nil {
		return Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return Lease{}, ErrClosed
	}
	if s.recoveryBlocked {
		return Lease{}, ErrIdempotencyRecovery
	}
	if blocked, _ := s.manager.RecoveryRequired(); blocked {
		return Lease{}, ErrIdempotencyRecovery
	}
	if s.admissionGate != nil && s.admissionGate.AdmissionBlocked() {
		return Lease{}, ErrIdempotencyRecovery
	}
	if len(s.queue) >= s.maxQueue {
		return Lease{}, ErrQueueFull
	}
	if err := s.markDirtyLocked(); err != nil {
		return Lease{}, fmt.Errorf("persist scheduler activity: %w", err)
	}
	if s.preloadSuppressed[model] {
		delete(s.preloadSuppressed, model)
		s.preloadState[model], s.preloadError[model] = PreloadOnDemand, ""
	}
	if s.preloadingID != "" && s.preloadingID != model && s.preloadCancel != nil {
		// An admitted request must not wait behind a background-only load.
		s.preloadCancel()
	}
	l := &Lease{ID: id, Model: model, State: Queued, CreatedAt: time.Now().UTC()}
	s.cancelResidenceLocked(model)
	s.leases[id] = l
	s.queue.push(id)
	s.notify()
	return cloneLease(*l), nil
}

func (s *Scheduler) Get(id string) (LeaseStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return LeaseStatus{}, ErrNotFound
	}
	result := LeaseStatus{Lease: s.leaseViewLocked(l)}
	if l.State == Queued {
		for i, queued := range s.queue {
			if queued == id {
				result.QueuePosition = i + 1
				break
			}
		}
	}
	return result, nil
}

// SetReleaseProtector installs the Runtime Core's single release guard.
func (s *Scheduler) SetReleaseProtector(protector ReleaseProtector) {
	s.mu.Lock()
	s.releaseProtector = protector
	s.mu.Unlock()
}

func (s *Scheduler) SetAdmissionGate(gate AdmissionGate) {
	s.mu.Lock()
	s.admissionGate = gate
	s.mu.Unlock()
}

// WaitActive waits for an automatically queued lease to become usable.
func (s *Scheduler) WaitActive(ctx context.Context, id string) error {
	for {
		s.mu.Lock()
		l, ok := s.leases[id]
		if !ok {
			s.mu.Unlock()
			return ErrNotFound
		}
		if l.State == Active {
			s.mu.Unlock()
			return nil
		}
		if l.State == Failed || l.State == Cancelled || l.State == Expired || l.State == Completed {
			s.mu.Unlock()
			return ErrConflict
		}
		changed := s.changedChannel()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// BeginInference atomically validates an active lease and pins its backend.
func (s *Scheduler) BeginInference(id, model string, cancel context.CancelFunc, requestIDs ...string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return nil, ErrNotFound
	}
	if l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) {
		proposed := cloneLease(*l)
		proposed.State = Expired
		if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
			return nil, fmt.Errorf("persist lease expiration: %w", err)
		}
		l.State = proposed.State
		s.retireLeaseLocked(id)
		s.notify()
	}
	if l.State != Active || s.activeID != id {
		if l.State == Expired {
			return nil, ErrExpired
		}
		return nil, ErrConflict
	}
	if l.Model != model {
		return nil, ErrConflict
	}
	s.nextInferenceID++
	inferenceID := s.nextInferenceID
	if s.inferences[id] == nil {
		s.inferences[id] = make(map[uint64]inferencePin)
	}
	requestID := ""
	if len(requestIDs) > 0 {
		requestID = requestIDs[0]
	}
	s.inferences[id][inferenceID] = inferencePin{requestID: requestID, cancel: cancel}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.inferences[id], inferenceID)
			if len(s.inferences[id]) == 0 {
				delete(s.inferences, id)
				if s.activeID == id && s.leases[id].State != Active {
					s.retireLeaseLocked(id)
				}
			}
			s.notify()
			s.mu.Unlock()
		})
	}, nil
}

func (s *Scheduler) retireLeaseLocked(id string) {
	if !s.isProtectedLocked(id) && s.activeID == id {
		model := s.leases[id].Model
		s.activeID = ""
		s.scheduleResidenceLocked(model)
	}
}

func (s *Scheduler) scheduleResidenceLocked(model string) {
	if model == "" {
		return
	}
	policy, ok := s.manager.Models()[model]
	s.residenceGen[model]++
	generation := s.residenceGen[model]
	if !ok || policy.KeepAlive {
		s.cancelResidenceLocked(model)
		return
	}
	if policy.TimeAlive == "" || policy.TimeAlive == "0s" {
		s.cancelResidenceLocked(model)
		s.unloadWhenIdle, s.unloadModel = true, model
		return
	}
	duration := policyDuration(policy.TimeAlive)
	if duration <= 0 {
		s.cancelResidenceLocked(model)
		s.unloadWhenIdle, s.unloadModel = true, model
		return
	}
	process, ready := s.manager.CurrentProcessForModel(model)
	if !ready || process.ProcessInstanceID == "" {
		return
	}
	s.cancelResidenceLocked(model)
	// cancelResidenceLocked increments the generation; capture the new value.
	generation = s.residenceGen[model]
	s.residenceDeadlines[model] = time.Now().Add(duration)
	s.residenceTimers[model] = time.AfterFunc(duration, func() {
		s.mu.Lock()
		if s.residenceGen[model] == generation {
			if current, currentReady := s.manager.CurrentProcessForModel(model); currentReady && current.ProcessInstanceID == process.ProcessInstanceID {
				s.residenceDue[model] = generation
			}
		}
		s.mu.Unlock()
		s.notify()
	})
}

func (s *Scheduler) cancelResidenceLocked(model string) {
	s.residenceGen[model]++
	if timer := s.residenceTimers[model]; timer != nil {
		timer.Stop()
		delete(s.residenceTimers, model)
	}
	delete(s.residenceDue, model)
	delete(s.residenceDeadlines, model)
	if s.unloadModel == model {
		s.unloadWhenIdle, s.unloadModel = false, ""
	}
}

func policyDuration(value string) time.Duration {
	d, _ := time.ParseDuration(value)
	return d
}

func (s *Scheduler) ActiveInferenceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, active := range s.inferences {
		count += len(active)
	}
	return count
}

func (s *Scheduler) Heartbeat(id string) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return Lease{}, ErrNotFound
	}
	if l.State == Expired {
		return cloneLease(*l), ErrExpired
	}
	if l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) && !s.isProtectedLocked(id) {
		proposed := cloneLease(*l)
		proposed.State = Expired
		if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
			return cloneLease(*l), fmt.Errorf("persist lease expiration: %w", err)
		}
		l.State = proposed.State
		s.retireLeaseLocked(id)
		s.notify()
		return cloneLease(*l), ErrExpired
	}
	if l.State != Active || s.activeID != id {
		return cloneLease(*l), ErrConflict
	}
	deadline := l.ActivatedAt.Add(s.maxLease)
	if !time.Now().Before(deadline) && !s.isProtectedLocked(id) {
		proposed := cloneLease(*l)
		proposed.State = Expired
		if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
			return cloneLease(*l), fmt.Errorf("persist lease expiration: %w", err)
		}
		l.State = proposed.State
		s.retireLeaseLocked(id)
		s.notify()
		return cloneLease(*l), ErrExpired
	}
	end := time.Now().Add(s.ttl)
	if end.After(deadline) {
		end = deadline
	}
	proposed := cloneLease(*l)
	proposed.ExpiresAt = &end
	if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
		return cloneLease(*l), fmt.Errorf("persist lease heartbeat: %w", err)
	}
	l.ExpiresAt = proposed.ExpiresAt
	s.notify()
	return cloneLease(*l), nil
}

func (s *Scheduler) Release(id string) (Lease, error) {
	return s.release(id, "", "")
}

// ReleaseAutomatic is the only automatic release entry point. The request
// identity is checked together with the lease before the scheduler changes it.
func (s *Scheduler) ReleaseAutomatic(id, requestID, model string) (Lease, error) {
	return s.release(id, requestID, model)
}

func (s *Scheduler) release(id, requestID, model string) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return Lease{}, ErrNotFound
	}
	if protected, reason := s.protectedLocked(id, requestID, model); protected {
		return cloneLease(*l), fmt.Errorf("%w: %s", ErrInferenceProtected, reason)
	}
	proposed := cloneLease(*l)
	cancelOperation := false
	switch proposed.State {
	case Queued:
		proposed.State = Cancelled
	case Preparing:
		proposed.State = Cancelled
		cancelOperation = s.preparingID == id && s.operationCancel != nil
	case Active:
		proposed.State = Completed
	default:
		return cloneLease(*l), nil
	}
	if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
		return cloneLease(*l), fmt.Errorf("persist lease release: %w", err)
	}
	*l = proposed
	if proposed.State == Cancelled {
		s.queue.remove(id)
	}
	if cancelOperation {
		s.operationCancel()
	}
	if proposed.State == Completed {
		s.retireLeaseLocked(id)
	}
	s.notify()
	return cloneLease(*l), nil
}

func (s *Scheduler) isProtectedLocked(id string) bool {
	protected, _ := s.protectedLocked(id, "", "")
	return protected
}

func (s *Scheduler) protectedLocked(id, requestID, model string) (bool, string) {
	if len(s.inferences[id]) > 0 {
		return true, "an admitted inference is still pinned"
	}
	if s.releaseProtector != nil {
		return s.releaseProtector.Protected(id, requestID, model)
	}
	return false, ""
}

func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result Snapshot
	models := s.manager.Models()
	for _, model := range models {
		if model.CapacityOccupied {
			result.CapacityUsed++
		}
	}
	result.CapacityLimit = len(models)
	// Manager owns the configured limit; resident models are the capacity unit.
	if s.manager != nil {
		result.CapacityLimit = s.manager.Capacity()
	}
	result.Pending = make([]Lease, 0, len(s.queue))
	for _, id := range s.queue {
		if l := s.leases[id]; l != nil {
			result.Pending = append(result.Pending, s.leaseViewLocked(l))
		}
	}
	if l := s.leases[s.activeID]; l != nil {
		v := s.leaseViewLocked(l)
		result.Active = &v
	}
	if l := s.leases[s.preparingID]; l != nil {
		v := s.leaseViewLocked(l)
		result.Preparing = &v
	}
	for _, model := range models {
		if model.PID == 0 && model.State == runtime.Stopped {
			continue
		}
		active, protected := s.modelLeaseInfoLocked(model.ID)
		remaining := ""
		if due, ok := s.residenceDeadlines[model.ID]; ok {
			if left := time.Until(due); left > 0 {
				remaining = left.Round(time.Millisecond).String()
			} else {
				remaining = "0s"
			}
		}
		preloadState, preloadError := s.preloadStatusLocked(model.ID)
		result.Residents = append(result.Residents, Resident{Model: model.ID, State: string(model.State), Residency: model.Residency, KeepAlive: model.KeepAlive, TimeAlive: model.TimeAlive, ProcessInstanceID: model.ProcessInstanceID, PID: model.PID, ActiveLeases: active, ProtectedInference: protected, ResidenceRemaining: remaining, CapacityOccupied: model.CapacityOccupied, RecoveryState: model.RecoveryState, OwnershipStatus: model.OwnershipStatus, PreloadState: preloadState, PreloadError: preloadError})
	}
	sort.Slice(result.Residents, func(i, j int) bool { return result.Residents[i].Model < result.Residents[j].Model })
	return result
}

// ModelsSnapshot enriches the existing runtime model contract with scheduler
// ownership and residency data without exposing a second administrative API.
func (s *Scheduler) ModelsSnapshot() map[string]runtime.ModelStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	models := s.manager.Models()
	for id, model := range models {
		active, protected := s.modelLeaseInfoLocked(id)
		model.ActiveLeases, model.ProtectedInference = active, protected
		if due, ok := s.residenceDeadlines[id]; ok {
			if left := time.Until(due); left > 0 {
				model.ResidenceRemaining = left.Round(time.Millisecond).String()
			} else {
				model.ResidenceRemaining = "0s"
			}
		}
		model.CapacityOccupied = model.PID != 0 || model.State == runtime.Starting || model.State == runtime.Stopping
		model.PreloadState, model.PreloadError = s.preloadStatusLocked(id)
		models[id] = model
	}
	return models
}

func (s *Scheduler) modelLeaseInfoLocked(model string) (int, bool) {
	active := 0
	protected := false
	for id, lease := range s.leases {
		if lease.Model != model {
			continue
		}
		if lease.State == Active {
			active++
			if s.isProtectedLocked(id) {
				protected = true
			}
		}
	}
	return active, protected
}

// RecoveryDiagnostics returns the durable scheduler evidence without exposing
// mutable scheduler state. It does not restore, release, or expire anything.
func (s *Scheduler) RecoveryDiagnostics() RecoveryDiagnostics {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RecoveryDiagnostics{StorePath: s.idempotencyPath, CleanShutdown: s.cleanShutdown, RecoveryBlocked: s.recoveryBlocked, Records: make([]RecoveryRecord, 0, len(s.idempotency))}
	for _, record := range s.idempotency {
		result.Records = append(result.Records, RecoveryRecord{Key: record.Key, Fingerprint: record.Fingerprint, Lease: cloneLease(record.Lease), CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt})
	}
	sort.Slice(result.Records, func(i, j int) bool { return result.Records[i].Key < result.Records[j].Key })
	return result
}

// Leases returns a stable, read-only view of every in-memory lease. Recovery
// uses it only for correlation; it never mutates this slice.
func (s *Scheduler) Leases() []Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Lease, 0, len(s.leases))
	for _, lease := range s.leases {
		result = append(result, cloneLease(*lease))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *Scheduler) leaseViewLocked(l *Lease) Lease {
	view := cloneLease(*l)
	view.Protected, view.ProtectionReason = s.protectedLocked(l.ID, "", "")
	return view
}

// Manual operations are accepted only when no lease is active or waiting, and
// run on the same worker as lease transitions.
func (s *Scheduler) ManualStart(ctx context.Context, id string) error {
	return s.manualOperation(ctx, true, id)
}
func (s *Scheduler) ManualStop(ctx context.Context, id string) error {
	return s.manualOperation(ctx, false, id)
}

func (s *Scheduler) manualOperation(ctx context.Context, start bool, id string) error {
	model, ok := s.manager.Models()[id]
	if !ok {
		return fmt.Errorf("%w: model %s", ErrNotFound, id)
	}
	alreadyStopped := !start && model.PID == 0 && model.State == runtime.Stopped
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.recoveryBlocked {
		s.mu.Unlock()
		return ErrIdempotencyRecovery
	}
	if blocked, _ := s.manager.RecoveryRequired(); blocked {
		s.mu.Unlock()
		return ErrIdempotencyRecovery
	}
	if s.admissionGate != nil && s.admissionGate.AdmissionBlocked() {
		s.mu.Unlock()
		return ErrIdempotencyRecovery
	}
	if !start && s.preloadQueuedLocked(id) {
		if err := s.markDirtyLocked(); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("persist scheduler activity: %w", err)
		}
		s.removePreloadLocked(id)
		s.preloadSuppressed[id] = true
		s.preloadState[id], s.preloadError[id] = PreloadAdminStopped, "administrative stop requested"
		s.notify()
		s.mu.Unlock()
		return nil
	}
	preloading := s.preloadingID == id
	if alreadyStopped && !preloading {
		s.mu.Unlock()
		return ErrAlreadyStopped
	}
	if preloading && !start {
		s.preloadSuppressed[id] = true
		s.preloadState[id], s.preloadError[id] = PreloadAdminStopped, "administrative stop requested"
		if s.preloadCancel != nil {
			s.preloadCancel()
		}
	}
	active := s.activeID != ""
	if s.manualBusy || active || s.preparingID != "" || start && (len(s.queue) != 0 || len(s.preloadQueue) != 0) {
		if !start && active {
			if lease := s.leases[s.activeID]; lease != nil && lease.Model == id && s.isProtectedLocked(s.activeID) {
				s.mu.Unlock()
				return fmt.Errorf("%w: protected inference is unresolved", ErrInferenceProtected)
			}
		}
		s.mu.Unlock()
		if !start && active {
			return fmt.Errorf("%w: %w", ErrConflict, ErrLeaseActive)
		}
		return ErrConflict
	}
	s.cancelResidenceLocked(id)
	if err := s.markDirtyLocked(); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("persist scheduler activity: %w", err)
	}
	s.manualBusy = true
	done := make(chan error, 1)
	s.manual = append(s.manual, manualCommand{start: start, id: id, ctx: ctx, done: done})
	s.notify()
	s.mu.Unlock()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		for model, timer := range s.residenceTimers {
			timer.Stop()
			delete(s.residenceTimers, model)
		}
		s.residenceDue = make(map[string]uint64)
		s.residenceDeadlines = make(map[string]time.Time)
		cancelled := append([]string(nil), s.queue...)
		if s.activeID != "" {
			cancelled = append(cancelled, s.activeID)
		}
		if s.preparingID != "" {
			cancelled = append(cancelled, s.preparingID)
		}
		for _, id := range s.queue {
			if l := s.leases[id]; l != nil {
				l.State = Cancelled
			}
		}
		s.queue = nil
		if l := s.leases[s.activeID]; l != nil {
			l.State = Cancelled
		}
		for _, active := range s.inferences {
			for _, pin := range active {
				if pin.cancel != nil {
					pin.cancel()
				}
			}
		}
		if len(s.inferences[s.activeID]) == 0 {
			s.activeID = ""
		}
		if l := s.leases[s.preparingID]; l != nil {
			l.State = Cancelled
		}
		s.syncIdempotencyLocked(cancelled...)
		if s.operationCancel != nil {
			s.operationCancel()
		}
		if s.preloadCancel != nil {
			s.preloadCancel()
		}
		for _, id := range s.preloadQueue {
			s.preloadState[id], s.preloadError[id] = PreloadCancelled, "scheduler shutdown"
		}
		s.preloadQueue = nil
		for _, cmd := range s.manual {
			cmd.done <- ErrClosed
		}
		s.manual = nil
		s.mu.Unlock()
		s.cancel()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) worker() {
	defer close(s.done)
	interval := s.ttl / 10
	if interval > time.Second {
		interval = time.Second
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.cleanup()
			return
		case <-s.wake:
		case <-ticker.C:
		}
		s.step()
	}
}

func (s *Scheduler) step() {
	s.mu.Lock()
	if s.recoveryBlocked {
		s.mu.Unlock()
		return
	}
	for model, generation := range s.residenceDue {
		if generation == 0 || s.residenceGen[model] != generation {
			delete(s.residenceDue, model)
			continue
		}
		if s.activeID != "" && s.leases[s.activeID] != nil && s.leases[s.activeID].Model == model || s.hasQueuedModelLocked(model) || len(s.inferencesForModelLocked(model)) != 0 {
			s.cancelResidenceLocked(model)
			continue
		}
		delete(s.residenceDue, model)
		if timer := s.residenceTimers[model]; timer != nil {
			timer.Stop()
			delete(s.residenceTimers, model)
		}
		delete(s.residenceDeadlines, model)
		s.preloadState[model], s.preloadError[model] = PreloadExpired, "time_alive expired"
		s.mu.Unlock()
		if err := s.manager.Stop(context.Background(), model); err != nil {
			log.Printf("residence expiry for model %q: %v", model, err)
		}
		s.mu.Lock()
		s.notify()
		s.mu.Unlock()
		return
	}
	if len(s.manual) > 0 {
		cmd := s.manual[0]
		s.manual = s.manual[1:]
		s.mu.Unlock()
		err := s.runManual(cmd)
		s.mu.Lock()
		s.manualBusy = false
		if cmd.start && err == nil {
			s.manualModel = true
		}
		if !cmd.start && err == nil {
			s.manualModel = false
			s.preloadSuppressed[cmd.id] = true
			s.preloadState[cmd.id], s.preloadError[cmd.id] = PreloadAdminStopped, "administrative stop requested"
		}
		cmd.done <- err
		if len(s.queue) > 0 {
			s.notify()
		}
		s.mu.Unlock()
		return
	}
	if s.activeID != "" {
		l := s.leases[s.activeID]
		if l != nil && l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) && !s.isProtectedLocked(s.activeID) {
			proposed := cloneLease(*l)
			proposed.State = Expired
			if err := s.persistLeaseChangeLocked(s.activeID, proposed); err != nil {
				s.recoveryBlocked = true
				log.Printf("persist lease expiration: %v", err)
				s.notify()
				s.mu.Unlock()
				return
			}
			l.State = proposed.State
			s.retireLeaseLocked(s.activeID)
			s.notify()
		} else {
			id, model := s.activeID, ""
			if l != nil {
				model = l.Model
			}
			s.mu.Unlock()
			status := s.manager.Status()
			if status.State != runtime.Ready || status.ActiveModel != model {
				// A recovery fixture may have completed the Runtime-owned stop
				// before Scheduler commits the correlated lease. Do not turn that
				// verified absence into FAILED: keep the lease protected so the
				// reconciliation authority can close it. Unexpected FAILED and
				// recovery-required runtime states retain the existing path below.
				if status.State == runtime.Stopping || status.State == runtime.Stopped && status.PID == 0 {
					return
				}
				s.mu.Lock()
				if s.activeID == id {
					if current := s.leases[id]; current != nil {
						proposed := cloneLease(*current)
						proposed.State = Failed
						proposed.Error = "runtime process exited unexpectedly"
						if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
							s.recoveryBlocked = true
							log.Printf("persist unexpected runtime failure: %v", err)
							s.notify()
							s.mu.Unlock()
							return
						}
						*current = proposed
					}
					s.retireLeaseLocked(id)
				}
				s.notify()
				s.mu.Unlock()
			}
			return
		}
	}
	if s.activeID != "" {
		s.mu.Unlock()
		return
	}
	if id, ok := s.queue.pop(); ok {
		l := s.leases[id]
		if l == nil || l.State != Queued {
			s.mu.Unlock()
			s.notify()
			return
		}
		proposed := cloneLease(*l)
		proposed.State = Preparing
		if err := s.persistLeaseChangeLocked(id, proposed); err != nil {
			s.recoveryBlocked = true
			log.Printf("persist lease preparation: %v", err)
			s.queue = append([]string{id}, s.queue...)
			s.notify()
			s.mu.Unlock()
			return
		}
		l.State = proposed.State
		s.preparingID = id
		opCtx, cancel := context.WithCancel(s.ctx)
		s.operationCancel = cancel
		model := l.Model
		s.mu.Unlock()
		err := s.activate(opCtx, id, model)
		cancel()
		s.finishActivation(id, err)
		return
	}
	if len(s.preloadQueue) > 0 {
		s.mu.Unlock()
		s.startNextPreload()
		return
	}
	unload := s.unloadWhenIdle
	unloadModel := s.unloadModel
	s.unloadWhenIdle = false
	s.unloadModel = ""
	manualModel := s.manualModel
	s.mu.Unlock()
	if unload && !manualModel {
		s.stopModel(unloadModel)
	}
}

func (s *Scheduler) hasQueuedModelLocked(model string) bool {
	for _, id := range s.queue {
		if lease := s.leases[id]; lease != nil && lease.Model == model {
			return true
		}
	}
	return false
}

func (s *Scheduler) inferencesForModelLocked(model string) []uint64 {
	var result []uint64
	for id, pins := range s.inferences {
		lease := s.leases[id]
		if lease == nil || lease.Model != model {
			continue
		}
		for inferenceID := range pins {
			result = append(result, inferenceID)
		}
	}
	return result
}

func (s *Scheduler) activate(ctx context.Context, id, model string) error {
	if _, ready := s.manager.CurrentProcessForModel(model); ready {
		return nil
	}
	if candidate := s.evictionCandidate(model); candidate != "" {
		if err := s.manager.Stop(ctx, candidate); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.manager.Start(ctx, model); err != nil {
		if errors.Is(err, runtime.ErrCapacity) {
			used, limit := s.capacityUsage()
			return preloadCapacityError(used, limit)
		}
		return err
	}
	return nil
}

func (s *Scheduler) capacityUsage() (int, int) {
	used := 0
	for _, status := range s.manager.Models() {
		if status.CapacityOccupied {
			used++
		}
	}
	return used, s.manager.Capacity()
}

func (s *Scheduler) evictionCandidate(target string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	models := s.manager.Models()
	ids := make([]string, 0, len(models))
	for id, status := range models {
		if id == target || status.PID == 0 || status.KeepAlive || (status.TimeAlive == "" || status.TimeAlive == "0s") && s.unloadModel != id {
			continue
		}
		if s.activeID != "" && s.leases[s.activeID] != nil && s.leases[s.activeID].Model == id {
			continue
		}
		if len(s.inferencesForModelLocked(id)) != 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return firstString(ids)
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *Scheduler) finishActivation(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.leases[id]
	s.preparingID, s.operationCancel = "", nil
	if l == nil {
		s.notify()
		return
	}
	if l.State == Cancelled || s.closing {
		if err == nil {
			s.unloadWhenIdle, s.unloadModel = true, modelOfLease(l)
		}
	} else if err != nil {
		if errors.Is(err, runtime.ErrRecoveryRequired) {
			s.recoveryBlocked = true
		}
		if blocked, _ := s.manager.RecoveryRequired(); blocked {
			s.recoveryBlocked = true
		}
		proposed := cloneLease(*l)
		proposed.State, proposed.Error = Failed, err.Error()
		if persistErr := s.persistLeaseChangeLocked(id, proposed); persistErr != nil {
			s.recoveryBlocked = true
			log.Printf("persist failed lease result: %v", persistErr)
			s.notify()
			return
		}
		*l = proposed
		s.unloadWhenIdle, s.unloadModel = true, modelOfLease(l)
	} else {
		now := time.Now().UTC()
		end := now.Add(s.ttl)
		maxEnd := now.Add(s.maxLease)
		if end.After(maxEnd) {
			end = maxEnd
		}
		proposed := cloneLease(*l)
		proposed.State, proposed.ActivatedAt, proposed.ExpiresAt = Active, &now, &end
		if persistErr := s.persistLeaseChangeLocked(id, proposed); persistErr != nil {
			s.recoveryBlocked = true
			log.Printf("persist active lease: %v", persistErr)
			s.notify()
			return
		}
		*l = proposed
		s.activeID = id
		s.manualModel = false
		s.unloadWhenIdle, s.unloadModel = false, ""
		s.cancelResidenceLocked(l.Model)
	}
	s.notify()
}

func modelOfLease(lease *Lease) string {
	if lease == nil {
		return ""
	}
	return lease.Model
}

func (s *Scheduler) runManual(cmd manualCommand) error {
	ctx, cancel := context.WithCancel(cmd.ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	if cmd.start {
		return s.manager.Start(ctx, cmd.id)
	}
	return s.manager.Stop(ctx, cmd.id)
}

func (s *Scheduler) stopCurrent() {
	for id, status := range s.manager.Models() {
		if status.PID == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := s.manager.Stop(ctx, id); err != nil {
			log.Printf("stop runtime model %q: %v", id, err)
		}
		cancel()
	}
}

func (s *Scheduler) stopModel(model string) {
	if model == "" {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	if err := s.manager.Stop(ctx, model); err != nil {
		log.Printf("stop runtime model %q: %v", model, err)
	}
}

func (s *Scheduler) cleanup() {
	s.mu.Lock()
	cancelled := append([]string(nil), s.queue...)
	if s.activeID != "" {
		cancelled = append(cancelled, s.activeID)
	}
	if s.preparingID != "" {
		cancelled = append(cancelled, s.preparingID)
	}
	for _, id := range s.queue {
		if l := s.leases[id]; l != nil {
			l.State = Cancelled
		}
	}
	s.queue = nil
	if l := s.leases[s.activeID]; l != nil {
		l.State = Cancelled
	}
	for _, active := range s.inferences {
		for _, pin := range active {
			if pin.cancel != nil {
				pin.cancel()
			}
		}
	}
	if len(s.inferences[s.activeID]) == 0 {
		s.activeID = ""
	}
	if l := s.leases[s.preparingID]; l != nil {
		l.State = Cancelled
	}
	s.syncIdempotencyLocked(cancelled...)
	if s.operationCancel != nil {
		s.operationCancel()
	}
	if s.preloadCancel != nil {
		s.preloadCancel()
	}
	for _, id := range s.preloadQueue {
		s.preloadState[id], s.preloadError[id] = PreloadCancelled, "scheduler shutdown"
	}
	s.preloadQueue = nil
	s.notify()
	for len(s.inferences) != 0 {
		changed := s.changedChannel()
		s.mu.Unlock()
		<-changed
		s.mu.Lock()
	}
	s.mu.Unlock()
	s.stopCurrent()
	status := s.manager.Status()
	if !s.recoveryBlocked && status.PID == 0 && status.State != runtime.Starting && status.State != runtime.Stopping {
		s.mu.Lock()
		s.cleanShutdown = true
		if err := s.writeIdempotencyLocked(); err != nil {
			log.Printf("persist clean scheduler shutdown: %v", err)
		}
		s.mu.Unlock()
	}
}

func (s *Scheduler) markDirtyLocked() error {
	previous := s.cleanShutdown
	s.cleanShutdown = false
	if err := s.writeIdempotencyLocked(); err != nil {
		s.cleanShutdown = previous
		return err
	}
	return nil
}

func (s *Scheduler) notify() {
	s.signalChanged()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) signalChanged() {
	s.changedMu.Lock()
	defer s.changedMu.Unlock()
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Scheduler) changedChannel() <-chan struct{} {
	s.changedMu.Lock()
	defer s.changedMu.Unlock()
	return s.changed
}
