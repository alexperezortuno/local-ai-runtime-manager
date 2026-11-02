package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
)

var (
	ErrNotFound  = errors.New("lease or model not found")
	ErrQueueFull = errors.New("scheduler queue is full")
	ErrConflict  = errors.New("operation conflicts with scheduler state")
	ErrExpired   = errors.New("lease has expired")
	ErrClosed    = errors.New("scheduler is shutting down")
)

type Scheduler struct {
	mu              sync.Mutex
	manager         *runtime.Manager
	maxQueue        int
	ttl             time.Duration
	heartbeat       time.Duration
	maxLease        time.Duration
	leases          map[string]*Lease
	queue           leaseQueue
	activeID        string
	preparingID     string
	operationCancel context.CancelFunc
	manual          []manualCommand
	manualBusy      bool
	manualModel     bool
	unloadWhenIdle  bool
	inferences      map[string]map[uint64]context.CancelFunc
	nextInferenceID uint64
	changed         chan struct{}
	closing         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wake            chan struct{}
	done            chan struct{}
	closeOnce       sync.Once
}

type manualCommand struct {
	start bool
	id    string
	ctx   context.Context
	done  chan error
}

func New(manager *runtime.Manager, cfg config.SchedulerConfig) *Scheduler {
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
	s := &Scheduler{manager: manager, maxQueue: cfg.MaxQueueSize, ttl: cfg.LeaseTTLDuration(), heartbeat: cfg.HeartbeatDuration(), maxLease: cfg.MaxLeaseDuration(), leases: make(map[string]*Lease), inferences: make(map[string]map[uint64]context.CancelFunc), changed: make(chan struct{}), ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go s.worker()
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
	if len(s.queue) >= s.maxQueue {
		return Lease{}, ErrQueueFull
	}
	l := &Lease{ID: id, Model: model, State: Queued, CreatedAt: time.Now().UTC()}
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
	result := LeaseStatus{Lease: cloneLease(*l)}
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
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// BeginInference atomically validates an active lease and pins its backend.
func (s *Scheduler) BeginInference(id, model string, cancel context.CancelFunc) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return nil, ErrNotFound
	}
	if l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) {
		l.State = Expired
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
		s.inferences[id] = make(map[uint64]context.CancelFunc)
	}
	s.inferences[id][inferenceID] = cancel
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.inferences[id], inferenceID)
			if len(s.inferences[id]) == 0 {
				delete(s.inferences, id)
				if s.activeID == id && s.leases[id].State != Active {
					s.activeID = ""
					s.unloadWhenIdle = true
				}
			}
			s.notify()
			s.mu.Unlock()
		})
	}, nil
}

func (s *Scheduler) retireLeaseLocked(id string) {
	if len(s.inferences[id]) == 0 && s.activeID == id {
		s.activeID = ""
		s.unloadWhenIdle = true
	}
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
	if l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) {
		l.State = Expired
		s.retireLeaseLocked(id)
		s.notify()
		return cloneLease(*l), ErrExpired
	}
	if l.State != Active || s.activeID != id {
		return cloneLease(*l), ErrConflict
	}
	deadline := l.ActivatedAt.Add(s.maxLease)
	if !time.Now().Before(deadline) {
		l.State = Expired
		s.retireLeaseLocked(id)
		s.notify()
		return cloneLease(*l), ErrExpired
	}
	end := time.Now().Add(s.ttl)
	if end.After(deadline) {
		end = deadline
	}
	l.ExpiresAt = &end
	s.notify()
	return cloneLease(*l), nil
}

func (s *Scheduler) Release(id string) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return Lease{}, ErrNotFound
	}
	switch l.State {
	case Queued:
		s.queue.remove(id)
		l.State = Cancelled
	case Preparing:
		l.State = Cancelled
		if s.preparingID == id && s.operationCancel != nil {
			s.operationCancel()
		}
	case Active:
		l.State = Completed
		s.retireLeaseLocked(id)
	default:
		return cloneLease(*l), nil
	}
	s.notify()
	return cloneLease(*l), nil
}

func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result Snapshot
	result.Pending = make([]Lease, 0, len(s.queue))
	for _, id := range s.queue {
		if l := s.leases[id]; l != nil {
			result.Pending = append(result.Pending, cloneLease(*l))
		}
	}
	if l := s.leases[s.activeID]; l != nil {
		v := cloneLease(*l)
		result.Active = &v
	}
	if l := s.leases[s.preparingID]; l != nil {
		v := cloneLease(*l)
		result.Preparing = &v
	}
	return result
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
	if _, ok := s.manager.Models()[id]; !ok {
		return fmt.Errorf("%w: model %s", ErrNotFound, id)
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.manualBusy || s.activeID != "" || s.preparingID != "" || len(s.queue) != 0 {
		s.mu.Unlock()
		return ErrConflict
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
			for _, cancel := range active {
				if cancel != nil {
					cancel()
				}
			}
		}
		if len(s.inferences[s.activeID]) == 0 {
			s.activeID = ""
		}
		if l := s.leases[s.preparingID]; l != nil {
			l.State = Cancelled
		}
		if s.operationCancel != nil {
			s.operationCancel()
		}
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
		if l != nil && l.State == Active && l.ExpiresAt != nil && !time.Now().Before(*l.ExpiresAt) {
			l.State = Expired
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
				s.mu.Lock()
				if s.activeID == id {
					if current := s.leases[id]; current != nil {
						current.State = Failed
						current.Error = "runtime process exited unexpectedly"
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
		l.State = Preparing
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
	unload := s.unloadWhenIdle
	s.unloadWhenIdle = false
	manualModel := s.manualModel
	s.mu.Unlock()
	if unload && !manualModel {
		s.stopCurrent()
	}
}

func (s *Scheduler) activate(ctx context.Context, id, model string) error {
	status := s.manager.Status()
	if status.State == runtime.Ready && status.ActiveModel == model {
		return nil
	}
	if status.State == runtime.Ready && status.ActiveModel != "" {
		if err := s.manager.Stop(ctx, status.ActiveModel); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.manager.Start(ctx, model)
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
			s.unloadWhenIdle = true
		}
	} else if err != nil {
		l.State, l.Error = Failed, err.Error()
		s.unloadWhenIdle = true
	} else {
		now := time.Now().UTC()
		end := now.Add(s.ttl)
		maxEnd := now.Add(s.maxLease)
		if end.After(maxEnd) {
			end = maxEnd
		}
		l.State, l.ActivatedAt, l.ExpiresAt = Active, &now, &end
		s.activeID = id
		s.manualModel = false
		s.unloadWhenIdle = false
	}
	s.notify()
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
	status := s.manager.Status()
	if status.State == runtime.Ready && status.ActiveModel != "" {
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		defer cancel()
		if err := s.manager.Stop(ctx, status.ActiveModel); err != nil {
			log.Printf("stop runtime model %q: %v", status.ActiveModel, err)
		}
	}
}

func (s *Scheduler) cleanup() {
	s.mu.Lock()
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
		for _, cancel := range active {
			if cancel != nil {
				cancel()
			}
		}
	}
	if len(s.inferences[s.activeID]) == 0 {
		s.activeID = ""
	}
	if l := s.leases[s.preparingID]; l != nil {
		l.State = Cancelled
	}
	if s.operationCancel != nil {
		s.operationCancel()
	}
	s.notify()
	for len(s.inferences) != 0 {
		changed := s.changed
		s.mu.Unlock()
		<-changed
		s.mu.Lock()
	}
	s.mu.Unlock()
	s.stopCurrent()
}

func (s *Scheduler) notify() {
	s.signalChanged()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) signalChanged() {
	close(s.changed)
	s.changed = make(chan struct{})
}
