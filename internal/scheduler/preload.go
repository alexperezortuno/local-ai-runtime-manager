package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"

	runtime "local-ai-runtime/internal/runtime"
)

const (
	PreloadNotRequired     = "NOT_REQUIRED"
	PreloadNotStarted      = "NOT_STARTED"
	PreloadQueued          = "QUEUED"
	PreloadStarting        = "STARTING"
	PreloadReady           = "READY"
	PreloadSkippedCapacity = "SKIPPED_CAPACITY"
	PreloadFailed          = "FAILED"
	PreloadBlockedRecovery = "BLOCKED_RECOVERY"
	PreloadCancelled       = "CANCELLED"
	PreloadAdminStopped    = "ADMIN_STOPPED"
	PreloadOnDemand        = "ON_DEMAND"
	PreloadExpired         = "EXPIRED"
)

// Preload admits only configured residency policies. It queues work on the
// existing scheduler worker and never creates a lease or a second process
// authority.
func (s *Scheduler) Preload() error {
	models := s.manager.Models()
	ids := make([]string, 0, len(models))
	for id, model := range models {
		if model.KeepAlive || policyDuration(model.TimeAlive) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return ErrClosed
	}
	if s.recoveryBlocked || s.managerRecoveryBlockedLocked() {
		for _, id := range ids {
			s.preloadState[id], s.preloadError[id] = PreloadBlockedRecovery, "recovery is required before starting a model"
		}
		s.notify()
		return ErrIdempotencyRecovery
	}
	for _, id := range ids {
		if s.preloadSuppressed[id] || s.preloadingID == id || s.preloadQueuedLocked(id) {
			continue
		}
		status := models[id]
		if status.State == runtime.Ready && status.PID != 0 {
			s.preloadState[id], s.preloadError[id] = PreloadReady, ""
			continue
		}
		s.preloadState[id], s.preloadError[id] = PreloadQueued, ""
		s.preloadQueue = append(s.preloadQueue, id)
	}
	s.notify()
	return nil
}

func (s *Scheduler) managerRecoveryBlockedLocked() bool {
	if blocked, _ := s.manager.RecoveryRequired(); blocked {
		return true
	}
	return s.admissionGate != nil && s.admissionGate.AdmissionBlocked()
}

func (s *Scheduler) preloadQueuedLocked(id string) bool {
	for _, queued := range s.preloadQueue {
		if queued == id {
			return true
		}
	}
	return false
}

func (s *Scheduler) removePreloadLocked(id string) {
	for index, queued := range s.preloadQueue {
		if queued == id {
			s.preloadQueue = append(s.preloadQueue[:index], s.preloadQueue[index+1:]...)
			return
		}
	}
}

func (s *Scheduler) nextPreloadLocked() string {
	for len(s.preloadQueue) > 0 {
		id := s.preloadQueue[0]
		s.preloadQueue = s.preloadQueue[1:]
		if s.preloadSuppressed[id] {
			s.preloadState[id], s.preloadError[id] = PreloadAdminStopped, "administrative stop requested"
			continue
		}
		return id
	}
	return ""
}

func (s *Scheduler) startNextPreload() bool {
	s.mu.Lock()
	if s.preloadingID != "" || len(s.preloadQueue) == 0 || s.closing || s.recoveryBlocked || s.managerRecoveryBlockedLocked() {
		s.mu.Unlock()
		return false
	}
	id := s.nextPreloadLocked()
	if id == "" {
		s.mu.Unlock()
		return true
	}
	if status, ok := s.manager.Models()[id]; !ok || status.State == runtime.Ready && status.PID != 0 {
		if ok {
			s.preloadState[id], s.preloadError[id] = PreloadReady, ""
		}
		s.mu.Unlock()
		s.notify()
		return true
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.preloadingID, s.preloadCancel = id, cancel
	s.preloadState[id], s.preloadError[id] = PreloadStarting, ""
	s.mu.Unlock()

	err := s.activate(ctx, "", id)
	cancel()
	s.finishPreload(id, err)
	return true
}

func (s *Scheduler) finishPreload(id string, err error) {
	s.mu.Lock()
	if s.preloadingID == id {
		s.preloadingID, s.preloadCancel = "", nil
	}
	switch {
	case s.closing:
		s.preloadState[id], s.preloadError[id] = PreloadCancelled, "scheduler shutdown"
	case s.preloadSuppressed[id]:
		s.preloadState[id], s.preloadError[id] = PreloadAdminStopped, "administrative stop requested"
	case err == nil:
		s.preloadState[id], s.preloadError[id] = PreloadReady, ""
		// A time_alive model has no lease to trigger its normal residence timer.
		s.scheduleResidenceLocked(id)
	case errors.Is(err, ErrCapacity):
		s.preloadState[id], s.preloadError[id] = PreloadSkippedCapacity, err.Error()
	case errors.Is(err, runtime.ErrRecoveryRequired):
		s.recoveryBlocked = true
		s.preloadState[id], s.preloadError[id] = PreloadBlockedRecovery, err.Error()
	default:
		s.preloadState[id], s.preloadError[id] = PreloadFailed, err.Error()
	}
	s.notify()
	s.mu.Unlock()
}

func (s *Scheduler) preloadStatusLocked(id string) (string, string) {
	state := s.preloadState[id]
	if state == "" {
		state = PreloadNotRequired
	}
	return state, s.preloadError[id]
}

func (s *Scheduler) cancelPreloadLocked(id string) {
	if s.preloadingID == id && s.preloadCancel != nil {
		s.preloadSuppressed[id] = true
		s.preloadCancel()
	}
}

func preloadCapacityError(used, limit int) error {
	return fmt.Errorf("%w: %d/%d resident slots occupied", ErrCapacity, used, limit)
}
