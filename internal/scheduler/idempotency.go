package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const idempotencyStoreVersion = 1

type idempotencyRecord struct {
	Key         string    `json:"key"`
	Fingerprint string    `json:"fingerprint"`
	Lease       Lease     `json:"lease"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type idempotencyFile struct {
	Version         int                             `json:"version"`
	CleanShutdown   bool                            `json:"clean_shutdown"`
	Records         map[string]idempotencyRecord    `json:"records"`
	Reconciliations map[string]reconciliationRecord `json:"reconciliations,omitempty"`
}

type reconciliationRecord struct {
	OperationKey string    `json:"operation_key"`
	Fingerprint  string    `json:"fingerprint"`
	Lease        Lease     `json:"lease"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// RequestIdempotent atomically reserves a key and appends one lease to FIFO.
// A replay returns the original lease status without touching TTL or queue order.
func (s *Scheduler) RequestIdempotent(key, model string) (LeaseStatus, bool, error) {
	key, err := normalizeIdempotencyKey(key)
	if err != nil {
		return LeaseStatus{}, false, err
	}
	fingerprint := acquisitionFingerprint(model)
	s.mu.Lock()
	defer s.mu.Unlock()
	if record, ok := s.idempotency[key]; ok {
		status := LeaseStatus{Lease: cloneLease(record.Lease)}
		if l := s.leases[record.Lease.ID]; l != nil {
			status = s.statusLocked(l)
		}
		if record.Fingerprint != fingerprint {
			return status, true, ErrIdempotencyConflict
		}
		if l := s.leases[record.Lease.ID]; l != nil {
			if l.State == Completed || l.State == Expired || l.State == Cancelled || l.State == Failed {
				return status, true, ErrIdempotencyTerminal
			}
			return status, true, nil
		}
		if record.Lease.State == RecoveryRequired {
			return status, true, ErrIdempotencyRecovery
		}
		return status, true, ErrIdempotencyTerminal
	}
	if s.closing {
		return LeaseStatus{}, false, ErrClosed
	}
	if s.recoveryBlocked {
		return LeaseStatus{}, false, ErrIdempotencyRecovery
	}
	if s.admissionGate != nil && s.admissionGate.AdmissionBlocked() {
		return LeaseStatus{}, false, ErrIdempotencyRecovery
	}
	if _, ok := s.manager.Models()[model]; !ok {
		return LeaseStatus{}, false, fmt.Errorf("%w: model %s", ErrNotFound, model)
	}
	if len(s.queue) >= s.maxQueue {
		return LeaseStatus{}, false, ErrQueueFull
	}
	id, err := newID()
	if err != nil {
		return LeaseStatus{}, false, err
	}
	now := time.Now().UTC()
	l := Lease{ID: id, Model: model, State: Queued, CreatedAt: now}
	wasClean := s.cleanShutdown
	s.cleanShutdown = false
	s.idempotency[key] = idempotencyRecord{Key: key, Fingerprint: fingerprint, Lease: l, CreatedAt: now, UpdatedAt: now}
	if err := s.writeIdempotencyLocked(); err != nil {
		delete(s.idempotency, key)
		s.cleanShutdown = wasClean
		return LeaseStatus{}, false, fmt.Errorf("persist idempotency reservation: %w", err)
	}
	s.leases[id] = &l
	s.queue.push(id)
	s.notify()
	return s.statusLocked(&l), false, nil
}

func (s *Scheduler) statusLocked(l *Lease) LeaseStatus {
	status := LeaseStatus{Lease: cloneLease(*l)}
	if l.State == Queued {
		for i, queued := range s.queue {
			if queued == l.ID {
				status.QueuePosition = i + 1
				break
			}
		}
	}
	return status
}

func normalizeIdempotencyKey(key string) (string, error) {
	if len(key) != 36 || key[8] != '-' || key[13] != '-' || key[18] != '-' || key[23] != '-' {
		return "", ErrInvalidIdempotencyKey
	}
	compact := strings.ReplaceAll(key, "-", "")
	if len(compact) != 32 {
		return "", ErrInvalidIdempotencyKey
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", ErrInvalidIdempotencyKey
	}
	return strings.ToLower(key), nil
}

func acquisitionFingerprint(model string) string {
	canonical, _ := json.Marshal(struct {
		Model string `json:"model"`
	}{Model: model})
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:])
}

func (s *Scheduler) syncIdempotencyLocked(leaseIDs ...string) {
	if len(leaseIDs) == 0 || len(s.idempotency) == 0 {
		return
	}
	updated := make(map[string]struct{}, len(leaseIDs))
	for _, id := range leaseIDs {
		updated[id] = struct{}{}
	}
	now := time.Now().UTC()
	for key, record := range s.idempotency {
		if _, ok := updated[record.Lease.ID]; !ok {
			continue
		}
		if lease := s.leases[record.Lease.ID]; lease != nil {
			record.Lease = cloneLease(*lease)
			record.UpdatedAt = now
			s.idempotency[key] = record
		}
	}
	if err := s.writeIdempotencyLocked(); err != nil {
		// The durable reservation remains; after restart its stale nonterminal state
		// is reconciled conservatively as RECOVERY_REQUIRED.
		fmt.Fprintf(os.Stderr, "persist idempotency update: %v\n", err)
	}
}

// persistLeaseChangeLocked prepares the idempotency snapshot before a lease
// becomes terminal/reusable. The caller must publish proposed only after this
// returns nil; otherwise a sync failure could expose an unjournaled release.
func (s *Scheduler) persistLeaseChangeLocked(id string, proposed Lease) error {
	if s.idempotencyPath == "" {
		return nil
	}
	previousClean := s.cleanShutdown
	previous := s.idempotency
	updated := make(map[string]idempotencyRecord, len(previous))
	for key, record := range previous {
		updated[key] = record
		if record.Lease.ID == id {
			record.Lease = cloneLease(proposed)
			record.UpdatedAt = time.Now().UTC()
			updated[key] = record
		}
	}
	s.idempotency = updated
	s.cleanShutdown = false
	if err := s.writeIdempotencyLocked(); err != nil {
		s.idempotency = previous
		s.cleanShutdown = previousClean
		return err
	}
	return nil
}

func (s *Scheduler) openIdempotencyStore(path string) error {
	if path == "" {
		return errors.New("idempotency store path is required")
	}
	s.idempotencyPath = path
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read idempotency store: %w", err)
	}
	if err == nil {
		var file idempotencyFile
		decoderErr := json.Unmarshal(data, &file)
		if decoderErr != nil || file.Version != idempotencyStoreVersion || file.Records == nil {
			return errors.New("idempotency store is corrupt or has an unsupported version")
		}
		s.cleanShutdown = file.CleanShutdown
		if !file.CleanShutdown {
			s.recoveryBlocked = true
		}
		for key, record := range file.Records {
			normalized, keyErr := normalizeIdempotencyKey(key)
			if keyErr != nil || normalized != key || record.Key != key || record.Lease.ID == "" || record.Lease.Model == "" || record.Fingerprint != acquisitionFingerprint(record.Lease.Model) || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
				return errors.New("idempotency store contains an invalid record")
			}
			if !terminalState(record.Lease.State) {
				record.Lease.State = RecoveryRequired
				record.Lease.ActivatedAt = nil
				record.Lease.ExpiresAt = nil
				record.UpdatedAt = time.Now().UTC()
				s.recoveryBlocked = true
			}
			s.idempotency[key] = record
		}
		if file.Reconciliations != nil {
			s.reconciliations = file.Reconciliations
		}
	} else {
		s.cleanShutdown = true
	}
	return s.writeIdempotencyLocked()
}

func terminalState(state LeaseState) bool {
	switch state {
	case Completed, Failed, Cancelled, Expired:
		return true
	default:
		return false
	}
}

func (s *Scheduler) writeIdempotencyLocked() error {
	if s.idempotencyPath == "" {
		return nil
	}
	if s.idempotencyFault != nil {
		if err := s.idempotencyFault("before_persist"); err != nil {
			return err
		}
	}
	dir := filepath.Dir(s.idempotencyPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(idempotencyFile{Version: idempotencyStoreVersion, CleanShutdown: s.cleanShutdown, Records: s.idempotency, Reconciliations: s.reconciliations}, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".larm-idempotency-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err = temp.Write(data); err == nil {
		if s.idempotencyFault != nil {
			if faultErr := s.idempotencyFault("during_persist"); faultErr != nil {
				err = faultErr
			}
		}
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tempName, s.idempotencyPath); err != nil {
		return err
	}
	if s.idempotencyFault != nil {
		if err := s.idempotencyFault("after_persist"); err != nil {
			return err
		}
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync idempotency store directory: %w", err)
	}
	return nil
}
