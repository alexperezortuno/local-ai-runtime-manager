package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

type ReconciliationRequest struct {
	OperationKey   string
	IncidentID     string
	PlanID         string
	EvidenceDigest string
	LeaseID        string
	ModelID        string
	RequestIDs     []string
}

type ReconciliationProtector interface {
	ReconciliationAllowed(leaseID, model string, requestIDs []string) error
}

// ReconcileLease is the only scheduler-owned physical reconciliation path. It
// writes the durable store before publishing a completed lease.
func (s *Scheduler) ReconcileLease(req ReconciliationRequest) (Lease, error) {
	if strings.TrimSpace(req.OperationKey) == "" || req.LeaseID == "" || req.ModelID == "" || len(req.RequestIDs) == 0 {
		return Lease{}, errors.New("lease reconciliation requires operation, lease, model and request identities")
	}
	fingerprint := reconciliationFingerprint(req)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.reconciliations[req.OperationKey]; ok {
		if old.Fingerprint != fingerprint {
			return Lease{}, ErrConflict
		}
		return cloneLease(old.Lease), nil
	}
	if s.idempotencyPath == "" {
		return Lease{}, ErrIdempotencyRecovery
	}
	if s.releaseProtector == nil {
		return Lease{}, ErrInferenceProtected
	}
	if protector, ok := s.releaseProtector.(ReconciliationProtector); !ok {
		return Lease{}, ErrInferenceProtected
	} else if err := protector.ReconciliationAllowed(req.LeaseID, req.ModelID, req.RequestIDs); err != nil {
		return Lease{}, err
	}
	lease, ok := s.leases[req.LeaseID]
	var proposed Lease
	if ok {
		if lease.Model != req.ModelID || len(s.inferences[req.LeaseID]) != 0 {
			return Lease{}, ErrInferenceProtected
		}
		if lease.State != Active && lease.State != Preparing && lease.State != Queued {
			return Lease{}, ErrConflict
		}
		proposed = cloneLease(*lease)
	} else {
		for _, record := range s.idempotency {
			if record.Lease.ID == req.LeaseID {
				proposed, ok = cloneLease(record.Lease), true
				break
			}
		}
		if !ok || proposed.Model != req.ModelID || proposed.State != RecoveryRequired {
			return Lease{}, ErrIdempotencyRecovery
		}
	}
	proposed.State = Completed
	proposed.Protected, proposed.ProtectionReason = false, ""
	record := reconciliationRecord{OperationKey: req.OperationKey, Fingerprint: fingerprint, Lease: cloneLease(proposed), UpdatedAt: time.Now().UTC()}
	updated := cloneReconciliationRecords(s.reconciliations)
	updated[req.OperationKey] = record
	previous := s.reconciliations
	changedIdempotency := make(map[string]idempotencyRecord)
	for key, old := range s.idempotency {
		if old.Lease.ID != proposed.ID {
			continue
		}
		changedIdempotency[key] = old
		old.Lease = cloneLease(proposed)
		old.UpdatedAt = time.Now().UTC()
		s.idempotency[key] = old
	}
	s.reconciliations = updated
	if err := s.writeIdempotencyLocked(); err != nil {
		s.reconciliations = previous
		for key, old := range changedIdempotency {
			s.idempotency[key] = old
		}
		return Lease{}, err
	}
	if lease != nil {
		*lease = proposed
		// ReconciliationAllowed is the physical gate for this operation. Do
		// not call the normal release protector here: UNKNOWN remains
		// semantically protected by design, but its already-verified physical
		// capacity must leave activeID so FIFO can admit the next lease.
		if s.activeID == req.LeaseID {
			s.activeID = ""
		}
		s.queue.remove(req.LeaseID)
	} else {
		// This is a completed tombstone reconstructed from the durable store;
		// publish only the terminal record, never active ownership.
		completed := cloneLease(proposed)
		s.leases[completed.ID] = &completed
	}
	// The scheduler commit is not the end of the cross-authority protocol.
	// Keep admission blocked until Recovery durably records COMPLETE.
	s.recoveryBlocked = true
	s.notify()
	return cloneLease(proposed), nil
}

// CompleteReconciliation releases the scheduler gate only after Recovery has
// durably committed the COMPLETE phase. It never recreates active ownership.
func (s *Scheduler) CompleteReconciliation(operationKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(operationKey) == "" {
		return errors.New("reconciliation completion requires an operation key")
	}
	if _, ok := s.reconciliations[operationKey]; !ok {
		return errors.New("reconciliation operation is not durable")
	}
	previousBlocked, previousClean := s.recoveryBlocked, s.cleanShutdown
	s.recoveryBlocked, s.cleanShutdown = false, true
	if err := s.writeIdempotencyLocked(); err != nil {
		s.recoveryBlocked, s.cleanShutdown = previousBlocked, previousClean
		return err
	}
	s.notify()
	return nil
}

func reconciliationFingerprint(req ReconciliationRequest) string {
	ids := append([]string(nil), req.RequestIDs...)
	sort.Strings(ids)
	b, _ := json.Marshal(struct {
		IncidentID, PlanID, EvidenceDigest, LeaseID, ModelID string
		RequestIDs                                           []string
	}{req.IncidentID, req.PlanID, req.EvidenceDigest, req.LeaseID, req.ModelID, ids})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func cloneReconciliationRecords(in map[string]reconciliationRecord) map[string]reconciliationRecord {
	out := make(map[string]reconciliationRecord, len(in))
	for key, value := range in {
		value.Lease = cloneLease(value.Lease)
		out[key] = value
	}
	return out
}
