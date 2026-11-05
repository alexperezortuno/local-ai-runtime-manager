package inference

import (
	"sort"
	"time"

	"local-ai-runtime/internal/corecontract"
)

type RecoveryStatus struct {
	State                 string    `json:"state"`
	RecoveryRequired      bool      `json:"recovery_required"`
	AdmissionAllowed      bool      `json:"admission_allowed"`
	PendingInferenceCount int       `json:"pending_inference_count"`
	ProtectedLeaseCount   int       `json:"protected_lease_count"`
	Models                []string  `json:"models,omitempty"`
	LARMInstanceID        string    `json:"larm_instance_id,omitempty"`
	CleanShutdown         bool      `json:"clean_shutdown"`
	JournalVersion        int       `json:"journal_version"`
	JournalPath           string    `json:"journal_path,omitempty"`
	JournalDurability     string    `json:"journal_durability,omitempty"`
	JournalUpdatedAt      time.Time `json:"journal_updated_at,omitempty"`
	JournalError          string    `json:"journal_error,omitempty"`
	RecoveryReason        string    `json:"recovery_reason,omitempty"`
	Errors                []string  `json:"errors,omitempty"`
}

type LeaseDiagnostic struct {
	LeaseID          string                     `json:"lease_id"`
	ModelID          string                     `json:"model_id"`
	ExecutionIDs     []string                   `json:"execution_ids,omitempty"`
	RequestIDs       []string                   `json:"request_ids,omitempty"`
	Protected        bool                       `json:"protected"`
	ProtectionReason string                     `json:"protection_reason,omitempty"`
	RecoveryState    corecontract.RecoveryState `json:"recovery_state"`
}

func (r *Registry) RecoveryStatus() RecoveryStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status := RecoveryStatus{State: "NORMAL", RecoveryRequired: r.recoveryRequired || r.journalError != "", AdmissionAllowed: !r.recoveryRequired && r.journalError == "", CleanShutdown: true, RecoveryReason: r.recoveryReason, JournalError: r.journalError}
	if status.RecoveryRequired {
		status.State = "RECOVERY_REQUIRED"
	}
	if r.journal != nil {
		meta := r.journal.Meta()
		status.LARMInstanceID, status.CleanShutdown, status.JournalVersion = meta.LARMInstanceID, meta.CleanShutdown, meta.Version
		status.JournalPath, status.JournalDurability, status.JournalUpdatedAt = meta.Path, meta.Durability, meta.UpdatedAt
	}
	models := map[string]struct{}{}
	leases := map[string]*LeaseDiagnostic{}
	for _, id := range r.order {
		e := r.records[id]
		if e == nil {
			continue
		}
		if !e.inference.IsTerminal() || e.inference.IsAmbiguous() || e.recoveryState == corecontract.RecoveryRequired {
			status.PendingInferenceCount++
		}
		models[e.inference.ModelID] = struct{}{}
		lease := leases[e.inference.LeaseID]
		if lease == nil {
			lease = &LeaseDiagnostic{LeaseID: e.inference.LeaseID, ModelID: e.inference.ModelID, RecoveryState: e.recoveryState}
			leases[e.inference.LeaseID] = lease
		}
		lease.RequestIDs = append(lease.RequestIDs, e.inference.RequestID)
		lease.ExecutionIDs = appendUnique(lease.ExecutionIDs, e.inference.ExecutionID)
		if e.recoveryState == corecontract.RecoveryRequired {
			lease.RecoveryState = corecontract.RecoveryRequired
		}
	}
	for _, lease := range leases {
		lease.Protected, lease.ProtectionReason = r.protectedLockedForDiagnostic(lease.LeaseID)
		if lease.Protected {
			status.ProtectedLeaseCount++
		}
	}
	for model := range models {
		status.Models = append(status.Models, model)
	}
	sort.Strings(status.Models)
	return status
}

func (r *Registry) RecoveryInferences() []Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Snapshot, 0)
	for _, id := range r.order {
		if e := r.records[id]; e != nil && (!e.inference.IsTerminal() || e.inference.IsAmbiguous() || e.recoveryState == corecontract.RecoveryRequired) {
			result = append(result, snapshot(e))
		}
	}
	return result
}

func (r *Registry) RecoveryLeases() []LeaseDiagnostic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	leases := map[string]*LeaseDiagnostic{}
	for _, id := range r.order {
		e := r.records[id]
		if e == nil {
			continue
		}
		lease := leases[e.inference.LeaseID]
		if lease == nil {
			lease = &LeaseDiagnostic{LeaseID: e.inference.LeaseID, ModelID: e.inference.ModelID, RecoveryState: e.recoveryState}
			leases[e.inference.LeaseID] = lease
		}
		lease.RequestIDs = append(lease.RequestIDs, e.inference.RequestID)
		lease.ExecutionIDs = appendUnique(lease.ExecutionIDs, e.inference.ExecutionID)
		if e.recoveryState == corecontract.RecoveryRequired {
			lease.RecoveryState = corecontract.RecoveryRequired
		}
	}
	result := make([]LeaseDiagnostic, 0, len(leases))
	for _, lease := range leases {
		lease.Protected, lease.ProtectionReason = r.protectedLockedForDiagnostic(lease.LeaseID)
		result = append(result, *lease)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LeaseID < result[j].LeaseID })
	return result
}

func (r *Registry) protectedLockedForDiagnostic(leaseID string) (bool, string) {
	if reason := r.blockedLeases[leaseID]; reason != "" {
		return true, reason
	}
	if r.journalError != "" {
		return true, "inference journal is unavailable"
	}
	if r.recoveryRequired {
		return true, "runtime recovery is required"
	}
	for _, id := range r.order {
		e := r.records[id]
		if e != nil && e.inference.LeaseID == leaseID && (!e.inference.IsTerminal() || e.inference.IsAmbiguous() || len(e.evidence) == 0 && e.inference.IsTerminal()) {
			return true, "an inference is active, ambiguous, or lacks terminal evidence"
		}
	}
	return false, ""
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
