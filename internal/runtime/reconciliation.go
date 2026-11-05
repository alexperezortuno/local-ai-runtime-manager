package runtime

import (
	"context"
	"errors"

	"local-ai-runtime/internal/corecontract"
)

// ProcessReconciliationRequest is deliberately metadata-only. Runtime owns
// the process evidence and Recovery never receives a PID or process handle.
// ReconcileProcess confirms an already durable termination observation. It
// never adopts, signals, or searches for a surviving process.
func (m *Manager) ReconcileProcess(_ context.Context, req corecontract.ProcessReconciliationRequest) error {
	if req.OperationKey == "" || req.ProcessInstanceID == "" || req.OwnerInstanceID == "" {
		return errors.New("process reconciliation requires operation, process and owner identities")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.processes[req.ProcessInstanceID]
	if !ok {
		return errors.New("process instance not found")
	}
	if instance.OwnerInstanceID != req.OwnerInstanceID || req.OwnerInstanceID != m.ownerID || instance.LARMInstanceID != "" && instance.LARMInstanceID != req.OwnerInstanceID {
		return errors.New("process ownership does not match the recovery owner")
	}
	if err := verifiedTermination(instance); err != nil {
		return err
	}
	if m.proc != nil && m.proc.instanceID == req.ProcessInstanceID {
		return errors.New("correlated process is still owned by the runtime")
	}
	if m.proc != nil {
		return errors.New("another runtime process is active")
	}
	if instance.State == corecontract.ProcessRecoveryRequired {
		proposed := instance
		proposed.State = corecontract.ProcessTerminatedVerified
		evidence := terminationEvidence(proposed)
		// Reuse the durable evidence instead of manufacturing a second record.
		for _, candidate := range instance.Evidence {
			if candidate.EvidenceID == instance.TerminationEvidenceID {
				evidence = candidate
				break
			}
		}
		if err := corecontract.ValidateProcessTransition(instance, proposed, &evidence); err != nil {
			return err
		}
		if err := m.persistProcessLocked(proposed); err != nil {
			return err
		}
		m.processes[req.ProcessInstanceID] = proposed
	}
	for _, candidate := range m.processes {
		if candidate.State != corecontract.ProcessTerminatedVerified && candidate.State != corecontract.ProcessFailed {
			return errors.New("another process instance still requires recovery")
		}
	}
	m.recovery = false
	m.recoveryReason = ""
	if m.state == RecoveryRequired {
		m.state = Stopped
	}
	return nil
}

func verifiedTermination(instance corecontract.ProcessInstance) error {
	if instance.State != corecontract.ProcessTerminatedVerified && instance.State != corecontract.ProcessRecoveryRequired {
		return errors.New("process termination is not verified")
	}
	if instance.PID == 0 || instance.PGID == 0 || instance.StartIdentity == "" || instance.OSProcessStartIdentity == "" || instance.StartIdentity != instance.OSProcessStartIdentity {
		return errors.New("historical process identity is incomplete")
	}
	if instance.ProcessIdentityStatus != "VERIFIED" || instance.OwnershipStatus != "OWNED_VERIFIED" || instance.TerminationStatus != "TERMINATED_VERIFIED" || instance.TerminatedAt.IsZero() || instance.WaitError == "" || instance.DescendantStatus != "VERIFIED_OBSERVED_SCOPE" || instance.TerminationEvidenceID == "" {
		return errors.New("termination evidence is incomplete")
	}
	validWait := instance.WaitError == "exited_zero" || instance.WaitError == "nonzero_exit" || instance.WaitError == "signaled_exit"
	if !validWait {
		return errors.New("cmd.Wait result is ambiguous")
	}
	var found *corecontract.TerminalEvidence
	for i := range instance.Evidence {
		candidate := instance.Evidence[i]
		if candidate.EvidenceID == instance.TerminationEvidenceID {
			found = &candidate
			break
		}
		if candidate.Type == corecontract.EvidenceProcessTerminationVerified && candidate.SubjectID == instance.ProcessInstanceID {
			found = &candidate
		}
	}
	if found == nil || found.Type != corecontract.EvidenceProcessTerminationVerified || found.SubjectID != instance.ProcessInstanceID || found.CorrelationID != instance.ProcessInstanceID || !found.ProcessAbsent || !found.ProcessIdentityVerified || found.Result != corecontract.EvidenceSuccess {
		return errors.New("correlated process termination evidence is missing")
	}
	if err := corecontract.ValidateEvidence(*found); err != nil {
		return err
	}
	return nil
}
