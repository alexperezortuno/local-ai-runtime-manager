package corecontract

import (
	"reflect"
	"strings"
)

func ValidateExecution(e Execution) error {
	if err := validateVersion(e.SchemaVersion, "execution"); err != nil {
		return err
	}
	if err := validateID(e.ExecutionID, "execution", "execution_id"); err != nil {
		return err
	}
	if e.ParentExecutionID != "" && !validID(e.ParentExecutionID) {
		return invalid(ErrCodeInvalidIdentity, "execution", "parent_execution_id", "must be a canonical UUIDv4")
	}
	if e.ParentExecutionID == e.ExecutionID && e.ParentExecutionID != "" {
		return invalid(ErrCodeInvariantViolation, "execution", "parent_execution_id", "cannot reference itself")
	}
	if strings.TrimSpace(e.Initiator) == "" || strings.ContainsAny(e.Initiator, "\x00\r\n") {
		return invalid(ErrCodeInvariantViolation, "execution", "initiator", "must be a non-empty opaque value")
	}
	if e.Lifetime != ExecutionPersistent && e.Lifetime != ExecutionEphemeral {
		return invalid(ErrCodeInvalidState, "execution", "lifetime", "unknown lifetime")
	}
	if e.ModelID == "" {
		return invalid(ErrCodeInvariantViolation, "execution", "model_id", "is required")
	}
	if !validExecutionState(e.State) {
		return invalid(ErrCodeInvalidState, "execution", "state", "unknown state")
	}
	if e.CreatedAt.IsZero() || e.UpdatedAt.IsZero() {
		return invalid(ErrCodeInvariantViolation, "execution", "timestamps", "created_at and updated_at are required")
	}
	if e.UpdatedAt.Before(e.CreatedAt) {
		return invalid(ErrCodeInvariantViolation, "execution", "updated_at", "cannot precede created_at")
	}
	if err := validateIDList(e.LeaseIDs, "execution", "lease_ids"); err != nil {
		return err
	}
	if err := validateIDList(e.InferenceIDs, "execution", "inference_ids"); err != nil {
		return err
	}
	if e.RecoveryState != RecoveryNormal && e.RecoveryState != RecoveryRequired && e.RecoveryState != RecoveryResolved {
		return invalid(ErrCodeInvalidState, "execution", "recovery_state", "unknown recovery state")
	}
	if e.State == ExecutionRecoveryRequired && e.RecoveryState != RecoveryRequired {
		return invalid(ErrCodeInvariantViolation, "execution", "recovery_state", "recovery-required execution must retain recovery ownership")
	}
	if e.Completion != nil {
		if !isTerminalExecution(e.State) {
			return invalid(ErrCodeInvariantViolation, "execution", "completion", "completion belongs only to a terminal execution")
		}
		if e.Completion.At.IsZero() || len(e.Completion.EvidenceIDs) == 0 {
			return invalid(ErrCodeInvariantViolation, "execution", "completion", "terminal completion requires timestamp and evidence")
		}
		if (e.State == ExecutionCompleted && e.Completion.Result != CompletionSucceeded) || (e.State == ExecutionCancelConfirmed && e.Completion.Result != CompletionCancelled) || (e.State == ExecutionFailed && e.Completion.Result != CompletionFailed) {
			return invalid(ErrCodeInvariantViolation, "execution", "completion", "completion result does not match execution state")
		}
	} else if isTerminalExecution(e.State) {
		return invalid(ErrCodeEvidenceRequired, "execution", "completion", "terminal execution requires completion metadata")
	}
	return nil
}

func ValidateInference(i Inference) error {
	if err := validateVersion(i.SchemaVersion, "inference"); err != nil {
		return err
	}
	for field, value := range map[string]string{"request_id": i.RequestID, "execution_id": i.ExecutionID, "lease_id": i.LeaseID} {
		if err := validateID(value, "inference", field); err != nil {
			return err
		}
	}
	if i.ModelID == "" {
		return invalid(ErrCodeInvariantViolation, "inference", "model_id", "is required")
	}
	if i.ProcessAssignment != ProcessPending && i.ProcessAssignment != ProcessUnverified && i.ProcessAssignment != ProcessAssigned {
		return invalid(ErrCodeInvalidState, "inference", "process_assignment", "unknown assignment state")
	}
	if i.ProcessAssignment == ProcessAssigned {
		if err := validateID(i.ProcessInstanceID, "inference", "process_instance_id"); err != nil {
			return err
		}
	} else if i.ProcessInstanceID != "" {
		return invalid(ErrCodeInvariantViolation, "inference", "process_instance_id", "must be empty while process assignment is pending or unverified")
	}
	if !validAdmission(i.Admission) || !validTransport(i.Transport) || !validSemantic(i.Semantic) || !validCancellation(i.Cancellation) {
		return invalid(ErrCodeInvalidState, "inference", "state", "unknown inference dimension state")
	}
	if i.CreatedAt.IsZero() || i.UpdatedAt.IsZero() || i.UpdatedAt.Before(i.CreatedAt) {
		return invalid(ErrCodeInvariantViolation, "inference", "timestamps", "invalid timestamps")
	}
	if i.ErrorClass != "" && !validErrorClass(i.ErrorClass) {
		return invalid(ErrCodeInvalidState, "inference", "error_class", "unknown error classification")
	}
	if i.SSE != nil && !i.Stream {
		return invalid(ErrCodeInvariantViolation, "inference", "sse", "SSE observation requires stream=true")
	}
	if i.SSE != nil && i.SSE.Truncated && i.SSE.DoneDetected {
		return invalid(ErrCodeInvariantViolation, "inference", "sse", "a stream cannot be both truncated and DONE")
	}
	if i.Admission == AdmissionAccepted && i.ProcessAssignment == ProcessPending && i.Transport != TransportNotStarted {
		return invalid(ErrCodeInvariantViolation, "inference", "transport_state", "transport cannot start before process assignment")
	}
	if i.Admission == AdmissionRejected && (i.Transport != TransportNotStarted || i.Semantic != SemanticNotEvaluated) {
		return invalid(ErrCodeInvariantViolation, "inference", "admission_state", "rejected inference cannot have transport or semantic progress")
	}
	return nil
}

func ValidateLease(l Lease) error {
	if err := validateVersion(l.SchemaVersion, "lease"); err != nil {
		return err
	}
	for field, value := range map[string]string{"lease_id": l.LeaseID, "execution_id": l.ExecutionID} {
		if err := validateID(value, "lease", field); err != nil {
			return err
		}
	}
	if l.ModelID == "" {
		return invalid(ErrCodeInvariantViolation, "lease", "model_id", "is required")
	}
	if l.Ownership != LeasePersistentOwnership && l.Ownership != LeaseEphemeralOwnership {
		return invalid(ErrCodeInvalidState, "lease", "ownership", "unknown ownership kind")
	}
	if !validFIFO(l.FIFO) || !validActivation(l.Activation) || !validRelease(l.Release) || !validRecovery(l.RecoveryState) {
		return invalid(ErrCodeInvalidState, "lease", "state", "unknown lease dimension state")
	}
	if err := validateIDList(l.InferenceIDs, "lease", "inference_ids"); err != nil {
		return err
	}
	if l.ExpiresAt != nil && l.HeartbeatAt != nil && l.HeartbeatAt.After(*l.ExpiresAt) {
		return invalid(ErrCodeInvariantViolation, "lease", "heartbeat_at", "heartbeat cannot be after expiry")
	}
	if l.FIFO == FIFOActive && (l.Activation != ActivationActive || l.Release == ReleaseReleased) {
		return invalid(ErrCodeInvariantViolation, "lease", "fifo_state", "active lease must have active activation and no release")
	}
	if l.Release == ReleaseReleased && l.FIFO != FIFOCompleted && l.FIFO != FIFOCancelled && l.FIFO != FIFOFailed && l.FIFO != FIFOExpired {
		return invalid(ErrCodeInvariantViolation, "lease", "release_state", "released lease must have terminal FIFO state")
	}
	return nil
}

func ValidateProcessInstance(p ProcessInstance) error {
	if err := validateVersion(p.SchemaVersion, "process_instance"); err != nil {
		return err
	}
	for field, value := range map[string]string{"process_instance_id": p.ProcessInstanceID, "owner_instance_id": p.OwnerInstanceID} {
		if err := validateID(value, "process_instance", field); err != nil {
			return err
		}
	}
	if p.ModelID == "" || p.ConfigFingerprint == "" {
		return invalid(ErrCodeInvariantViolation, "process_instance", "identity", "model_id and config_fingerprint are required")
	}
	if p.LARMInstanceID != "" {
		if err := validateID(p.LARMInstanceID, "process_instance", "larm_instance_id"); err != nil {
			return err
		}
	}
	if !validProcessState(p.State) || !validRecovery(p.RecoveryState) {
		return invalid(ErrCodeInvalidState, "process_instance", "state", "unknown process state")
	}
	if p.StartupDiagnostic != "" && !validStartupDiagnostic(p.StartupDiagnostic) {
		return invalid(ErrCodeInvalidState, "process_instance", "startup_diagnostic", "unknown startup diagnostic category")
	}
	if err := validateIDList(p.InferenceIDs, "process_instance", "inference_ids"); err != nil {
		return err
	}
	if p.PID < 0 || p.PGID < 0 {
		return invalid(ErrCodeInvariantViolation, "process_instance", "pid", "pid and pgid cannot be negative")
	}
	if p.State == ProcessReady {
		if p.PID == 0 || p.PGID == 0 || p.StartIdentity == "" || p.ReadinessEvidenceID == "" {
			return invalid(ErrCodeInvariantViolation, "process_instance", "identity", "READY requires verified process identity and readiness evidence")
		}
		if p.OwnershipStatus != "" && p.OwnershipStatus != "OWNED_VERIFIED" {
			return invalid(ErrCodeOwnershipViolation, "process_instance", "ownership_status", "READY requires verified ownership")
		}
		if p.ProcessIdentityStatus != "" && p.ProcessIdentityStatus != "VERIFIED" {
			return invalid(ErrCodeOwnershipViolation, "process_instance", "process_identity_status", "READY requires verified process identity")
		}
	}
	if p.State == ProcessTerminatedVerified && p.TerminationEvidenceID == "" {
		return invalid(ErrCodeEvidenceRequired, "process_instance", "termination_evidence_id", "verified termination requires evidence")
	}
	for _, evidence := range p.Evidence {
		if err := ValidateEvidence(evidence); err != nil {
			return err
		}
		if evidence.SubjectKind != SubjectProcess || evidence.SubjectID != p.ProcessInstanceID {
			return invalid(ErrCodeEvidenceMismatch, "process_instance", "evidence", "process evidence must target this instance")
		}
	}
	return nil
}

func validStartupDiagnostic(category StartupDiagnosticCategory) bool {
	switch category {
	case StartupDiagnosticSpawnError, StartupDiagnosticStartupCancelled, StartupDiagnosticReadinessTimeout, StartupDiagnosticBackendExited, StartupDiagnosticIdentityUnverified, StartupDiagnosticUnknown:
		return true
	default:
		return false
	}
}

func ValidateEvidence(e TerminalEvidence) error {
	if err := validateVersion(e.SchemaVersion, "evidence"); err != nil {
		return err
	}
	if err := validateID(e.EvidenceID, "evidence", "evidence_id"); err != nil {
		return err
	}
	if e.SubjectID == "" || strings.TrimSpace(e.Source) == "" || strings.TrimSpace(e.CorrelationID) == "" || e.ObservedAt.IsZero() {
		return invalid(ErrCodeInvariantViolation, "evidence", "metadata", "subject, source, correlation and observed_at are required")
	}
	if e.SubjectKind != SubjectRequest && e.SubjectKind != SubjectProcess && e.SubjectKind != SubjectLease && e.SubjectKind != SubjectExecution {
		return invalid(ErrCodeInvalidState, "evidence", "subject_kind", "unknown subject kind")
	}
	if !validEvidenceType(e.Type) {
		return invalid(ErrCodeInvalidState, "evidence", "type", "unknown evidence type")
	}
	if e.Result != EvidenceSuccess && e.Result != EvidenceFailure && e.Result != EvidenceAmbiguous {
		return invalid(ErrCodeInvalidState, "evidence", "result", "unknown result")
	}
	if e.Type == EvidenceSSECompleted && (!e.SSEDone || !e.EOF || e.Result != EvidenceSuccess) {
		return invalid(ErrCodeEvidenceMismatch, "evidence", "type", "SSE completion requires DONE, EOF and success")
	}
	if e.Type == EvidenceSSETruncated && (e.SubjectKind != SubjectRequest || e.SSEDone || e.Result == EvidenceSuccess) {
		return invalid(ErrCodeEvidenceMismatch, "evidence", "type", "truncated SSE cannot be successful or DONE")
	}
	if e.Type == EvidenceProcessTerminationVerified && (e.SubjectKind != SubjectProcess || !e.ProcessAbsent || !e.ProcessIdentityVerified || e.Result != EvidenceSuccess) {
		return invalid(ErrCodeEvidenceMismatch, "evidence", "type", "process termination requires verified identity and absence")
	}
	if e.Type == EvidenceCancellationConfirmed && (e.SubjectKind != SubjectRequest && e.SubjectKind != SubjectExecution) || e.Type == EvidenceCancellationConfirmed && (!e.TransportTerminated && !e.ProcessAbsent) {
		return invalid(ErrCodeEvidenceMismatch, "evidence", "type", "cancellation requires managed transport or verified process termination")
	}
	if e.Type == EvidenceExecutionCompleted && e.SubjectKind != SubjectExecution {
		return invalid(ErrCodeEvidenceMismatch, "evidence", "type", "execution evidence must target an execution")
	}
	return nil
}

func ValidateParentChild(parent, child Execution) error {
	if err := ValidateExecution(parent); err != nil {
		return err
	}
	if err := ValidateExecution(child); err != nil {
		return err
	}
	if child.ParentExecutionID != parent.ExecutionID {
		return invalid(ErrCodeInvariantViolation, "execution", "parent_execution_id", "child does not reference the supplied parent")
	}
	if isTerminalExecution(parent.State) {
		return invalid(ErrCodeInvariantViolation, "execution", "parent_execution_id", "a terminal execution cannot accept a child")
	}
	return nil
}

// ValidateInferenceOwnership is the admission-time cross-entity check. It is
// deliberately separate from entity validation so the package remains pure.
func ValidateInferenceOwnership(inference Inference, execution Execution, lease Lease) error {
	if err := ValidateInference(inference); err != nil {
		return err
	}
	if err := ValidateExecution(execution); err != nil {
		return err
	}
	if err := ValidateLease(lease); err != nil {
		return err
	}
	if inference.ExecutionID != execution.ExecutionID || inference.LeaseID != lease.LeaseID || lease.ExecutionID != execution.ExecutionID || inference.ModelID != execution.ModelID || lease.ModelID != execution.ModelID {
		return invalid(ErrCodeOwnershipViolation, "inference", "ownership", "inference, execution, lease and model identities do not match")
	}
	if execution.State == ExecutionCancelRequested || isTerminalExecution(execution.State) {
		return invalid(ErrCodeOwnershipViolation, "inference", "execution_id", "cancelled or terminal execution cannot admit inference")
	}
	if lease.Release == ReleaseReleased || lease.RecoveryState != RecoveryNormal {
		return invalid(ErrCodeRecoveryRequired, "inference", "lease_id", "released or recovery-protected lease cannot admit inference")
	}
	return nil
}

// ValidateRetry makes retry identity explicit: only the request identity is
// new; execution, lease and model ownership remain the same.
func ValidateRetry(previous, retry Inference) error {
	if err := ValidateInference(previous); err != nil {
		return err
	}
	if err := ValidateInference(retry); err != nil {
		return err
	}
	if previous.RequestID == retry.RequestID {
		return invalid(ErrCodeInvariantViolation, "inference", "request_id", "a retry must use a new request identity")
	}
	if previous.ExecutionID != retry.ExecutionID || previous.LeaseID != retry.LeaseID || previous.ModelID != retry.ModelID {
		return invalid(ErrCodeOwnershipViolation, "inference", "ownership", "a retry cannot silently change execution, lease or model")
	}
	return nil
}

func ValidateExecutionTransition(current, proposed Execution, evidence *TerminalEvidence, inferences []Inference) error {
	if err := ValidateExecution(current); err != nil {
		return err
	}
	if err := ValidateExecution(proposed); err != nil {
		return err
	}
	if !sameExecutionIdentity(current, proposed) {
		return invalid(ErrCodeInvariantViolation, "execution", "identity", "execution identity and parentage are immutable")
	}
	if current.State == proposed.State && reflect.DeepEqual(current, proposed) {
		return nil
	}
	if isTerminalExecution(current.State) || (current.State == ExecutionUnknown && proposed.State != ExecutionRecoveryRequired) {
		return invalid(ErrCodeTerminal, "execution", "state", "terminal or ambiguous execution cannot be reactivated")
	}
	if !executionTransitionAllowed(current.State, proposed.State) {
		return invalid(ErrCodeInvalidTransition, "execution", "state", "transition is not allowed")
	}
	if proposed.State == ExecutionCancelRequested && len(proposed.InferenceIDs) > len(current.InferenceIDs) {
		return invalid(ErrCodeInvalidTransition, "execution", "inference_ids", "cancel-requested execution cannot accept new inferences")
	}
	if isTerminalExecution(proposed.State) {
		if err := validateTerminalEvidence(evidence, proposed.ExecutionID, proposed.State); err != nil {
			return err
		}
		if err := allInferencesTerminal(current, proposed, inferences); err != nil {
			return err
		}
	}
	if proposed.State == ExecutionRecoveryRequired && proposed.RecoveryState != RecoveryRequired {
		return invalid(ErrCodeInvariantViolation, "execution", "recovery_state", "recovery-required state must retain ownership")
	}
	return nil
}

func ValidateInferenceTransition(current, proposed Inference, evidence *TerminalEvidence) error {
	if err := ValidateInference(current); err != nil {
		return err
	}
	if err := ValidateInference(proposed); err != nil {
		return err
	}
	if !sameInferenceIdentity(current, proposed) {
		return invalid(ErrCodeInvariantViolation, "inference", "identity", "request, execution, lease, model and stream are immutable")
	}
	if current.IsTerminal() && !reflect.DeepEqual(current, proposed) {
		return invalid(ErrCodeTerminal, "inference", "state", "late event cannot modify a terminal inference")
	}
	if current.Admission != proposed.Admission && !admissionTransitionAllowed(current.Admission, proposed.Admission) {
		return invalid(ErrCodeInvalidTransition, "inference", "admission_state", "transition is not allowed")
	}
	if current.Transport != proposed.Transport && !transportTransitionAllowed(current.Transport, proposed.Transport) {
		return invalid(ErrCodeInvalidTransition, "inference", "transport_state", "transition is not allowed")
	}
	if current.Semantic != proposed.Semantic && !semanticTransitionAllowed(current.Semantic, proposed.Semantic) {
		return invalid(ErrCodeInvalidTransition, "inference", "semantic_state", "transition is not allowed")
	}
	if current.Cancellation != proposed.Cancellation && !cancellationTransitionAllowed(current.Cancellation, proposed.Cancellation) {
		return invalid(ErrCodeInvalidTransition, "inference", "cancellation_state", "transition is not allowed")
	}
	if current.ProcessAssignment == ProcessAssigned && proposed.ProcessInstanceID != current.ProcessInstanceID {
		return invalid(ErrCodeOwnershipViolation, "inference", "process_instance_id", "process identity cannot be replaced")
	}
	if proposed.Transport != TransportNotStarted && proposed.Admission != AdmissionAccepted {
		return invalid(ErrCodeInvariantViolation, "inference", "admission_state", "transport requires accepted admission")
	}
	if proposed.Transport != TransportNotStarted && proposed.ProcessAssignment == ProcessPending {
		return invalid(ErrCodeInvariantViolation, "inference", "process_assignment", "active transport requires an assigned process instance")
	}
	if proposed.Semantic == SemanticSucceeded {
		if evidence == nil {
			return invalid(ErrCodeEvidenceRequired, "inference", "semantic_state", "semantic success requires protocol evidence")
		}
		if err := ValidateEvidence(*evidence); err != nil {
			return err
		}
		if proposed.Stream {
			if evidence.Type != EvidenceSSECompleted || !evidence.SSEDone || !evidence.EOF {
				return invalid(ErrCodeEvidenceMismatch, "inference", "semantic_state", "SSE success requires DONE followed by EOF")
			}
		} else if evidence.Type != EvidenceHTTPCompleted || !evidence.EOF {
			return invalid(ErrCodeEvidenceMismatch, "inference", "semantic_state", "HTTP success requires completed transport")
		}
		if evidence.SubjectID != proposed.RequestID {
			return invalid(ErrCodeEvidenceMismatch, "inference", "request_id", "evidence does not belong to request")
		}
	}
	if proposed.Semantic == SemanticTruncated && (evidence == nil || evidence.Type != EvidenceSSETruncated || evidence.SubjectID != proposed.RequestID) {
		return invalid(ErrCodeEvidenceMismatch, "inference", "semantic_state", "truncation requires correlated SSE truncation evidence")
	}
	if proposed.Semantic == SemanticTruncated {
		if err := ValidateEvidence(*evidence); err != nil {
			return err
		}
	}
	if proposed.Cancellation == CancelConfirmed {
		if evidence == nil || evidence.Type != EvidenceCancellationConfirmed || evidence.SubjectID != proposed.RequestID {
			return invalid(ErrCodeEvidenceMismatch, "inference", "cancellation_state", "cancellation confirmation requires correlated evidence")
		}
		if err := ValidateEvidence(*evidence); err != nil {
			return err
		}
	}
	if proposed.Stream && proposed.SSE != nil && proposed.SSE.EOF && !proposed.SSE.DoneDetected && proposed.Semantic == SemanticSucceeded {
		return invalid(ErrCodeEvidenceMismatch, "inference", "sse", "EOF without DONE is not semantic success")
	}
	return nil
}

func ValidateLeaseTransition(current, proposed Lease, evidence *TerminalEvidence, inferences []Inference) error {
	if err := ValidateLease(current); err != nil {
		return err
	}
	if err := ValidateLease(proposed); err != nil {
		return err
	}
	if current.LeaseID != proposed.LeaseID || current.ExecutionID != proposed.ExecutionID || current.ModelID != proposed.ModelID || current.Ownership != proposed.Ownership {
		return invalid(ErrCodeOwnershipViolation, "lease", "identity", "lease owner and model are immutable")
	}
	if current.Release == ReleaseReleased && !reflect.DeepEqual(current, proposed) {
		return invalid(ErrCodeTerminal, "lease", "release_state", "released lease cannot be reactivated")
	}
	if current.FIFO != proposed.FIFO && !fifoTransitionAllowed(current.FIFO, proposed.FIFO) {
		return invalid(ErrCodeInvalidTransition, "lease", "fifo_state", "transition is not allowed")
	}
	if current.Activation != proposed.Activation && !activationTransitionAllowed(current.Activation, proposed.Activation) {
		return invalid(ErrCodeInvalidTransition, "lease", "activation_state", "transition is not allowed")
	}
	if current.Release != proposed.Release && !releaseTransitionAllowed(current.Release, proposed.Release) {
		return invalid(ErrCodeInvalidTransition, "lease", "release_state", "transition is not allowed")
	}
	if proposed.Release == ReleaseReleased {
		for _, id := range proposed.InferenceIDs {
			found := false
			for _, inference := range inferences {
				if inference.RequestID == id {
					found = true
					if inference.LeaseID != proposed.LeaseID || !inference.IsTerminal() || inference.IsAmbiguous() {
						return invalid(ErrCodeOwnershipViolation, "lease", "release_state", "lease cannot release with a non-terminal or ambiguous inference")
					}
				}
			}
			if !found {
				return invalid(ErrCodeInvariantViolation, "lease", "inference_ids", "release requires every admitted inference to be supplied")
			}
		}
		if current.RecoveryState != RecoveryNormal && evidence == nil {
			return invalid(ErrCodeEvidenceRequired, "lease", "release_state", "recovery-protected lease requires explicit release evidence")
		}
	}
	return nil
}

func ValidateProcessTransition(current, proposed ProcessInstance, evidence *TerminalEvidence) error {
	if err := ValidateProcessInstance(current); err != nil {
		return err
	}
	if err := ValidateProcessInstance(proposed); err != nil {
		return err
	}
	if current.ProcessInstanceID != proposed.ProcessInstanceID || current.OwnerInstanceID != proposed.OwnerInstanceID || current.ModelID != proposed.ModelID || current.ConfigFingerprint != proposed.ConfigFingerprint {
		return invalid(ErrCodeOwnershipViolation, "process_instance", "identity", "process identity is immutable")
	}
	if current.State == ProcessTerminatedVerified && !reflect.DeepEqual(current, proposed) {
		return invalid(ErrCodeTerminal, "process_instance", "state", "verified termination cannot be reactivated")
	}
	if current.State != proposed.State && !processTransitionAllowed(current.State, proposed.State) {
		return invalid(ErrCodeInvalidTransition, "process_instance", "state", "transition is not allowed")
	}
	if proposed.State == ProcessReady && (proposed.PID == 0 || proposed.PGID == 0 || proposed.StartIdentity == "") {
		return invalid(ErrCodeInvariantViolation, "process_instance", "identity", "READY requires PID, PGID and start identity")
	}
	if proposed.State == ProcessTerminatedVerified {
		if evidence == nil || evidence.Type != EvidenceProcessTerminationVerified || evidence.SubjectID != proposed.ProcessInstanceID || !evidence.ProcessAbsent || !evidence.ProcessIdentityVerified {
			return invalid(ErrCodeEvidenceMismatch, "process_instance", "termination_evidence_id", "termination must verify this process identity is absent")
		}
	}
	return nil
}

// ValidateSingleModelResidence expresses the current one-resident-model policy
// without introducing a scheduler or changing FIFO behavior.
func ValidateSingleModelResidence(leases []Lease) error {
	model := ""
	active := 0
	for _, lease := range leases {
		if lease.FIFO != FIFOActive {
			continue
		}
		active++
		if active > 1 {
			return invalid(ErrCodeInvariantViolation, "lease", "fifo_state", "only one active model residence is supported")
		}
		if model == "" {
			model = lease.ModelID
			continue
		}
		if lease.ModelID != model {
			return invalid(ErrCodeInvariantViolation, "lease", "model_id", "more than one model is resident")
		}
	}
	return nil
}

func allInferencesTerminal(current, proposed Execution, inferences []Inference) error {
	ids := proposed.InferenceIDs
	if len(ids) == 0 {
		ids = current.InferenceIDs
	}
	for _, id := range ids {
		found := false
		for _, inference := range inferences {
			if inference.RequestID == id {
				found = true
				if !inference.IsTerminal() || inference.IsAmbiguous() {
					return invalid(ErrCodeInvariantViolation, "execution", "inference_ids", "execution cannot finish with active or ambiguous inference")
				}
			}
		}
		if !found {
			return invalid(ErrCodeInvariantViolation, "execution", "inference_ids", "terminal transition requires every inference state")
		}
	}
	return nil
}

func validateTerminalEvidence(e *TerminalEvidence, subject string, state ExecutionState) error {
	if e == nil {
		return invalid(ErrCodeEvidenceRequired, "execution", "completion", "terminal transition requires evidence")
	}
	if err := ValidateEvidence(*e); err != nil {
		return err
	}
	if e.SubjectID != subject {
		return invalid(ErrCodeEvidenceMismatch, "execution", "completion", "evidence does not belong to execution")
	}
	if state == ExecutionCompleted && e.Result != EvidenceSuccess {
		return invalid(ErrCodeEvidenceMismatch, "execution", "completion", "completed execution requires successful evidence")
	}
	if state == ExecutionCancelConfirmed && e.Type != EvidenceCancellationConfirmed {
		return invalid(ErrCodeEvidenceMismatch, "execution", "completion", "cancelled execution requires cancellation evidence")
	}
	return nil
}

func sameExecutionIdentity(a, b Execution) bool {
	return a.SchemaVersion == b.SchemaVersion && a.ExecutionID == b.ExecutionID && a.ParentExecutionID == b.ParentExecutionID && a.Initiator == b.Initiator && a.Lifetime == b.Lifetime && a.ModelID == b.ModelID
}
func sameInferenceIdentity(a, b Inference) bool {
	return a.SchemaVersion == b.SchemaVersion && a.RequestID == b.RequestID && a.ExecutionID == b.ExecutionID && a.LeaseID == b.LeaseID && a.ModelID == b.ModelID && a.Stream == b.Stream
}
func validateVersion(v, entity string) error {
	if v != ContractVersion {
		return invalid(ErrCodeInvalidVersion, entity, "schema_version", "unsupported contract version")
	}
	return nil
}
func validateID(v, entity, field string) error {
	if !validID(v) {
		return invalid(ErrCodeInvalidIdentity, entity, field, "must be a canonical UUIDv4")
	}
	return nil
}
func validateIDList(values []string, entity, field string) error {
	seen := map[string]bool{}
	for _, v := range values {
		if err := validateID(v, entity, field); err != nil {
			return err
		}
		if seen[v] {
			return invalid(ErrCodeInvariantViolation, entity, field, "contains a duplicate identity")
		}
		seen[v] = true
	}
	return nil
}
func isTerminalExecution(s ExecutionState) bool {
	return s == ExecutionCompleted || s == ExecutionCancelConfirmed || s == ExecutionFailed
}
func validExecutionState(s ExecutionState) bool {
	switch s {
	case ExecutionCreated, ExecutionWaitingModel, ExecutionActive, ExecutionFinalizing, ExecutionCompleted, ExecutionCancelRequested, ExecutionCancelConfirmed, ExecutionFailed, ExecutionUnknown, ExecutionRecoveryRequired:
		return true
	}
	return false
}
func validAdmission(s AdmissionState) bool {
	return s == AdmissionPending || s == AdmissionAccepted || s == AdmissionRejected
}
func validTransport(s TransportState) bool {
	switch s {
	case TransportNotStarted, TransportStarted, TransportHeaders, TransportBodyCompleted, TransportCompleted, TransportFailed, TransportUnknown:
		return true
	}
	return false
}
func validSemantic(s SemanticState) bool {
	return s == SemanticNotEvaluated || s == SemanticSucceeded || s == SemanticFailed || s == SemanticTruncated || s == SemanticUnknown
}
func validErrorClass(s ErrorClass) bool {
	switch s {
	case ErrorNone, ErrorTransport, ErrorBackend, ErrorSSETruncated, ErrorClientDisconnect, ErrorCancellation, ErrorUnknown:
		return true
	}
	return false
}
func validCancellation(s CancellationState) bool {
	return s == CancelNone || s == CancelRequested || s == CancelInProgress || s == CancelConfirmed || s == CancelUnknown
}
func validFIFO(s FIFOState) bool {
	switch s {
	case FIFOQueued, FIFOPreparing, FIFOActive, FIFOCompleted, FIFOFailed, FIFOCancelled, FIFOExpired, FIFOUnknown:
		return true
	}
	return false
}
func validActivation(s ActivationState) bool {
	return s == ActivationInactive || s == ActivationPreparing || s == ActivationActive || s == ActivationReleasing
}
func validRelease(s ReleaseState) bool {
	return s == ReleaseNotRequested || s == ReleaseRequested || s == ReleaseInProgress || s == ReleaseReleased || s == ReleaseUnknown
}
func validRecovery(s RecoveryState) bool {
	return s == RecoveryNormal || s == RecoveryRequired || s == RecoveryResolved
}
func validProcessState(s ProcessState) bool {
	switch s {
	case ProcessPreparing, ProcessStarting, ProcessWaitingReadiness, ProcessReady, ProcessTerminationRequested, ProcessTerminating, ProcessTerminatedVerified, ProcessTerminationUnknown, ProcessLost, ProcessFailed, ProcessIdentityUnknown, ProcessRecoveryRequired:
		return true
	}
	return false
}
func validEvidenceType(t EvidenceType) bool {
	switch t {
	case EvidenceHTTPCompleted, EvidenceSSECompleted, EvidenceSSETruncated, EvidenceTransportError, EvidenceBackendError, EvidenceCancellationConfirmed, EvidenceProcessTerminationVerified, EvidenceProcessReady, EvidenceExecutionCompleted, EvidenceUnknown:
		return true
	}
	return false
}

func executionTransitionAllowed(from, to ExecutionState) bool {
	return transition(from, to, map[ExecutionState][]ExecutionState{ExecutionCreated: {ExecutionWaitingModel, ExecutionCancelRequested, ExecutionFailed, ExecutionRecoveryRequired}, ExecutionWaitingModel: {ExecutionActive, ExecutionFinalizing, ExecutionCancelRequested, ExecutionFailed, ExecutionUnknown, ExecutionRecoveryRequired}, ExecutionActive: {ExecutionWaitingModel, ExecutionFinalizing, ExecutionCancelRequested, ExecutionFailed, ExecutionUnknown, ExecutionRecoveryRequired}, ExecutionFinalizing: {ExecutionCompleted, ExecutionCancelConfirmed, ExecutionFailed, ExecutionUnknown, ExecutionRecoveryRequired}, ExecutionCancelRequested: {ExecutionCancelConfirmed, ExecutionFailed, ExecutionUnknown, ExecutionRecoveryRequired}, ExecutionUnknown: {ExecutionRecoveryRequired}, ExecutionRecoveryRequired: {ExecutionCompleted, ExecutionCancelConfirmed, ExecutionFailed}})
}
func admissionTransitionAllowed(from, to AdmissionState) bool {
	return transition(from, to, map[AdmissionState][]AdmissionState{AdmissionPending: {AdmissionAccepted, AdmissionRejected}})
}
func transportTransitionAllowed(from, to TransportState) bool {
	return transition(from, to, map[TransportState][]TransportState{TransportNotStarted: {TransportStarted, TransportFailed, TransportUnknown}, TransportStarted: {TransportHeaders, TransportFailed, TransportUnknown}, TransportHeaders: {TransportBodyCompleted, TransportCompleted, TransportFailed, TransportUnknown}, TransportBodyCompleted: {TransportCompleted, TransportFailed, TransportUnknown}})
}
func semanticTransitionAllowed(from, to SemanticState) bool {
	return transition(from, to, map[SemanticState][]SemanticState{SemanticNotEvaluated: {SemanticSucceeded, SemanticFailed, SemanticTruncated, SemanticUnknown}})
}
func cancellationTransitionAllowed(from, to CancellationState) bool {
	return transition(from, to, map[CancellationState][]CancellationState{CancelNone: {CancelRequested}, CancelRequested: {CancelInProgress, CancelConfirmed, CancelUnknown}, CancelInProgress: {CancelConfirmed, CancelUnknown}})
}
func fifoTransitionAllowed(from, to FIFOState) bool {
	return transition(from, to, map[FIFOState][]FIFOState{FIFOQueued: {FIFOPreparing, FIFOCancelled, FIFOExpired, FIFOUnknown}, FIFOPreparing: {FIFOActive, FIFOCancelled, FIFOFailed, FIFOUnknown}, FIFOActive: {FIFOCompleted, FIFOCancelled, FIFOExpired, FIFOFailed, FIFOUnknown}})
}
func activationTransitionAllowed(from, to ActivationState) bool {
	return transition(from, to, map[ActivationState][]ActivationState{ActivationInactive: {ActivationPreparing}, ActivationPreparing: {ActivationActive, ActivationInactive}, ActivationActive: {ActivationReleasing}, ActivationReleasing: {ActivationInactive}})
}
func releaseTransitionAllowed(from, to ReleaseState) bool {
	return transition(from, to, map[ReleaseState][]ReleaseState{ReleaseNotRequested: {ReleaseRequested, ReleaseUnknown}, ReleaseRequested: {ReleaseInProgress, ReleaseUnknown}, ReleaseInProgress: {ReleaseReleased, ReleaseUnknown}, ReleaseUnknown: {ReleaseInProgress, ReleaseReleased}})
}
func processTransitionAllowed(from, to ProcessState) bool {
	return transition(from, to, map[ProcessState][]ProcessState{ProcessPreparing: {ProcessStarting, ProcessFailed, ProcessIdentityUnknown, ProcessLost, ProcessRecoveryRequired}, ProcessStarting: {ProcessWaitingReadiness, ProcessFailed, ProcessIdentityUnknown, ProcessLost, ProcessRecoveryRequired}, ProcessWaitingReadiness: {ProcessReady, ProcessFailed, ProcessIdentityUnknown, ProcessLost, ProcessRecoveryRequired}, ProcessReady: {ProcessTerminationRequested, ProcessFailed, ProcessIdentityUnknown, ProcessLost, ProcessRecoveryRequired}, ProcessTerminationRequested: {ProcessTerminating, ProcessTerminationUnknown, ProcessIdentityUnknown, ProcessRecoveryRequired}, ProcessTerminating: {ProcessTerminatedVerified, ProcessTerminationUnknown, ProcessFailed, ProcessIdentityUnknown, ProcessRecoveryRequired}, ProcessTerminationUnknown: {ProcessRecoveryRequired}, ProcessLost: {ProcessRecoveryRequired}, ProcessIdentityUnknown: {ProcessRecoveryRequired}, ProcessRecoveryRequired: {ProcessTerminatedVerified, ProcessFailed}})
}
func transition[T comparable](from, to T, table map[T][]T) bool {
	for _, candidate := range table[from] {
		if candidate == to {
			return true
		}
	}
	return false
}
