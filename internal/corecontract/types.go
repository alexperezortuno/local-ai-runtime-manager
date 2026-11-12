package corecontract

import "time"

type ExecutionState string

const (
	ExecutionCreated          ExecutionState = "CREATED"
	ExecutionWaitingModel     ExecutionState = "WAITING_MODEL"
	ExecutionActive           ExecutionState = "ACTIVE"
	ExecutionFinalizing       ExecutionState = "FINALIZING"
	ExecutionCompleted        ExecutionState = "COMPLETED"
	ExecutionCancelRequested  ExecutionState = "CANCEL_REQUESTED"
	ExecutionCancelConfirmed  ExecutionState = "CANCEL_CONFIRMED"
	ExecutionFailed           ExecutionState = "FAILED"
	ExecutionUnknown          ExecutionState = "UNKNOWN"
	ExecutionRecoveryRequired ExecutionState = "RECOVERY_REQUIRED"
)

type ExecutionLifetime string

const (
	ExecutionPersistent ExecutionLifetime = "PERSISTENT"
	ExecutionEphemeral  ExecutionLifetime = "EPHEMERAL"
)

type CompletionResult string

const (
	CompletionSucceeded CompletionResult = "SUCCEEDED"
	CompletionCancelled CompletionResult = "CANCELLED"
	CompletionFailed    CompletionResult = "FAILED"
)

type Completion struct {
	Result      CompletionResult `json:"result"`
	EvidenceIDs []string         `json:"evidence_ids,omitempty"`
	At          time.Time        `json:"at"`
}

type Execution struct {
	SchemaVersion     string            `json:"schema_version"`
	ExecutionID       string            `json:"execution_id"`
	ParentExecutionID string            `json:"parent_execution_id,omitempty"`
	Initiator         string            `json:"initiator"`
	Lifetime          ExecutionLifetime `json:"lifetime"`
	ModelID           string            `json:"model_id"`
	State             ExecutionState    `json:"state"`
	LeaseIDs          []string          `json:"lease_ids,omitempty"`
	InferenceIDs      []string          `json:"inference_ids,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	Completion        *Completion       `json:"completion,omitempty"`
	RecoveryState     RecoveryState     `json:"recovery_state"`
}

type AdmissionState string

const (
	AdmissionPending  AdmissionState = "PENDING"
	AdmissionAccepted AdmissionState = "ACCEPTED"
	AdmissionRejected AdmissionState = "REJECTED"
)

type TransportState string

const (
	TransportNotStarted    TransportState = "NOT_STARTED"
	TransportStarted       TransportState = "STARTED"
	TransportHeaders       TransportState = "HEADERS_RECEIVED"
	TransportBodyCompleted TransportState = "BODY_COMPLETED"
	TransportCompleted     TransportState = "COMPLETED"
	TransportFailed        TransportState = "FAILED"
	TransportUnknown       TransportState = "UNKNOWN"
)

type SemanticState string

const (
	SemanticNotEvaluated SemanticState = "NOT_EVALUATED"
	SemanticSucceeded    SemanticState = "SUCCEEDED"
	SemanticFailed       SemanticState = "FAILED"
	SemanticTruncated    SemanticState = "TRUNCATED"
	SemanticUnknown      SemanticState = "UNKNOWN"
)

type ErrorClass string

const (
	ErrorNone             ErrorClass = "NONE"
	ErrorTransport        ErrorClass = "TRANSPORT"
	ErrorBackend          ErrorClass = "BACKEND"
	ErrorSSETruncated     ErrorClass = "SSE_TRUNCATED"
	ErrorClientDisconnect ErrorClass = "CLIENT_DISCONNECT"
	ErrorCancellation     ErrorClass = "CANCELLATION"
	ErrorUnknown          ErrorClass = "UNKNOWN"
)

type SSEEventType string

const (
	SSEHeadersReceivedEvent    SSEEventType = "HEADERS_RECEIVED"
	SSEFirstChunkEvent         SSEEventType = "FIRST_CHUNK"
	SSEChunkEvent              SSEEventType = "CHUNK"
	SSEDoneDetectedEvent       SSEEventType = "DONE_DETECTED"
	SSEEOFEvent                SSEEventType = "EOF"
	SSEReadErrorEvent          SSEEventType = "READ_ERROR"
	SSEClientDisconnectedEvent SSEEventType = "CLIENT_DISCONNECTED"
	SSEBackendErrorEvent       SSEEventType = "BACKEND_ERROR"
	SSETruncatedEvent          SSEEventType = "STREAM_TRUNCATED"
)

type CancellationState string

const (
	CancelNone       CancellationState = "NONE"
	CancelRequested  CancellationState = "CANCEL_REQUESTED"
	CancelInProgress CancellationState = "CANCEL_IN_PROGRESS"
	CancelConfirmed  CancellationState = "CANCEL_CONFIRMED"
	CancelUnknown    CancellationState = "CANCEL_UNKNOWN"
)

type ProcessAssignment string

const (
	ProcessPending    ProcessAssignment = "PENDING"
	ProcessUnverified ProcessAssignment = "UNVERIFIED"
	ProcessAssigned   ProcessAssignment = "ASSIGNED"
)

type SSEObservation struct {
	HeadersReceived    bool   `json:"headers_received"`
	FirstChunk         bool   `json:"first_chunk"`
	ChunkCount         uint64 `json:"chunk_count"`
	DoneDetected       bool   `json:"done_detected"`
	EOF                bool   `json:"eof"`
	ReadError          bool   `json:"read_error"`
	ClientDisconnected bool   `json:"client_disconnected"`
	BackendError       bool   `json:"backend_error"`
	Truncated          bool   `json:"truncated"`
}

type Inference struct {
	SchemaVersion       string            `json:"schema_version"`
	RequestID           string            `json:"request_id"`
	ExecutionID         string            `json:"execution_id"`
	LeaseID             string            `json:"lease_id"`
	ModelID             string            `json:"model_id"`
	ProcessInstanceID   string            `json:"process_instance_id,omitempty"`
	ProcessAssignment   ProcessAssignment `json:"process_assignment"`
	Stream              bool              `json:"stream"`
	Admission           AdmissionState    `json:"admission_state"`
	Transport           TransportState    `json:"transport_state"`
	Semantic            SemanticState     `json:"semantic_state"`
	Cancellation        CancellationState `json:"cancellation_state"`
	SSE                 *SSEObservation   `json:"sse,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	ErrorClass          ErrorClass        `json:"error_class,omitempty"`
	TerminalEvidenceIDs []string          `json:"terminal_evidence_ids,omitempty"`
}

type FIFOState string

const (
	FIFOQueued    FIFOState = "QUEUED"
	FIFOPreparing FIFOState = "PREPARING"
	FIFOActive    FIFOState = "ACTIVE"
	FIFOCompleted FIFOState = "COMPLETED"
	FIFOFailed    FIFOState = "FAILED"
	FIFOCancelled FIFOState = "CANCELLED"
	FIFOExpired   FIFOState = "EXPIRED"
	FIFOUnknown   FIFOState = "UNKNOWN"
)

type ActivationState string

const (
	ActivationInactive  ActivationState = "INACTIVE"
	ActivationPreparing ActivationState = "PREPARING"
	ActivationActive    ActivationState = "ACTIVE"
	ActivationReleasing ActivationState = "RELEASING"
)

type ReleaseState string

const (
	ReleaseNotRequested ReleaseState = "NOT_REQUESTED"
	ReleaseRequested    ReleaseState = "REQUESTED"
	ReleaseInProgress   ReleaseState = "IN_PROGRESS"
	ReleaseReleased     ReleaseState = "RELEASED"
	ReleaseUnknown      ReleaseState = "UNKNOWN"
)

type RecoveryState string

const (
	RecoveryNormal   RecoveryState = "NORMAL"
	RecoveryRequired RecoveryState = "RECOVERY_REQUIRED"
	RecoveryResolved RecoveryState = "RESOLVED"
)

type LeaseOwnership string

const (
	LeasePersistentOwnership LeaseOwnership = "PERSISTENT_EXECUTION"
	LeaseEphemeralOwnership  LeaseOwnership = "EPHEMERAL_EXECUTION"
)

type Lease struct {
	SchemaVersion string          `json:"schema_version"`
	LeaseID       string          `json:"lease_id"`
	ExecutionID   string          `json:"execution_id"`
	Ownership     LeaseOwnership  `json:"ownership"`
	ModelID       string          `json:"model_id"`
	FIFO          FIFOState       `json:"fifo_state"`
	Activation    ActivationState `json:"activation_state"`
	InferenceIDs  []string        `json:"inference_ids,omitempty"`
	ExpiresAt     *time.Time      `json:"expires_at,omitempty"`
	HeartbeatAt   *time.Time      `json:"heartbeat_at,omitempty"`
	Release       ReleaseState    `json:"release_state"`
	RecoveryState RecoveryState   `json:"recovery_state"`
}

type ProcessState string

const (
	ProcessPreparing            ProcessState = "PREPARING"
	ProcessStarting             ProcessState = "STARTING"
	ProcessWaitingReadiness     ProcessState = "WAITING_READINESS"
	ProcessReady                ProcessState = "READY"
	ProcessTerminationRequested ProcessState = "TERMINATION_REQUESTED"
	ProcessTerminating          ProcessState = "TERMINATING"
	ProcessTerminatedVerified   ProcessState = "TERMINATED_VERIFIED"
	ProcessTerminationUnknown   ProcessState = "TERMINATION_UNKNOWN"
	ProcessLost                 ProcessState = "PROCESS_LOST"
	ProcessFailed               ProcessState = "FAILED"
	ProcessIdentityUnknown      ProcessState = "IDENTITY_UNKNOWN"
	ProcessRecoveryRequired     ProcessState = "RECOVERY_REQUIRED"
)

// StartupDiagnosticCategory is metadata only; it does not change the public
// process or scheduler state machine.
type StartupDiagnosticCategory string

const (
	StartupDiagnosticSpawnError         StartupDiagnosticCategory = "SPAWN_ERROR"
	StartupDiagnosticStartupCancelled   StartupDiagnosticCategory = "STARTUP_CANCELLED"
	StartupDiagnosticReadinessTimeout   StartupDiagnosticCategory = "READINESS_TIMEOUT"
	StartupDiagnosticBackendExited      StartupDiagnosticCategory = "BACKEND_EXITED"
	StartupDiagnosticIdentityUnverified StartupDiagnosticCategory = "IDENTITY_UNVERIFIED"
	StartupDiagnosticUnknown            StartupDiagnosticCategory = "UNKNOWN"
)

type ProcessInstance struct {
	SchemaVersion             string                    `json:"schema_version"`
	ProcessInstanceID         string                    `json:"process_instance_id"`
	OwnerInstanceID           string                    `json:"owner_instance_id"`
	LARMInstanceID            string                    `json:"larm_instance_id,omitempty"`
	ModelID                   string                    `json:"model_id"`
	PID                       int                       `json:"pid,omitempty"`
	PGID                      int                       `json:"pgid,omitempty"`
	Executable                string                    `json:"executable,omitempty"`
	StartIdentity             string                    `json:"start_identity,omitempty"`
	OSProcessStartIdentity    string                    `json:"os_process_start_identity,omitempty"`
	StartedAt                 time.Time                 `json:"started_at,omitempty"`
	TerminatedAt              time.Time                 `json:"terminated_at,omitempty"`
	ConfigFingerprint         string                    `json:"config_fingerprint,omitempty"`
	State                     ProcessState              `json:"state"`
	ProcessIdentityStatus     string                    `json:"process_identity_status,omitempty"`
	OwnershipStatus           string                    `json:"ownership_status,omitempty"`
	TerminationStatus         string                    `json:"termination_status,omitempty"`
	TerminationSignals        []string                  `json:"termination_signals,omitempty"`
	SignalRequestedByLARM     bool                      `json:"signal_requested_by_larm,omitempty"`
	SignalRequestResult       string                    `json:"signal_request_result,omitempty"`
	StartupDiagnostic         StartupDiagnosticCategory `json:"startup_diagnostic,omitempty"`
	StartupCancellationReason string                    `json:"startup_cancellation_reason,omitempty"`
	StartupError              string                    `json:"startup_error,omitempty"`
	WaitError                 string                    `json:"wait_error,omitempty"`
	ExitCode                  *int                      `json:"exit_code,omitempty"`
	TerminationSignal         string                    `json:"termination_signal,omitempty"`
	DescendantStatus          string                    `json:"descendant_status,omitempty"`
	ReadinessEvidenceID       string                    `json:"readiness_evidence_id,omitempty"`
	InferenceIDs              []string                  `json:"inference_ids,omitempty"`
	Evidence                  []TerminalEvidence        `json:"evidence,omitempty"`
	TerminationEvidenceID     string                    `json:"termination_evidence_id,omitempty"`
	UpdatedAt                 time.Time                 `json:"updated_at,omitempty"`
	RecoveryState             RecoveryState             `json:"recovery_state"`
}

type ProcessReconciliationRequest struct {
	OperationKey      string
	ProcessInstanceID string
	OwnerInstanceID   string
}

type SubjectKind string

const (
	SubjectRequest   SubjectKind = "REQUEST"
	SubjectProcess   SubjectKind = "PROCESS_INSTANCE"
	SubjectLease     SubjectKind = "LEASE"
	SubjectExecution SubjectKind = "EXECUTION"
)

type EvidenceType string

const (
	EvidenceHTTPCompleted              EvidenceType = "HTTP_COMPLETED"
	EvidenceSSECompleted               EvidenceType = "SSE_COMPLETED"
	EvidenceSSETruncated               EvidenceType = "SSE_TRUNCATED"
	EvidenceTransportError             EvidenceType = "TRANSPORT_ERROR"
	EvidenceBackendError               EvidenceType = "BACKEND_ERROR"
	EvidenceCancellationConfirmed      EvidenceType = "CANCELLATION_CONFIRMED"
	EvidenceProcessTerminationVerified EvidenceType = "PROCESS_TERMINATION_VERIFIED"
	EvidenceProcessReady               EvidenceType = "PROCESS_READY"
	EvidenceExecutionCompleted         EvidenceType = "EXECUTION_COMPLETED"
	EvidenceUnknown                    EvidenceType = "UNKNOWN"
)

type EvidenceResult string

const (
	EvidenceSuccess   EvidenceResult = "SUCCESS"
	EvidenceFailure   EvidenceResult = "FAILURE"
	EvidenceAmbiguous EvidenceResult = "AMBIGUOUS"
)

type GuaranteeScope string

const (
	GuaranteeTransport    GuaranteeScope = "TRANSPORT"
	GuaranteeSemantic     GuaranteeScope = "SEMANTIC"
	GuaranteeCancellation GuaranteeScope = "CANCELLATION"
	GuaranteeProcess      GuaranteeScope = "PROCESS_TERMINATION"
	GuaranteeRecovery     GuaranteeScope = "RECOVERY"
)

type TerminalEvidence struct {
	SchemaVersion           string         `json:"schema_version"`
	EvidenceID              string         `json:"evidence_id"`
	SubjectKind             SubjectKind    `json:"subject_kind"`
	SubjectID               string         `json:"subject_id"`
	RequestID               string         `json:"request_id,omitempty"`
	ProcessInstanceID       string         `json:"process_instance_id,omitempty"`
	Source                  string         `json:"source"`
	Type                    EvidenceType   `json:"type"`
	ObservedAt              time.Time      `json:"observed_at"`
	Result                  EvidenceResult `json:"result"`
	CorrelationID           string         `json:"correlation_id"`
	Guarantee               GuaranteeScope `json:"guarantee"`
	HTTPStatus              int            `json:"http_status,omitempty"`
	SSEDone                 bool           `json:"sse_done,omitempty"`
	EOF                     bool           `json:"eof,omitempty"`
	TransportTerminated     bool           `json:"transport_terminated,omitempty"`
	ProcessAbsent           bool           `json:"process_absent,omitempty"`
	ProcessIdentityVerified bool           `json:"process_identity_verified,omitempty"`
}

func NewEphemeralExecution(executionID, modelID string, now time.Time) Execution {
	return Execution{SchemaVersion: ContractVersion, ExecutionID: executionID, Initiator: "openai-compatible", Lifetime: ExecutionEphemeral, ModelID: modelID, State: ExecutionCreated, CreatedAt: now, UpdatedAt: now, RecoveryState: RecoveryNormal}
}

func (i Inference) IsAmbiguous() bool {
	return i.Transport == TransportUnknown || i.Semantic == SemanticUnknown || i.Cancellation == CancelUnknown
}

func (i Inference) IsTerminal() bool {
	if i.Admission == AdmissionRejected || i.Cancellation == CancelConfirmed {
		return true
	}
	return !i.IsAmbiguous() && (i.Transport == TransportCompleted || i.Transport == TransportFailed) && (i.Semantic == SemanticSucceeded || i.Semantic == SemanticFailed || i.Semantic == SemanticTruncated)
}
