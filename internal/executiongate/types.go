package executiongate

import (
	"errors"
	"time"
)

var (
	ErrBusy             = errors.New("session is owned by another execution")
	ErrCancelled        = errors.New("operation cancelled")
	ErrConflict         = errors.New("identity or state conflict")
	ErrNotFound         = errors.New("execution or request not found")
	ErrActive           = errors.New("execution still has active requests")
	ErrNotOwner         = errors.New("execution does not own session")
	ErrNotTerminal      = errors.New("execution has no verified terminal state")
	ErrRecoveryRequired = errors.New("execution state is unknown and requires explicit recovery")
)

type ExecutionState string

const (
	ExecutionCreated            ExecutionState = "CREATED"
	ExecutionActive             ExecutionState = "ACTIVE"
	ExecutionFinalizing         ExecutionState = "FINALIZING"
	ExecutionCompleted          ExecutionState = "COMPLETED"
	ExecutionCancelled          ExecutionState = "CANCELLED"
	ExecutionFailed             ExecutionState = "FAILED"
	ExecutionClientDisconnected ExecutionState = "CLIENT_DISCONNECTED"
	ExecutionUnknown            ExecutionState = "UNKNOWN"
	ExecutionDeleted            ExecutionState = "DELETED"
)

type RequestState string

const (
	RequestCreated   RequestState = "CREATED"
	RequestInFlight  RequestState = "IN_FLIGHT"
	RequestCompleted RequestState = "COMPLETED"
	RequestCancelled RequestState = "CANCELLED"
	RequestFailed    RequestState = "FAILED"
)

type AcquireResult string

const (
	Acquired     AcquireResult = "ACQUIRED"
	Busy         AcquireResult = "BUSY"
	Cancelled    AcquireResult = "CANCELLED"
	AcquireError AcquireResult = "ERROR"
)

type Execution struct {
	ExecutionID       string         `json:"execution_id"`
	ParentExecutionID string         `json:"parent_execution_id,omitempty"`
	SessionID         string         `json:"session_id,omitempty"`
	ServerID          string         `json:"server_id,omitempty"`
	ModelID           string         `json:"model_id,omitempty"`
	IdempotencyKey    string         `json:"idempotency_key,omitempty"`
	LeaseID           string         `json:"lease_id,omitempty"`
	LeaseState        string         `json:"lease_state,omitempty"`
	Initiator         string         `json:"initiator"`
	State             ExecutionState `json:"state"`
	ReleaseReason     string         `json:"release_reason,omitempty"`
	TerminalState     ExecutionState `json:"terminal_state,omitempty"`
	CreatedAt         time.Time      `json:"created_at"`
	TerminalAt        *time.Time     `json:"terminal_at,omitempty"`
	DeletedAt         *time.Time     `json:"deleted_at,omitempty"`
	RequestIDs        []string       `json:"request_ids,omitempty"`
}

type Request struct {
	RequestID       string       `json:"request_id"`
	ExecutionID     string       `json:"execution_id"`
	SessionID       string       `json:"session_id"`
	ModelID         string       `json:"model_id"`
	LeaseID         string       `json:"lease_id"`
	Stream          bool         `json:"stream"`
	State           RequestState `json:"request_state"`
	HeadersReceived bool         `json:"headers_received"`
	HTTPStatus      int          `json:"http_status,omitempty"`
	BodyStarted     bool         `json:"body_started"`
	SSEDone         bool         `json:"sse_done"`
	EOF             bool         `json:"eof"`
	TransportError  bool         `json:"transport_error"`
	CancelConfirmed bool         `json:"cancel_confirmed"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

type Counts struct {
	ActiveExecutions   int `json:"active_executions"`
	TerminalExecutions int `json:"terminal_executions"`
	PendingCleanup     int `json:"pending_cleanup"`
	DeletedExecutions  int `json:"deleted_executions"`
	ActiveRequests     int `json:"active_requests"`
	OwnedSessions      int `json:"owned_sessions"`
}

type Snapshot struct {
	Executions map[string]Execution `json:"executions"`
	Requests   map[string]Request   `json:"requests"`
	Owners     map[string]string    `json:"owners"`
	Counts     Counts               `json:"counts"`
}

type CreateExecutionInput struct {
	ExecutionID       string `json:"execution_id"`
	ParentExecutionID string `json:"parent_execution_id,omitempty"`
	Initiator         string `json:"initiator"`
}

type BindLeaseInput struct {
	ModelID        string `json:"model_id"`
	IdempotencyKey string `json:"idempotency_key"`
	LeaseID        string `json:"lease_id"`
	LeaseState     string `json:"lease_state"`
}

type LeaseIntentInput struct {
	ModelID        string `json:"model_id"`
	IdempotencyKey string `json:"idempotency_key"`
}
