package scheduler

import "time"

type LeaseState string

const (
	Queued    LeaseState = "QUEUED"
	Preparing LeaseState = "PREPARING"
	Active    LeaseState = "ACTIVE"
	Completed LeaseState = "COMPLETED"
	Failed    LeaseState = "FAILED"
	Cancelled LeaseState = "CANCELLED"
	Expired   LeaseState = "EXPIRED"
	// RecoveryRequired is an idempotency response state, not a scheduler transition.
	RecoveryRequired LeaseState = "RECOVERY_REQUIRED"
)

type Lease struct {
	ID               string     `json:"lease_id"`
	Model            string     `json:"model"`
	State            LeaseState `json:"state"`
	CreatedAt        time.Time  `json:"created_at"`
	ActivatedAt      *time.Time `json:"activated_at,omitempty"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	Error            string     `json:"error,omitempty"`
	Protected        bool       `json:"protected,omitempty"`
	ProtectionReason string     `json:"protection_reason,omitempty"`
}

type LeaseStatus struct {
	Lease
	QueuePosition int `json:"queue_position"`
}

type Snapshot struct {
	Active        *Lease     `json:"active,omitempty"`
	Pending       []Lease    `json:"pending"`
	Preparing     *Lease     `json:"preparing,omitempty"`
	Residents     []Resident `json:"residents,omitempty"`
	CapacityUsed  int        `json:"capacity_used"`
	CapacityLimit int        `json:"capacity_limit"`
}

type Resident struct {
	Model              string `json:"model"`
	State              string `json:"state"`
	Residency          string `json:"residency"`
	KeepAlive          bool   `json:"keep_alive"`
	TimeAlive          string `json:"time_alive"`
	ProcessInstanceID  string `json:"process_instance_id,omitempty"`
	PID                int    `json:"pid,omitempty"`
	ActiveLeases       int    `json:"active_leases"`
	ProtectedInference bool   `json:"protected_inference"`
	ResidenceRemaining string `json:"residence_remaining,omitempty"`
	CapacityOccupied   bool   `json:"capacity_occupied"`
	RecoveryState      string `json:"recovery_state,omitempty"`
	OwnershipStatus    string `json:"ownership_status,omitempty"`
	PreloadState       string `json:"preload_state,omitempty"`
	PreloadError       string `json:"preload_error,omitempty"`
}

// RecoveryRecord is the metadata retained by the LARM-005 idempotency store.
// It is intentionally read-only: recovery never recreates a lease from it.
type RecoveryRecord struct {
	Key         string    `json:"key"`
	Fingerprint string    `json:"fingerprint"`
	Lease       Lease     `json:"lease"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type RecoveryDiagnostics struct {
	StorePath       string           `json:"store_path,omitempty"`
	CleanShutdown   bool             `json:"clean_shutdown"`
	RecoveryBlocked bool             `json:"recovery_blocked"`
	Records         []RecoveryRecord `json:"records"`
}

func cloneLease(l Lease) Lease {
	if l.ActivatedAt != nil {
		t := *l.ActivatedAt
		l.ActivatedAt = &t
	}
	if l.ExpiresAt != nil {
		t := *l.ExpiresAt
		l.ExpiresAt = &t
	}
	return l
}
