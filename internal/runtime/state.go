package runtime

type State string

const (
	Stopped          State = "STOPPED"
	Starting         State = "STARTING"
	Ready            State = "READY"
	Stopping         State = "STOPPING"
	Failed           State = "FAILED"
	RecoveryRequired State = "RECOVERY_REQUIRED"
)

type Status struct {
	Runtime               string `json:"runtime"`
	State                 State  `json:"state"`
	ActiveModel           string `json:"active_model"`
	PID                   int    `json:"pid"`
	PGID                  int    `json:"pgid,omitempty"`
	ProcessInstanceID     string `json:"process_instance_id,omitempty"`
	ProcessIdentityStatus string `json:"process_identity_status,omitempty"`
	OwnershipStatus       string `json:"ownership_status,omitempty"`
	TerminationStatus     string `json:"termination_status,omitempty"`
	DescendantStatus      string `json:"descendant_status,omitempty"`
	RecoveryRequired      bool   `json:"recovery_required,omitempty"`
	RecoveryReason        string `json:"recovery_reason,omitempty"`
	UptimeSeconds         int64  `json:"uptime_seconds"`
}
