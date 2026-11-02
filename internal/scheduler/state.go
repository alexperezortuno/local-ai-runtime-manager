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
)

type Lease struct {
	ID          string     `json:"lease_id"`
	Model       string     `json:"model"`
	State       LeaseState `json:"state"`
	CreatedAt   time.Time  `json:"created_at"`
	ActivatedAt *time.Time `json:"activated_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type LeaseStatus struct {
	Lease
	QueuePosition int `json:"queue_position"`
}

type Snapshot struct {
	Active    *Lease  `json:"active,omitempty"`
	Pending   []Lease `json:"pending"`
	Preparing *Lease  `json:"preparing,omitempty"`
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
