package runtime

type State string

const (
	Stopped  State = "STOPPED"
	Starting State = "STARTING"
	Ready    State = "READY"
	Stopping State = "STOPPING"
	Failed   State = "FAILED"
)

type Status struct {
	Runtime       string `json:"runtime"`
	State         State  `json:"state"`
	ActiveModel   string `json:"active_model"`
	PID           int    `json:"pid"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}
