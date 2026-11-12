package memory

import (
	"context"
	"time"
)

// Availability is deliberately separate from numeric values: nil byte values
// mean unknown, never zero.
type Availability string

const (
	Available        Availability = "AVAILABLE"
	Partial          Availability = "PARTIAL"
	Unavailable      Availability = "UNAVAILABLE"
	Stale            Availability = "STALE"
	Absent           Availability = "ABSENT"
	PermissionDenied Availability = "PERMISSION_DENIED"
)

type ProcessRef struct {
	ModelID           string
	PID               int
	ProcessInstanceID string
	StartIdentity     string
	RuntimeState      string
	OwnershipStatus   string
}

type SystemMemory struct {
	Architecture     string       `json:"architecture"`
	TotalBytes       *uint64      `json:"total_bytes"`
	AvailableBytes   *uint64      `json:"available_bytes"`
	UsedBytes        *uint64      `json:"used_bytes"`
	SwapTotalBytes   *uint64      `json:"swap_total_bytes"`
	SwapUsedBytes    *uint64      `json:"swap_used_bytes"`
	MeasuredAt       *time.Time   `json:"measured_at"`
	Status           Availability `json:"status"`
	Source           string       `json:"source"`
	CgroupLimitBytes *uint64      `json:"cgroup_limit_bytes"`
	CgroupUsedBytes  *uint64      `json:"cgroup_used_bytes"`
	CgroupStatus     Availability `json:"cgroup_status"`
	CgroupSource     string       `json:"cgroup_source"`
}

type GPUMemory struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	TotalBytes *uint64      `json:"total_bytes"`
	UsedBytes  *uint64      `json:"used_bytes"`
	FreeBytes  *uint64      `json:"free_bytes"`
	MeasuredAt *time.Time   `json:"measured_at"`
	Source     string       `json:"source"`
	Status     Availability `json:"status"`
}

type GPUProvider struct {
	Status Availability `json:"status"`
	Source string       `json:"source"`
	Error  string       `json:"error,omitempty"`
}

type ProcessMemory struct {
	ModelID           string       `json:"model_id"`
	PID               int          `json:"pid"`
	ProcessInstanceID string       `json:"process_instance_id"`
	RuntimeState      string       `json:"runtime_state"`
	RSSBytes          *uint64      `json:"rss_bytes"`
	PrivateBytes      *uint64      `json:"private_bytes"`
	GPUBytes          *uint64      `json:"gpu_bytes"`
	MeasuredAt        *time.Time   `json:"measured_at"`
	Source            string       `json:"source"`
	GPUStatus         Availability `json:"gpu_status"`
	Status            Availability `json:"status"`
	Reason            string       `json:"reason,omitempty"`
}

type Snapshot struct {
	System      SystemMemory    `json:"system"`
	GPUs        []GPUMemory     `json:"gpus"`
	GPUProvider GPUProvider     `json:"gpu_provider"`
	Processes   []ProcessMemory `json:"processes"`
	MeasuredAt  *time.Time      `json:"measured_at"`
	Status      Availability    `json:"status"`
	Source      string          `json:"source"`
}

type GPUResult struct {
	GPUs   []GPUMemory
	Status Availability
	Source string
	Error  string
}

type Provider interface {
	System(context.Context) SystemMemory
	Process(context.Context, ProcessRef) ProcessMemory
	GPUs(context.Context) GPUResult
}

func ptr(value uint64) *uint64 { return &value }
