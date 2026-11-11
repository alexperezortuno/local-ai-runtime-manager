//go:build !linux && !darwin

package memory

import (
	"context"
	"runtime"
	"time"
)

type unavailableProvider struct{}

func newPlatformProvider() Provider { return unavailableProvider{} }

func (unavailableProvider) System(context.Context) SystemMemory {
	now := time.Now().UTC()
	return SystemMemory{Architecture: runtime.GOARCH, MeasuredAt: &now, Status: Unavailable, Source: "unsupported-platform"}
}

func (unavailableProvider) Process(_ context.Context, ref ProcessRef) ProcessMemory {
	now := time.Now().UTC()
	return ProcessMemory{ModelID: ref.ModelID, PID: ref.PID, ProcessInstanceID: ref.ProcessInstanceID, RuntimeState: ref.RuntimeState, MeasuredAt: &now, Status: Unavailable, Source: "unsupported-platform", GPUStatus: Unavailable, Reason: "unsupported_platform"}
}

func (unavailableProvider) GPUs(context.Context) GPUResult {
	return GPUResult{Status: Absent, Source: "unsupported-platform"}
}
