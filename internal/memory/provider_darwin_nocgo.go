//go:build darwin && !cgo

package memory

import (
	"context"
	"runtime"
	"time"
)

type unavailableDarwinProvider struct{}

func newPlatformProvider() Provider { return unavailableDarwinProvider{} }

func (unavailableDarwinProvider) System(context.Context) SystemMemory {
	now := time.Now().UTC()
	return SystemMemory{Architecture: runtime.GOARCH, MeasuredAt: &now, Status: Unavailable, Source: "macOS:cgo-unavailable"}
}

func (unavailableDarwinProvider) Process(_ context.Context, ref ProcessRef) ProcessMemory {
	now := time.Now().UTC()
	return ProcessMemory{ModelID: ref.ModelID, PID: ref.PID, ProcessInstanceID: ref.ProcessInstanceID, RuntimeState: ref.RuntimeState, MeasuredAt: &now, Status: Unavailable, Source: "macOS:cgo-unavailable", GPUStatus: Unavailable, Reason: "cgo_unavailable"}
}

func (unavailableDarwinProvider) GPUs(context.Context) GPUResult {
	return GPUResult{Status: Unavailable, Source: "apple-unified-memory", Error: "cgo unavailable"}
}
