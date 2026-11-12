//go:build darwin && cgo

package memory

import (
	"context"
	"os"
	"testing"
)

func TestDarwinProviderPhysicalAndUnifiedMemory(t *testing.T) {
	provider := darwinProvider{}
	system := provider.System(context.Background())
	if system.Status != Available || system.TotalBytes == nil || *system.TotalBytes == 0 {
		t.Fatalf("physical memory unavailable: %+v", system)
	}
	t.Logf("macOS memory: architecture=%s total=%d available=%d used=%d swap_used=%d status=%s source=%s", system.Architecture, *system.TotalBytes, valueOrZero(system.AvailableBytes), valueOrZero(system.UsedBytes), valueOrZero(system.SwapUsedBytes), system.Status, system.Source)
	if system.Architecture != "unified" && system.Architecture != "shared" {
		t.Fatalf("unexpected memory architecture: %s", system.Architecture)
	}
	identity, ok := darwinProcessIdentity(os.Getpid())
	if !ok {
		t.Fatal("current process identity unavailable")
	}
	process := provider.Process(context.Background(), ProcessRef{ModelID: "fixture", PID: os.Getpid(), ProcessInstanceID: "fixture-instance", StartIdentity: identity})
	if process.Status != Available || process.RSSBytes == nil || process.PrivateBytes == nil {
		t.Fatalf("process memory unavailable: %+v", process)
	}
}

func valueOrZero(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}
