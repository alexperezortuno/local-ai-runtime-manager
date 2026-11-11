//go:build linux

package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxProviderReadsMeminfoCgroupAndProcessIdentity(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, filepath.Join(root, "meminfo"), "MemTotal:       1048576 kB\nMemAvailable:    524288 kB\nSwapTotal:       8192 kB\nSwapFree:        2048 kB\n")
	writeFixture(t, filepath.Join(root, "sys/kernel/random/boot_id"), "boot-fixture\n")
	pid := filepath.Join(root, "123")
	writeFixture(t, filepath.Join(pid, "stat"), "123 (llama) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20\n")
	writeFixture(t, filepath.Join(pid, "status"), "Name:\tllama\nVmRSS:\t4096 kB\n")
	writeFixture(t, filepath.Join(pid, "smaps_rollup"), "Private_Clean:\t1024 kB\nPrivate_Dirty:\t2048 kB\n")
	writeFixture(t, filepath.Join(root, "cgroup/memory.max"), "1073741824\n")
	writeFixture(t, filepath.Join(root, "cgroup/memory.current"), "536870912\n")
	provider := &linuxProvider{procRoot: root, cgroupRoot: filepath.Join(root, "cgroup"), gpu: func(context.Context) GPUResult { return GPUResult{Status: Absent, Source: "fixture"} }}
	system := provider.System(context.Background())
	if *system.TotalBytes != 1024*1024*1024 || *system.AvailableBytes != 512*1024*1024 || *system.SwapUsedBytes != 6144*1024 {
		t.Fatalf("unexpected system memory: %+v", system)
	}
	if *system.CgroupLimitBytes != 1073741824 || *system.CgroupUsedBytes != 536870912 {
		t.Fatalf("unexpected cgroup memory: %+v", system)
	}
	identity := "linux:boot-fixture:20"
	process := provider.Process(context.Background(), ProcessRef{ModelID: "model", PID: 123, ProcessInstanceID: "instance", StartIdentity: identity, RuntimeState: "READY"})
	if process.Status != Available || *process.RSSBytes != 4096*1024 || *process.PrivateBytes != 3072*1024 {
		t.Fatalf("unexpected process memory: %+v", process)
	}
	process = provider.Process(context.Background(), ProcessRef{ModelID: "model", PID: 123, ProcessInstanceID: "old", StartIdentity: "linux:boot-fixture:19"})
	if process.Status != Unavailable || process.Reason != "identity_mismatch" || process.RSSBytes != nil {
		t.Fatalf("PID reuse was attributed: %+v", process)
	}
	process = provider.Process(context.Background(), ProcessRef{ModelID: "model", PID: 124, ProcessInstanceID: "gone", StartIdentity: identity})
	if process.Status != Unavailable || process.Reason != "process_ended_or_unreadable" || process.RSSBytes != nil {
		t.Fatalf("terminated process was attributed: %+v", process)
	}
}

func TestNVIDIAProviderFixtureStates(t *testing.T) {
	provider := &linuxProvider{gpu: func(context.Context) GPUResult {
		total, used, free := uint64(8<<30), uint64(2<<30), uint64(6<<30)
		return GPUResult{Status: Available, Source: "fixture:nvml", GPUs: []GPUMemory{{ID: "0", Name: "fixture", TotalBytes: &total, UsedBytes: &used, FreeBytes: &free, Status: Available, Source: "fixture:nvml"}}}
	}}
	result := provider.GPUs(context.Background())
	if result.Status != Available || len(result.GPUs) != 1 || *result.GPUs[0].FreeBytes != 6<<30 {
		t.Fatalf("unexpected GPU fixture: %+v", result)
	}
}

func writeFixture(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
