//go:build linux

package memory

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type linuxProvider struct {
	procRoot   string
	cgroupRoot string
	gpu        func(context.Context) GPUResult
}

func newPlatformProvider() Provider {
	return &linuxProvider{procRoot: "/proc", cgroupRoot: "/sys/fs/cgroup", gpu: queryNVIDIA}
}

func (p *linuxProvider) System(_ context.Context) SystemMemory {
	now := time.Now().UTC()
	result := SystemMemory{Architecture: "host", MeasuredAt: &now, Status: Available, Source: "linux:/proc/meminfo", CgroupStatus: Unavailable, CgroupSource: "linux:cgroup"}
	values, err := readMeminfo(filepath.Join(p.procRoot, "meminfo"))
	if err != nil {
		result.Status, result.Source = Unavailable, "linux:/proc/meminfo"
		return result
	}
	total, totalOK := values["MemTotal"]
	available, availableOK := values["MemAvailable"]
	if !availableOK {
		available, availableOK = values["MemFree"]+values["Buffers"]+values["Cached"], true
	}
	if totalOK {
		result.TotalBytes = ptr(total)
	}
	if availableOK && totalOK && available <= total {
		result.AvailableBytes, result.UsedBytes = ptr(available), ptr(total-available)
	} else {
		result.Status = Partial
	}
	if swapTotal, ok := values["SwapTotal"]; ok {
		result.SwapTotalBytes = ptr(swapTotal)
		if swapFree, freeOK := values["SwapFree"]; freeOK && swapFree <= swapTotal {
			result.SwapUsedBytes = ptr(swapTotal - swapFree)
		}
	}
	result.CgroupLimitBytes, result.CgroupUsedBytes, result.CgroupStatus = readCgroup(p.cgroupRoot)
	if result.CgroupLimitBytes == nil {
		result.CgroupStatus = Absent
	}
	return result
}

func (p *linuxProvider) Process(ctx context.Context, ref ProcessRef) ProcessMemory {
	now := time.Now().UTC()
	result := ProcessMemory{ModelID: ref.ModelID, PID: ref.PID, ProcessInstanceID: ref.ProcessInstanceID, RuntimeState: ref.RuntimeState, MeasuredAt: &now, Source: "linux:/proc", GPUStatus: Unavailable, Status: Unavailable}
	if ref.PID <= 0 || ctx.Err() != nil {
		result.Reason = "invalid_or_cancelled_process"
		return result
	}
	identity, err := linuxStartIdentity(p.procRoot, ref.PID)
	if err != nil {
		result.Reason = "process_ended_or_unreadable"
		return result
	}
	if ref.StartIdentity == "" || identity != ref.StartIdentity {
		result.Reason = "identity_mismatch"
		return result
	}
	status, err := os.ReadFile(filepath.Join(p.procRoot, strconv.Itoa(ref.PID), "status"))
	if err != nil {
		result.Reason = "process_ended_or_unreadable"
		return result
	}
	if rss, ok := statusBytes(string(status), "VmRSS:"); ok {
		result.RSSBytes = ptr(rss)
	}
	if private, ok := readPrivateBytes(filepath.Join(p.procRoot, strconv.Itoa(ref.PID), "smaps_rollup")); ok {
		result.PrivateBytes = ptr(private)
	}
	if current, err := linuxStartIdentity(p.procRoot, ref.PID); err != nil || current != identity {
		result.RSSBytes, result.PrivateBytes, result.Reason = nil, nil, "identity_changed_during_measurement"
		return result
	}
	if result.RSSBytes == nil {
		result.Reason = "rss_unavailable"
		return result
	}
	result.Status = Available
	return result
}

func (p *linuxProvider) GPUs(ctx context.Context) GPUResult {
	return p.gpu(ctx)
}

func readMeminfo(path string) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make(map[string]uint64)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) > 1 && fields[1] == "kB" {
			n *= 1024
		}
		values[strings.TrimSpace(key)] = n
	}
	return values, scanner.Err()
}

func statusBytes(data, key string) (uint64, bool) {
	for _, line := range strings.Split(data, "\n") {
		if !strings.HasPrefix(line, key) {
			continue
		}
		fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, key)))
		if len(fields) == 0 {
			return 0, false
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		if len(fields) > 1 && fields[1] == "kB" {
			n *= 1024
		}
		return n, true
	}
	return 0, false
}

func readPrivateBytes(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var total uint64
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		for _, key := range []string{"Private_Clean:", "Private_Dirty:"} {
			if value, ok := statusBytes(line, key); ok {
				total += value
				found = true
			}
		}
	}
	return total, found
}

func linuxStartIdentity(root string, pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", err
	}
	_, rest, ok := strings.Cut(string(data), "(")
	if !ok {
		return "", fmt.Errorf("invalid process stat")
	}
	close := strings.LastIndex(rest, ") ")
	if close < 0 {
		return "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(rest[close+2:])
	if len(fields) <= 19 {
		return "", fmt.Errorf("invalid process stat")
	}
	boot, err := os.ReadFile(filepath.Join(root, "sys/kernel/random/boot_id"))
	if err != nil {
		// Fixture roots may provide the kernel file at the real absolute path.
		boot, err = os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			return "", err
		}
	}
	return "linux:" + strings.TrimSpace(string(boot)) + ":" + fields[19], nil
}

func readCgroup(root string) (*uint64, *uint64, Availability) {
	if root == "" {
		return nil, nil, Unavailable
	}
	limitData, limitErr := os.ReadFile(filepath.Join(root, "memory.max"))
	currentData, currentErr := os.ReadFile(filepath.Join(root, "memory.current"))
	if limitErr != nil || currentErr != nil {
		return nil, nil, Unavailable
	}
	limitText := strings.TrimSpace(string(limitData))
	if limitText == "max" {
		return nil, nil, Absent
	}
	limit, err := strconv.ParseUint(limitText, 10, 64)
	if err != nil {
		return nil, nil, Unavailable
	}
	current, err := strconv.ParseUint(strings.TrimSpace(string(currentData)), 10, 64)
	if err != nil {
		return nil, nil, Unavailable
	}
	return ptr(limit), ptr(current), Available
}

func queryNVIDIA(ctx context.Context) GPUResult {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	command := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,memory.used,memory.free", "--format=csv,noheader,nounits")
	output, err := command.Output()
	if err != nil {
		if ctx.Err() != nil {
			return GPUResult{Status: Unavailable, Source: "nvidia-smi", Error: "timeout"}
		}
		if _, lookupErr := exec.LookPath("nvidia-smi"); lookupErr != nil {
			return GPUResult{Status: Absent, Source: "nvidia-smi", Error: "provider_not_installed"}
		}
		return GPUResult{Status: Unavailable, Source: "nvidia-smi", Error: "driver_or_permission_unavailable"}
	}
	result := GPUResult{Status: Available, Source: "nvidia-smi"}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Split(line, ",")
		if len(fields) != 5 {
			result.Status = Partial
			continue
		}
		id, name := strings.TrimSpace(fields[0]), strings.TrimSpace(fields[1])
		total, totalOK := csvMiB(fields[2])
		used, usedOK := csvMiB(fields[3])
		free, freeOK := csvMiB(fields[4])
		if !totalOK || !usedOK || !freeOK {
			result.Status = Partial
		}
		result.GPUs = append(result.GPUs, GPUMemory{ID: id, Name: name, TotalBytes: maybePtr(total, totalOK), UsedBytes: maybePtr(used, usedOK), FreeBytes: maybePtr(free, freeOK), Status: result.Status, Source: result.Source})
	}
	if len(result.GPUs) == 0 {
		result.Status = Absent
	}
	return result
}

func csvMiB(value string) (uint64, bool) {
	n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	return n * 1024 * 1024, err == nil
}

func maybePtr(value uint64, ok bool) *uint64 {
	if !ok {
		return nil
	}
	return ptr(value)
}
