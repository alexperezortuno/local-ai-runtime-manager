//go:build darwin && cgo

package memory

/*
#include <libproc.h>
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <unistd.h>

static int larm_system_memory(unsigned long long *total, unsigned long long *available, unsigned long long *swap_total, unsigned long long *swap_used, int *swap_available) {
    size_t total_size = sizeof(*total);
    if (sysctlbyname("hw.memsize", total, &total_size, NULL, 0) != 0) return 0;
    vm_size_t page_size = 0;
    if (host_page_size(mach_host_self(), &page_size) != KERN_SUCCESS) return 0;
    vm_statistics64_data_t vm = {0};
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    if (host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info64_t)&vm, &count) != KERN_SUCCESS) return 0;
    *available = (unsigned long long)(vm.free_count + vm.inactive_count + vm.speculative_count) * page_size;
    struct xsw_usage swap = {0};
    size_t swap_size = sizeof(swap);
    if (sysctlbyname("vm.swapusage", &swap, &swap_size, NULL, 0) == 0) {
        *swap_total = swap.xsu_total;
        *swap_used = swap.xsu_used;
        *swap_available = 1;
    } else {
        *swap_total = 0;
        *swap_used = 0;
        *swap_available = 0;
    }
    return 1;
}

static int larm_process_memory(pid_t pid, unsigned long long *sec, unsigned long long *usec, unsigned long long *resident, unsigned long long *footprint) {
    struct proc_bsdinfo info = {0};
    if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, sizeof(info)) != sizeof(info)) return 0;
    struct rusage_info_v4 usage = {0};
    if (proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t)&usage) != 0) return 0;
    struct rusage_info_v4 *v4 = &usage;
    *sec = info.pbi_start_tvsec;
    *usec = info.pbi_start_tvusec;
    *resident = v4->ri_resident_size;
    *footprint = v4->ri_phys_footprint;
    return info.pbi_pid == pid;
}
*/
import "C"

import (
	"context"
	"runtime"
	"time"
)

type darwinProvider struct{}

func newPlatformProvider() Provider { return darwinProvider{} }

func (darwinProvider) System(context.Context) SystemMemory {
	now := time.Now().UTC()
	result := SystemMemory{Architecture: "unified", MeasuredAt: &now, Status: Available, Source: "macOS:sysctl+Mach VM"}
	var total, available, swapTotal, swapUsed C.ulonglong
	var swapAvailable C.int
	if C.larm_system_memory(&total, &available, &swapTotal, &swapUsed, &swapAvailable) == 0 {
		result.Status = Unavailable
		return result
	}
	result.TotalBytes, result.AvailableBytes = ptr(uint64(total)), ptr(uint64(available))
	if total >= available {
		result.UsedBytes = ptr(uint64(total - available))
	}
	if swapAvailable != 0 {
		result.SwapTotalBytes, result.SwapUsedBytes = ptr(uint64(swapTotal)), ptr(uint64(swapUsed))
	}
	if runtime.GOARCH != "arm64" {
		result.Architecture = "shared"
	}
	return result
}

func (darwinProvider) Process(_ context.Context, ref ProcessRef) ProcessMemory {
	now := time.Now().UTC()
	result := ProcessMemory{ModelID: ref.ModelID, PID: ref.PID, ProcessInstanceID: ref.ProcessInstanceID, RuntimeState: ref.RuntimeState, MeasuredAt: &now, Source: "macOS:libproc/rusage", GPUStatus: Unavailable, Status: Unavailable}
	var sec, usec, resident, footprint C.ulonglong
	if ref.PID <= 0 || C.larm_process_memory(C.pid_t(ref.PID), &sec, &usec, &resident, &footprint) == 0 {
		result.Reason = "process_ended_or_unreadable"
		return result
	}
	identity := "darwin:" + itoa(uint64(sec)) + ":" + itoa(uint64(usec)) + ":" + itoa(uint64(ref.PID))
	if ref.StartIdentity == "" || identity != ref.StartIdentity {
		result.Reason = "identity_mismatch"
		return result
	}
	result.RSSBytes, result.PrivateBytes, result.Status = ptr(uint64(resident)), ptr(uint64(footprint)), Available
	return result
}

func darwinProcessIdentity(pid int) (string, bool) {
	var sec, usec, resident, footprint C.ulonglong
	if pid <= 0 || C.larm_process_memory(C.pid_t(pid), &sec, &usec, &resident, &footprint) == 0 {
		return "", false
	}
	return "darwin:" + itoa(uint64(sec)) + ":" + itoa(uint64(usec)) + ":" + itoa(uint64(pid)), true
}

func (darwinProvider) GPUs(context.Context) GPUResult {
	return GPUResult{Status: Unavailable, Source: "apple-unified-memory", Error: "dedicated GPU memory is not exposed by public API"}
}

func itoa(value uint64) string {
	// Avoid fmt in the cgo-facing file; the values are decimal identity fields.
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = digits[value%10]
		value /= 10
	}
	return string(buf[i:])
}
