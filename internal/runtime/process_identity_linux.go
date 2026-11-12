//go:build linux

package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func readOSProcessIdentity(pid int) (osProcessIdentity, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return osProcessIdentity{}, err
	}
	_, rest, ok := strings.Cut(string(data), "(")
	if !ok {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	// comm is parenthesized and may contain spaces or ')'. The final ") " is
	// the delimiter before state; splitting from the right avoids a false field
	// offset for unusual executable names.
	close := strings.LastIndex(rest, ") ")
	if close < 0 {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	fields := strings.Fields(rest[close+2:])
	if len(fields) <= 19 {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	return osProcessIdentity{PID: pid, PPID: ppid, PGID: pgid, Start: "linux:" + strings.TrimSpace(string(boot)) + ":" + fields[19], Executable: executablePath(pid)}, nil
}

func listOSProcessIdentities() ([]osProcessIdentity, error) {
	entries, err := filepath.Glob("/proc/[0-9]*")
	if err != nil {
		return nil, err
	}
	result := make([]osProcessIdentity, 0, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(filepath.Base(entry))
		if err != nil {
			continue
		}
		identity, err := processIdentity(pid)
		if err == nil {
			result = append(result, identity)
		}
	}
	return result, nil
}
