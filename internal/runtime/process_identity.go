package runtime

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var errProcessIdentityUnavailable = errors.New("OS process identity is unavailable")

type osProcessIdentity struct {
	PID        int
	PPID       int
	PGID       int
	Start      string
	Executable string
}

func processIdentity(pid int) (osProcessIdentity, error) {
	identity, err := readOSProcessIdentity(pid)
	if err != nil {
		return osProcessIdentity{}, err
	}
	if identity.PID == 0 {
		identity.PID = pid
	}
	if identity.PGID == 0 {
		identity.PGID, err = syscall.Getpgid(pid)
		if err != nil {
			return osProcessIdentity{}, err
		}
	}
	if identity.Executable == "" {
		identity.Executable = executablePath(pid)
	}
	if identity.Start == "" {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	return identity, nil
}

func executablePath(pid int) string {
	if path, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid)); err == nil {
		return path
	}
	return ""
}

func sameProcessIdentity(expected, actual osProcessIdentity) bool {
	return expected.PID != 0 && expected.PID == actual.PID && expected.Start != "" && expected.Start == actual.Start
}
