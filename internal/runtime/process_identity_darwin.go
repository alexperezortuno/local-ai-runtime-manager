//go:build darwin && cgo

package runtime

/*
#cgo LDFLAGS: -lproc
#include <libproc.h>
#include <sys/types.h>
#include <string.h>

static int larm_proc_identity(pid_t pid, unsigned long long *sec, unsigned long long *usec, char *path, int pathlen) {
	struct proc_bsdinfo info;
	int n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, sizeof(info));
	if (n != sizeof(info)) return 0;
	*sec = (unsigned long long)info.pbi_start_tvsec;
	*usec = (unsigned long long)info.pbi_start_tvusec;
	if (proc_pidpath(pid, path, (uint32_t)pathlen) <= 0) path[0] = '\0';
	return info.pbi_pid == pid;
}
*/
import "C"

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func readOSProcessIdentity(pid int) (osProcessIdentity, error) {
	var sec, usec C.ulonglong
	var path [C.PROC_PIDPATHINFO_MAXSIZE]C.char
	if C.larm_proc_identity(C.pid_t(pid), &sec, &usec, &path[0], C.int(len(path))) == 0 {
		return osProcessIdentity{}, errProcessIdentityUnavailable
	}
	executable := C.GoString(&path[0])
	return osProcessIdentity{PID: pid, Start: fmt.Sprintf("darwin:%d:%d:%d", uint64(sec), uint64(usec), pid), Executable: executable}, nil
}

func listOSProcessIdentities() ([]osProcessIdentity, error) {
	out, err := exec.Command("/bin/ps", "-axo", "pid=,ppid=,pgid=").Output()
	if err != nil {
		return nil, err
	}
	result := make([]osProcessIdentity, 0)
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, e1 := strconv.Atoi(fields[0])
		ppid, e2 := strconv.Atoi(fields[1])
		pgid, e3 := strconv.Atoi(fields[2])
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		identity, err := processIdentity(pid)
		if err != nil {
			continue
		}
		identity.PPID, identity.PGID = ppid, pgid
		result = append(result, identity)
	}
	return result, nil
}
