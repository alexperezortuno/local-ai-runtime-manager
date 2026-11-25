//go:build darwin && cgo

package main

/*
#cgo LDFLAGS: -lproc
#include <libproc.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

static int lw_process_info(pid_t pid, unsigned long long *sec,
	unsigned long long *usec, pid_t *ppid, pid_t *pgid, uid_t *euid,
	char *path, int pathlen) {
	struct proc_bsdinfo info;
	int n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, sizeof(info));
	if (n != sizeof(info) || info.pbi_pid != pid) return 0;
	*sec = (unsigned long long)info.pbi_start_tvsec;
	*usec = (unsigned long long)info.pbi_start_tvusec;
	*ppid = info.pbi_ppid;
	*pgid = info.pbi_pgid;
	*euid = info.pbi_uid;
	if (proc_pidpath(pid, path, (uint32_t)pathlen) <= 0) path[0] = '\0';
	return 1;
}
*/
import "C"

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type identity struct {
	PID          int    `json:"pid"`
	PPID         int    `json:"ppid"`
	PGID         int    `json:"pgid"`
	StartSeconds uint64 `json:"start_seconds"`
	StartMicros  uint64 `json:"start_microseconds"`
	Start        string `json:"start_identity"`
	EffectiveUID int    `json:"effective_uid"`
	Executable   string `json:"executable,omitempty"`
}

type ready struct {
	ChildPID       int      `json:"child_pid"`
	Child          identity `json:"child_identity"`
	SupervisorPID  int      `json:"supervisor_pid"`
	Supervisor     identity `json:"supervisor_identity"`
	RequesterPID   int      `json:"requester_pid"`
	Requester      identity `json:"requester_identity"`
	ChildLog       string   `json:"child_log"`
	SupervisorLog  string   `json:"supervisor_log"`
	DescendantMode bool     `json:"descendant_mode"`
}

func processIdentity(pid int) (identity, error) {
	var sec, usec C.ulonglong
	var ppid, pgid C.pid_t
	var euid C.uid_t
	var path [C.PROC_PIDPATHINFO_MAXSIZE]C.char
	if C.lw_process_info(
		C.pid_t(pid), &sec, &usec, &ppid, &pgid, &euid,
		&path[0], C.int(len(path)),
	) == 0 {
		return identity{}, fmt.Errorf("proc_pidinfo(%d): process unavailable", pid)
	}
	return identity{
		PID:          pid,
		PPID:         int(ppid),
		PGID:         int(pgid),
		StartSeconds: uint64(sec),
		StartMicros:  uint64(usec),
		Start:        fmt.Sprintf("darwin:%d:%d", uint64(sec), uint64(usec)),
		EffectiveUID: int(euid),
		Executable:   C.GoString(&path[0]),
	}, nil
}

func selfIdentity() (identity, error) {
	return processIdentity(os.Getpid())
}

func appendEvent(path, kind string, fields map[string]any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	event := map[string]any{
		"time":  time.Now().UTC().Format(time.RFC3339Nano),
		"event": kind,
	}
	for key, value := range fields {
		event[key] = value
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, string(encoded))
	return err
}

func writeJSON(path string, value any) error {
	tmp := path + ".tmp-" + strconv.Itoa(os.Getpid())
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, append(data, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readReady(path string) (ready, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ready{}, err
	}
	var result ready
	if err := json.Unmarshal(data, &result); err != nil {
		return ready{}, err
	}
	return result, nil
}

func signalResult(pid, signal int) map[string]any {
	err := syscall.Kill(pid, syscall.Signal(signal))
	result := map[string]any{
		"target_pid": pid,
		"signal":     signalName(signal),
		"errno":      "none",
		"success":    err == nil,
	}
	if err != nil {
		result["errno"] = err.Error()
	}
	return result
}

func signalName(signal int) string {
	switch syscall.Signal(signal) {
	case syscall.SIGUSR1:
		return "SIGUSR1"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return strconv.Itoa(signal)
	}
}

func runRequester(logPath, readyPath, childLog string, descendants bool) error {
	self, err := selfIdentity()
	if err != nil {
		return err
	}
	if err := appendEvent(logPath, "requester_started", map[string]any{
		"identity":              self,
		"effective_uid_from_os": os.Geteuid(),
	}); err != nil {
		return err
	}

	args := []string{
		"-role=supervisor",
		"-log=" + logPath,
		"-ready=" + readyPath,
		"-child-log=" + childLog,
		"-descendants=" + strconv.FormatBool(descendants),
		"-requester-pid=" + strconv.Itoa(os.Getpid()),
		"-requester-start=" + self.Start,
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = nil
	stderr, err := os.OpenFile(logPath+".stderr", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stderr.Close()
		return err
	}
	if err := appendEvent(logPath, "supervisor_started_by_requester", map[string]any{
		"supervisor_pid": cmd.Process.Pid,
	}); err != nil {
		return err
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyPath); err == nil {
			return appendEvent(logPath, "requester_exiting_after_spawn", map[string]any{
				"supervisor_pid": cmd.Process.Pid,
			})
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("requester timed out waiting for supervisor ready file")
}

func runSupervisor(logPath, readyPath, childLog string, descendants bool, requesterPID int, requesterStart string) error {
	self, err := selfIdentity()
	if err != nil {
		return err
	}
	if err := appendEvent(logPath, "supervisor_started", map[string]any{
		"identity":                 self,
		"effective_uid_from_os":    os.Geteuid(),
		"requester_pid":            requesterPID,
		"requester_start_identity": requesterStart,
	}); err != nil {
		return err
	}

	args := []string{"-role=child", "-log=" + childLog}
	if descendants {
		args = append(args, "-descendants=true")
	}
	child := exec.Command(os.Args[0], args...)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		return err
	}
	childPID := child.Process.Pid
	childIdentity, err := processIdentity(childPID)
	if err != nil {
		return err
	}
	r := ready{
		ChildPID:       childPID,
		Child:          childIdentity,
		SupervisorPID:  self.PID,
		Supervisor:     self,
		RequesterPID:   requesterPID,
		ChildLog:       childLog,
		SupervisorLog:  logPath,
		DescendantMode: descendants,
	}
	if requester, e := processIdentity(requesterPID); e == nil {
		r.Requester = requester
	} else {
		r.Requester = identity{PID: requesterPID, Start: requesterStart}
	}
	if err := appendEvent(logPath, "spawn", map[string]any{
		"child_identity":       childIdentity,
		"child_pgid_isolation": childIdentity.PGID == childIdentity.PID,
	}); err != nil {
		return err
	}
	if err := writeJSON(readyPath, r); err != nil {
		return err
	}

	// The requester exits immediately after ready. This is the survival test.
	reparented := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		current, e := selfIdentity()
		if e == nil && current.PPID != self.PPID {
			reparented = true
			if err := appendEvent(logPath, "supervisor_reparented", map[string]any{
				"before": self,
				"after":  current,
			}); err != nil {
				return err
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !reparented {
		_ = appendEvent(logPath, "supervisor_reparent_timeout", map[string]any{
			"initial_ppid": self.PPID,
		})
	}

	// Safe probe: SIGUSR1 is handled by the child and cannot terminate it.
	// A nil result proves that same-UID isolation is not supplied by macOS.
	probe := signalResult(childPID, int(syscall.SIGUSR1))
	if err := appendEvent(logPath, "supervisor_signal_attempt", map[string]any{
		"authorization":   "forbidden_by_design",
		"sender_identity": self,
		"operation":       probe,
	}); err != nil {
		return err
	}

	var status syscall.WaitStatus
	var usage syscall.Rusage
	waitPID, waitErr := syscall.Wait4(childPID, &status, 0, &usage)
	wait := map[string]any{
		"wait4_pid":            waitPID,
		"wait4_errno":          "none",
		"raw_status":           uint32(status),
		"exited":               status.Exited(),
		"exit_code":            status.ExitStatus(),
		"signaled":             status.Signaled(),
		"signal":               "",
		"child_identity":       childIdentity,
		"pid_matches_identity": waitPID == childIdentity.PID,
		"start_identity_saved": childIdentity.Start,
	}
	if status.Signaled() {
		wait["signal"] = signalName(int(status.Signal()))
	}
	if waitErr != nil {
		wait["wait4_errno"] = waitErr.Error()
	}
	return appendEvent(logPath, "wait4", wait)
}

func runChild(logPath string, descendants bool) error {
	self, err := selfIdentity()
	if err != nil {
		return err
	}
	if err := appendEvent(logPath, "child_started", map[string]any{
		"identity":              self,
		"effective_uid_from_os": os.Geteuid(),
	}); err != nil {
		return err
	}

	signals := make(chan os.Signal, 2)
	signalNotify(signals, syscall.SIGUSR1, syscall.SIGTERM)
	var grandchild *exec.Cmd
	if descendants {
		grandchild = exec.Command(os.Args[0], "-role=grandchild", "-log="+logPath)
		if err := grandchild.Start(); err != nil {
			return err
		}
		grandchildIdentity, e := processIdentity(grandchild.Process.Pid)
		if e != nil {
			return e
		}
		if err := appendEvent(logPath, "descendant_spawn", map[string]any{
			"parent_identity":     self,
			"descendant_identity": grandchildIdentity,
			"same_process_group":  grandchildIdentity.PGID == self.PGID,
		}); err != nil {
			return err
		}
	}

	for sig := range signals {
		if err := appendEvent(logPath, "child_signal_received", map[string]any{
			"signal":          signalName(int(sig.(syscall.Signal))),
			"child_identity":  self,
			"sender_identity": "not observable through kill(2)",
		}); err != nil {
			return err
		}
		if sig != syscall.SIGTERM {
			continue
		}
		if grandchild != nil {
			result := signalResult(grandchild.Process.Pid, int(syscall.SIGTERM))
			_ = appendEvent(logPath, "descendant_cleanup_signal", result)
			_ = grandchild.Wait()
		}
		return nil
	}
	return errors.New("signal channel closed before SIGTERM")
}

func runGrandchild(logPath string) error {
	self, err := selfIdentity()
	if err != nil {
		return err
	}
	if err := appendEvent(logPath, "descendant_started", map[string]any{
		"identity": self,
	}); err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signalNotify(signals, syscall.SIGTERM)
	<-signals
	return appendEvent(logPath, "descendant_signal_received", map[string]any{
		"identity": self,
		"signal":   "SIGTERM",
	})
}

func runRuntime(logPath, readyPath string) error {
	r, err := readReady(readyPath)
	if err != nil {
		return err
	}
	self, err := selfIdentity()
	if err != nil {
		return err
	}
	current, err := processIdentity(r.ChildPID)
	if err != nil {
		return err
	}
	identityMatches := current.PID == r.Child.PID &&
		current.Start == r.Child.Start &&
		current.EffectiveUID == r.Child.EffectiveUID
	if err := appendEvent(logPath, "runtime_started", map[string]any{
		"identity":                 self,
		"target_identity_recorded": r.Child,
		"target_identity_current":  current,
		"target_identity_matches":  identityMatches,
		"effective_uid_from_os":    os.Geteuid(),
	}); err != nil {
		return err
	}
	if !identityMatches {
		return appendEvent(logPath, "runtime_signal_attempt", map[string]any{
			"authorization": "authorized",
			"success":       false,
			"errno":         "physical identity mismatch; signal withheld",
		})
	}
	result := signalResult(r.ChildPID, int(syscall.SIGTERM))
	return appendEvent(logPath, "runtime_signal_attempt", map[string]any{
		"authorization":   "authorized",
		"sender_identity": self,
		"operation":       result,
	})
}

// Kept in one function so the prototype makes the signal subscription explicit.
func signalNotify(ch chan<- os.Signal, signals ...os.Signal) {
	signal.Notify(ch, signals...)
}

func main() {
	role := flag.String("role", "", "requester, supervisor, child, grandchild, or runtime")
	logPath := flag.String("log", "", "JSONL event log")
	readyPath := flag.String("ready", "", "ready JSON file")
	childLog := flag.String("child-log", "", "child JSONL event log")
	descendants := flag.Bool("descendants", false, "create a controlled grandchild")
	requesterPID := flag.Int("requester-pid", 0, "requester PID")
	requesterStart := flag.String("requester-start", "", "requester start identity")
	flag.Parse()

	var err error
	switch *role {
	case "requester":
		err = runRequester(*logPath, *readyPath, *childLog, *descendants)
	case "supervisor":
		err = runSupervisor(*logPath, *readyPath, *childLog, *descendants, *requesterPID, *requesterStart)
	case "child":
		err = runChild(*logPath, *descendants)
	case "grandchild":
		err = runGrandchild(*logPath)
	case "runtime":
		err = runRuntime(*logPath, *readyPath)
	default:
		err = fmt.Errorf("unknown role %q", *role)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
