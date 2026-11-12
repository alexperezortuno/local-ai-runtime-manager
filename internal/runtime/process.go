package runtime

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type process struct {
	cmd          *exec.Cmd
	done         chan struct{}
	wait         processWaitResult
	identity     osProcessIdentity
	observed     map[int]string
	instanceID   string
	processGroup int
	observeMu    sync.Mutex
	signalsMu    sync.Mutex
	signals      []string
}

type processWaitResult struct {
	err          error
	errorClass   string
	exitCode     *int
	signal       string
	terminatedAt time.Time
}

type processStartError struct {
	kind string
	err  error
}

func (e *processStartError) Error() string { return e.err.Error() }
func (e *processStartError) Unwrap() error { return e.err }

const (
	processStartSpawnError    = "spawn"
	processStartIdentityError = "identity"
)

func startProcess(binary string, args []string, instanceID string) (*process, error) {
	cmd := exec.Command(binary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, &processStartError{kind: processStartSpawnError, err: err}
	}
	identity, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		// Do not signal a process whose identity has not been verified. Reap the
		// child without introducing an unsafe PID-based cleanup path.
		go func() { _ = cmd.Wait() }()
		return nil, &processStartError{kind: processStartIdentityError, err: err}
	}
	if identity.PGID != identity.PID {
		go func() { _ = cmd.Wait() }()
		return nil, &processStartError{kind: processStartIdentityError, err: errors.New("started process is not the leader of its isolated process group")}
	}
	p := &process{cmd: cmd, done: make(chan struct{}), identity: identity, observed: map[int]string{identity.PID: identity.Start}, instanceID: instanceID, processGroup: identity.PGID}
	go func() {
		err := cmd.Wait()
		p.wait = collectWaitResult(err, cmd.ProcessState)
		close(p.done)
	}()
	return p, nil
}

func (p *process) waitError() error {
	<-p.done
	return p.wait.err
}

func (p *process) waitResult() processWaitResult {
	<-p.done
	return p.wait
}

func (p *process) terminationObserved(timeout time.Duration) error {
	result, err := p.waitForTermination(timeout)
	if err != nil {
		return err
	}
	if result.errorClass == "wait_error" {
		return errors.New("cmd.Wait returned an ambiguous result")
	}
	gone, err := p.waitGroupGone(timeout)
	if err != nil {
		return err
	}
	if !gone {
		return errors.New("process group or descendants remain after leader exit")
	}
	return nil
}

func (p *process) waitForTermination(timeout time.Duration) (processWaitResult, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.waitResult(), nil
	case <-timer.C:
		return processWaitResult{}, errors.New("cmd.Wait is still pending")
	}
}

func collectWaitResult(err error, state *os.ProcessState) processWaitResult {
	result := processWaitResult{err: err, terminatedAt: time.Now().UTC()}
	if state == nil {
		result.errorClass = "wait_error"
		return result
	}
	if code := state.ExitCode(); code >= 0 {
		result.exitCode = &code
		result.errorClass = "exited_zero"
		if code != 0 {
			result.errorClass = "nonzero_exit"
		}
		return result
	}
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		result.signal = signalName(status.Signal())
		result.errorClass = "signaled_exit"
		return result
	}
	result.errorClass = "wait_error"
	return result
}

func (p *process) identityVerified() (osProcessIdentity, error) {
	actual, err := processIdentity(p.identity.PID)
	if err != nil {
		return osProcessIdentity{}, err
	}
	if !sameProcessIdentity(p.identity, actual) || actual.PGID != p.processGroup {
		return osProcessIdentity{}, errors.New("process identity or group identity no longer matches")
	}
	return actual, nil
}

// signalGroup never falls back to an unverified PID. Once the leader is gone,
// the PGID may have been reused and is therefore not safe to signal.
func (p *process) signalGroup(sig syscall.Signal) error {
	if _, err := p.identityVerified(); err != nil {
		return err
	}
	err := syscall.Kill(-p.processGroup, sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	if err == nil {
		p.signalsMu.Lock()
		p.signals = append(p.signals, signalName(sig))
		p.signalsMu.Unlock()
	}
	return err
}

func signalName(signal syscall.Signal) string {
	switch signal {
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGINT:
		return "SIGINT"
	default:
		return signal.String()
	}
}

func (p *process) signalsSent() []string {
	p.signalsMu.Lock()
	defer p.signalsMu.Unlock()
	return append([]string(nil), p.signals...)
}

func (p *process) observe() ([]osProcessIdentity, error) {
	p.observeMu.Lock()
	defer p.observeMu.Unlock()
	current, err := listOSProcessIdentities()
	if err != nil {
		return nil, err
	}
	leader, leaderErr := processIdentity(p.identity.PID)
	leaderMatches := leaderErr == nil && sameProcessIdentity(p.identity, leader) && leader.PGID == p.processGroup
	for _, item := range current {
		if item.PID == p.identity.PID && !sameProcessIdentity(p.identity, item) {
			continue
		}
		if (leaderMatches && item.PGID == p.processGroup) || p.observed[item.PPID] == item.Start || item.PID == p.identity.PID {
			p.observed[item.PID] = item.Start
		}
	}
	associated := make([]osProcessIdentity, 0)
	for _, item := range current {
		if start, ok := p.observed[item.PID]; ok && start == item.Start {
			associated = append(associated, item)
		}
	}
	return associated, nil
}

func (p *process) waitGroupGone(timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		members, err := p.observe()
		if err != nil {
			lastErr = err
			if time.Now().After(deadline) {
				return false, lastErr
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if len(members) == 0 {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *process) leaderAndGroupGone(timeout time.Duration) (bool, error) {
	if _, err := p.waitForTermination(timeout); err != nil {
		return false, err
	}
	return p.waitGroupGone(timeout)
}
