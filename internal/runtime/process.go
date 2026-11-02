package runtime

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startProcess(binary string, args []string) (*process, error) {
	cmd := exec.Command(binary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &process{cmd: cmd, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *process) waitError() error {
	<-p.done
	return p.err
}

func (p *process) signal(sig syscall.Signal) error {
	if p == nil || p.cmd.Process == nil {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-p.cmd.Process.Pid, sig)
	if errors.Is(err, syscall.EPERM) {
		// Sandboxed runtimes can deny the process-group signal but allow the owned child.
		return p.cmd.Process.Signal(sig)
	}
	return err
}

func (p *process) killGroup() error {
	err := p.signal(syscall.SIGKILL)
	if err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
