package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"local-ai-runtime/internal/backend"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/models"
)

var (
	ErrNotFound    = errors.New("model not found")
	ErrConflict    = errors.New("runtime operation conflicts with current state")
	ErrUnavailable = errors.New("runtime unavailable")
)

type Manager struct {
	mu          sync.Mutex
	registry    *models.Registry
	config      config.RuntimeConfig
	state       State
	active      string
	proc        *process
	started     time.Time
	modelStates map[string]State
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewManager(cfg config.Config) *Manager {
	states := make(map[string]State, len(cfg.Models))
	for id := range cfg.Models {
		states[id] = Stopped
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{registry: models.NewRegistry(cfg.Models), config: cfg.Runtime, state: Stopped, modelStates: states, ctx: ctx, cancel: cancel}
}

func (m *Manager) Start(ctx context.Context, id string) error {
	model, err := m.registry.Get(id)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	m.mu.Lock()
	if m.state == Ready && m.active == id {
		m.mu.Unlock()
		return nil
	}
	if m.proc != nil || m.state == Starting || m.state == Stopping {
		m.mu.Unlock()
		return ErrConflict
	}
	m.active, m.state = id, Starting
	m.modelStates[id] = Starting
	m.mu.Unlock()

	p, err := startProcess(m.config.Binary, backend.Args(model))
	if err != nil {
		m.fail(id, nil)
		return fmt.Errorf("%w: start llama-server: %v", ErrUnavailable, err)
	}
	m.mu.Lock()
	m.proc = p
	m.started = time.Now()
	m.mu.Unlock()
	go m.watch(p, id)

	combinedCtx, cancelCombined := context.WithCancel(ctx)
	stopManagerCancel := context.AfterFunc(m.ctx, cancelCombined)
	defer func() {
		stopManagerCancel()
		cancelCombined()
	}()
	startupCtx, cancel := context.WithTimeout(combinedCtx, m.config.StartupDuration())
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if backend.Healthy(startupCtx, model.Port) {
			m.mu.Lock()
			select {
			case <-p.done:
				m.mu.Unlock()
				if err := m.terminate(p, m.config.ShutdownDuration()); err != nil {
					return fmt.Errorf("%w: startup cleanup failed: %v", ErrUnavailable, err)
				}
				m.fail(id, p)
				return fmt.Errorf("%w: process exited before readiness", ErrUnavailable)
			default:
			}
			if m.proc == p && m.state == Starting {
				m.state = Ready
				m.modelStates[id] = Ready
				m.mu.Unlock()
				return nil
			}
			m.mu.Unlock()
			return fmt.Errorf("%w: process exited before readiness", ErrUnavailable)
		}
		select {
		case <-p.done:
			err := p.waitError()
			if cleanupErr := m.terminate(p, m.config.ShutdownDuration()); cleanupErr != nil {
				return fmt.Errorf("%w: startup cleanup failed: %v", ErrUnavailable, cleanupErr)
			}
			m.fail(id, p)
			return fmt.Errorf("%w: llama-server exited before readiness: %v", ErrUnavailable, err)
		case <-startupCtx.Done():
			if err := m.terminate(p, m.config.ShutdownDuration()); err != nil {
				return fmt.Errorf("%w: startup cleanup failed: %v", ErrUnavailable, err)
			}
			m.fail(id, p)
			return fmt.Errorf("%w: startup failed: %v", ErrUnavailable, startupCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) Stop(ctx context.Context, id string) error {
	m.mu.Lock()
	if _, err := m.registry.Get(id); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if m.active != id {
		m.mu.Unlock()
		return ErrConflict
	}
	if m.state == Starting || m.state == Stopping {
		m.mu.Unlock()
		return ErrConflict
	}
	p := m.proc
	if p == nil {
		m.state, m.active = Stopped, ""
		m.modelStates[id] = Stopped
		m.mu.Unlock()
		return nil
	}
	m.state = Stopping
	m.modelStates[id] = Stopping
	m.mu.Unlock()

	if err := m.terminate(p, m.config.ShutdownDuration()); err != nil {
		return fmt.Errorf("%w: stop process: %v", ErrUnavailable, err)
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		// Stop must reap its owned child even if the HTTP caller disconnects.
	}
	m.mu.Lock()
	if m.proc == p {
		m.proc, m.active, m.state = nil, "", Stopped
		m.modelStates[id] = Stopped
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) terminate(p *process, timeout time.Duration) error {
	select {
	case <-p.done:
		_ = p.killGroup()
		return nil
	default:
	}
	if err := p.signal(syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		if err := p.killGroup(); err != nil {
			return err
		}
		return nil
	case <-timer.C:
	}
	if err := p.signal(syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	<-p.done
	return nil
}

func (m *Manager) fail(id string, p *process) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p == nil || m.proc == p {
		m.proc = nil
		m.state = Failed
		m.active = ""
		m.modelStates[id] = Failed
	}
}

func (m *Manager) watch(p *process, id string) {
	<-p.done
	_ = p.killGroup()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc == p && m.state == Ready {
		m.proc = nil
		m.state = Failed
		m.active = ""
		m.modelStates[id] = Failed
	}
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Status{Runtime: m.config.Backend, State: m.state, ActiveModel: m.active}
	if m.proc != nil {
		s.PID = m.proc.cmd.Process.Pid
		if m.state == Ready {
			s.UptimeSeconds = int64(time.Since(m.started).Seconds())
		}
	}
	return s
}

func (m *Manager) Models() map[string]ModelStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]ModelStatus)
	for id, model := range m.registry.List() {
		state := m.modelStates[id]
		result[id] = ModelStatus{ID: id, Path: model.Path, Port: model.Port, State: state}
	}
	return result
}

type ModelStatus struct {
	ID    string `json:"id"`
	Path  string `json:"path"`
	Port  int    `json:"port"`
	State State  `json:"state"`
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.cancel()
	for {
		m.mu.Lock()
		state := m.state
		id := m.active
		m.mu.Unlock()
		if state != Starting && state != Stopping {
			if id == "" || state == Stopped {
				return nil
			}
			return m.Stop(ctx, id)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
