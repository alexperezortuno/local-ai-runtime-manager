package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"local-ai-runtime/internal/backend"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/models"
)

var (
	ErrNotFound         = errors.New("model not found")
	ErrConflict         = errors.New("runtime operation conflicts with current state")
	ErrUnavailable      = errors.New("runtime unavailable")
	ErrRecoveryRequired = errors.New("runtime recovery is required")
	ErrCapacity         = errors.New("runtime capacity is exhausted")
)

type Manager struct {
	mu             sync.Mutex
	registry       *models.Registry
	config         config.RuntimeConfig
	state          State
	active         string
	proc           *process
	procs          map[string]*process
	finishedProc   *process
	finishedModel  string
	started        time.Time
	modelStates    map[string]State
	startedModels  map[string]time.Time
	capacity       int
	ctx            context.Context
	cancel         context.CancelFunc
	journal        ProcessJournal
	ownerID        string
	processes      map[string]corecontract.ProcessInstance
	recovery       bool
	recoveryReason string
	stopDone       chan struct{}
	stopErr        error
}

func NewManager(cfg config.Config, journals ...ProcessJournal) *Manager {
	states := make(map[string]State, len(cfg.Models))
	for id := range cfg.Models {
		states[id] = Stopped
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{registry: models.NewRegistry(cfg.Models), config: cfg.Runtime, state: Stopped, modelStates: states, startedModels: map[string]time.Time{}, procs: map[string]*process{}, capacity: 1, ctx: ctx, cancel: cancel, processes: map[string]corecontract.ProcessInstance{}}
	if len(journals) > 0 && journals[0] != nil {
		m.journal = journals[0]
		m.ownerID = journals[0].OwnerInstanceID()
		for _, instance := range journals[0].ProcessInstances() {
			if instance.State != corecontract.ProcessTerminatedVerified && instance.State != corecontract.ProcessFailed {
				instance.State = corecontract.ProcessRecoveryRequired
				instance.TerminationStatus = "UNKNOWN"
				actual, err := processIdentity(instance.PID)
				switch {
				case err != nil:
					instance.ProcessIdentityStatus = "UNVERIFIED"
					instance.OwnershipStatus = "UNKNOWN"
				case instance.StartIdentity == "" || actual.Start != instance.StartIdentity:
					instance.ProcessIdentityStatus = "UNVERIFIED"
					instance.OwnershipStatus = "FOREIGN"
				default:
					// Matching OS identity is diagnostic evidence only after restart;
					// the new daemon never adopts the survivor.
					instance.ProcessIdentityStatus, instance.OwnershipStatus = "VERIFIED", "UNKNOWN"
				}
			}
			m.processes[instance.ProcessInstanceID] = instance
			if instance.State != corecontract.ProcessTerminatedVerified && instance.State != corecontract.ProcessFailed {
				m.recovery = true
				m.recoveryReason = "a persisted process instance requires explicit recovery; survivors are not adopted"
			}
		}
	}
	if m.ownerID == "" {
		m.ownerID, _ = corecontract.NewID()
	}
	return m
}

// SetMaxActiveModels is called by the scheduler before it starts work. Direct
// Manager users retain the historical single-process default.
func (m *Manager) SetMaxActiveModels(n int) {
	if n < 1 {
		n = 1
	}
	m.mu.Lock()
	m.capacity = n
	m.mu.Unlock()
}

func (m *Manager) Capacity() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.capacity
}

func (m *Manager) residentCountLocked() int {
	count := 0
	for id, state := range m.modelStates {
		if m.procs[id] != nil || state == Starting || state == Stopping {
			count++
		}
	}
	return count
}

func (m *Manager) modelProcessLocked(id string) (corecontract.ProcessInstance, bool) {
	for _, instance := range m.processes {
		if instance.ModelID == id && m.procs[id] != nil && instance.ProcessInstanceID == m.procs[id].instanceID {
			return instance, true
		}
	}
	return corecontract.ProcessInstance{}, false
}

func (m *Manager) refreshLegacyStateLocked() {
	if m.active != "" && m.procs[m.active] != nil && m.modelStates[m.active] == Ready {
		m.state = Ready
		m.proc = m.procs[m.active]
		return
	}
	for id, p := range m.procs {
		if p != nil && m.modelStates[id] == Ready {
			m.active, m.proc, m.state = id, p, Ready
			return
		}
	}
	if m.active == "" || m.procs[m.active] == nil {
		m.proc, m.state = nil, Stopped
	}
}

func (m *Manager) Start(ctx context.Context, id string) error {
	model, err := m.registry.Get(id)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	m.mu.Lock()
	if m.recovery {
		m.mu.Unlock()
		return ErrRecoveryRequired
	}
	if m.modelStates[id] == Ready && m.procs[id] != nil {
		m.mu.Unlock()
		return nil
	}
	if m.procs[id] != nil || m.modelStates[id] == Starting || m.modelStates[id] == Stopping {
		m.mu.Unlock()
		return ErrConflict
	}
	if m.residentCountLocked() >= m.capacity {
		m.mu.Unlock()
		return fmt.Errorf("%w: %w", ErrCapacity, ErrConflict)
	}
	instanceID, idErr := corecontract.NewID()
	if idErr != nil {
		m.mu.Unlock()
		return idErr
	}
	m.finishedProc, m.finishedModel = nil, ""
	now := time.Now().UTC()
	instance := corecontract.ProcessInstance{SchemaVersion: corecontract.ContractVersion, ProcessInstanceID: instanceID, OwnerInstanceID: m.ownerID, LARMInstanceID: m.ownerID, ModelID: id, Executable: m.config.Binary, ConfigFingerprint: configFingerprint(m.config.Binary, backend.Args(model)), State: corecontract.ProcessPreparing, ProcessIdentityStatus: "UNVERIFIED", OwnershipStatus: "OWNED_VERIFIED", TerminationStatus: "NOT_REQUESTED", DescendantStatus: "NOT_OBSERVED", RecoveryState: corecontract.RecoveryNormal, UpdatedAt: now}
	if err := m.persistProcessLocked(instance); err != nil {
		m.recovery = true
		m.recoveryReason = "process spawn intent could not be durably recorded"
		m.mu.Unlock()
		return fmt.Errorf("%w: persist spawn intent: %v", ErrUnavailable, err)
	}
	m.processes[instanceID] = instance
	m.active, m.state = id, Starting
	m.modelStates[id] = Starting
	m.mu.Unlock()

	spawnedAt := time.Now().UTC()
	p, err := startProcess(m.config.Binary, backend.Args(model), instanceID)
	if err != nil {
		var startErr *processStartError
		if errors.As(err, &startErr) && startErr.kind == processStartIdentityError {
			m.markIdentityUnknown(id, instance, err)
			return fmt.Errorf("%w: process identity unavailable: %v", ErrUnavailable, err)
		}
		m.markStartFailure(id, instance, nil, corecontract.StartupDiagnosticSpawnError, "", err, false)
		return fmt.Errorf("%w: start llama-server: %v", ErrUnavailable, err)
	}
	instance.PID, instance.PGID = p.identity.PID, p.identity.PGID
	instance.StartIdentity, instance.OSProcessStartIdentity = p.identity.Start, p.identity.Start
	instance.ProcessIdentityStatus = "VERIFIED"
	instance.DescendantStatus = "SUPERVISED_GROUP"
	instance.StartedAt, instance.UpdatedAt = spawnedAt, time.Now().UTC()
	instance.State = corecontract.ProcessStarting
	if err := m.publishProcess(instance); err != nil {
		m.failAfterSpawn(id, p, instance, err)
		return fmt.Errorf("%w: persist process identity: %v", ErrUnavailable, err)
	}
	m.mu.Lock()
	m.procs[id], m.startedModels[id] = p, instance.StartedAt
	m.proc, m.started = p, instance.StartedAt
	m.mu.Unlock()
	go m.watch(p, id, instanceID)

	combinedCtx, cancelCombined := context.WithCancel(ctx)
	stopManagerCancel := context.AfterFunc(m.ctx, cancelCombined)
	defer func() { stopManagerCancel(); cancelCombined() }()
	startupCtx, cancel := context.WithTimeout(combinedCtx, m.config.StartupDuration())
	defer cancel()
	if err := m.publishProcessState(instanceID, corecontract.ProcessWaitingReadiness, func(p *corecontract.ProcessInstance) {}); err != nil {
		m.failAfterSpawn(id, p, instance, err)
		return fmt.Errorf("%w: persist readiness state: %v", ErrUnavailable, err)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if backend.Healthy(startupCtx, model.Port) {
			if _, identityErr := p.identityVerified(); identityErr != nil {
				m.markIdentityUnknown(id, instance, identityErr)
				return fmt.Errorf("%w: process identity could not be verified: %v", ErrUnavailable, identityErr)
			}
			select {
			case <-p.done:
				m.markStartFailure(id, instance, p, corecontract.StartupDiagnosticBackendExited, "", p.waitError(), false)
				return fmt.Errorf("%w: process exited before readiness", ErrUnavailable)
			default:
			}
			ready := instanceForReady(m, instanceID)
			ready.State = corecontract.ProcessReady
			ready.ReadinessEvidenceID, _ = corecontract.NewID()
			ready.UpdatedAt = time.Now().UTC()
			ready.Evidence = append(ready.Evidence, readyEvidence(ready, ready.ReadinessEvidenceID))
			if err := m.publishProcess(ready); err != nil {
				m.failAfterSpawn(id, p, instance, err)
				return fmt.Errorf("%w: persist READY: %v", ErrUnavailable, err)
			}
			m.mu.Lock()
			if m.procs[id] == p && !m.recovery {
				m.state, m.modelStates[id] = Ready, Ready
			}
			recovered := m.recovery
			m.mu.Unlock()
			if recovered {
				return ErrRecoveryRequired
			}
			return nil
		}
		select {
		case <-p.done:
			m.markStartFailure(id, instance, p, corecontract.StartupDiagnosticBackendExited, "", p.waitError(), false)
			return fmt.Errorf("%w: llama-server exited before readiness", ErrUnavailable)
		case <-startupCtx.Done():
			category, cancellationReason := startupDiagnosticForContext(ctx, m.ctx, startupCtx)
			if err := m.terminate(p, m.config.ShutdownDuration()); err != nil {
				m.markStartupCleanupFailure(id, instance, p, category, cancellationReason, startupCtx.Err(), err)
				return fmt.Errorf("%w: startup cleanup failed: %v", ErrUnavailable, err)
			}
			m.markStartFailure(id, instance, p, category, cancellationReason, startupCtx.Err(), true)
			return fmt.Errorf("%w: startup failed: %v", ErrUnavailable, startupCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) Stop(ctx context.Context, id string) error {
	if _, err := m.registry.Get(id); err != nil {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	m.mu.Lock()
	if m.active == id && m.state == Stopping && m.stopDone != nil {
		done := m.stopDone
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.Lock()
			err := m.stopErr
			m.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.recovery {
		m.mu.Unlock()
		return ErrRecoveryRequired
	}
	if m.modelStates[id] == Starting || m.modelStates[id] == Stopping {
		m.mu.Unlock()
		return ErrConflict
	}
	p := m.procs[id]
	instance, found := m.modelProcessLocked(id)
	if !found && m.finishedProc != nil && m.finishedModel == id {
		p = m.finishedProc
		for _, candidate := range m.processes {
			if candidate.ProcessInstanceID == p.instanceID {
				instance, found = candidate, true
				break
			}
		}
	}
	if !found {
		if m.modelStates[id] == Stopped || m.modelStates[id] == Failed {
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		return ErrConflict
	}
	if p == nil || instance.ProcessInstanceID == "" {
		m.modelStates[id] = Stopped
		if m.active == id {
			m.state, m.active = Stopped, ""
		}
		m.modelStates[id] = Stopped
		m.mu.Unlock()
		return nil
	}
	m.stopDone = make(chan struct{})
	m.stopErr = nil
	m.state, m.modelStates[id] = Stopping, Stopping
	instance.State = corecontract.ProcessTerminationRequested
	instance.TerminationStatus, instance.UpdatedAt = "STOP_REQUESTED", time.Now().UTC()
	if err := m.persistProcessLocked(instance); err != nil {
		m.recovery, m.recoveryReason = true, "stop intent could not be durably recorded"
		m.state, m.modelStates[id] = RecoveryRequired, RecoveryRequired
		result := fmt.Errorf("%w: persist stop intent: %v", ErrUnavailable, err)
		m.completeStopLocked(result)
		m.mu.Unlock()
		return result
	}
	m.processes[instance.ProcessInstanceID] = instance
	if _, err := p.identityVerified(); err != nil {
		if reconcileErr := p.terminationObserved(m.config.ShutdownDuration()); reconcileErr == nil {
			instance.State, instance.TerminationStatus, instance.UpdatedAt = corecontract.ProcessTerminatedVerified, "TERMINATED_VERIFIED", time.Now().UTC()
			applyWaitResult(&instance, p)
			instance.DescendantStatus = "VERIFIED_OBSERVED_SCOPE"
			evidence := terminationEvidence(instance)
			instance.TerminationEvidenceID, instance.Evidence = evidence.EvidenceID, append(instance.Evidence, evidence)
			if persistErr := m.persistProcessLocked(instance); persistErr == nil {
				m.processes[instance.ProcessInstanceID] = instance
				delete(m.procs, id)
				delete(m.startedModels, id)
				if m.active == id {
					m.proc, m.active, m.state = nil, "", Stopped
				}
				m.modelStates[id] = Stopped
				m.finishedProc, m.finishedModel = nil, ""
				m.state = Stopped
				m.refreshLegacyStateLocked()
				m.completeStopLocked(nil)
			} else {
				m.recovery, m.recoveryReason = true, "verified process absence could not be durably recorded"
				m.state, m.modelStates[id] = RecoveryRequired, RecoveryRequired
				err := fmt.Errorf("%w: persist termination evidence: %v", ErrUnavailable, persistErr)
				m.completeStopLocked(err)
				m.mu.Unlock()
				return err
			}
			err := m.stopErr
			m.mu.Unlock()
			return err
		}
		instance.State, instance.ProcessIdentityStatus, instance.OwnershipStatus, instance.TerminationStatus = corecontract.ProcessRecoveryRequired, "UNVERIFIED", "UNKNOWN", "UNKNOWN"
		instance.UpdatedAt = time.Now().UTC()
		if persistErr := m.persistProcessLocked(instance); persistErr == nil {
			m.processes[instance.ProcessInstanceID] = instance
		}
		m.recovery, m.recoveryReason = true, "process identity changed before termination; no signal was sent"
		m.state, m.modelStates[id] = RecoveryRequired, RecoveryRequired
		m.completeStopLocked(ErrRecoveryRequired)
		m.mu.Unlock()
		return ErrRecoveryRequired
	}
	m.state, m.modelStates[id] = Stopping, Stopping
	instance.State, instance.TerminationStatus, instance.UpdatedAt = corecontract.ProcessTerminating, "TERMINATING", time.Now().UTC()
	if err := m.persistProcessLocked(instance); err != nil {
		m.recovery, m.recoveryReason = true, "termination state could not be durably recorded"
		m.state, m.modelStates[id] = RecoveryRequired, RecoveryRequired
		result := fmt.Errorf("%w: persist terminating state: %v", ErrUnavailable, err)
		m.completeStopLocked(result)
		m.mu.Unlock()
		return result
	}
	m.processes[instance.ProcessInstanceID] = instance
	m.mu.Unlock()

	result := m.terminate(p, m.config.ShutdownDuration())
	if result != nil {
		m.mu.Lock()
		unknown := instance
		unknown.State, unknown.TerminationStatus, unknown.DescendantStatus, unknown.UpdatedAt = corecontract.ProcessTerminationUnknown, "UNKNOWN", "UNKNOWN", time.Now().UTC()
		if persistErr := m.persistProcessLocked(unknown); persistErr == nil {
			m.processes[unknown.ProcessInstanceID] = unknown
		}
		m.recovery, m.recoveryReason = true, "process termination was not fully verifiable"
		err := fmt.Errorf("%w: stop process: %v", ErrUnavailable, result)
		m.completeStopLocked(err)
		m.mu.Unlock()
		return err
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		// The manager still owns the child and keeps waiting below.
		<-p.done
	}
	m.mu.Lock()
	final := instance
	final.State = corecontract.ProcessTerminatedVerified
	final.TerminationStatus, final.UpdatedAt = "TERMINATED_VERIFIED", time.Now().UTC()
	final.TerminationSignals = p.signalsSent()
	final.SignalRequestResult = signalRequestResult(final.TerminationSignals)
	final.SignalRequestedByLARM = len(final.TerminationSignals) > 0
	applyWaitResult(&final, p)
	final.DescendantStatus = "VERIFIED_OBSERVED_SCOPE"
	evidence := terminationEvidence(final)
	final.TerminationEvidenceID = evidence.EvidenceID
	final.Evidence = append(final.Evidence, evidence)
	if err := m.persistProcessLocked(final); err != nil {
		m.recovery, m.recoveryReason = true, "termination evidence could not be durably recorded"
		m.state, m.modelStates[id] = RecoveryRequired, RecoveryRequired
		result := fmt.Errorf("%w: persist termination evidence: %v", ErrUnavailable, err)
		m.completeStopLocked(result)
		m.mu.Unlock()
		return result
	}
	m.processes[final.ProcessInstanceID] = final
	if m.procs[id] == p {
		delete(m.procs, id)
		delete(m.startedModels, id)
		if m.active == id {
			m.proc, m.active, m.state = nil, "", Stopped
		}
		m.modelStates[id] = Stopped
		m.finishedProc, m.finishedModel = nil, ""
	}
	if m.finishedModel == id {
		m.finishedProc, m.finishedModel = nil, ""
		m.state = Stopped
		m.modelStates[id] = Stopped
	}
	m.refreshLegacyStateLocked()
	m.completeStopLocked(nil)
	m.mu.Unlock()
	return nil
}

// terminate returns nil only after the leader, process group and observed
// descendants are absent. Wait() alone is intentionally insufficient.
func (m *Manager) terminate(p *process, timeout time.Duration) error {
	if p == nil {
		return nil
	}
	if _, err := p.identityVerified(); err != nil {
		if observedErr := p.terminationObserved(timeout); observedErr == nil {
			return nil
		}
		return err
	}
	if _, err := p.observe(); err != nil {
		if observedErr := p.terminationObserved(timeout); observedErr == nil {
			return nil
		}
		return err
	}
	if err := p.signalGroup(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		if observedErr := p.terminationObserved(timeout); observedErr == nil {
			return nil
		}
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.terminationObserved(timeout)
	case <-timer.C:
	}
	// SIGTERM may have exited the leader at the same instant the timeout
	// fired. Re-check Wait before considering SIGKILL; a gone leader cannot
	// safely authorize a reused PGID.
	select {
	case <-p.done:
		return p.terminationObserved(timeout)
	default:
	}
	if _, err := p.identityVerified(); err != nil {
		if observedErr := p.terminationObserved(timeout); observedErr == nil {
			return nil
		}
		return fmt.Errorf("cannot safely escalate after identity change: %w", err)
	}
	if err := p.signalGroup(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		if observedErr := p.terminationObserved(timeout); observedErr == nil {
			return nil
		}
		return err
	}
	<-p.done
	return p.terminationObserved(timeout)
}

func (m *Manager) watch(p *process, id, instanceID string) {
	<-p.done
	if p.waitResult().errorClass == "wait_error" {
		m.markProcessLost(id, instanceID, "cmd.Wait returned an ambiguous result")
		return
	}
	if _, err := p.waitGroupGone(m.config.ShutdownDuration()); err != nil {
		m.markProcessLost(id, instanceID, "cannot enumerate associated processes after leader exit")
		return
	}
	if members, err := p.observe(); err != nil || len(members) != 0 {
		m.markProcessLost(id, instanceID, "leader exited while associated processes remained")
		return
	}
	m.mu.Lock()
	if m.procs[id] == p && m.modelStates[id] == Ready {
		instance, ok := m.modelProcessLocked(id)
		if !ok {
			m.mu.Unlock()
			return
		}
		instance.State, instance.TerminationStatus, instance.UpdatedAt = corecontract.ProcessFailed, "LEADER_EXITED", time.Now().UTC()
		instance.StartupDiagnostic = corecontract.StartupDiagnosticBackendExited
		applyWaitResult(&instance, p)
		if err := m.persistProcessLocked(instance); err != nil {
			m.recovery, m.recoveryReason = true, "unexpected process exit evidence was not durable"
		} else {
			m.processes[instance.ProcessInstanceID] = instance
		}
		m.finishedProc, m.finishedModel = p, id
		delete(m.procs, id)
		delete(m.startedModels, id)
		m.modelStates[id] = Failed
		if m.active == id {
			m.proc, m.state, m.active = nil, Failed, ""
		}
	}
	m.mu.Unlock()
}

func (m *Manager) markProcessLost(id, instanceID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.processes[instanceID]
	if !ok {
		return
	}
	instance.State, instance.OwnershipStatus, instance.TerminationStatus = corecontract.ProcessRecoveryRequired, "UNKNOWN", "UNKNOWN"
	instance.UpdatedAt = time.Now().UTC()
	m.recovery, m.recoveryReason = true, reason
	if err := m.persistProcessLocked(instance); err != nil {
		m.recoveryReason = reason + "; process evidence persistence failed"
	} else {
		m.processes[instanceID] = instance
	}
	if m.active == id {
		m.state, m.modelStates[id] = Failed, Failed
		m.active = ""
		m.proc = nil
	} else {
		m.modelStates[id] = Failed
	}
	delete(m.procs, id)
	delete(m.startedModels, id)
}

func (m *Manager) markStartFailure(id string, instance corecontract.ProcessInstance, p *process, category corecontract.StartupDiagnosticCategory, cancellationReason string, reason error, signalRequested bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.processes[instance.ProcessInstanceID]; ok {
		current.State, current.TerminationStatus, current.UpdatedAt = corecontract.ProcessFailed, "START_FAILED", time.Now().UTC()
		current.StartupDiagnostic = category
		current.StartupCancellationReason = cancellationReason
		current.StartupError = sanitizeStartupError(reason)
		current.SignalRequestedByLARM = signalRequested
		if p != nil {
			current.TerminationSignals = p.signalsSent()
			current.SignalRequestResult = signalRequestResult(current.TerminationSignals)
			current.SignalRequestedByLARM = signalRequested && len(current.TerminationSignals) > 0
			if processDone(p) {
				applyWaitResult(&current, p)
			}
		}
		if err := m.persistProcessLocked(current); err == nil {
			m.processes[current.ProcessInstanceID] = current
		} else {
			m.recovery, m.recoveryReason = true, "startup failure evidence was not durable"
		}
	}
	if m.active == id {
		m.proc, m.state, m.active = nil, Failed, ""
		m.modelStates[id] = Failed
	}
	delete(m.procs, id)
	delete(m.startedModels, id)
}

func (m *Manager) markStartupCleanupFailure(id string, instance corecontract.ProcessInstance, p *process, category corecontract.StartupDiagnosticCategory, cancellationReason string, reason, cleanupErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.processes[instance.ProcessInstanceID]; ok {
		current.State = corecontract.ProcessRecoveryRequired
		current.TerminationStatus = "UNKNOWN"
		current.StartupDiagnostic = category
		current.StartupCancellationReason = cancellationReason
		current.StartupError = sanitizeStartupError(reason)
		if p != nil {
			current.TerminationSignals = p.signalsSent()
			current.SignalRequestResult = signalRequestResult(current.TerminationSignals)
			current.SignalRequestedByLARM = len(current.TerminationSignals) > 0
			if processDone(p) {
				applyWaitResult(&current, p)
			}
		}
		if err := m.persistProcessLocked(current); err == nil {
			m.processes[current.ProcessInstanceID] = current
		}
	}
	m.recovery, m.recoveryReason = true, "spawned process could not be safely terminated after startup diagnostic: "+sanitizeStartupError(cleanupErr)
	m.state, m.active = RecoveryRequired, ""
	delete(m.procs, id)
	delete(m.startedModels, id)
}

func (m *Manager) markIdentityUnknown(id string, instance corecontract.ProcessInstance, reason error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.processes[instance.ProcessInstanceID]; ok {
		current.State, current.ProcessIdentityStatus, current.OwnershipStatus, current.TerminationStatus = corecontract.ProcessIdentityUnknown, "UNVERIFIED", "UNVERIFIED", "UNKNOWN"
		current.StartupDiagnostic = corecontract.StartupDiagnosticIdentityUnverified
		current.StartupError = sanitizeStartupError(reason)
		current.UpdatedAt = time.Now().UTC()
		if err := m.persistProcessLocked(current); err == nil {
			m.processes[current.ProcessInstanceID] = current
		}
	}
	m.recovery, m.recoveryReason = true, "process identity could not be verified: "+sanitizeStartupError(reason)
	m.state, m.active = RecoveryRequired, ""
	m.modelStates[id] = RecoveryRequired
}

func (m *Manager) failAfterSpawn(id string, p *process, instance corecontract.ProcessInstance, reason error) {
	if p != nil {
		if terminateErr := m.terminate(p, m.config.ShutdownDuration()); terminateErr != nil {
			m.mu.Lock()
			m.recovery, m.recoveryReason = true, "spawned process could not be safely terminated after persistence failure"
			m.state, m.active = RecoveryRequired, ""
			m.mu.Unlock()
			return
		}
	}
	m.markStartFailure(id, instance, p, corecontract.StartupDiagnosticUnknown, "", reason, false)
	m.mu.Lock()
	m.recovery = true
	if m.recoveryReason == "" {
		m.recoveryReason = "a spawned process crossed a persistence boundary without durable lifecycle evidence"
	}
	m.state = RecoveryRequired
	m.active = ""
	m.mu.Unlock()
}

func (m *Manager) publishProcess(instance corecontract.ProcessInstance) error {
	if err := corecontract.ValidateProcessInstance(instance); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.persistProcessLocked(instance); err != nil {
		return err
	}
	m.processes[instance.ProcessInstanceID] = instance
	return nil
}

func (m *Manager) publishProcessState(id string, state corecontract.ProcessState, mutate func(*corecontract.ProcessInstance)) error {
	m.mu.Lock()
	instance, ok := m.processes[id]
	if !ok {
		m.mu.Unlock()
		return errors.New("process instance not found")
	}
	instance.State = state
	mutate(&instance)
	instance.UpdatedAt = time.Now().UTC()
	if err := m.persistProcessLocked(instance); err != nil {
		m.recovery, m.recoveryReason = true, "process lifecycle evidence was not durable"
		m.mu.Unlock()
		return err
	}
	m.processes[id] = instance
	m.mu.Unlock()
	return nil
}

func (m *Manager) persistProcessLocked(instance corecontract.ProcessInstance) error {
	if m.journal == nil {
		return corecontract.ValidateProcessInstance(instance)
	}
	return m.journal.PersistProcess(instance)
}

func (m *Manager) currentProcessLocked() corecontract.ProcessInstance {
	if m.active != "" {
		if instance, ok := m.modelProcessLocked(m.active); ok {
			return instance
		}
	}
	for id := range m.procs {
		if instance, ok := m.modelProcessLocked(id); ok {
			return instance
		}
	}
	return corecontract.ProcessInstance{}
}

func (m *Manager) completeStopLocked(err error) {
	if m.stopDone == nil {
		return
	}
	m.stopErr = err
	done := m.stopDone
	m.stopDone = nil
	close(done)
}

func instanceForReady(m *Manager, id string) corecontract.ProcessInstance {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processes[id]
}

func (m *Manager) ProcessInstances() []corecontract.ProcessInstance {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]corecontract.ProcessInstance, 0, len(m.processes))
	for _, instance := range m.processes {
		result = append(result, instance)
	}
	return result
}

func (m *Manager) ProcessInstance(id string) (corecontract.ProcessInstance, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.processes[id]
	return instance, ok
}

func (m *Manager) CurrentProcess() (corecontract.ProcessInstance, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance := m.currentProcessLocked()
	if instance.ProcessInstanceID == "" || m.recovery {
		return instance, false
	}
	p := m.procs[instance.ModelID]
	if p == nil || m.modelStates[instance.ModelID] != Ready {
		return instance, false
	}
	if _, err := p.identityVerified(); err != nil {
		return instance, false
	}
	return instance, true
}

func (m *Manager) CurrentProcessForModel(id string) (corecontract.ProcessInstance, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.modelProcessLocked(id)
	if !ok || m.recovery || m.modelStates[id] != Ready {
		return instance, false
	}
	if _, err := m.procs[id].identityVerified(); err != nil {
		return instance, false
	}
	return instance, true
}

func (m *Manager) RecoveryRequired() (bool, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recovery, m.recoveryReason
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Status{Runtime: m.config.Backend, State: m.state, ActiveModel: m.active, RecoveryRequired: m.recovery, RecoveryReason: m.recoveryReason}
	if s.ActiveModel == "" {
		for id := range m.procs {
			s.ActiveModel = id
			break
		}
	}
	if instance := m.currentProcessLocked(); instance.ProcessInstanceID != "" {
		s.ProcessInstanceID, s.PGID = instance.ProcessInstanceID, instance.PGID
		s.ProcessIdentityStatus, s.OwnershipStatus, s.TerminationStatus = instance.ProcessIdentityStatus, instance.OwnershipStatus, instance.TerminationStatus
		s.DescendantStatus = "SUPERVISED"
		if m.procs[instance.ModelID] == nil {
			s.DescendantStatus = "UNKNOWN"
		}
	}
	if p := m.procs[s.ActiveModel]; p != nil {
		s.PID = p.identity.PID
		if m.modelStates[s.ActiveModel] == Ready {
			s.UptimeSeconds = int64(time.Since(m.startedModels[s.ActiveModel]).Seconds())
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
		if m.recovery && state == Stopped {
			state = RecoveryRequired
		}
		status := ModelStatus{ID: id, Path: model.Path, Port: model.Port, State: state, KeepAlive: model.KeepAlive, TimeAlive: model.TimeAlive, RecoveryState: "NORMAL", PreloadState: "NOT_REQUIRED"}
		if m.recovery {
			status.RecoveryState = "RECOVERY_REQUIRED"
		}
		if p := m.procs[id]; p != nil {
			status.PID = p.identity.PID
			status.ProcessInstanceID = p.instanceID
			status.Residency = "READY_RETAINED"
			if model.KeepAlive {
				status.Residency = "READY_PERSISTENT"
			}
			if instance, ok := m.modelProcessLocked(id); ok {
				status.OwnershipStatus = instance.OwnershipStatus
				status.ProcessIdentityStatus = instance.ProcessIdentityStatus
				status.TerminationStatus = instance.TerminationStatus
			}
		}
		status.CapacityOccupied = status.PID != 0 || state == Starting || state == Stopping
		result[id] = status
	}
	return result
}

type ModelStatus struct {
	ID                    string `json:"id"`
	Path                  string `json:"-"`
	Port                  int    `json:"port"`
	State                 State  `json:"state"`
	PID                   int    `json:"pid,omitempty"`
	ProcessInstanceID     string `json:"process_instance_id,omitempty"`
	KeepAlive             bool   `json:"keep_alive"`
	TimeAlive             string `json:"time_alive"`
	Residency             string `json:"residency,omitempty"`
	ActiveLeases          int    `json:"active_leases"`
	ProtectedInference    bool   `json:"protected_inference"`
	ResidenceRemaining    string `json:"residence_remaining,omitempty"`
	CapacityOccupied      bool   `json:"capacity_occupied"`
	RecoveryState         string `json:"recovery_state"`
	OwnershipStatus       string `json:"ownership_status,omitempty"`
	ProcessIdentityStatus string `json:"process_identity_status,omitempty"`
	TerminationStatus     string `json:"termination_status,omitempty"`
	PreloadState          string `json:"preload_state,omitempty"`
	PreloadError          string `json:"preload_error,omitempty"`
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.cancel()
	m.mu.Lock()
	ids := make([]string, 0, len(m.procs))
	for id := range m.procs {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		if err := m.Stop(ctx, id); err != nil && !errors.Is(err, ErrConflict) && !errors.Is(err, ErrRecoveryRequired) {
			return err
		}
	}
	return nil
}

func startupDiagnosticForContext(requestCtx, managerCtx, startupCtx context.Context) (corecontract.StartupDiagnosticCategory, string) {
	if errors.Is(startupCtx.Err(), context.DeadlineExceeded) {
		if requestCtx.Err() != nil || managerCtx.Err() != nil {
			return corecontract.StartupDiagnosticUnknown, "startup_timeout_and_cancellation_observed"
		}
		return corecontract.StartupDiagnosticReadinessTimeout, "startup_timeout"
	}
	if err := requestCtx.Err(); err != nil {
		return corecontract.StartupDiagnosticStartupCancelled, cancellationReason(err, "request_context_cancelled")
	}
	if err := managerCtx.Err(); err != nil {
		return corecontract.StartupDiagnosticStartupCancelled, cancellationReason(err, "manager_shutdown")
	}
	return corecontract.StartupDiagnosticUnknown, "startup_context_cancelled_without_attribution"
}

func cancellationReason(err error, fallback string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fallback + ":deadline_exceeded"
	}
	return fallback
}

func sanitizeStartupError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "context_cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "startup_timeout"
	case errors.Is(err, os.ErrNotExist):
		return "executable_not_found"
	case errors.Is(err, syscall.EACCES):
		return "permission_denied"
	case errors.Is(err, syscall.ENOEXEC):
		return "invalid_executable"
	default:
		// Do not persist error.Error(): exec errors can contain arbitrary paths
		// or command arguments supplied by configuration.
		return "startup_error"
	}
}

func processDone(p *process) bool {
	if p == nil {
		return false
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func applyWaitResult(instance *corecontract.ProcessInstance, p *process) {
	if instance == nil || p == nil || !processDone(p) {
		return
	}
	result := p.waitResult()
	instance.TerminatedAt = result.terminatedAt
	instance.WaitError = result.errorClass
	instance.ExitCode = result.exitCode
	instance.TerminationSignal = result.signal
}

func signalRequestResult(signals []string) string {
	for _, signal := range signals {
		if signal == "SIGKILL" {
			return "SIGKILL_SENT"
		}
	}
	for _, signal := range signals {
		if signal == "SIGTERM" {
			return "SIGTERM_SENT"
		}
	}
	return "NOT_SENT"
}

func configFingerprint(binary string, args []string) string {
	b, _ := json.Marshal(struct {
		Binary string   `json:"binary"`
		Args   []string `json:"args"`
	}{binary, args})
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func readyEvidence(instance corecontract.ProcessInstance, id string) corecontract.TerminalEvidence {
	return corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: id, SubjectKind: corecontract.SubjectProcess, SubjectID: instance.ProcessInstanceID, ProcessInstanceID: instance.ProcessInstanceID, Source: "larm-process-supervisor", Type: corecontract.EvidenceProcessReady, ObservedAt: time.Now().UTC(), Result: corecontract.EvidenceSuccess, CorrelationID: instance.ProcessInstanceID, Guarantee: corecontract.GuaranteeProcess, ProcessIdentityVerified: true}
}

func terminationEvidence(instance corecontract.ProcessInstance) corecontract.TerminalEvidence {
	id, _ := corecontract.NewID()
	return corecontract.TerminalEvidence{SchemaVersion: corecontract.ContractVersion, EvidenceID: id, SubjectKind: corecontract.SubjectProcess, SubjectID: instance.ProcessInstanceID, ProcessInstanceID: instance.ProcessInstanceID, Source: "larm-process-supervisor", Type: corecontract.EvidenceProcessTerminationVerified, ObservedAt: time.Now().UTC(), Result: corecontract.EvidenceSuccess, CorrelationID: instance.ProcessInstanceID, Guarantee: corecontract.GuaranteeProcess, ProcessAbsent: true, ProcessIdentityVerified: true, TransportTerminated: true}
}
