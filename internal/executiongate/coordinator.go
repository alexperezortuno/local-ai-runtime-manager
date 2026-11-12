package executiongate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type diskState struct {
	Version    int                  `json:"version"`
	Executions map[string]Execution `json:"executions"`
	Requests   map[string]Request   `json:"requests"`
	Owners     map[string]string    `json:"owners"`
}

// Coordinator shares authority through a durable directory. All operations
// use a short OS-level file lock, so separate wrapper processes serialize the
// metadata transaction without serializing inference on different sessions.
type Coordinator struct {
	dir string
}

func Open(dir string) (*Coordinator, error) {
	if dir == "" {
		return nil, errors.New("coordinator state directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create coordinator directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect coordinator directory: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("coordinator directory must not be accessible by group or others")
	}
	c := &Coordinator{dir: dir}
	lockPath := filepath.Join(dir, "coordinator.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		lock, err = os.OpenFile(lockPath, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, fmt.Errorf("open coordinator lock: %w", err)
	}
	if created {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
			_ = lock.Close()
			return nil, fmt.Errorf("initialize coordinator lock: %w", err)
		}
		s := diskState{Version: 1, Executions: map[string]Execution{}, Requests: map[string]Request{}, Owners: map[string]string{}}
		err := c.save(s)
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	_ = lock.Close()
	if err := c.transact(context.Background(), func(s *diskState) (bool, error) {
		changed := false
		for id, execution := range s.Executions {
			if execution.State == ExecutionActive && execution.LeaseState == "ACQUIRING" && execution.LeaseID == "" {
				execution.State = ExecutionUnknown
				s.Executions[id] = execution
				changed = true
			}
		}
		return changed, nil
	}); err != nil {
		return nil, err
	}
	return c, nil
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:16]), nil
}

func (c *Coordinator) CreateExecution(ctx context.Context, in CreateExecutionInput) (Execution, error) {
	if !uuidV4.MatchString(in.ExecutionID) || in.Initiator == "" {
		return Execution{}, errors.New("execution_id must be UUIDv4 and initiator is required")
	}
	var result Execution
	err := c.transact(ctx, func(s *diskState) (bool, error) {
		if old, ok := s.Executions[in.ExecutionID]; ok {
			if old.State == ExecutionDeleted || old.ParentExecutionID != in.ParentExecutionID || old.Initiator != in.Initiator {
				return false, ErrConflict
			}
			result = old
			return false, nil
		}
		if in.ParentExecutionID != "" {
			parent, ok := s.Executions[in.ParentExecutionID]
			if !ok || parent.State == ExecutionDeleted || parent.TerminalAt != nil || parent.ExecutionID == in.ExecutionID {
				return false, ErrNotFound
			}
		}
		result = Execution{ExecutionID: in.ExecutionID, ParentExecutionID: in.ParentExecutionID, Initiator: in.Initiator, State: ExecutionCreated, CreatedAt: time.Now().UTC()}
		s.Executions[in.ExecutionID] = result
		return true, nil
	})
	return result, err
}

func (c *Coordinator) AcquireSession(ctx context.Context, serverID, sessionID, executionID string) (AcquireResult, error) {
	if err := validScope(serverID, sessionID); err != nil {
		return AcquireError, err
	}
	if !uuidV4.MatchString(executionID) {
		return AcquireError, errors.New("execution_id must be UUIDv4")
	}
	if ctx.Err() != nil {
		return Cancelled, ErrCancelled
	}
	result := AcquireError
	err := c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State == ExecutionDeleted {
			return false, ErrNotFound
		}
		key := scopeKey(serverID, sessionID)
		if owner := s.Owners[key]; owner != "" {
			if owner == executionID {
				result = Acquired
				return false, nil
			}
			result = Busy
			return false, nil
		}
		if e.State != ExecutionCreated || (e.SessionID != "" && (e.ServerID != serverID || e.SessionID != sessionID)) {
			return false, ErrConflict
		}
		e.ServerID, e.SessionID, e.State = serverID, sessionID, ExecutionActive
		s.Executions[executionID] = e
		s.Owners[key] = executionID
		result = Acquired
		return true, nil
	})
	if errors.Is(err, ErrCancelled) {
		return Cancelled, err
	}
	if err != nil {
		return AcquireError, err
	}
	return result, nil
}

func (c *Coordinator) BeginLeaseAcquisition(ctx context.Context, executionID string, in LeaseIntentInput) error {
	if !uuidV4.MatchString(executionID) || !uuidV4.MatchString(in.IdempotencyKey) || in.ModelID == "" {
		return errors.New("lease intent requires UUIDv4 execution and idempotency key IDs and a model")
	}
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State != ExecutionActive || e.SessionID == "" || s.Owners[scopeKey(e.ServerID, e.SessionID)] != executionID {
			return false, ErrNotOwner
		}
		if e.LeaseState != "" {
			if e.ModelID == in.ModelID && e.IdempotencyKey == in.IdempotencyKey && e.LeaseState == "ACQUIRING" {
				return false, nil
			}
			return false, ErrConflict
		}
		for otherID, other := range s.Executions {
			if otherID != executionID && other.IdempotencyKey == in.IdempotencyKey {
				return false, ErrConflict
			}
		}
		e.ModelID, e.IdempotencyKey, e.LeaseState = in.ModelID, in.IdempotencyKey, "ACQUIRING"
		s.Executions[executionID] = e
		return true, nil
	})
}

func (c *Coordinator) BindLease(ctx context.Context, executionID string, in BindLeaseInput) error {
	if !uuidV4.MatchString(executionID) || !uuidV4.MatchString(in.IdempotencyKey) || !uuidV4.MatchString(in.LeaseID) || in.ModelID == "" || in.LeaseState != "ACTIVE" {
		return errors.New("active lease binding requires UUIDv4 execution, idempotency and lease IDs and a model")
	}
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State != ExecutionActive || e.SessionID == "" || s.Owners[scopeKey(e.ServerID, e.SessionID)] != executionID {
			return false, ErrNotOwner
		}
		if e.LeaseID != "" {
			if e.ModelID == in.ModelID && e.IdempotencyKey == in.IdempotencyKey && e.LeaseID == in.LeaseID {
				return false, nil
			}
			return false, ErrConflict
		}
		if e.LeaseState != "" && (e.ModelID != in.ModelID || e.IdempotencyKey != in.IdempotencyKey || e.LeaseState != "ACQUIRING") {
			return false, ErrConflict
		}
		for otherID, other := range s.Executions {
			if otherID != executionID && (other.IdempotencyKey == in.IdempotencyKey || other.LeaseID == in.LeaseID) {
				return false, ErrConflict
			}
		}
		e.ModelID, e.IdempotencyKey, e.LeaseID, e.LeaseState = in.ModelID, in.IdempotencyKey, in.LeaseID, "ACTIVE"
		s.Executions[executionID] = e
		return true, nil
	})
}

func (c *Coordinator) CreateRequest(ctx context.Context, requestID, executionID, modelID string, stream bool) (Request, error) {
	if !uuidV4.MatchString(requestID) || !uuidV4.MatchString(executionID) || modelID == "" {
		return Request{}, errors.New("request_id and execution_id must be UUIDv4; model_id is required")
	}
	var result Request
	err := c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State != ExecutionActive || e.SessionID == "" || e.LeaseID == "" || s.Owners[scopeKey(e.ServerID, e.SessionID)] != executionID {
			return false, ErrNotOwner
		}
		if e.ModelID != modelID {
			return false, ErrConflict
		}
		if _, exists := s.Requests[requestID]; exists {
			return false, ErrConflict
		}
		now := time.Now().UTC()
		result = Request{RequestID: requestID, ExecutionID: executionID, SessionID: e.SessionID, ModelID: modelID, LeaseID: e.LeaseID, Stream: stream, State: RequestCreated, CreatedAt: now, UpdatedAt: now}
		s.Requests[requestID] = result
		e.RequestIDs = append(e.RequestIDs, requestID)
		s.Executions[executionID] = e
		return true, nil
	})
	return result, err
}

func (c *Coordinator) StartRequest(ctx context.Context, executionID, requestID string) error {
	return c.updateRequest(ctx, executionID, requestID, func(s *diskState, r *Request, e Execution) error {
		if e.State != ExecutionActive || s.Owners[scopeKey(e.ServerID, e.SessionID)] != executionID || r.State != RequestCreated {
			return ErrConflict
		}
		r.State = RequestInFlight
		return nil
	})
}

type RequestEvent struct {
	HeadersReceived bool `json:"headers_received,omitempty"`
	HTTPStatus      int  `json:"http_status,omitempty"`
	BodyStarted     bool `json:"body_started,omitempty"`
	SSEDone         bool `json:"sse_done,omitempty"`
	EOF             bool `json:"eof,omitempty"`
	TransportError  bool `json:"transport_error,omitempty"`
	CancelConfirmed bool `json:"cancel_confirmed,omitempty"`
}

func (c *Coordinator) ObserveRequest(ctx context.Context, executionID, requestID string, ev RequestEvent) (Request, error) {
	var result Request
	err := c.updateRequest(ctx, executionID, requestID, func(s *diskState, r *Request, e Execution) error {
		if r.State != RequestInFlight {
			return ErrConflict
		}
		if ev.HTTPStatus != 0 && (ev.HTTPStatus < 100 || ev.HTTPStatus > 599) {
			return errors.New("http_status must be between 100 and 599")
		}
		if ev.HTTPStatus != 0 && r.HTTPStatus != 0 && r.HTTPStatus != ev.HTTPStatus {
			return ErrConflict
		}
		r.HeadersReceived = r.HeadersReceived || ev.HeadersReceived
		if ev.HTTPStatus != 0 {
			r.HeadersReceived = true
			r.HTTPStatus = ev.HTTPStatus
		}
		r.BodyStarted = r.BodyStarted || ev.BodyStarted
		r.SSEDone = r.SSEDone || ev.SSEDone
		r.EOF = r.EOF || ev.EOF
		r.TransportError = r.TransportError || ev.TransportError
		r.CancelConfirmed = r.CancelConfirmed || ev.CancelConfirmed
		switch {
		case r.CancelConfirmed:
			r.State = RequestCancelled
		case r.TransportError:
			r.State = RequestFailed
		case r.EOF && r.HTTPStatus >= 300:
			r.State = RequestFailed
		case r.EOF && r.Stream && !r.SSEDone:
			r.State = RequestFailed // EOF without the SSE terminal marker is truncated.
		case r.EOF && (!r.Stream || r.SSEDone):
			r.State = RequestCompleted
		}
		r.UpdatedAt = time.Now().UTC()
		result = *r
		return nil
	})
	return result, err
}

func (c *Coordinator) updateRequest(ctx context.Context, executionID, requestID string, update func(*diskState, *Request, Execution) error) error {
	return c.transact(ctx, func(s *diskState) (bool, error) {
		r, ok := s.Requests[requestID]
		if !ok {
			return false, ErrNotFound
		}
		if r.ExecutionID != executionID {
			return false, ErrConflict
		}
		e, ok := s.Executions[executionID]
		if !ok || e.State == ExecutionDeleted {
			return false, ErrNotFound
		}
		if err := update(s, &r, e); err != nil {
			return false, err
		}
		s.Requests[requestID] = r
		return true, nil
	})
}

func (c *Coordinator) SetExecutionState(ctx context.Context, executionID string, state ExecutionState) error {
	if state != ExecutionUnknown && state != ExecutionClientDisconnected {
		return errors.New("only UNKNOWN or CLIENT_DISCONNECTED may be marked without terminal evidence")
	}
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State == ExecutionDeleted {
			return false, ErrNotFound
		}
		if e.State == ExecutionUnknown {
			if state == ExecutionUnknown {
				return false, nil
			}
			return false, ErrRecoveryRequired
		}
		if e.State != ExecutionActive && e.State != ExecutionFinalizing && e.State != ExecutionClientDisconnected {
			return false, ErrConflict
		}
		e.State = state
		s.Executions[executionID] = e
		return true, nil
	})
}

func (c *Coordinator) BeginFinalize(ctx context.Context, executionID string, state ExecutionState, reason string) error {
	if (state != ExecutionCompleted && state != ExecutionCancelled && state != ExecutionFailed) || strings.TrimSpace(reason) == "" {
		return ErrNotTerminal
	}
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State == ExecutionDeleted {
			return false, ErrNotFound
		}
		if e.State == ExecutionUnknown {
			return false, ErrRecoveryRequired
		}
		if e.State == ExecutionFinalizing {
			if e.TerminalState == state && e.ReleaseReason == reason {
				return false, nil
			}
			return false, ErrConflict
		}
		if e.State != ExecutionActive && e.State != ExecutionClientDisconnected {
			return false, ErrConflict
		}
		e.State, e.TerminalState, e.ReleaseReason = ExecutionFinalizing, state, reason
		s.Executions[executionID] = e
		return true, nil
	})
}

// Finalize is the only normal release path. Disconnection and UNKNOWN are not
// terminal evidence; callers must confirm all request endings first.
func (c *Coordinator) Finalize(ctx context.Context, executionID string, state ExecutionState, reason string) error {
	if (state != ExecutionCompleted && state != ExecutionCancelled && state != ExecutionFailed) || strings.TrimSpace(reason) == "" {
		return ErrNotTerminal
	}
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok || e.State == ExecutionDeleted {
			return false, ErrNotFound
		}
		if e.State == ExecutionUnknown {
			return false, ErrRecoveryRequired
		}
		if e.State == ExecutionCreated {
			if e.SessionID != "" || len(e.RequestIDs) != 0 || state != ExecutionFailed {
				return false, ErrConflict
			}
		} else if e.State != ExecutionActive && e.State != ExecutionFinalizing && e.State != ExecutionClientDisconnected {
			return false, ErrConflict
		}
		if e.State == ExecutionFinalizing && (e.TerminalState != state || e.ReleaseReason != reason) {
			return false, ErrConflict
		}
		for _, id := range e.RequestIDs {
			r := s.Requests[id]
			if r.State == RequestCreated || r.State == RequestInFlight {
				return false, ErrActive
			}
		}
		if e.SessionID != "" {
			key := scopeKey(e.ServerID, e.SessionID)
			if s.Owners[key] != executionID {
				return false, ErrNotOwner
			}
			delete(s.Owners, key)
		}
		now := time.Now().UTC()
		e.State, e.ReleaseReason, e.TerminalAt = state, reason, &now
		e.TerminalState = state
		s.Executions[executionID] = e
		return true, nil
	})
}

// Cleanup removes terminal associations and request details, leaving a small
// execution tombstone so stale lifecycle events cannot be mistaken for a new run.
func (c *Coordinator) Cleanup(ctx context.Context, executionID string) error {
	return c.transact(ctx, func(s *diskState) (bool, error) {
		e, ok := s.Executions[executionID]
		if !ok {
			return false, ErrNotFound
		}
		if e.State == ExecutionDeleted {
			return false, nil
		}
		if e.TerminalAt == nil {
			return false, ErrNotTerminal
		}
		for _, child := range s.Executions {
			if child.ParentExecutionID == executionID && child.State != ExecutionDeleted {
				return false, ErrActive
			}
		}
		for _, id := range e.RequestIDs {
			if r, ok := s.Requests[id]; ok && (r.State == RequestCreated || r.State == RequestInFlight) {
				return false, ErrActive
			}
			delete(s.Requests, id)
		}
		now := time.Now().UTC()
		e = Execution{ExecutionID: executionID, State: ExecutionDeleted, DeletedAt: &now}
		s.Executions[executionID] = e
		return true, nil
	})
}

func (c *Coordinator) Snapshot(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	err := c.transact(ctx, func(s *diskState) (bool, error) {
		out = Snapshot{Executions: cloneExecutions(s.Executions), Requests: cloneRequests(s.Requests), Owners: cloneOwners(s.Owners)}
		for _, e := range s.Executions {
			switch {
			case e.State == ExecutionDeleted:
				out.Counts.DeletedExecutions++
			case e.TerminalAt != nil:
				out.Counts.TerminalExecutions++
				out.Counts.PendingCleanup++
			default:
				out.Counts.ActiveExecutions++
			}
		}
		out.Counts.OwnedSessions = len(s.Owners)
		for _, r := range s.Requests {
			if r.State == RequestCreated || r.State == RequestInFlight {
				out.Counts.ActiveRequests++
			}
		}
		return false, nil
	})
	return out, err
}

func validScope(serverID, sessionID string) error {
	if serverID == "" || sessionID == "" || strings.ContainsAny(serverID+sessionID, "\x00\r\n") || len(serverID) > 512 || len(sessionID) > 512 {
		return errors.New("server_id and session_id must be non-empty bounded opaque values")
	}
	return nil
}

func scopeKey(serverID, sessionID string) string { return serverID + "\x00" + sessionID }

func (c *Coordinator) transact(ctx context.Context, fn func(*diskState) (bool, error)) error {
	lock, err := os.OpenFile(filepath.Join(c.dir, "coordinator.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("open coordinator lock: %w", err)
	}
	defer lock.Close()
	for {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			break
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("lock coordinator: %w", err)
		}
		select {
		case <-ctx.Done():
			return ErrCancelled
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err := ctx.Err(); err != nil {
		return ErrCancelled
	}
	s, err := c.load()
	if err != nil {
		return err
	}
	changed, opErr := fn(&s)
	if opErr != nil || !changed {
		return opErr
	}
	return c.save(s)
}

func (c *Coordinator) load() (diskState, error) {
	path := filepath.Join(c.dir, "registry.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return diskState{}, errors.New("coordinator registry is missing; refusing to assume the gate is empty")
	}
	if err != nil {
		return diskState{}, fmt.Errorf("read coordinator registry: %w", err)
	}
	var s diskState
	if err := json.Unmarshal(data, &s); err != nil || s.Version != 1 || s.Executions == nil || s.Requests == nil || s.Owners == nil {
		return diskState{}, errors.New("coordinator registry is corrupt or has unsupported version; refusing ownership changes")
	}
	return s, nil
}

func (c *Coordinator) save(s diskState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dir, ".registry-*.tmp")
	if err != nil {
		return fmt.Errorf("create coordinator journal: %w", err)
	}
	name := f.Name()
	defer os.Remove(name)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(c.dir, "registry.json")); err != nil {
		return fmt.Errorf("commit coordinator journal: %w", err)
	}
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func cloneExecutions(in map[string]Execution) map[string]Execution {
	out := make(map[string]Execution, len(in))
	for k, v := range in {
		v.RequestIDs = append([]string(nil), v.RequestIDs...)
		out[k] = v
	}
	return out
}

func cloneRequests(in map[string]Request) map[string]Request {
	out := make(map[string]Request, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneOwners(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
