// Package adminauth implements the private, local administrative channel.
// It is intentionally separate from the public HTTP server.
package adminauth

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/recovery"
)

type PeerIdentity struct {
	UID int
	GID int
	PID int
}

func (p PeerIdentity) String() string { return fmt.Sprintf("uid:%d", p.UID) }

type Server struct {
	listener     net.Listener
	path         string
	created      os.FileInfo
	service      *recovery.Service
	policy       recovery.CapabilityPolicy
	config       config.AdminConfig
	modelStopper ModelStopper
	stop         chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

// ModelStopper is implemented by Scheduler. The admin socket never signals a
// process directly; the scheduler remains the serialization authority.
type ModelStopper interface {
	ManualStop(context.Context, string) error
}

type request struct {
	Operation        string               `json:"operation"`
	IncidentID       string               `json:"incident_id,omitempty"`
	PlanID           string               `json:"plan_id,omitempty"`
	AllowedOperation string               `json:"allowed_operation,omitempty"`
	TTLSeconds       int                  `json:"ttl_seconds,omitempty"`
	Capability       *recovery.Capability `json:"capability,omitempty"`
	ModelID          string               `json:"model_id,omitempty"`
}

type response struct {
	OK         bool                        `json:"ok"`
	Error      string                      `json:"error,omitempty"`
	Decision   recovery.CapabilityDecision `json:"decision,omitempty"`
	Reason     string                      `json:"reason,omitempty"`
	Identity   string                      `json:"identity,omitempty"`
	Payload    *recovery.Capability        `json:"payload,omitempty"`
	Capability map[string]any              `json:"capability,omitempty"`
	Phase      string                      `json:"phase,omitempty"`
}

func NewServer(cfg config.AdminConfig, instanceID string, service *recovery.Service) (*Server, error) {
	if !cfg.Enabled {
		return nil, errors.New("administrative authentication is disabled")
	}
	if service == nil || cfg.AllowedUID == nil || cfg.SocketPath == "" || cfg.PublicKeyPath == "" {
		return nil, errors.New("admin authentication configuration is incomplete")
	}
	keys, err := loadPublicKeys(cfg.PublicKeyPath)
	if err != nil {
		return nil, err
	}
	audience := cfg.Audience
	if audience == "" {
		audience = "larm-admin"
	}
	var stopper ModelStopper
	return &Server{path: cfg.SocketPath, service: service, policy: recovery.CapabilityPolicy{Issuer: recovery.CapabilityIssuer, Audience: audience, InstanceID: instanceID, AllowedUID: *cfg.AllowedUID, AllowedKeys: keys, AllowedActions: map[string]bool{recovery.RecoveryReconcileAction: true, recovery.ModelStopAction: true}}, config: cfg, modelStopper: stopper, stop: make(chan struct{})}, nil
}

// SetModelStopper wires the already-created Scheduler without changing the
// existing constructor used by recovery validation fixtures.
func (s *Server) SetModelStopper(stopper ModelStopper) { s.modelStopper = stopper }

func (s *Server) Listen() error {
	if err := secureParent(filepath.Dir(s.path)); err != nil {
		return err
	}
	if info, err := os.Lstat(s.path); err == nil {
		return fmt.Errorf("admin socket already exists and cannot be attributed safely: %s", info.Mode())
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", s.path)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.path, 0600); err != nil {
		_ = listener.Close()
		_ = os.Remove(s.path)
		return err
	}
	info, err := os.Lstat(s.path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 || info.Sys() == nil {
		_ = listener.Close()
		_ = os.Remove(s.path)
		return errors.New("admin socket failed private ownership and permission checks")
	}
	if uid, ok := fileOwner(info); ok && uid != os.Geteuid() {
		_ = listener.Close()
		_ = os.Remove(s.path)
		return errors.New("admin socket is not owned by the LARM process user")
	}
	s.listener, s.created = listener, info
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
			}
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(conn) }()
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	identity, err := peerIdentity(conn)
	if err != nil || identity.UID != s.policy.AllowedUID {
		writeResponse(conn, response{Error: "administrative peer identity is not authorized"})
		return
	}
	var req request
	decoder := json.NewDecoder(io.LimitReader(bufio.NewReader(conn), 1<<20))
	if err := decoder.Decode(&req); err != nil {
		writeResponse(conn, response{Error: "invalid administrative request"})
		return
	}
	operator := identity.String()
	switch req.Operation {
	case "status":
		writeResponse(conn, response{OK: true, Identity: operator})
	case "issue_prepare":
		ttl := time.Duration(req.TTLSeconds) * time.Second
		payload, err := s.service.PrepareCapability(recovery.CapabilityRequest{IncidentID: req.IncidentID, PlanID: req.PlanID, AllowedOperation: req.AllowedOperation, ModelID: req.ModelID, TTL: ttl}, operator, s.policy, time.Now().UTC())
		if err != nil {
			if auditErr := s.service.RecordCapabilityAttempt(req.IncidentID, req.PlanID, operator, s.policy.InstanceID, "CAPABILITY_PREPARE_REJECTED", "REJECTED", err.Error()); auditErr != nil {
				err = auditErr
			}
			writeResponse(conn, response{Error: err.Error()})
			return
		}
		writeResponse(conn, response{OK: true, Payload: &payload})
	case "issue_commit", "inspect":
		if req.Capability == nil {
			writeResponse(conn, response{Error: "capability is required"})
			return
		}
		result, err := s.service.AuthorizeCapability(*req.Capability, operator, s.policy, time.Now().UTC())
		if err != nil {
			writeResponse(conn, response{Decision: result.Decision, Reason: result.Reason, Error: err.Error()})
			return
		}
		if result.Decision != recovery.Authorized {
			writeResponse(conn, response{Decision: result.Decision, Reason: result.Reason})
			return
		}
		writeResponse(conn, response{OK: true, Decision: result.Decision, Capability: map[string]any{"capability_id": req.Capability.CapabilityID, "incident_id": req.Capability.IncidentID, "plan_id": req.Capability.PlanID, "model_id": req.Capability.ModelID, "expires_at": req.Capability.ExpiresAt, "allowed_operation": req.Capability.AllowedOperation}})
	case "reconcile":
		if req.Capability == nil {
			writeResponse(conn, response{Error: "capability is required"})
			return
		}
		reserved, err := s.service.ReserveCapability(*req.Capability, operator, s.policy, time.Now().UTC())
		if err != nil || reserved.Decision != recovery.Authorized {
			writeResponse(conn, response{Decision: reserved.Decision, Reason: reserved.Reason, Error: errorText(err, reserved.Reason)})
			return
		}
		auth := recovery.Authorization{IncidentID: req.Capability.IncidentID, PlanID: req.Capability.PlanID, SnapshotVersion: req.Capability.SnapshotVersion, EvidenceDigest: req.Capability.EvidenceDigest, OperatorID: operator, Authenticated: true, Mechanism: "unix-peer+ed25519", CapabilityID: req.Capability.CapabilityID, Action: req.Capability.AllowedOperation}
		if err := s.service.Execute(auth); err != nil {
			phase := ""
			if operation, operationErr := s.service.Operation(req.Capability.IncidentID, req.Capability.PlanID); operationErr == nil {
				phase = string(operation.Phase)
			}
			writeResponse(conn, response{Decision: recovery.Rejected, Reason: err.Error(), Error: err.Error(), Phase: phase})
			return
		}
		consumed, consumeErr := s.service.ConsumeCapability(*req.Capability, operator, s.policy, time.Now().UTC())
		if consumeErr != nil || consumed.Decision != recovery.Authorized && consumed.Decision != recovery.Replayed {
			writeResponse(conn, response{Decision: consumed.Decision, Reason: consumed.Reason, Error: errorText(consumeErr, consumed.Reason)})
			return
		}
		writeResponse(conn, response{OK: true, Decision: recovery.Authorized, Phase: "COMPLETE", Capability: map[string]any{"capability_id": req.Capability.CapabilityID, "incident_id": req.Capability.IncidentID, "plan_id": req.Capability.PlanID, "result": "RECONCILED"}})
	case "model_stop":
		if s.modelStopper == nil {
			writeResponse(conn, response{Error: "model stop authority is unavailable"})
			return
		}
		if req.Capability == nil || req.Capability.AllowedOperation != recovery.ModelStopAction || req.Capability.ModelID == "" || req.ModelID != req.Capability.ModelID {
			writeResponse(conn, response{Error: "model stop capability scope is invalid"})
			return
		}
		reserved, err := s.service.ReserveCapability(*req.Capability, operator, s.policy, time.Now().UTC())
		if err != nil || reserved.Decision != recovery.Authorized {
			writeResponse(conn, response{Decision: reserved.Decision, Reason: reserved.Reason, Error: errorText(err, reserved.Reason)})
			return
		}
		if err := s.service.RecordAdministrativeAction(req.Capability.ModelID, operator, "MODEL_STOP_REQUESTED", "REQUESTED", ""); err != nil {
			writeResponse(conn, response{Decision: recovery.Rejected, Reason: err.Error(), Error: err.Error()})
			return
		}
		if err := s.modelStopper.ManualStop(context.Background(), req.Capability.ModelID); err != nil {
			_ = s.service.RecordAdministrativeAction(req.Capability.ModelID, operator, "MODEL_STOP", "REJECTED", err.Error())
			writeResponse(conn, response{Decision: recovery.Rejected, Reason: err.Error(), Error: err.Error()})
			return
		}
		if err := s.service.RecordAdministrativeAction(req.Capability.ModelID, operator, "MODEL_STOP", "STOPPED", ""); err != nil {
			writeResponse(conn, response{Decision: recovery.Rejected, Reason: err.Error(), Error: err.Error()})
			return
		}
		consumed, consumeErr := s.service.ConsumeCapability(*req.Capability, operator, s.policy, time.Now().UTC())
		if consumeErr != nil || consumed.Decision != recovery.Authorized && consumed.Decision != recovery.Replayed {
			writeResponse(conn, response{Decision: consumed.Decision, Reason: consumed.Reason, Error: errorText(consumeErr, consumed.Reason)})
			return
		}
		writeResponse(conn, response{OK: true, Decision: consumed.Decision, Phase: "COMPLETE", Capability: map[string]any{"capability_id": req.Capability.CapabilityID, "model_id": req.Capability.ModelID, "result": "STOPPED"}})
	default:
		writeResponse(conn, response{Error: "unknown administrative operation"})
	}
}

func errorText(err error, fallback string) string {
	if err != nil {
		return err.Error()
	}
	return fallback
}

func writeResponse(conn net.Conn, value response) {
	data, _ := json.Marshal(value)
	_, _ = conn.Write(append(data, '\n'))
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stop)
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.wg.Wait()
		if s.listener != nil {
			if info, statErr := os.Lstat(s.path); statErr == nil && os.SameFile(info, s.created) {
				if removeErr := os.Remove(s.path); err == nil {
					err = removeErr
				}
			}
		}
	})
	return err
}

func secureParent(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	parts := strings.Split(filepath.Clean(abs), string(filepath.Separator))
	current := string(filepath.Separator)
	if parts[0] == "" {
		parts = parts[1:]
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			if err := os.Mkdir(current, 0700); err != nil {
				return err
			}
			info, statErr = os.Lstat(current)
		}
		// Ancestor aliases such as macOS /var -> /private/var are resolved by
		// the OS; the socket itself is still rejected if it is a symlink.
		if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			resolved, resolveErr := filepath.EvalSymlinks(current)
			if resolveErr != nil {
				return resolveErr
			}
			current = resolved
			info, statErr = os.Lstat(current)
		}
		if statErr != nil || !info.IsDir() {
			return errors.New("admin socket parent must be a private, non-symlink directory")
		}
	}
	info, err := os.Lstat(abs)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("admin socket parent must be a private, non-symlink directory")
	}
	if uid, ok := fileOwner(info); ok && uid != os.Geteuid() {
		return errors.New("admin socket parent is not owned by the LARM process user")
	}
	return nil
}
