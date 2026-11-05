package recovery

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	CapabilityVersion       = 1
	CapabilityAlgorithm     = "Ed25519"
	CapabilityIssuer        = "larm-admin"
	RecoveryReconcileAction = "recovery.reconcile"
	DefaultCapabilityTTL    = 5 * time.Minute
	MaxCapabilityTTL        = 15 * time.Minute
)

var (
	ErrCapabilityInvalid = errors.New("invalid administrative capability")
	ErrCapabilityExpired = errors.New("administrative capability expired")
	ErrCapabilityReplay  = errors.New("administrative capability replayed")
	ErrCapabilityScope   = errors.New("administrative capability scope is invalid")
)

type CapabilityDecision string

const (
	Authorized       CapabilityDecision = "AUTHORIZED"
	Rejected         CapabilityDecision = "REJECTED"
	Expired          CapabilityDecision = "EXPIRED"
	StalePlan        CapabilityDecision = "STALE_PLAN"
	Replayed         CapabilityDecision = "REPLAYED"
	InvalidSignature CapabilityDecision = "INVALID_SIGNATURE"
	InvalidOperator  CapabilityDecision = "INVALID_OPERATOR"
	InvalidScope     CapabilityDecision = "INVALID_SCOPE"
)

// Capability is the complete signed envelope. Signature is over every other
// field in canonical field order; no field is accepted from an operator ID
// supplied by the client.
type Capability struct {
	CapabilityID     string    `json:"capability_id"`
	Version          int       `json:"version"`
	Issuer           string    `json:"issuer"`
	Audience         string    `json:"audience"`
	InstanceID       string    `json:"instance_id"`
	OperatorIdentity string    `json:"operator_identity"`
	IncidentID       string    `json:"incident_id"`
	PlanID           string    `json:"plan_id"`
	SnapshotVersion  string    `json:"snapshot_version"`
	EvidenceDigest   string    `json:"evidence_digest"`
	IssuedAt         time.Time `json:"issued_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	Nonce            string    `json:"nonce"`
	AllowedOperation string    `json:"allowed_operation"`
	KeyID            string    `json:"key_id"`
	Algorithm        string    `json:"algorithm"`
	Signature        string    `json:"signature"`
}

type capabilityPayload struct {
	CapabilityID     string    `json:"capability_id"`
	Version          int       `json:"version"`
	Issuer           string    `json:"issuer"`
	Audience         string    `json:"audience"`
	InstanceID       string    `json:"instance_id"`
	OperatorIdentity string    `json:"operator_identity"`
	IncidentID       string    `json:"incident_id"`
	PlanID           string    `json:"plan_id"`
	SnapshotVersion  string    `json:"snapshot_version"`
	EvidenceDigest   string    `json:"evidence_digest"`
	IssuedAt         time.Time `json:"issued_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	Nonce            string    `json:"nonce"`
	AllowedOperation string    `json:"allowed_operation"`
	KeyID            string    `json:"key_id"`
	Algorithm        string    `json:"algorithm"`
}

type CapabilityRequest struct {
	IncidentID       string
	PlanID           string
	AllowedOperation string
	TTL              time.Duration
}

type CapabilityPolicy struct {
	Issuer         string
	Audience       string
	InstanceID     string
	AllowedUID     int
	AllowedKeys    map[string]ed25519.PublicKey
	AllowedActions map[string]bool
}

type CapabilityResult struct {
	Decision CapabilityDecision
	Reason   string
}

// RecordCapabilityAttempt records rejected requests that do not contain a
// structurally valid capability, such as an unknown plan.
func (s *Service) RecordCapabilityAttempt(incidentID, planID, operator, instanceID, action, result, reason string) error {
	if incidentID == "" {
		incidentID = "UNSPECIFIED"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendAuditLocked(AuditEvent{IncidentID: incidentID, PlanID: planID, Actor: operator, OperatorIdentity: operator, InstanceID: instanceID, AuthorizationMechanism: "unix-peer+ed25519", Action: action, Result: result, Rejection: reason}); err != nil {
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return s.storeError
	}
	return nil
}

type capabilityRecord struct {
	CapabilityID     string    `json:"capability_id"`
	Fingerprint      string    `json:"fingerprint"`
	IncidentID       string    `json:"incident_id"`
	PlanID           string    `json:"plan_id"`
	EvidenceDigest   string    `json:"evidence_digest"`
	AllowedOperation string    `json:"allowed_operation"`
	ReservedAt       time.Time `json:"reserved_at"`
	Consumed         bool      `json:"consumed"`
}

// PrepareCapability creates the unsigned, server-bound payload. The private
// signing key remains in the external admin client, never in LARM.
func (s *Service) PrepareCapability(req CapabilityRequest, operator string, policy CapabilityPolicy, now time.Time) (Capability, error) {
	if strings.TrimSpace(operator) == "" || strings.TrimSpace(req.IncidentID) == "" || strings.TrimSpace(req.AllowedOperation) == "" {
		return Capability{}, fmt.Errorf("%w: incomplete request", ErrCapabilityScope)
	}
	if !policy.AllowedActions[req.AllowedOperation] {
		return Capability{}, fmt.Errorf("%w: operation is not allowed", ErrCapabilityScope)
	}
	if req.TTL <= 0 {
		req.TTL = DefaultCapabilityTTL
	}
	if req.TTL > MaxCapabilityTTL {
		return Capability{}, fmt.Errorf("%w: capability TTL is too long", ErrCapabilityScope)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := s.Refresh(); err != nil {
		return Capability{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, ok := s.plans[req.PlanID]
	if !ok || plan.IncidentID != req.IncidentID {
		return Capability{}, ErrNotFound
	}
	current := s.planForIncidentLocked(req.IncidentID)
	if current.PlanID != req.PlanID || current.SnapshotVersion != plan.SnapshotVersion || current.EvidenceDigest != plan.EvidenceDigest {
		return Capability{}, ErrPlanStale
	}
	incident := s.incidents[req.IncidentID]
	if incident.Status == StatusResolved {
		return Capability{}, fmt.Errorf("%w: incident is already resolved", ErrCapabilityScope)
	}
	id, err := randomID()
	if err != nil {
		return Capability{}, err
	}
	nonce, err := randomID()
	if err != nil {
		return Capability{}, err
	}
	issuer := policy.Issuer
	if issuer == "" {
		issuer = CapabilityIssuer
	}
	audience := policy.Audience
	if audience == "" {
		audience = "larm-admin"
	}
	capability := Capability{CapabilityID: id, Version: CapabilityVersion, Issuer: issuer, Audience: audience, InstanceID: policy.InstanceID, OperatorIdentity: operator, IncidentID: req.IncidentID, PlanID: req.PlanID, SnapshotVersion: plan.SnapshotVersion, EvidenceDigest: plan.EvidenceDigest, IssuedAt: now.UTC(), ExpiresAt: now.UTC().Add(req.TTL), Nonce: nonce, AllowedOperation: req.AllowedOperation, Algorithm: CapabilityAlgorithm}
	if err := s.appendAuditLocked(AuditEvent{IncidentID: req.IncidentID, PlanID: req.PlanID, Actor: operator, OperatorIdentity: operator, CapabilityID: capability.CapabilityID, EvidenceDigest: plan.EvidenceDigest, InstanceID: policy.InstanceID, AuthorizationMechanism: "unix-peer+ed25519", Action: "CAPABILITY_PREPARED", Result: "PREPARED"}); err != nil {
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return Capability{}, s.storeError
	}
	return capability, nil
}

func (c Capability) Payload() ([]byte, error) {
	return json.Marshal(capabilityPayload{CapabilityID: c.CapabilityID, Version: c.Version, Issuer: c.Issuer, Audience: c.Audience, InstanceID: c.InstanceID, OperatorIdentity: c.OperatorIdentity, IncidentID: c.IncidentID, PlanID: c.PlanID, SnapshotVersion: c.SnapshotVersion, EvidenceDigest: c.EvidenceDigest, IssuedAt: c.IssuedAt.UTC(), ExpiresAt: c.ExpiresAt.UTC(), Nonce: c.Nonce, AllowedOperation: c.AllowedOperation, KeyID: c.KeyID, Algorithm: c.Algorithm})
}

func (c Capability) Sign(private ed25519.PrivateKey, keyID string) (Capability, error) {
	if len(private) != ed25519.PrivateKeySize || strings.TrimSpace(keyID) == "" {
		return Capability{}, errors.New("capability signing key is invalid")
	}
	c.KeyID = keyID
	payload, err := c.Payload()
	if err != nil {
		return Capability{}, err
	}
	c.Signature = base64.RawStdEncoding.EncodeToString(ed25519.Sign(private, payload))
	return c, nil
}

func (c Capability) VerifySignature(keys map[string]ed25519.PublicKey) error {
	if c.Algorithm != CapabilityAlgorithm || c.Version != CapabilityVersion || c.KeyID == "" || c.Signature == "" {
		return ErrCapabilityInvalid
	}
	key, ok := keys[c.KeyID]
	if !ok || len(key) != ed25519.PublicKeySize {
		return ErrCapabilityInvalid
	}
	signature, err := base64.RawStdEncoding.DecodeString(c.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return ErrCapabilityInvalid
	}
	payload, err := c.Payload()
	if err != nil || !ed25519.Verify(key, payload, signature) {
		return ErrCapabilityInvalid
	}
	// Keep an explicit constant-time check for the decoded signature length and
	// content path; ed25519.Verify itself performs the cryptographic check.
	if subtle.ConstantTimeEq(int32(len(signature)), ed25519.SignatureSize) != 1 {
		return ErrCapabilityInvalid
	}
	return nil
}

func (s *Service) ValidateCapability(c Capability, operator string, policy CapabilityPolicy, now time.Time) CapabilityResult {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := c.VerifySignature(policy.AllowedKeys); err != nil {
		return CapabilityResult{Decision: InvalidSignature, Reason: err.Error()}
	}
	if c.ExpiresAt.IsZero() || !now.Before(c.ExpiresAt) {
		return CapabilityResult{Decision: Expired, Reason: ErrCapabilityExpired.Error()}
	}
	if c.IssuedAt.IsZero() || c.IssuedAt.After(now.Add(time.Minute)) {
		return CapabilityResult{Decision: Rejected, Reason: "capability issued outside the accepted clock window"}
	}
	uidText := strings.TrimPrefix(operator, "uid:")
	uid, uidErr := strconv.Atoi(uidText)
	if !strings.HasPrefix(operator, "uid:") || uidErr != nil || uid != policy.AllowedUID || c.Issuer != policy.Issuer || c.Audience != policy.Audience || c.InstanceID != policy.InstanceID || c.Version != CapabilityVersion || c.Algorithm != CapabilityAlgorithm || c.OperatorIdentity != operator || c.AllowedOperation == "" || !policy.AllowedActions[c.AllowedOperation] {
		return CapabilityResult{Decision: InvalidScope, Reason: ErrCapabilityScope.Error()}
	}
	if err := s.Refresh(); err != nil {
		return CapabilityResult{Decision: Rejected, Reason: err.Error()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revokedNonce[c.Nonce] {
		return CapabilityResult{Decision: Replayed, Reason: "capability nonce has been revoked"}
	}
	plan, ok := s.plans[c.PlanID]
	if !ok || plan.IncidentID != c.IncidentID {
		return CapabilityResult{Decision: Rejected, Reason: ErrNotFound.Error()}
	}
	current := s.planForIncidentLocked(c.IncidentID)
	if current.PlanID != c.PlanID || current.SnapshotVersion != c.SnapshotVersion || current.EvidenceDigest != c.EvidenceDigest {
		return CapabilityResult{Decision: StalePlan, Reason: ErrPlanStale.Error()}
	}
	if c.CapabilityID == "" || c.Nonce == "" || c.EvidenceDigest == "" || c.SnapshotVersion == "" {
		return CapabilityResult{Decision: InvalidScope, Reason: ErrCapabilityScope.Error()}
	}
	if record, ok := s.capabilities[c.CapabilityID]; ok {
		if record.Fingerprint != capabilityFingerprint(c) {
			return CapabilityResult{Decision: Replayed, Reason: ErrCapabilityReplay.Error()}
		}
		return CapabilityResult{Decision: Replayed, Reason: ErrCapabilityReplay.Error()}
	}
	return CapabilityResult{Decision: Authorized}
}

// RevokeNonce is the durable emergency revocation hook. Key rotation is
// represented by removing a key ID from the loaded public keyring.
func (s *Service) RevokeNonce(nonce string) error {
	if strings.TrimSpace(nonce) == "" {
		return errors.New("capability nonce is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revokedNonce[nonce] = true
	if err := s.persistLocked(); err != nil {
		delete(s.revokedNonce, nonce)
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return s.storeError
	}
	return nil
}

// AuthorizeCapability audits every validation attempt. It does not reserve or
// consume the capability and therefore cannot imply that recovery executed.
func (s *Service) AuthorizeCapability(c Capability, operator string, policy CapabilityPolicy, now time.Time) (CapabilityResult, error) {
	result := s.ValidateCapability(c, operator, policy, now)
	s.mu.Lock()
	defer s.mu.Unlock()
	incidentID := c.IncidentID
	if incidentID == "" {
		incidentID = "UNSPECIFIED"
	}
	event := AuditEvent{IncidentID: incidentID, PlanID: c.PlanID, Actor: operator, OperatorIdentity: operator, CapabilityID: c.CapabilityID, EvidenceDigest: c.EvidenceDigest, InstanceID: c.InstanceID, AuthorizationMechanism: "unix-peer+ed25519", Action: "CAPABILITY_VALIDATED", Result: string(result.Decision), Rejection: result.Reason}
	if event.Actor == "" {
		event.Actor = "UNVERIFIED"
	}
	if err := s.appendAuditLocked(event); err != nil {
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return CapabilityResult{Decision: Rejected, Reason: ErrAuditPersistence.Error()}, s.storeError
	}
	return result, nil
}

// ReserveCapability is the future executor boundary. Validation alone never
// marks a capability as used; reservation does, durably and atomically under
// the recovery sidecar lock.
func (s *Service) ReserveCapability(c Capability, operator string, policy CapabilityPolicy, now time.Time) (CapabilityResult, error) {
	result, err := s.AuthorizeCapability(c, operator, policy, now)
	if result.Decision == Replayed {
		s.mu.Lock()
		record, exists := s.capabilities[c.CapabilityID]
		s.mu.Unlock()
		if exists && record.Fingerprint == capabilityFingerprint(c) && record.IncidentID == c.IncidentID && record.PlanID == c.PlanID && record.EvidenceDigest == c.EvidenceDigest && record.AllowedOperation == c.AllowedOperation {
			return CapabilityResult{Decision: Authorized, Reason: "durable reservation replay"}, err
		}
	}
	if result.Decision != Authorized {
		return result, err
	}
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if record, ok := s.capabilities[c.CapabilityID]; ok {
		if record.Fingerprint == capabilityFingerprint(c) && record.IncidentID == c.IncidentID && record.PlanID == c.PlanID && record.EvidenceDigest == c.EvidenceDigest && record.AllowedOperation == c.AllowedOperation {
			return CapabilityResult{Decision: Authorized, Reason: "durable reservation replay"}, nil
		}
		return CapabilityResult{Decision: Replayed, Reason: ErrCapabilityReplay.Error()}, nil
	}
	s.capabilities[c.CapabilityID] = capabilityRecord{CapabilityID: c.CapabilityID, Fingerprint: capabilityFingerprint(c), IncidentID: c.IncidentID, PlanID: c.PlanID, EvidenceDigest: c.EvidenceDigest, AllowedOperation: c.AllowedOperation, ReservedAt: time.Now().UTC()}
	if err := s.appendAuditLocked(AuditEvent{IncidentID: c.IncidentID, PlanID: c.PlanID, Actor: operator, OperatorIdentity: operator, CapabilityID: c.CapabilityID, EvidenceDigest: c.EvidenceDigest, InstanceID: c.InstanceID, AuthorizationMechanism: "unix-peer+ed25519", Action: "CAPABILITY_RESERVED", Result: "RESERVED"}); err != nil {
		delete(s.capabilities, c.CapabilityID)
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return CapabilityResult{Decision: Rejected, Reason: ErrAuditPersistence.Error()}, s.storeError
	}
	return result, nil
}

func (s *Service) ConsumeCapability(c Capability, operator string, policy CapabilityPolicy, now time.Time) (CapabilityResult, error) {
	result, err := s.AuthorizeCapability(c, operator, policy, now)
	if result.Decision == Replayed {
		s.mu.Lock()
		record, exists := s.capabilities[c.CapabilityID]
		if exists && record.Fingerprint == capabilityFingerprint(c) && !record.Consumed {
			result = CapabilityResult{Decision: Authorized, Reason: "durable reservation consumed"}
		}
		s.mu.Unlock()
	}
	if result.Decision != Authorized {
		return result, err
	}
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.capabilities[c.CapabilityID]
	if !ok || record.Fingerprint != capabilityFingerprint(c) || record.Consumed {
		return CapabilityResult{Decision: Replayed, Reason: ErrCapabilityReplay.Error()}, nil
	}
	record.Consumed = true
	s.capabilities[c.CapabilityID] = record
	if err := s.appendAuditLocked(AuditEvent{IncidentID: c.IncidentID, PlanID: c.PlanID, Actor: operator, OperatorIdentity: operator, CapabilityID: c.CapabilityID, EvidenceDigest: c.EvidenceDigest, InstanceID: c.InstanceID, AuthorizationMechanism: "unix-peer+ed25519", Action: "CAPABILITY_CONSUMED", Result: "CONSUMED"}); err != nil {
		record.Consumed = false
		s.capabilities[c.CapabilityID] = record
		s.storeError = fmt.Errorf("%w: %v", ErrAuditPersistence, err)
		return CapabilityResult{Decision: Rejected, Reason: ErrAuditPersistence.Error()}, s.storeError
	}
	return result, nil
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func capabilityFingerprint(c Capability) string {
	payload, _ := c.Payload()
	return fmt.Sprintf("%x:%s", payload, c.Signature)
}
