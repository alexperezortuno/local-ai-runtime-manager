package corecontract

import "fmt"

// ErrorCode identifies a rejected contract operation without exposing
// implementation-specific errors from a future adapter.
type ErrorCode string

const (
	ErrCodeInvalidVersion     ErrorCode = "INVALID_VERSION"
	ErrCodeInvalidIdentity    ErrorCode = "INVALID_IDENTITY"
	ErrCodeInvalidState       ErrorCode = "INVALID_STATE"
	ErrCodeInvalidTransition  ErrorCode = "INVALID_TRANSITION"
	ErrCodeInvariantViolation ErrorCode = "INVARIANT_VIOLATION"
	ErrCodeOwnershipViolation ErrorCode = "OWNERSHIP_VIOLATION"
	ErrCodeEvidenceRequired   ErrorCode = "EVIDENCE_REQUIRED"
	ErrCodeEvidenceMismatch   ErrorCode = "EVIDENCE_MISMATCH"
	ErrCodeTerminal           ErrorCode = "TERMINAL_STATE"
	ErrCodeRecoveryRequired   ErrorCode = "RECOVERY_REQUIRED"
)

// ValidationError is returned by all pure contract validators.
type ValidationError struct {
	Code   ErrorCode
	Entity string
	Field  string
	Detail string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s %s: %s", e.Entity, e.Code, e.Detail)
	}
	return fmt.Sprintf("%s %s (%s): %s", e.Entity, e.Code, e.Field, e.Detail)
}

func invalid(code ErrorCode, entity, field, detail string) error {
	return &ValidationError{Code: code, Entity: entity, Field: field, Detail: detail}
}
