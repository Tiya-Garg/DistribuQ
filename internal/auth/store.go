package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTokenNotFound = errors.New("auth: token not found")
	ErrTokenExpired  = errors.New("auth: token has expired")
	ErrUnauthorized  = errors.New("auth: unauthorized access")
	ErrForbidden     = errors.New("auth: insufficient permissions")
)

// TokenStore manages tokens in memory with thread safety.
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]*Token // key: hashed token
}

// NewTokenStore initializes an empty TokenStore.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		tokens: make(map[string]*Token),
	}
}

// HashToken computes the SHA-256 hex digest of a raw token string.
func HashToken(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

// GenerateRawToken generates a cryptographically secure random token string.
func GenerateRawToken(prefix string) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}
	if prefix == "" {
		prefix = "dq"
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b)), nil
}

// RegisterToken registers a raw token with an identity, role, custom scopes, and expiration.
func (s *TokenStore) RegisterToken(rawToken, name string, role Role, customScopes []Scope, expiresAt time.Time) (*Token, error) {
	if rawToken == "" {
		return nil, errors.New("raw token cannot be empty")
	}

	scopes := make([]Scope, 0)
	if defaultScopes, ok := DefaultRoleScopes[role]; ok {
		scopes = append(scopes, defaultScopes...)
	}
	if len(customScopes) > 0 {
		scopes = append(scopes, customScopes...)
	}

	hashed := HashToken(rawToken)

	t := &Token{
		ID:          hashed[:12],
		Name:        name,
		Role:        role,
		Scopes:      scopes,
		HashedToken: hashed,
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   expiresAt,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[hashed] = t
	return t, nil
}

// Validate verifies a raw token and returns the Token metadata if valid.
func (s *TokenStore) Validate(rawToken string) (*Token, error) {
	if rawToken == "" {
		return nil, ErrUnauthorized
	}

	hashed := HashToken(rawToken)

	s.mu.RLock()
	defer s.mu.RUnlock()

	t, exists := s.tokens[hashed]
	if !exists {
		return nil, ErrTokenNotFound
	}

	if !t.ExpiresAt.IsZero() && time.Now().UTC().After(t.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	return t, nil
}

// CheckPermission validates the raw token and verifies if it holds the required scope.
func (s *TokenStore) CheckPermission(rawToken string, requiredScope Scope) (*Token, error) {
	t, err := s.Validate(rawToken)
	if err != nil {
		return nil, err
	}

	if !t.HasScope(requiredScope) {
		return nil, fmt.Errorf("%w: missing required scope '%s'", ErrForbidden, requiredScope)
	}

	return t, nil
}

// Count returns the number of active registered tokens.
func (s *TokenStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tokens)
}
