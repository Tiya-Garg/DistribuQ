package auth

import (
	"testing"
	"time"
)

func TestTokenStoreRegisterAndValidate(t *testing.T) {
	store := NewTokenStore()
	raw, err := GenerateRawToken("test")
	if err != nil {
		t.Fatalf("GenerateRawToken failed: %v", err)
	}

	token, err := store.RegisterToken(raw, "ci-runner", RoleSubmitter, nil, time.Time{})
	if err != nil {
		t.Fatalf("RegisterToken failed: %v", err)
	}

	if token.Name != "ci-runner" {
		t.Errorf("expected name ci-runner, got %s", token.Name)
	}

	validToken, err := store.Validate(raw)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if validToken.ID != token.ID {
		t.Errorf("token ID mismatch: expected %s, got %s", token.ID, validToken.ID)
	}

	// Non-existent token
	if _, err := store.Validate("invalid_token"); err != ErrTokenNotFound {
		t.Errorf("expected ErrTokenNotFound, got %v", err)
	}

	// Empty token
	if _, err := store.Validate(""); err != ErrUnauthorized {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}
}

func TestTokenStoreCheckPermission(t *testing.T) {
	store := NewTokenStore()
	rawSub, _ := GenerateRawToken("sub")
	_, _ = store.RegisterToken(rawSub, "dev-submitter", RoleSubmitter, nil, time.Time{})

	rawAdmin, _ := GenerateRawToken("adm")
	_, _ = store.RegisterToken(rawAdmin, "admin-user", RoleAdmin, nil, time.Time{})

	// Submitter should have tasks:submit
	if _, err := store.CheckPermission(rawSub, ScopeTasksSubmit); err != nil {
		t.Errorf("expected submitter to have ScopeTasksSubmit, got %v", err)
	}

	// Submitter should NOT have workers:drain
	if _, err := store.CheckPermission(rawSub, ScopeWorkersDrain); err == nil {
		t.Errorf("expected error for workers:drain on submitter, got nil")
	}

	// Admin should have workers:drain via wildcard/role
	if _, err := store.CheckPermission(rawAdmin, ScopeWorkersDrain); err != nil {
		t.Errorf("expected admin to have ScopeWorkersDrain, got %v", err)
	}
}

func TestTokenStoreExpiration(t *testing.T) {
	store := NewTokenStore()
	raw, _ := GenerateRawToken("exp")
	expiredTime := time.Now().UTC().Add(-1 * time.Hour)
	_, _ = store.RegisterToken(raw, "expired-token", RoleViewer, nil, expiredTime)

	if _, err := store.Validate(raw); err != ErrTokenExpired {
		t.Errorf("expected ErrTokenExpired, got %v", err)
	}
}
