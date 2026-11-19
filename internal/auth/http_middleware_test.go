package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPMiddleware(t *testing.T) {
	store := NewTokenStore()
	rawSub, _ := GenerateRawToken("sub")
	_, _ = store.RegisterToken(rawSub, "sub-client", RoleSubmitter, nil, time.Time{})

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
	})

	// 1. Auth disabled
	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rr := httptest.NewRecorder()
	handlerDisabled := HTTPMiddleware(store, false, ScopeTasksRead, dummyHandler)
	handlerDisabled.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK when auth disabled, got %d", rr.Code)
	}

	// 2. Missing authorization header
	reqNoAuth := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rrNoAuth := httptest.NewRecorder()
	handlerEnabled := HTTPMiddleware(store, true, ScopeTasksRead, dummyHandler)
	handlerEnabled.ServeHTTP(rrNoAuth, reqNoAuth)
	if rrNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for missing header, got %d", rrNoAuth.Code)
	}

	// 3. Valid authorization header
	reqValid := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	reqValid.Header.Set("Authorization", "Bearer "+rawSub)
	rrValid := httptest.NewRecorder()
	handlerEnabled.ServeHTTP(rrValid, reqValid)
	if rrValid.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for valid token, got %d", rrValid.Code)
	}

	// 4. Forbidden (missing required scope)
	handlerDrain := HTTPMiddleware(store, true, ScopeWorkersDrain, dummyHandler)
	reqDrain := httptest.NewRequest(http.MethodPost, "/drain", nil)
	reqDrain.Header.Set("Authorization", "Bearer "+rawSub)
	rrDrain := httptest.NewRecorder()
	handlerDrain.ServeHTTP(rrDrain, reqDrain)
	if rrDrain.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for insufficient scope, got %d", rrDrain.Code)
	}
}
