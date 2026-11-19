package auth

import (
	"encoding/json"
	"net/http"
	"strings"
)

// HTTPMiddleware wraps an http.Handler to enforce token authentication and authorization.
func HTTPMiddleware(store *TokenStore, authEnabled bool, requiredScope Scope, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authEnabled {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeJSONError(w, http.StatusUnauthorized, "authorization header missing")
			return
		}

		rawToken := strings.TrimPrefix(authHeader, "Bearer ")
		rawToken = strings.TrimSpace(rawToken)

		if rawToken == "" {
			writeJSONError(w, http.StatusUnauthorized, "bearer token required")
			return
		}

		_, err := store.CheckPermission(rawToken, requiredScope)
		if err != nil {
			if errorsIsForbidden(err) {
				writeJSONError(w, http.StatusForbidden, err.Error())
				return
			}
			writeJSONError(w, http.StatusUnauthorized, err.Error())
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
