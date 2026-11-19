package auth

import "time"

// Scope defines a permission scope within the DistribuQ cluster.
type Scope string

const (
	ScopeTasksSubmit  Scope = "tasks:submit"
	ScopeTasksRead    Scope = "tasks:read"
	ScopeTasksCancel  Scope = "tasks:cancel"
	ScopeLogsRead     Scope = "logs:read"
	ScopeWorkersDrain Scope = "workers:drain"
	ScopeClusterRead  Scope = "cluster:read"
	ScopeAdmin        Scope = "admin:*"
)

// Role represents a pre-defined set of permission scopes.
type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
	RoleSubmitter Role = "submitter"
)

// DefaultRoleScopes maps known roles to their assigned scopes.
var DefaultRoleScopes = map[Role][]Scope{
	RoleAdmin: {
		ScopeAdmin,
		ScopeTasksSubmit,
		ScopeTasksRead,
		ScopeTasksCancel,
		ScopeLogsRead,
		ScopeWorkersDrain,
		ScopeClusterRead,
	},
	RoleOperator: {
		ScopeTasksSubmit,
		ScopeTasksRead,
		ScopeTasksCancel,
		ScopeLogsRead,
		ScopeWorkersDrain,
		ScopeClusterRead,
	},
	RoleSubmitter: {
		ScopeTasksSubmit,
		ScopeTasksRead,
		ScopeLogsRead,
	},
	RoleViewer: {
		ScopeTasksRead,
		ScopeClusterRead,
		ScopeLogsRead,
	},
}

// Token represents an issued API token with its associated identity and scopes.
type Token struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Role        Role      `json:"role"`
	Scopes      []Scope   `json:"scopes"`
	HashedToken string    `json:"hashed_token"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// HasScope checks whether a token has the requested scope or the admin wildcard scope.
func (t *Token) HasScope(req Scope) bool {
	for _, s := range t.Scopes {
		if s == ScopeAdmin || s == req {
			return true
		}
	}
	return false
}
