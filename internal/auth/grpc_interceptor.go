package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MethodScopeMap defines which permission scope is required for specific gRPC full methods.
var MethodScopeMap = map[string]Scope{
	"/proto.DistribuQ/SubmitTask":     ScopeTasksSubmit,
	"/proto.DistribuQ/GetTaskStatus":  ScopeTasksRead,
	"/proto.DistribuQ/CancelTask":     ScopeTasksCancel,
	"/proto.DistribuQ/StreamTaskLogs": ScopeLogsRead,
	"/proto.DistribuQ/GetClusterState": ScopeClusterRead,
	"/proto.DistribuQ/DrainWorker":    ScopeWorkersDrain,
}

// UnaryAuthInterceptor returns a gRPC UnaryServerInterceptor that enforces token authorization.
// If authEnabled is false, the interceptor allows all calls unconditionally.
func UnaryAuthInterceptor(store *TokenStore, authEnabled bool) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if !authEnabled {
			return handler(ctx, req)
		}

		requiredScope, requiresAuth := MethodScopeMap[info.FullMethod]
		if !requiresAuth {
			// Method does not require explicit scope check (e.g. heartbeat or public RPCs)
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "authorization header missing")
		}

		rawToken := strings.TrimPrefix(authHeaders[0], "Bearer ")
		rawToken = strings.TrimSpace(rawToken)

		if rawToken == "" {
			return nil, status.Errorf(codes.Unauthenticated, "bearer token required")
		}

		_, err := store.CheckPermission(rawToken, requiredScope)
		if err != nil {
			if errorsIsForbidden(err) {
				return nil, status.Errorf(codes.PermissionDenied, "forbidden: %v", err)
			}
			return nil, status.Errorf(codes.Unauthenticated, "unauthorized: %v", err)
		}

		return handler(ctx, req)
	}
}

func errorsIsForbidden(err error) bool {
	return strings.Contains(err.Error(), ErrForbidden.Error())
}
