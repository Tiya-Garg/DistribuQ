package auth

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func dummyHandler(ctx context.Context, req interface{}) (interface{}, error) {
	return "ok", nil
}

func TestUnaryAuthInterceptorDisabled(t *testing.T) {
	store := NewTokenStore()
	interceptor := UnaryAuthInterceptor(store, false)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/proto.DistribuQ/SubmitTask",
	}

	resp, err := interceptor(context.Background(), nil, info, dummyHandler)
	if err != nil {
		t.Fatalf("expected nil error when auth is disabled, got %v", err)
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}
}

func TestUnaryAuthInterceptorAuthorized(t *testing.T) {
	store := NewTokenStore()
	raw, _ := GenerateRawToken("submit")
	_, _ = store.RegisterToken(raw, "submit-agent", RoleSubmitter, nil, time.Time{})

	interceptor := UnaryAuthInterceptor(store, true)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/proto.DistribuQ/SubmitTask",
	}

	md := metadata.Pairs("authorization", "Bearer "+raw)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	resp, err := interceptor(ctx, nil, info, dummyHandler)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}
}

func TestUnaryAuthInterceptorMissingHeader(t *testing.T) {
	store := NewTokenStore()
	interceptor := UnaryAuthInterceptor(store, true)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/proto.DistribuQ/SubmitTask",
	}

	_, err := interceptor(context.Background(), nil, info, dummyHandler)
	if err == nil {
		t.Fatal("expected error for missing metadata, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated code, got %v", st.Code())
	}
}

func TestUnaryAuthInterceptorForbidden(t *testing.T) {
	store := NewTokenStore()
	raw, _ := GenerateRawToken("viewer")
	_, _ = store.RegisterToken(raw, "viewer-agent", RoleViewer, nil, time.Time{})

	interceptor := UnaryAuthInterceptor(store, true)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/proto.DistribuQ/SubmitTask", // viewer does not have tasks:submit
	}

	md := metadata.Pairs("authorization", "Bearer "+raw)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptor(ctx, nil, info, dummyHandler)
	if err == nil {
		t.Fatal("expected forbidden error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied code, got %v", st.Code())
	}
}
