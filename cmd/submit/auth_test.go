package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestSubmitTokenContextInjection(t *testing.T) {
	token := "dq_test_secret_token_123"
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata to be present")
	}

	auths := md.Get("authorization")
	if len(auths) == 0 || auths[0] != "Bearer "+token {
		t.Fatalf("unexpected authorization header: %v", auths)
	}
}
