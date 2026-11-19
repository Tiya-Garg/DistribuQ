package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestStatusTokenContextInjection(t *testing.T) {
	token := "dq_status_readonly_token"
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
