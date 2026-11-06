package main

import (
	"errors"
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// shouldRetryError determines if an RPC error should trigger trying the next replica.
func shouldRetryError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true // network/dial error, retry next
	}
	switch st.Code() {
	case codes.AlreadyExists, codes.InvalidArgument:
		return false // non-retryable terminal errors
	case codes.FailedPrecondition:
		// Not the leader -> retry next
		return true
	default:
		return true
	}
}

func TestShouldRetryError(t *testing.T) {
	if shouldRetryError(nil) {
		t.Error("expected nil error not to be retried")
	}

	alreadyExistsErr := status.Error(codes.AlreadyExists, "idempotency key conflict")
	if shouldRetryError(alreadyExistsErr) {
		t.Error("expected AlreadyExists not to be retried")
	}

	invalidArgErr := status.Error(codes.InvalidArgument, "task id empty")
	if shouldRetryError(invalidArgErr) {
		t.Error("expected InvalidArgument not to be retried")
	}

	notLeaderErr := status.Error(codes.FailedPrecondition, scheduler.ErrNotLeader.Error())
	if !shouldRetryError(notLeaderErr) {
		t.Error("expected ErrNotLeader to be retried on next replica")
	}

	dialErr := errors.New("dial tcp: connection refused")
	if !shouldRetryError(dialErr) {
		t.Error("expected network dial error to be retried")
	}
}
