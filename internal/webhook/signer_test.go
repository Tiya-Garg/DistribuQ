package webhook

import (
	"testing"
)

func TestSignAndVerifySignature(t *testing.T) {
	secret := "super-secret-key"
	payload := []byte(`{"event":"task.completed","task_id":"t1"}`)

	sig := SignPayload(secret, payload)
	if sig == "" {
		t.Fatalf("expected non-empty signature")
	}

	if !VerifySignature(secret, payload, sig) {
		t.Fatalf("expected signature verification to succeed")
	}

	if VerifySignature("wrong-secret", payload, sig) {
		t.Fatalf("expected signature verification to fail with wrong secret")
	}

	tamperedPayload := []byte(`{"event":"task.completed","task_id":"t2"}`)
	if VerifySignature(secret, tamperedPayload, sig) {
		t.Fatalf("expected signature verification to fail for tampered payload")
	}

	// Empty secret handling
	if SignPayload("", payload) != "" {
		t.Fatalf("expected empty signature when secret is blank")
	}
	if !VerifySignature("", payload, "") {
		t.Fatalf("expected empty signature verification to succeed for empty secret")
	}
}
