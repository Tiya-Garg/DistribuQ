package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// SignPayload computes an HMAC-SHA256 signature for a payload body given a shared secret.
func SignPayload(secret string, body []byte) string {
	if secret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature validates that a hex-encoded signature matches the expected HMAC-SHA256 digest.
func VerifySignature(secret string, body []byte, signature string) bool {
	if secret == "" {
		return signature == ""
	}
	expected := SignPayload(secret, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
