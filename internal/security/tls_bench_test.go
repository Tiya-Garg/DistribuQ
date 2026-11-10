package security

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTLSConfigLoading(b *testing.B) {
	dir := b.TempDir()
	caPEM, certPEM, keyPEM, err := GenerateTestPKI([]string{"localhost"})
	if err != nil {
		b.Fatalf("GenerateTestPKI: %v", err)
	}

	caCert := filepath.Join(dir, "ca.crt")
	nodeCert := filepath.Join(dir, "node.crt")
	nodeKey := filepath.Join(dir, "node.key")

	_ = os.WriteFile(caCert, caPEM, 0o644)
	_ = os.WriteFile(nodeCert, certPEM, 0o644)
	_ = os.WriteFile(nodeKey, keyPEM, 0o600)

	cfg := &TLSConfig{
		CACertPath: caCert,
		CertPath:   nodeCert,
		KeyPath:    nodeKey,
		ServerName: "localhost",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cfg.ServerOption(); err != nil {
			b.Fatalf("ServerOption failed: %v", err)
		}
	}
}
