package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLSConfig holds the filesystem paths to the certificates required for mTLS.
type TLSConfig struct {
	CACertPath string
	CertPath   string
	KeyPath    string
	ServerName string
}

// IsEnabled returns true if all required TLS certificate files are specified.
func (c *TLSConfig) IsEnabled() bool {
	return c != nil && c.CACertPath != "" && c.CertPath != "" && c.KeyPath != ""
}

// ServerOption returns a grpc.ServerOption configuring mTLS, or nil if TLS is disabled.
func (c *TLSConfig) ServerOption() (grpc.ServerOption, error) {
	if !c.IsEnabled() {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("security: failed to load server keypair: %w", err)
	}

	caPEM, err := os.ReadFile(c.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("security: failed to read CA cert: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("security: failed to append CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	return grpc.Creds(credentials.NewTLS(tlsConfig)), nil
}

// DialOption returns a grpc.DialOption configuring mTLS, or insecure credentials if TLS is disabled.
func (c *TLSConfig) DialOption() (grpc.DialOption, error) {
	if !c.IsEnabled() {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("security: failed to load client keypair: %w", err)
	}

	caPEM, err := os.ReadFile(c.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("security: failed to read CA cert: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("security: failed to append CA certificate to pool")
	}

	serverName := c.ServerName
	if serverName == "" {
		serverName = "localhost"
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

// GenerateTestPKI generates an ephemeral in-memory CA and signed leaf certificates for tests.
func GenerateTestPKI(hosts []string) (caPEM, certPEM, keyPEM []byte, err error) {
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "DistribuQ Test CA",
			Organization: []string{"DistribuQ"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPriv.PublicKey, caPriv)
	if err != nil {
		return nil, nil, nil, err
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	// Leaf cert
	leafPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}

	leafTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName:   "DistribuQ Node",
			Organization: []string{"DistribuQ"},
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			leafTemplate.IPAddresses = append(leafTemplate.IPAddresses, ip)
		} else {
			leafTemplate.DNSNames = append(leafTemplate.DNSNames, h)
		}
	}
	if len(leafTemplate.DNSNames) == 0 && len(leafTemplate.IPAddresses) == 0 {
		leafTemplate.DNSNames = append(leafTemplate.DNSNames, "localhost")
		leafTemplate.IPAddresses = append(leafTemplate.IPAddresses, net.ParseIP("127.0.0.1"))
	}

	leafBytes, err := x509.CreateCertificate(rand.Reader, &leafTemplate, &caTemplate, &leafPriv.PublicKey, caPriv)
	if err != nil {
		return nil, nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafBytes})

	leafKeyBytes, err := x509.MarshalECPrivateKey(leafPriv)
	if err != nil {
		return nil, nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyBytes})

	return caPEM, certPEM, keyPEM, nil
}
