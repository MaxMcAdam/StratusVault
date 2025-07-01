package backend

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"

	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"google.golang.org/grpc/credentials"
)

type BackendConfig struct {
	Storage    *storage.Config  `json:"storageConfig"`
	Metadata   *metadata.Config `json:"metadataConfig"`
	TLSCreds   *credentials.TransportCredentials
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	CaCertFile string `json:"caCertFile"`
}

func (b *BackendConfig) setDefaults() {
	if b.CertFile == "" {
		b.CertFile = "./server-cert.pem"
	}
	if b.KeyFile == "" {
		b.KeyFile = "./server-key.pem"
	}
	if b.CaCertFile == "" {
		b.CaCertFile = "../ca-cert.pem"
	}
}

func NewConfig(configPath string) (*BackendConfig, error) {
	b := BackendConfig{Storage: storage.NewConfig(), Metadata: metadata.NewConfig()}

	if configPath != "" {
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(configBytes, &b); err != nil {
			return nil, err
		}
	}

	b.setDefaults()

	if err := b.importCert(); err != nil {
		return nil, err
	}

	return &b, nil
}

func (b *BackendConfig) importCert() error {
	// Load CA certificate for verifying client certificates
	caCertPem, err := os.ReadFile(b.CaCertFile)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCertPem) {
		return fmt.Errorf("failed to parse CA certificate")
	}

	// Load server certificate and key
	serverCert, err := tls.LoadX509KeyPair(b.CertFile, b.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load server certificate: %w", err)
	}

	tlsConfig := tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   "localhost",
	}

	tlsCreds := credentials.NewTLS(&tlsConfig)
	b.TLSCreds = &tlsCreds

	return nil
}
