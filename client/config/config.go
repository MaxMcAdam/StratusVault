package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

type ClientConfig struct {
	ChunkSize  int64
	TLSCreds   credentials.TransportCredentials
	CaCertFile string `json:"caCertFile"`
	ClientCert string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
}

func NewConfig(configPath string) (*ClientConfig, error) {
	c := ClientConfig{}

	if configPath != "" {
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(configBytes, &c); err != nil {
			return nil, err
		}
	}

	c.setDefaults()

	if err := c.importTLSCreds(); err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *ClientConfig) setDefaults() {
	if c.ChunkSize == 0 {
		c.ChunkSize = 16000 // 16 kb
	}
	if c.CaCertFile == "" {
		c.CaCertFile = "../ca-cert.pem"
	}
	if c.ClientCert == "" {
		c.ClientCert = "../client/client-cert.pem"
	}
	if c.KeyFile == "" {
		c.KeyFile = "../client/client-key.pem"
	}
}

func getCurrentDir() string {
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "unknown"
}

func (c *ClientConfig) importTLSCreds() error {
	for _, file := range []string{c.CaCertFile, c.ClientCert, c.KeyFile} {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			return fmt.Errorf("certificate file does not exist: %s (current dir: %s)",
				file, getCurrentDir())
		}
	}

	caCert, err := os.ReadFile(c.CaCertFile)
	if err != nil {
		return err
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caCert); !ok {
		return err
	}

	clientCert, err := tls.LoadX509KeyPair(c.ClientCert, c.KeyFile)
	if err != nil {
		return err
	}

	// set config of tls credential
	config := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   "localhost",
	}

	tlsCred := credentials.NewTLS(config)

	c.TLSCreds = tlsCred

	return nil
}
