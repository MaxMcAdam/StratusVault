package backend

import (
	"crypto/tls"

	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
)

type BackendConfig struct {
	Storage  *storage.Config
	Metadata *metadata.Config
	TLSCert  tls.Certificate
}

func importCert(string) (*tls.Certificate, error) {
	// certBytes, err := os.ReadFile(string)
	// if err != nil {
	// 	return nil, fmt.Errorf("Error reading cert file: %v", err)
	// }

}
