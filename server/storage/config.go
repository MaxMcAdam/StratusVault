package storage

import (
	"fmt"
	"os"
	"os/user"
	"path"
)

const (
	STRATUS_VAULT_FOLDER = "stratus_vault/"
)

type Config struct {
	ChunkSize   int64  `json:"chunkSize"`
	MaxFileSize int64  `Json:"maxFileSize"`
	StoragePath string `json:"storagePath"`
}

func NewConfig() *Config {
	c := &Config{}

	c.SetDefaults()

	return c
}

func (s *Config) SetDefaults() {
	if s.ChunkSize == 0 {
		s.ChunkSize = 16000 // 16 kb
	}
	if s.MaxFileSize == 0 {
		s.MaxFileSize = 10737418240 // 10 gb
	}
	if s.StoragePath == "" {
		user, err := user.Current()
		if err != nil {
			fmt.Printf("Error getting user info for home directory: %v", err)
			os.Exit(1)
		}
		s.StoragePath = path.Join(user.HomeDir, STRATUS_VAULT_FOLDER)
	}
}
