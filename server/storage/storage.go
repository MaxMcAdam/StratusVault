package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	STRATUS_VAULT_FOLDER = "stratus_vault/"
)

type StorageBackend struct {
	UploadSemaphore   *semaphore.Weighted // Limit concurrent uploads
	DownloadSemaphore *semaphore.Weighted // Limit concurrent downloads
	config            *Config
}

func NewStorageBackend(maxUploads, maxDownloads, chunkSize, maxFileSize int64) *StorageBackend {
	return &StorageBackend{UploadSemaphore: semaphore.NewWeighted(maxUploads),
		DownloadSemaphore: semaphore.NewWeighted(maxDownloads),
		config:            &Config{ChunkSize: chunkSize, MaxFileSize: maxFileSize}}
}

type Config struct {
	ChunkSize   int64
	MaxFileSize int64
	StoragePath string
}

func New() *StorageBackend {
	user, err := user.Current()
	if err != nil {
		fmt.Printf("Error getting user info for home directory: %v", err)
		os.Exit(1)
	}
	fp := path.Join(user.HomeDir, STRATUS_VAULT_FOLDER)
	return &StorageBackend{
		UploadSemaphore:   semaphore.NewWeighted(3),
		DownloadSemaphore: semaphore.NewWeighted(3),
		config: &Config{ChunkSize: 1000,
			MaxFileSize: 10000,
			StoragePath: fp,
		}}
}

func NewWithConfig(cfg *Config) *StorageBackend {
	return &StorageBackend{
		UploadSemaphore:   semaphore.NewWeighted(3),
		DownloadSemaphore: semaphore.NewWeighted(3),
		config:            cfg,
	}
}

func (s *StorageBackend) AddPath(f string) string {
	return filepath.Join(s.config.StoragePath, f)
}

func (s *StorageBackend) ValidateUpload(ctx context.Context, filename string, size int64) error {
	return nil
}

func (s *StorageBackend) ValidateFileContent(ctx context.Context, tempPath string) error {
	return nil
}

func (s *StorageBackend) ProcessChunk(ctx context.Context, bytes []byte,
	buffer *bytes.Buffer, checksum hash.Hash, totalSize *int64, tempPath string) error {

	fmt.Printf("Processing chunk for %s\n", tempPath)

	if len(bytes) == 0 {
		return nil // Skip empty chunks
	}

	// Update checksum
	checksum.Write(bytes)
	*totalSize += int64(len(bytes))

	// Check size limits
	if *totalSize > s.config.MaxFileSize {
		return status.Error(codes.InvalidArgument, "file too large")
	}

	// Buffer chunks for efficient storage writes
	buffer.Write(bytes)

	// Flush buffer when it gets large enough
	if buffer.Len() >= int(s.config.ChunkSize) {
		if err := s.AppendToFile(ctx, tempPath, buffer.Bytes()); err != nil {
			return status.Error(codes.Internal, "storage write failed")
		}
		buffer.Reset()
	}

	return nil
}

func (s *StorageBackend) FinalizeUpload(ctx context.Context, buffer *bytes.Buffer,
	tempPath, filename string, totalSize int64, checksumBytes []byte, l *log.Logger) error {

	// Flush remaining buffer
	if buffer.Len() > 0 {
		if err := s.AppendToFile(ctx, tempPath, buffer.Bytes()); err != nil {
			s.CleanupFailedUpload(tempPath, l)
			return status.Error(codes.Internal, fmt.Sprintf("final write failed. Error: %v", err))
		}
	}

	// Validate file content
	if err := s.ValidateFileContent(ctx, tempPath); err != nil {
		s.CleanupFailedUpload(tempPath, l)
		return status.Error(codes.InvalidArgument, "file validation failed: "+err.Error())
	}

	return nil
}

func (s *StorageBackend) CleanupFailedUpload(tempPath string, l *log.Logger) {
	if tempPath != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := s.Delete(ctx, tempPath); err != nil {
			l.Printf("failed to cleanup temp file path: %s error: %s", s.AddPath(tempPath), err)
		}
	}
}

func (s *StorageBackend) Delete(ctx context.Context, tempPath string) error {
	return os.Remove(s.AddPath(tempPath))
}

func (s *StorageBackend) RenameFile(ctx context.Context, oldName, newName string) error {
	return os.Rename(s.AddPath(oldName), s.AddPath(newName))
}

func (s *StorageBackend) MoveFile(ctx context.Context, tempPath string, id string, l *log.Logger) (int64, error) {
	destPath := s.AddPath(id)

	srcFile, err := os.OpenFile(s.AddPath(tempPath), os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return 0, fmt.Errorf("cannot create destination directory: %w", err)
	}

	dstFile, err := os.Create(destPath)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	bWritten, err := io.Copy(dstFile, srcFile)
	if err != nil {
		fmt.Printf("Error copying file: %v\n", err)
		return 0, err
	}

	if err = dstFile.Sync(); err != nil {
		return 0, err
	}

	err = s.Delete(ctx, tempPath)
	if err != nil {
		return 0, err
	}

	return bWritten, nil
}

func (s *StorageBackend) AppendToFile(ctx context.Context, tempPath string, b []byte) error {
	tempPath = s.AddPath(tempPath)

	fmt.Printf("Writing to file %s\n", tempPath)

	dirPath := filepath.Dir(tempPath)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return fmt.Errorf("error creating directories: %v", err)
	}

	file, err := os.OpenFile(tempPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(b)
	if err != nil {
		fmt.Printf("Error appending to file: %v\n", err)
		return err
	}

	return nil
}
