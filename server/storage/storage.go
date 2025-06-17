package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
}

func (s *StorageBackend) ProcessFirstChunk(req *proto.UploadFileRequest, fileInfo *proto.FileInfo) (bool, error) {
	fmt.Printf("Processing first chunk for %s\n", fileInfo.Id)
	metadata := req.GetMetadata()
	if metadata == nil {
		return false, status.Error(codes.InvalidArgument, "first chunk must contain metadata")
	}

	// Validate filename
	if err := s.ValidateFilename(metadata.Name); err != nil {
		return false, status.Error(codes.InvalidArgument, err.Error())
	}

	// Update the fileInfo with the information provided in the request metadata
	fileInfo.Name = metadata.Name
	fileInfo.MimeType = metadata.MimeType
	fileInfo.Size = metadata.Size

	return metadata.Overwrite, nil
}

func (s *StorageBackend) ValidateFilename(f string) error {
	return nil
}

func (s *StorageBackend) ValidateUpload(ctx context.Context, filename string, size int64) error {
	return nil
}

func (s *StorageBackend) ValidateFileContent(ctx context.Context, tempPath string) error {
	return nil
}

func (s *StorageBackend) ProcessChunk(ctx context.Context, req *proto.UploadFileRequest,
	buffer *bytes.Buffer, checksum hash.Hash, totalSize *int64, tempPath string) error {

	fmt.Printf("Processing chunk for %s\n", tempPath)

	chunk := req.GetChunk()
	if chunk == nil {
		return fmt.Errorf("Error: recieved nil chunk. %v", req.GetMetadata())
	}
	if len(chunk.Data) == 0 {
		return nil // Skip empty chunks
	}

	// Update checksum
	checksum.Write(chunk.Data)
	*totalSize += int64(len(chunk.Data))

	// Check size limits
	if *totalSize > s.config.MaxFileSize {
		return status.Error(codes.InvalidArgument, "file too large")
	}

	// Buffer chunks for efficient storage writes
	buffer.Write(chunk.Data)

	// Flush buffer when it gets large enough
	if buffer.Len() >= int(s.config.ChunkSize) {
		if err := s.AppendToFile(ctx, tempPath, buffer.Bytes()); err != nil {
			return status.Error(codes.Internal, "storage write failed")
		}
		buffer.Reset()
	}

	return nil
}

func (s *StorageBackend) FinalizeUpload(ctx context.Context, stream proto.FileService_UploadFileServer,
	buffer *bytes.Buffer, fileID, tempPath, filename string, totalSize int64, checksumBytes []byte, l *log.Logger) error {

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
			l.Printf("failed to cleanup temp file path: %s error: %s", tempPath, err)
		}
	}
	// fmt.Printf(string(debug.Stack()))
}

func (s *StorageBackend) Delete(ctx context.Context, tempPath string) error {
	return os.Remove(tempPath)
}

func (s *StorageBackend) MoveFile(ctx context.Context, tempPath string, info *proto.FileInfo, l *log.Logger) (int64, error) {
	srcFile, err := os.OpenFile(tempPath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	destDir := filepath.Dir(info.Id)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return 0, fmt.Errorf("cannot create destination directory: %w", err)
	}

	dstFile, err := os.Create(info.Id)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	fmt.Printf("Source: %s\n", tempPath)
	fmt.Printf("Destination: %s\n", info.Id)
	fmt.Printf("Destination dir: %s\n", filepath.Dir(info.Id))

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

	}

	return bWritten, nil
}

func (s *StorageBackend) AppendToFile(ctx context.Context, tempPath string, b []byte) error {
	dirPath := filepath.Dir(tempPath)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return fmt.Errorf("error creating directories:", err)
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
