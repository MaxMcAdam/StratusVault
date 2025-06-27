package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UploadRequest represents a single upload chunk or metadata
type UploadRequest struct {
	Metadata  *proto.FileMetadata
	ChunkData []byte
	IsEOF     bool
}

// UploadResult contains the final result of an upload operation
type UploadResult struct {
	FileID       string
	BytesWritten int64
	Error        error
}

// uploadState encapsulates all upload-related state and operations
type uploadState struct {
	info         *metadata.FileInfo
	existingInfo *metadata.FileInfo // Only set for overwrites
	totalSize    int64
	checksum     hash.Hash
	buffer       bytes.Buffer
	overwrite    bool
}

// cleanup manages rollback operations for failed uploads
type cleanup struct {
	tempFile string
	uploadID string
	oldID    string
}

// onFailure performs cleanup operations when upload fails
func (c *cleanup) onFailure(ctx context.Context, s *FileServiceServer) {
	if c.tempFile != "" {
		if err := s.storage.Delete(ctx, c.tempFile); err != nil {
			s.logger.Printf("Failed to cleanup temp file %s: %v", c.tempFile, err)
		}
	}
	if c.uploadID != "" {
		s.metaDB.CleanupFailedUpload(ctx, c.uploadID, s.logger)
	}
	if c.oldID != "" {
		s.metaDB.SetFileInfoStatus(ctx, c.oldID, metadata.STATUS_ACTIVE, true, s.logger)
	}
}

// newUploadState creates initialized upload state
func newUploadState(info *metadata.FileInfo) *uploadState {
	return &uploadState{
		info:     info,
		checksum: sha256.New(),
	}
}

// UploadFile handles the gRPC streaming upload
func (s *FileServiceServer) UploadFile(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse]) error {
	s.logger.Printf("Starting file upload")

	// Create buffered channel to handle backpressure
	requests := make(chan UploadRequest, 10) // Increased buffer for better throughput

	// Start goroutine to read from stream
	go s.streamReader(stream, requests)

	// Process the upload
	result := s.ProcessUpload(stream.Context(), requests)
	if result.Error != nil {
		s.logger.Printf("Upload failed: %v", result.Error)
		return result.Error
	}

	s.logger.Printf("Upload completed: fileID=%s, bytes=%d", result.FileID, result.BytesWritten)
	return stream.SendAndClose(&proto.UploadFileResponse{
		FileId:       result.FileID,
		BytesWritten: result.BytesWritten,
	})
}

// streamReader reads chunks from the gRPC stream
func (s *FileServiceServer) streamReader(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse], requests chan<- UploadRequest) {
	defer close(requests)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			requests <- UploadRequest{IsEOF: true}
			return
		}
		if err != nil {
			s.logger.Printf("Stream read error: %v", err)
			return
		}

		uploadReq := UploadRequest{
			Metadata:  req.GetMetadata(),
			ChunkData: req.GetChunk().GetData(),
		}

		select {
		case requests <- uploadReq:
		case <-stream.Context().Done():
			s.logger.Printf("Stream context cancelled")
			return
		}
	}
}

// ProcessUpload handles the core upload logic
func (s *FileServiceServer) ProcessUpload(ctx context.Context, requests <-chan UploadRequest) *UploadResult {
	// Apply rate limiting
	if err := s.storage.UploadSemaphore.Acquire(ctx, 1); err != nil {
		return &UploadResult{Error: status.Error(codes.ResourceExhausted, "too many concurrent uploads")}
	}
	defer s.storage.UploadSemaphore.Release(1)

	// Setup cleanup handler
	artifacts := cleanup{}
	defer artifacts.onFailure(ctx, s)

	// Initialize upload state
	info := &metadata.FileInfo{
		UploadId: s.genUUID(),
		Status:   metadata.STATUS_UPLOADING,
	}
	state := newUploadState(info)

	// Process first chunk (contains metadata)
	firstChunk := <-requests
	meta, err := s.processFirstChunk(ctx, firstChunk)
	if err != nil {
		return &UploadResult{Error: fmt.Errorf("processing metadata: %w", err)}
	}

	// Handle existing file logic
	if err := s.handleExistingFile(ctx, meta, state, &artifacts); err != nil {
		return &UploadResult{Error: err}
	}

	// Set up file metadata
	s.setupFileMetadata(info, meta)
	artifacts.uploadID = info.Id

	// Create metadata record
	if err := s.metaDB.SetFileInfo(ctx, info); err != nil {
		return &UploadResult{Error: fmt.Errorf("creating file metadata: %w", err)}
	}

	// Set temp file for cleanup
	artifacts.tempFile = s.getTempPath(info.UploadId)

	// Process all data chunks
	if err := s.processDataChunks(ctx, requests, state); err != nil {
		return &UploadResult{Error: err}
	}

	// Finalize the upload
	if err := s.finalizeUpload(ctx, state, &artifacts); err != nil {
		return &UploadResult{Error: err}
	}
	artifacts.uploadID = ""

	return &UploadResult{
		FileID:       info.Id,
		BytesWritten: state.totalSize,
	}
}

// processFirstChunk validates and extracts metadata from first chunk
func (s *FileServiceServer) processFirstChunk(ctx context.Context, req UploadRequest) (*proto.FileMetadata, error) {
	if req.Metadata == nil {
		return nil, fmt.Errorf("first chunk missing metadata")
	}
	return req.Metadata, nil
}

// handleExistingFile manages overwrite logic and existing file checks
func (s *FileServiceServer) handleExistingFile(ctx context.Context, meta *proto.FileMetadata, state *uploadState, artifacts *cleanup) error {
	existingInfo, err := s.metaDB.GetFileInfo(ctx, "", meta.Name)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("checking existing file: %w error is redis.Nil %v", err, errors.Is(err, redis.Nil))
	}

	// Validate overwrite conditions
	if meta.Overwrite && existingInfo == nil {
		return fmt.Errorf("no existing file to overwrite: %s", meta.Name)
	}
	if !meta.Overwrite && existingInfo != nil {
		return fmt.Errorf("file already exists")
	}

	// Handle overwrite case
	if meta.Overwrite {
		state.existingInfo = existingInfo
		state.overwrite = true
		artifacts.oldID = existingInfo.Id

		// Lock existing file to prevent concurrent overwrites
		if _, err := s.metaDB.SetFileInfoStatus(ctx, existingInfo.Id, metadata.STATUS_OVERWRITING, false, s.logger); err != nil {
			return fmt.Errorf("locking existing file: %w", err)
		}
	}

	return nil
}

// setupFileMetadata populates file info from metadata
func (s *FileServiceServer) setupFileMetadata(info *metadata.FileInfo, meta *proto.FileMetadata) {
	info.Name = meta.Name
	info.MimeType = meta.MimeType
	info.Size = meta.Size
	info.Id = info.UploadId
}

// processDataChunks handles all data chunks from the stream
func (s *FileServiceServer) processDataChunks(ctx context.Context, requests <-chan UploadRequest, state *uploadState) error {
	for req := range requests {
		if req.IsEOF {
			break
		}

		if err := s.processDataChunk(ctx, req, state); err != nil {
			return fmt.Errorf("processing chunk: %w", err)
		}
	}
	return nil
}

// processDataChunk handles individual chunk processing
func (s *FileServiceServer) processDataChunk(ctx context.Context, req UploadRequest, state *uploadState) error {
	return s.storage.ProcessChunk(
		ctx,
		req.ChunkData,
		&state.buffer,
		state.checksum,
		&state.totalSize,
		s.getTempPath(state.info.UploadId),
	)
}

// finalizeUpload completes the upload process
func (s *FileServiceServer) finalizeUpload(ctx context.Context, state *uploadState, artifacts *cleanup) error {
	// Finalize file storage
	tempPath := s.getTempPath(state.info.UploadId)
	if err := s.storage.FinalizeUpload(ctx, &state.buffer, tempPath, state.info.Name, state.totalSize, nil, s.logger); err != nil {
		return fmt.Errorf("finalizing storage: %w", err)
	}

	// Update metadata to temp status
	if err := s.updateFileMetadata(state); err != nil {
		return fmt.Errorf("updating metadata: %w", err)
	}

	// Move to final location
	if err := s.moveToFinalLocation(ctx, state); err != nil {
		return fmt.Errorf("moving to final location: %w", err)
	}
	artifacts.tempFile = "" // Clear temp file from cleanup

	// Activate the file
	if err := s.activateFile(ctx, state); err != nil {
		return fmt.Errorf("activating file: %w", err)
	}
	artifacts.oldID = "" // Clear old ID from cleanup

	return nil
}

// updateFileMetadata sets the file to temp upload status
func (s *FileServiceServer) updateFileMetadata(state *uploadState) error {
	now := s.now()
	state.info.Status = metadata.STATUS_TEMP_UPLOAD
	state.info.UpdatedAt = &now

	if state.overwrite {
		state.info.CreatedAt = state.existingInfo.CreatedAt
	} else {
		state.info.CreatedAt = &now
	}

	return s.metaDB.SetFileInfo(context.Background(), state.info)
}

// moveToFinalLocation handles file movement logic
func (s *FileServiceServer) moveToFinalLocation(ctx context.Context, state *uploadState) error {
	if !state.overwrite {
		return s.moveNewFile(ctx, state)
	}
	return s.overwriteExistingFile(ctx, state)
}

// moveNewFile moves a new file to its final location
func (s *FileServiceServer) moveNewFile(ctx context.Context, state *uploadState) error {
	tempPath := s.getTempPath(state.info.UploadId)
	_, err := s.storage.MoveFile(ctx, tempPath, state.info.Id, s.logger)
	return err
}

// overwriteExistingFile handles the overwrite process
func (s *FileServiceServer) overwriteExistingFile(ctx context.Context, state *uploadState) error {
	// Create backup path
	backupPath := fmt.Sprintf("overwriting_%s", state.existingInfo.Id)

	// Rename existing file to backup
	if err := s.storage.RenameFile(ctx, state.existingInfo.Id, backupPath); err != nil {
		return fmt.Errorf("backing up existing file: %w", err)
	}

	// Use existing file's ID
	state.info.Id = state.existingInfo.Id

	// Move new file to final location
	if err := s.moveNewFile(ctx, state); err != nil {
		return fmt.Errorf("moving new file: %w", err)
	}

	// Delete backup (best effort)
	if err := s.storage.Delete(ctx, backupPath); err != nil {
		s.logger.Printf("Warning: failed to delete backup file %s: %v", backupPath, err)
	}

	return nil
}

// activateFile sets the file to active status
func (s *FileServiceServer) activateFile(ctx context.Context, state *uploadState) error {
	// Update index for new files
	if !state.overwrite {
		if err := s.metaDB.SetFileIdInIndex(ctx, state.info.Name, state.info.Id); err != nil {
			return fmt.Errorf("updating file index: %w", err)
		}
	}

	// Set file to active
	state.info.Status = metadata.STATUS_ACTIVE
	return s.metaDB.SetFileInfo(ctx, state.info)
}

// getTempPath generates temporary file path
func (s *FileServiceServer) getTempPath(uploadID string) string {
	return fmt.Sprintf("temp_%s", uploadID)
}
