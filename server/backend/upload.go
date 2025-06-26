package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"log"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *FileServiceServer) UploadFile(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse]) error {
	// return an err if a file by the given name exists and overwrite is not set
	fileInfo := &metadata.FileInfo{
		UploadId: generateFileID(),
		Status:   metadata.STATUS_UPLOADING,
	}

	// Add the file metadata to the metadatadb with a temporary status
	if err := s.metaDB.SetFileInfo(stream.Context(), fileInfo); err != nil {
		fmt.Printf("Error was %v", err)
		return err
	}

	// Download the file in a temp folder
	var err error
	existingFile := ""
	fileInfo, existingFile, err = s.handleTempFileUpload(stream, s.logger, fileInfo)
	if err != nil {
		if fileInfo.Status == metadata.STATUS_UPLOADING {
			s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		}
		return err
	}

	fileInfo, err = s.metaDB.OverwriteFileInfo(stream.Context(), fileInfo, existingFile)
	if err != nil {
		return err
	}

	// Update the file's status to reflect successful upload
	fileInfo.Status = metadata.STATUS_TEMP_UPLOAD
	now := time.Now()
	fileInfo.UpdatedAt = &now
	if fileInfo.CreatedAt == nil {
		fileInfo.CreatedAt = &now
	}
	if err := s.metaDB.SetFileInfo(stream.Context(), fileInfo); err != nil {
		s.storage.CleanupFailedUpload(temp(fileInfo.UploadId), s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// Add the file's name in the id index
	if err := s.metaDB.SetFileIdInIndex(stream.Context(), fileInfo.Name, fileInfo.Id); err != nil {
		s.storage.CleanupFailedUpload(temp(fileInfo.UploadId), s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// Move the file to the permenant location
	bWritten, err := s.storage.MoveFile(stream.Context(), temp(fileInfo.UploadId), fileInfo.Id, s.logger)
	if err != nil {
		s.storage.CleanupFailedUpload(temp(fileInfo.UploadId), s.logger)
		s.storage.CleanupFailedUpload(fileInfo.Id, s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	stream.SendAndClose(&proto.UploadFileResponse{FileId: fileInfo.Id, BytesWritten: bWritten})

	return nil
}

// uploadState encapsulates all upload-related state
type uploadState struct {
	info           *metadata.FileInfo
	filename       string
	existingFileId string
	totalSize      int64
	checksum       hash.Hash
	buffer         bytes.Buffer
	firstChunk     bool
}

// newUploadState creates initialized upload state
func newUploadState(info *metadata.FileInfo) *uploadState {
	return &uploadState{
		info:       info,
		checksum:   sha256.New(),
		firstChunk: true,
	}
}

// handleTempFileUpload orchestrates the upload process with clear separation of concerns
func (s *FileServiceServer) handleTempFileUpload(
	stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse],
	l *log.Logger,
	info *metadata.FileInfo,
) (*metadata.FileInfo, string, error) {

	var err error

	// Rate limiting
	if err := s.storage.UploadSemaphore.Acquire(stream.Context(), 1); err != nil {
		return info, "", status.Error(codes.ResourceExhausted, "too many concurrent uploads")
	}
	defer s.storage.UploadSemaphore.Release(1)

	// Initialize upload state
	state := newUploadState(info)

	// Ensure cleanup on any failure
	cleanup := &uploadCleanup{
		storage:  s.storage,
		uploadId: temp(info.UploadId),
		logger:   l,
	}
	defer cleanup.onFailure(&err)

	// Process the upload stream
	if err := s.processUploadStream(stream, state); err != nil {
		return state.info, "", err
	}

	// Finalize the upload
	err = s.storage.FinalizeUpload(
		stream.Context(),
		&state.buffer,
		temp(state.info.UploadId),
		state.filename,
		state.totalSize,
		state.checksum.Sum(nil),
		l,
	)

	if err != nil {
		return state.info, "", err
	}

	cleanup.disable() // Success - don't cleanup
	return state.info, state.existingFileId, nil
}

// uploadCleanup handles cleanup with RAII-style pattern
type uploadCleanup struct {
	storage  interface{ CleanupFailedUpload(string, *log.Logger) }
	uploadId string
	logger   *log.Logger
	disabled bool
}

func (c *uploadCleanup) disable() {
	c.disabled = true
}

func (c *uploadCleanup) onFailure(err *error) {
	if !c.disabled && *err != nil {
		c.storage.CleanupFailedUpload(c.uploadId, c.logger)
	}
}

// processUploadStream handles the streaming logic with clear flow
func (s *FileServiceServer) processUploadStream(
	stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse],
	state *uploadState,
) error {
	ctx := stream.Context()

	for {
		// Check for cancellation first
		select {
		case <-ctx.Done():
			fmt.Printf("Received ctx.Done()\n")
			return ctx.Err()
		default:
		}

		// Receive next chunk
		req, err := stream.Recv()
		if err == io.EOF {
			return nil // Normal completion
		}
		if err != nil {
			return status.Error(codes.Internal, "stream error: "+err.Error())
		}

		// Process the chunk
		if err := s.processChunk(ctx, req, state); err != nil {
			return err
		}
	}
}

// processChunk handles individual chunk processing
func (s *FileServiceServer) processChunk(
	ctx context.Context,
	req *proto.UploadFileRequest,
	state *uploadState,
) error {
	if state.firstChunk {
		return s.handleFirstChunk(ctx, req, state)
	}

	return s.storage.ProcessChunk(
		ctx,
		req,
		&state.buffer,
		state.checksum,
		&state.totalSize,
		temp(state.info.UploadId),
	)
}

// processFirstChunk handles metadata initialization
func (s *FileServiceServer) handleFirstChunk(
	ctx context.Context,
	req *proto.UploadFileRequest,
	state *uploadState,
) error {
	// Extract and validate metadata
	fileMeta, err := s.processFirstChunk(req)
	if err != nil {
		return err
	}

	// Check for existing file
	state.existingFileId, _ = s.metaDB.GetFileIdInIndex(ctx, fileMeta.Name)

	// Update file info with metadata
	state.info, err = s.HandleNewMetadata(ctx, fileMeta, state.info, state.existingFileId)
	if err != nil {
		return fmt.Errorf("failed to handle new metadata: %v", err)
	}

	// Validate upload parameters
	if err := s.storage.ValidateUpload(ctx, state.filename, state.info.Size); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	state.firstChunk = false
	return nil
}

func (s *FileServiceServer) processFirstChunk(req *proto.UploadFileRequest) (*proto.FileMetadata, error) {
	metadata := req.GetMetadata()
	if metadata == nil {
		return nil, status.Error(codes.InvalidArgument, "first chunk must contain metadata")
	}

	// Validate filename
	if err := s.ValidateFilename(metadata.Name); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return metadata, nil
}

func (s *FileServiceServer) HandleNewMetadata(ctx context.Context, newMetadata *proto.FileMetadata, info *metadata.FileInfo, existingFile string) (*metadata.FileInfo, error) {
	if existingFile != "" && !newMetadata.Overwrite {
		return info, fmt.Errorf("File %s already exists.", newMetadata.Name)
	}

	info.Name = newMetadata.Name
	info.MimeType = newMetadata.MimeType
	info.Size = newMetadata.Size

	return info, nil
}

func (s *FileServiceServer) ValidateFilename(f string) error {
	return nil
}
