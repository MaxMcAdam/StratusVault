package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
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

	fmt.Printf("info is  %v\n", fileInfo)

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

func (s *FileServiceServer) handleTempFileUpload(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse], l *log.Logger, info *metadata.FileInfo) (*metadata.FileInfo, string, error) {
	ctx := stream.Context()

	// Rate limiting
	if err := s.storage.UploadSemaphore.Acquire(ctx, 1); err != nil {
		return info, "", status.Error(codes.ResourceExhausted, "too many concurrent uploads")
	}
	defer s.storage.UploadSemaphore.Release(1)

	// Initialize upload state
	var (
		filename       string
		existingFileId string
		totalSize      int64
		checksum       = sha256.New()
		buffer         bytes.Buffer
		firstChunk     = true
	)

	// Process streaming chunks
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Recieved ctx.Done()\n")
			// Client cancelled or timed out
			s.storage.CleanupFailedUpload(temp(info.UploadId), l)
			return info, "", ctx.Err()

		default:
			req, err := stream.Recv()
			if err == io.EOF {
				// Client finished sending, finalize upload
				return info, existingFileId, s.storage.FinalizeUpload(ctx, stream, &buffer, info.Id, temp(info.UploadId), filename,
					totalSize, checksum.Sum(nil), l)
			}
			if err != nil {
				s.storage.CleanupFailedUpload(temp(info.UploadId), l)
				return info, "", status.Error(codes.Internal, "stream error: "+err.Error())
			}

			// Handle first chunk (contains metadata)
			if firstChunk {
				fileMeta, err := s.ProcessFirstChunk(req)
				if err != nil {
					return info, "", err
				}
				existingFileId, _ = s.metaDB.GetFileIdInIndex(ctx, fileMeta.Name)
				fmt.Printf("existing file for name %v is: %v\n", fileMeta.Name, existingFileId)
				info, err = s.HandleNewMetadata(ctx, fileMeta, info, existingFileId)
				if err != nil {
					return info, "", fmt.Errorf("Failed to handle new metadata: %v", err)
				}

				// Validate file before starting upload
				if err := s.storage.ValidateUpload(ctx, filename, info.Size); err != nil {
					return info, "", status.Error(codes.InvalidArgument, err.Error())
				}

				firstChunk = false
				continue
			}

			// Process chunk data
			if err := s.storage.ProcessChunk(ctx, req, &buffer, checksum, &totalSize, temp(info.UploadId)); err != nil {
				s.storage.CleanupFailedUpload(temp(info.UploadId), l)
				return info, "", err
			}
		}
	}
}

func (s *FileServiceServer) ProcessFirstChunk(req *proto.UploadFileRequest) (*proto.FileMetadata, error) {
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
