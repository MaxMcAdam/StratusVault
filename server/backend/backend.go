package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FileServiceServer struct {
	storage *storage.StorageBackend
	metaDB  *metadata.MetadataDB
	logger  *log.Logger

	proto.UnimplementedFileServiceServer
}

func Init() *FileServiceServer {
	metaDB := metadata.Init()

	return &FileServiceServer{metaDB: metaDB, storage: storage.NewStorageBackend(1, 1, 10, 1000000), logger: log.New(os.Stdout, "", 1)}
}

func (s *FileServiceServer) UploadFile(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse]) error {
	// return an err if a file by the given name exists and overwrite is not set
	fileInfo := &proto.FileInfo{
		Id:     generateFileID(),
		Status: metadata.STATUS_UPLOADING,
	}

	tempPath := temp(fileInfo.Id)

	// Add the file metadata to the metadatadb with a temporary status
	if err := s.metaDB.SetFileInfo(stream.Context(), fileInfo); err != nil {
		fmt.Printf("Error was %v", err)
		return err
	}

	// Download the file in a temp folder
	if err := s.handleTempFileUpload(stream, s.logger, fileInfo); err != nil {
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// th id may have changed if this is an overwrite
	tempPath = temp(fileInfo.Id)

	fmt.Printf("info is  %v\n", fileInfo)

	// Update the file's status to reflect successful upload
	fileInfo.Status = metadata.STATUS_ACTIVE
	now := timestamppb.Now()
	fileInfo.UpdatedAt = now
	if fileInfo.CreatedAt == nil {
		fileInfo.CreatedAt = now
	}
	if err := s.metaDB.SetFileInfo(stream.Context(), fileInfo); err != nil {
		s.storage.CleanupFailedUpload(tempPath, s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// Add the file's name in the id index
	if err := s.metaDB.SetFileIdInIndex(stream.Context(), fileInfo.Name, fileInfo.Id); err != nil {
		s.storage.CleanupFailedUpload(tempPath, s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// Move the file to the permenant location
	bWritten, err := s.storage.MoveFile(stream.Context(), tempPath, fileInfo, s.logger)
	if err != nil {
		s.storage.CleanupFailedUpload(tempPath, s.logger)
		s.storage.CleanupFailedUpload(fileInfo.Id, s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	stream.SendAndClose(&proto.UploadFileResponse{FileId: fileInfo.Id, BytesWritten: bWritten})

	return nil
}

func (s *FileServiceServer) handleTempFileUpload(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse], l *log.Logger, metadata *proto.FileInfo) error {
	ctx := stream.Context()

	// Rate limiting
	if err := s.storage.UploadSemaphore.Acquire(ctx, 1); err != nil {
		return status.Error(codes.ResourceExhausted, "too many concurrent uploads")
	}
	defer s.storage.UploadSemaphore.Release(1)

	// Initialize upload state
	var (
		tempPath   = temp(metadata.Id)
		filename   string
		totalSize  int64
		checksum   = sha256.New()
		buffer     bytes.Buffer
		firstChunk = true
	)

	// Process streaming chunks
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Recieved ctx.Done()\n")
			// Client cancelled or timed out
			s.storage.CleanupFailedUpload(tempPath, l)
			return ctx.Err()

		default:
			req, err := stream.Recv()
			if err == io.EOF {
				// Client finished sending, finalize upload
				return s.storage.FinalizeUpload(ctx, stream, &buffer, metadata.Id, tempPath, filename,
					totalSize, checksum.Sum(nil), l)
			}
			if err != nil {
				s.storage.CleanupFailedUpload(tempPath, l)
				return status.Error(codes.Internal, "stream error: "+err.Error())
			}

			// Handle first chunk (contains metadata)
			if firstChunk {
				if overwrite, err := s.storage.ProcessFirstChunk(req, metadata); err != nil {
					return err
				} else if existingFile, _ := s.metaDB.GetFileIdInIndex(ctx, metadata.Name); existingFile != "" && !overwrite {
					return fmt.Errorf("File %s already exists.", metadata.Name)
				} else if existingFile != "" && overwrite {
					// delete the temprary metadata since this file already exists
					s.metaDB.DeleteFileInfo(ctx, metadata.Id, "")

					existingMetadata, err := s.metaDB.GetFileInfo(ctx, existingFile, "")
					*metadata = *existingMetadata
					if err != nil {
						return err
					}
					tempPath = temp(metadata.Id)
				}

				// Validate file before starting upload
				if err := s.storage.ValidateUpload(ctx, filename, metadata.Size); err != nil {
					return status.Error(codes.InvalidArgument, err.Error())
				}

				firstChunk = false
				continue
			}

			// Process chunk data
			if err := s.storage.ProcessChunk(ctx, req, &buffer, checksum, &totalSize, tempPath); err != nil {
				s.storage.CleanupFailedUpload(tempPath, l)
				return err
			}
		}
	}
}

func (s *FileServiceServer) DownloadFile(req *proto.DownloadFileRequest, stream grpc.ServerStreamingServer[proto.DownloadFileResponse]) error {
	fileId := req.GetFileId()

	var err error
	if fileId == "" {
		fileId, err = s.metaDB.GetFileIdInIndex(stream.Context(), req.GetFileName())
		if err != nil {
			return err
		}
	}

	f, err := os.Open(fileId)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	isLast := false
	bytesToRead := req.GetLimit()

	if bytesToRead == 0 {
		bytesToRead = info.Size()
	}

	offset := int64(0)

	for !isLast {
		// io.ReadFull will return an error if it does not fill the buffer
		if info.Size()-offset >= req.GetLimit() {
			isLast = true
			bytesToRead = info.Size() - offset
		}

		buf := make([]byte, bytesToRead)

		if _, err = f.Seek(offset, 0); err != nil {
			return err
		}

		if _, err = io.ReadFull(f, buf); err != nil {
			return err
		}

		err = stream.Send(&proto.DownloadFileResponse{
			Chunk: &proto.FileChunk{
				Data:   buf,
				Offset: offset,
				IsLast: isLast,
			}})

		if err != nil {
			s.logger.Printf("Error sending file chunk: %v", err)
		}
	}

	return nil
}

func (s *FileServiceServer) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	fmt.Printf("Recieved request %v", req)
	return (*s.metaDB).ListFiles(ctx, req)
}

func (s *FileServiceServer) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	fmt.Printf("Recieved request %v", req)
	return (*s.metaDB).GetFileInfo(ctx, req.FileId, req.FileName)
}

func (s *FileServiceServer) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (*emptypb.Empty, error) {
	var err error
	fileId := req.FileId
	if fileId == "" {
		fileId, err = metadata.Init().GetFileIdInIndex(ctx, req.FileName)
	}

	// Mark file metadata as being currently deleted
	info, err := s.metaDB.SetFileInfoStatus(ctx, fileId, metadata.STATUS_DELETING, false, s.logger)
	if err != nil {
		return &emptypb.Empty{}, err
	}

	// Delete file from filesystem
	if err = s.storage.Delete(ctx, fileId); err != nil {
		// Update the file status to active
		s.metaDB.SetFileInfoStatus(ctx, fileId, metadata.STATUS_ACTIVE, true, s.logger)
		return &emptypb.Empty{}, err
	}

	// Delete file metadata
	if err := s.metaDB.DeleteFileInfo(ctx, fileId, info.Name); err != nil {
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}

func (s *FileServiceServer) WatchFiles(*proto.WatchFilesRequest, grpc.ServerStreamingServer[proto.FileEvent]) error {
	return nil
}

func generateFileID() string {
	return uuid.New().String()
}

func temp(id string) string {
	return fmt.Sprintf("temp/%s", id)
}
