package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	port := "50051"
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	metaDB := metadata.Init()

	storage := storage.NewStorageBackend(1, 1, 10, 1000000)

	log := log.New(os.Stdout, "", 1)

	s := grpc.NewServer()
	proto.RegisterFileServiceServer(s, &fileServiceServer{metaDB: metaDB, storage: storage, logger: log})

	log.Printf("Server starting on :%s\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

type fileServiceServer struct {
	storage *storage.StorageBackend
	metaDB  *metadata.MetadataDB
	logger  *log.Logger

	proto.UnimplementedFileServiceServer
}

func Init() *fileServiceServer {
	metaDB := metadata.Init()

	return &fileServiceServer{metaDB: metaDB}
}

func (s *fileServiceServer) UploadFile(stream grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse]) error {
	fileId := generateFileID()
	fileInfo := &proto.FileInfo{
		Id:     fileId,
		Status: metadata.STATUS_UPLOADING,
	}

	tempPath := fmt.Sprintf("temp/%s", fileInfo.Id)

	// Add the file metadata to the metadatadb with a temporary status
	if err := s.metaDB.SetFileInfo(stream.Context(), fileInfo); err != nil {
		fmt.Printf("Error was %v", err)
		return err
	}

	// Download the file in a temp folder
	if err := s.storage.HandleTempFileUpload(stream, s.logger, fileInfo); err != nil {
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	// Update the file's status to reflect successful upload
	fileInfo.Status = metadata.STATUS_ACTIVE
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
		s.storage.CleanupFailedUpload(fileId, s.logger)
		s.metaDB.CleanupFailedUpload(stream.Context(), fileInfo.Id, s.logger)
		return err
	}

	stream.SendAndClose(&proto.UploadFileResponse{FileId: fileId, BytesWritten: bWritten})

	return nil
}

func (s *fileServiceServer) DownloadFile(req *proto.DownloadFileRequest, stream grpc.ServerStreamingServer[proto.DownloadFileResponse]) error {
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

func (s *fileServiceServer) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	fmt.Printf("Recieved request %v", req)
	return (*s.metaDB).ListFiles(ctx, req)
}

func (s *fileServiceServer) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	fmt.Printf("Recieved request %v", req)
	return (*s.metaDB).GetFileInfo(ctx, req)
}

func (s *fileServiceServer) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (*emptypb.Empty, error) {
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

func (s *fileServiceServer) WatchFiles(*proto.WatchFilesRequest, grpc.ServerStreamingServer[proto.FileEvent]) error {
	return nil
}

func generateFileID() string {
	return uuid.New().String()
}
