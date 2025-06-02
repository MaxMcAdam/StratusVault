package main

import (
	"context"
	"log"
	"net"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	proto.RegisterFileServiceServer(s, &fileServiceServer{})

	log.Println("Server starting on :8080")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

type fileServiceServer struct {
	storage *storage.StorageBackend
	metaDB  *metadata.MetadataStore
	logger  log.Logger
}

func (s *fileServiceServer) UploadFile(grpc.ClientStreamingServer[proto.UploadFileRequest, proto.UploadFileResponse]) error {

}

func (s *fileServiceServer) DownloadFile(*proto.DownloadFileRequest, grpc.ServerStreamingServer[proto.DownloadFileResponse]) error {

}

func (s *fileServiceServer) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	return (*s.metaDB).ListFiles(ctx, req)
}

func (s *fileServiceServer) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	return (*s.metaDB).GetFileInfo(ctx, req)
}

func (s *fileServiceServer) DeleteFile(context.Context, *proto.DeleteFileRequest) (*emptypb.Empty, error) {

}

func (s *fileServiceServer) WatchFiles(*proto.WatchFilesRequest, grpc.ServerStreamingServer[FileEvent]) error {

}
