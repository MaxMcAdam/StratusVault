package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/MaxMcAdam/StratusVault/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Establish connection to the gRPC server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create the client
	client := proto.NewFileServiceClient(conn)

	if client == nil {
		fmt.Printf("Client is nil\n")
	}

	// err = uploadFile(client, "./testfile")

	// if err != nil {
	// 	fmt.Printf("Error uploading testfile: %v", err)
	// 	os.Exit(1)
	// }

	// fmt.Printf("Finished uploading file.\n")

	resp, err := client.ListFiles(context.Background(), &proto.ListFilesRequest{PageSize: 3, PageToken: 0})
	fmt.Printf("Resp: %v\nErr: %v", resp, err)
}

func uploadFile(client proto.FileServiceClient, filePath string) error {
	fmt.Printf("Starting upload.\n")
	// Get the stream
	stream, err := client.UploadFile(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	fileSize, err := getFileSize(filePath)
	if err != nil {
		return fmt.Errorf("failed to find filesize: %v", err)
	}

	// Send metadata first
	err = stream.Send(&proto.UploadFileRequest{
		Request: &proto.UploadFileRequest_Metadata{
			Metadata: &proto.FileMetadata{
				Name: filepath.Base(filePath),
				Size: fileSize,
			}},
	})
	if err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	// Open and read the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Stream file chunks
	buffer := make([]byte, 32*1024) // 32KB chunks
	offset := 0
	isLast := false
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
			// isLast = true
		} else if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Send chunk
		err = stream.Send(&proto.UploadFileRequest{
			Request: &proto.UploadFileRequest_Chunk{
				Chunk: &proto.FileChunk{
					Data:   buffer[:n],
					Offset: int64(offset),
					IsLast: isLast,
				}},
		})

		if err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
		if isLast {
			break
		}
	}

	// Close stream and get response
	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to receive response: %w", err)
	}

	fmt.Printf("Upload successful: %+v\n", response)
	return nil
}

func getFileSize(filePath string) (int64, error) {
	if fileInfo, err := os.Stat(filePath); err != nil {
		return 0, err
	} else {
		return fileInfo.Size(), nil
	}
}
