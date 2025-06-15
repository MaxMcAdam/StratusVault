package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/MaxMcAdam/StratusVault/client/config"
	"github.com/MaxMcAdam/StratusVault/proto"
)

func UploadFile(client proto.FileServiceClient, filePath string) error {
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

func DownloadFile(client proto.FileServiceClient, fileName string, fileId string, config *config.ClientConfig) error {
	// Get the fileId and check the size
	info, err := client.GetFileInfo(context.Background(), &proto.GetFileInfoRequest{FileId: fileId, FileName: fileName})
	if err != nil {
		return fmt.Errorf("Failed to get file infomation: %v", err)
	}

	fileSize := info.Size
	if fileId == "" {
		fileId = info.Id
	}

	var req *proto.DownloadFileRequest

	if config.ChunkSize != 0 && config.ChunkSize < fileSize {
		req = &proto.DownloadFileRequest{FileId: fileId, Limit: config.ChunkSize}
	} else {
		req = &proto.DownloadFileRequest{FileId: fileId}
	}

	stream, err := client.DownloadFile(context.Background(), req)
	if err != nil {
		return fmt.Errorf("Error creating file download stream: %v", err)
	}

	dirPath := filepath.Dir(fileName)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		return fmt.Errorf("Error creating directories:", err)
	}

	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open file for download: %v", err)
	}
	defer f.Close()

	var resp *proto.DownloadFileResponse

	for err == nil {
		resp, err = stream.Recv()

		if err != nil && err != io.EOF {
			fmt.Errorf("Error getting file download response: %v", err)
		}

		if resp != nil && resp.Chunk != nil && resp.Chunk.Data != nil {
			_, wErr := f.Write(resp.Chunk.Data)
			if wErr != nil {
				return fmt.Errorf("Error appending to file: %v\n", wErr)
			}
		}
	}

	return nil
}

func getFileSize(filePath string) (int64, error) {
	if fileInfo, err := os.Stat(filePath); err != nil {
		return 0, err
	} else {
		return fileInfo.Size(), nil
	}
}
