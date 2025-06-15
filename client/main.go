package main

import (
	"context"
	"fmt"
	"log"

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

	// err = UploadFile(client, "./testfile")
	// if err != nil {
	// 	fmt.Printf("Error uploading testfile: %v", err)
	// }

	// err = DownloadFile(client, "testfile", "", &config.ClientConfig{ChunkSize: 5})
	// if err != nil {
	// 	fmt.Printf("Error downloading testfile: %v", err)
	// }

	_, err = client.DeleteFile(context.Background(), &proto.DeleteFileRequest{FileId: "69251d23-0932-4888-8157-1fa9dd0a07f8"})
	fmt.Printf("Error is %v", err)

	// resp, err := client.ListFiles(context.Background(), &proto.ListFilesRequest{PageSize: 3, PageToken: 0})
	// fmt.Printf("Resp: %v\nErr: %v", resp, err)
}
