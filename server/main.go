package main

import (
	"fmt"
	"log"
	"net"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/backend"
	"google.golang.org/grpc"
)

func main() {
	port := "50051"
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	back, err := backend.New()
	if err != nil {
		log.Fatalf("Failed to initialize backend: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(*back.Config.TLSCreds))
	proto.RegisterFileServiceServer(s, back)

	log.Printf("Server starting on :%s\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
