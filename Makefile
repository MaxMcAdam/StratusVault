.PHONY: proto clean build run-server run-client

proto:
#	protoc --go_out=. proto/*.proto
	protoc --go_out=. --go-grpc_out=. proto/*.proto

clean:
	rm -rf proto/*.pb.go

build: proto
	go build -o bin/server ./server
	go build -o bin/client ./client

run-server: build
	./bin/server

run-client: build
	./bin/client

install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest