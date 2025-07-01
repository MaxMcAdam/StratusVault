.PHONY: proto clean build run-server run-client

proto:
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

certs: 
	openssl req -x509 -newkey rsa:4096 -nodes -days 365 -keyout ca-key.pem -out ca-cert.pem -subj '/C=US/ST=NEW YORK/L=ROCHESTER/O=DEV/CN=localhost' \
  		-addext 'subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1,IP:::1'
	openssl req -newkey rsa:4096 -nodes -keyout server/server-key.pem -out server/server-req.pem -subj '/C=US/ST=NEW YORK/L=ROCHESTER/O=DEV/CN=localhost'\
  		-addext 'subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1,IP:::1'
	openssl x509 -req -in server/server-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server/server-cert.pem \
  		-CAcreateserial -out server/server-cert.pem -days 365 -copy_extensions copy
	openssl req -newkey rsa:4096 -nodes -keyout client/client-key.pem -out client/client-req.pem -subj '/C=US/ST=NEW YORK/L=ROCHESTER/O=DEV/CN=localhost'\
  		-addext 'subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1,IP:::1'
	openssl x509 -req -in client/client-req.pem -days 60 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client/client-cert.pem \
  		-CAcreateserial -out client/client-cert.pem -copy_extensions copy