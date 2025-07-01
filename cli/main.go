package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/MaxMcAdam/StratusVault/client"
	"github.com/MaxMcAdam/StratusVault/client/config"
	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var CLI struct {
	Upload struct {
		Filepath string `arg:"" name:"path" help:"File to upload." type:"path"`
	} `cmd: "" help: "Upload a file."`

	Update struct {
		Filepath string `arg:"" name:"path" help:"File to overwrite." type:"path"`
	} `cmd: "" help: "Update a file."`

	Download struct {
		Filename string `arg:"" optional: "" name: "file" help:"Name of the file to download."`
	} `cmd: "" help: "Download a file."`

	List struct {
	} `cmd: "" help: "List all files."`

	Get struct {
		Filename string `arg:"" optional: "" name: "file" help:"Name of the file to get the metadata of."`
	} `cmd: "" help: "Get the metadata of a single file."`

	Delete struct {
		Filename string `arg:"" optional: "" name: "file" help:"Name of the file to delete."`
		FileId   string `help:"FileId of the file to delete. Mutually exclusive with filename argument." short:"i"`
	} `cmd: "" help: "Delete a single file."`
}

func Validate(ctx *kong.Context) error {
	switch ctx.Command() {
	case "delete <filename>":

	}

	return nil
}

func main() {
	cfg, err := config.NewConfig("")
	if err != nil {
		fmt.Printf("Failed to create config: %v\n", err)
		os.Exit(1)
	}

	creds := cfg.TLSCreds
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	// Establish connection to the gRPC server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(creds))
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create the client
	c := proto.NewFileServiceClient(conn)

	ctx := kong.Parse(&CLI)

	switch ctx.Command() {
	case "upload <path>":
		path := CLI.Upload.Filepath
		err = client.UploadFile(c, path, false)
		if err != nil {
			fmt.Printf("Error uploading file %s: %v\n", path, err)
		}
	case "update <path>":
		path := CLI.Update.Filepath
		err = client.UploadFile(c, path, true)
		if err != nil {
			fmt.Printf("Error uploading file %s: %v\n", path, err)
		}
	case "download <filename>":
		name := CLI.Download.Filename
		err = client.DownloadFile(c, name, "", cfg)
		if err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
		}
	case "list":
		files, err := c.ListFiles(context.Background(), &proto.ListFilesRequest{PageSize: 10, Filter: "*"})
		if err != nil {
			fmt.Printf("Error listing files: %v\n", err)
		} else {
			for _, f := range files.Files {
				b, err := json.MarshalIndent(f, "", "	")
				if err != nil {
					fmt.Printf("Error marshalling output: %v\n", err)
				} else {
					fmt.Println(string(b))
				}
			}
		}
	case "get <filename>":
		file := CLI.Get.Filename
		info, err := c.GetFileInfo(context.Background(), &proto.GetFileInfoRequest{FileName: file})
		if err != nil {
			fmt.Printf("Error getting metadata: %v\n", err)
		} else {
			b, err := json.MarshalIndent(info, "", "	")
			if err != nil {
				fmt.Printf("Error marshalling output: %v\n", err)
			} else {
				fmt.Println(string(b))
			}
		}
	case "delete <filename>":

		file := CLI.Delete.Filename
		id := CLI.Delete.FileId
		if file != "" {
			_, err = c.DeleteFile(context.Background(), &proto.DeleteFileRequest{FileName: file})
		} else {
			_, err = c.DeleteFile(context.Background(), &proto.DeleteFileRequest{FileId: id})

		}
		if err != nil {
			fmt.Printf("Error deleting file: %v", err)
		} else {
			fmt.Printf("Successfully deleted %s%s", file, id)
		}
	case "delete":
		file := CLI.Delete.Filename
		id := CLI.Delete.FileId
		if file != "" {
			_, err = c.DeleteFile(context.Background(), &proto.DeleteFileRequest{FileName: file})
		} else {
			_, err = c.DeleteFile(context.Background(), &proto.DeleteFileRequest{FileId: id})

		}
		if err != nil {
			fmt.Printf("Error deleting file: %v", err)
		} else {
			fmt.Printf("Successfully deleted %s%s", file, id)
		}
	}
}
