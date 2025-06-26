package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/MaxMcAdam/StratusVault/proto"
)

func Test_UploadProcess(t *testing.T) {
	testbytes := []byte("Testdata goes here.")
	reqChunk := &proto.UploadFileRequest{
		Request: &proto.UploadFileRequest_Chunk{
			Chunk: &proto.FileChunk{
				Data:   testbytes,
				Offset: 0,
				IsLast: true,
			}},
	}

	testFolder := t.TempDir()
	tempLoc := "temp_file"
	finalLoc := "folder_id/file_id"
	_, finalName := filepath.Split(finalLoc)
	strg := NewWithConfig(&Config{ChunkSize: 1000, MaxFileSize: 10000, StoragePath: testFolder})
	buf := &bytes.Buffer{}
	size := int64(len(testbytes))

	// process the chunk
	err := strg.ProcessChunk(context.Background(), reqChunk, buf, sha256.New(), &size, "testfile")
	if err != nil {
		t.Fatalf("Failed to process chunk: %v", err)
	}

	// write the rest of the buffer to file and validate file contents
	err = strg.FinalizeUpload(context.Background(), buf, tempLoc,
		"testfile", size, []byte{}, log.New(os.Stdout, "", 1))
	if err != nil {
		t.Fatalf("Failed to finalize upload: %v", err)
	}

	// verify the file was created
	fileInfo, err := os.Stat(filepath.Join(testFolder, tempLoc))
	if err != nil {
		t.Fatalf("Failed to stat created file: %v", err)
	} else if fileInfo.Name() != tempLoc {
		t.Fatalf("File created with incorrect name. Expected: %s. Found: %s.", tempLoc, fileInfo.Name())
	}

	// move the file to its final location
	bWritten, err := strg.MoveFile(context.Background(), tempLoc, finalLoc, log.New(os.Stdout, "", 1))
	if err != nil {
		t.Fatalf("Error moving file: %v", err)
	} else if bWritten != fileInfo.Size() {
		t.Fatalf("Unexpected number of bytes moved. Expected: %v. Moved: %v.", size, bWritten)
	}

	// verify the file was moved
	newFileInfo, err := os.Stat(filepath.Join(testFolder, finalLoc))
	if err != nil {
		t.Fatalf("Failed to stat created file: %v", err)
	} else if newFileInfo.Name() != finalName {
		t.Fatalf("File created with incorrect name. Expected: %s. Found: %s.", finalName, fileInfo.Name())
	} else if newFileInfo.Size() != fileInfo.Size() {
		t.Fatalf("Created file is not the expected size. Expected: %v. Found %v.", size, fileInfo.Size())
	}

	// verify temp file was removed
	_, err = os.Stat(filepath.Join(testFolder, tempLoc))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to cleanup temp file.")
	}

	strg.CleanupFailedUpload(finalLoc, log.New(os.Stdout, "", 1))
	_, err = os.Stat(filepath.Join(testFolder, finalLoc))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to cleanup final file.")
	}
}
