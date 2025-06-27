package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"github.com/go-redis/redismock/v9"
)

const (
	FILE_UUID   = "12345"
	FILE_NAME   = "testfile"
	FILE_PREFIX = "file_metadata"
)

func Test_UploadFile(t *testing.T) {
	test_data := []byte("testing 123 testing 456 testing 789")
	c := make(chan UploadRequest)

	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: 35, Overwrite: false}
		c <- UploadRequest{Metadata: meta}

		offset := 0
		chunkLen := 4
		finished := false
		for !finished {
			end := offset + chunkLen
			if end > len(test_data) {
				end = len(test_data)
				finished = true
			}
			c <- UploadRequest{ChunkData: test_data[offset:end]}
			offset = offset + chunkLen
		}
		c <- UploadRequest{IsEOF: true}
	}()

	db, mock := redismock.NewClientMock()
	ctx := context.Background()
	mock.ExpectHGet(metadata.NAME_INDEX, "testfile").RedisNil()
	info := metadata.FileInfo{
		UploadId: FILE_UUID,
		Status:   metadata.STATUS_UPLOADING,
		Name:     FILE_NAME,
		MimeType: "jpeg",
		Size:     35,
		Id:       FILE_UUID,
	}
	bInfo, err := json.Marshal(info)
	if err != nil {
		t.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")
	info.Status = metadata.STATUS_TEMP_UPLOAD
	now := mockTime()
	info.CreatedAt = &now
	info.UpdatedAt = &now
	bInfo, err = json.Marshal(info)
	if err != nil {
		t.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")
	mock.ExpectHSet(metadata.NAME_INDEX, FILE_NAME, FILE_UUID).SetVal(1)
	info.Status = metadata.STATUS_ACTIVE
	bInfo, err = json.Marshal(info)
	if err != nil {
		t.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")

	testDir := t.TempDir()
	s := FileServiceServer{metaDB: metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  log.New(os.Stdout, "", 1),
		genUUID: mockUUID,
		now:     mockTime,
	}

	res := s.ProcessUpload(ctx, c)
	if res.Error != nil {
		t.Errorf("Error proccessing file: %v", res.Error)
	}
}

func mockUUID() string {
	return FILE_UUID
}

func mockTime() time.Time {
	loc, _ := time.LoadLocation("")
	return time.Date(2025, time.June, 27, 16, 59, 8, 0, loc)
}

func getKey(fileId string) string {
	return fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)
}
