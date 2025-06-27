package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
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

// successful file upload
func Test_UploadFile_Success(t *testing.T) {
	test_data := []byte("testing 123 testing 456 testing 789")

	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	// tell the client what calls to expect
	mock, err = setupMockRedisForUploadSuccess(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
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

	// create the file service server
	testDir := t.TempDir()
	s := FileServiceServer{metaDB: metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  log.New(os.Stdout, "", 1),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error != nil {
		t.Errorf("Error proccessing file: %v", res.Error)
	}

	_, err = os.Stat(path.Join(testDir, FILE_UUID))
	if err != nil {
		t.Errorf("Error stating uploaded file; %v", err)
	}

	_, err = os.Stat(path.Join(testDir, FILE_UUID))
	if err != nil {
		t.Errorf("Error stating uploaded file; %v", err)
	}

	_, err = os.Stat(path.Join(testDir, s.getTempPath(FILE_UUID)))
	if !os.IsNotExist(err) {
		t.Errorf("Expected file does not exist error. Got: %v", err)
	}
}

// add the expeected redis calls to the mock client
func setupMockRedisForUploadSuccess(mock redismock.ClientMock) (redismock.ClientMock, error) {
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
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")
	info.Status = metadata.STATUS_TEMP_UPLOAD
	now := mockTime()
	info.CreatedAt = &now
	info.UpdatedAt = &now
	bInfo, err = json.Marshal(info)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")
	mock.ExpectHSet(metadata.NAME_INDEX, FILE_NAME, FILE_UUID).SetVal(1)
	info.Status = metadata.STATUS_ACTIVE
	bInfo, err = json.Marshal(info)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetVal("OK")

	return mock, nil
}

// upload fails because a file with the same name already exists and overwrite is not set
func Test_UploadFile_Failure1(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	// tell the client what calls to expect
	mock, err = setupMockRedisForUploadFailure1(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
	c := make(chan UploadRequest)
	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: 35, Overwrite: false}
		c <- UploadRequest{Metadata: meta}
	}()

	// create the file service server
	testDir := t.TempDir()
	s := FileServiceServer{metaDB: metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  log.New(os.Stdout, "", 1),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || res.Error.Error() != "file already exists" {
		t.Errorf("Incorrect error from failed upload: %v", res.Error)
	}
}

// add the expeected redis calls to the mock client
func setupMockRedisForUploadFailure1(mock redismock.ClientMock) (redismock.ClientMock, error) {
	mock.ExpectHGet(metadata.NAME_INDEX, "testfile").SetVal("987654")
	info := metadata.FileInfo{
		UploadId: FILE_UUID,
		Status:   metadata.STATUS_ACTIVE,
		Name:     FILE_NAME,
		Id:       "987654",
	}
	bInfo, err := json.Marshal(info)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	mock.ExpectGet(getKey("987654")).SetVal(string(bInfo))

	return mock, nil
}

// deterministic uuid to allow for redis call validation
func mockUUID() string {
	return FILE_UUID
}

// deterministic time to allow for redis call validation
func mockTime() time.Time {
	loc, _ := time.LoadLocation("")
	return time.Date(2025, time.June, 27, 16, 59, 8, 0, loc)
}

func getKey(fileId string) string {
	return fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)
}
