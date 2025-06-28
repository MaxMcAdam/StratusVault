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

// logger type to write output to t.Logf
type testLogWriter struct {
	t *testing.T
}

func (w *testLogWriter) Write(p []byte) (n int, err error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func newTestLogger(t *testing.T) *log.Logger {
	return log.New(&testLogWriter{t: t}, "", log.LstdFlags)
}

// Helper function to create time pointer
func timePtr(t time.Time) *time.Time {
	return &t
}

func getKey(fileId string) string {
	return fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)
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
		logger:  newTestLogger(t),
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
func Test_UploadFile_Failure_FileExists(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	// tell the client what calls to expect
	mock, err = setupMockRedisForUploadFailureFileExists(mock)
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
		logger:  newTestLogger(t),
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
func setupMockRedisForUploadFailureFileExists(mock redismock.ClientMock) (redismock.ClientMock, error) {
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

// upload fails when trying to overwrite a file that doesn't exist
func Test_UploadFile_Failure_OverwriteNonExistent(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	// tell the client what calls to expect
	mock, err = setupMockRedisForOverwriteNonExistent(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
	c := make(chan UploadRequest)
	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: 35, Overwrite: true}
		c <- UploadRequest{Metadata: meta}
	}()

	// create the file service server
	testDir := t.TempDir()
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	expectedError := fmt.Sprintf("no existing file to overwrite: %s", FILE_NAME)
	if res.Error == nil || res.Error.Error() != expectedError {
		t.Errorf("Expected error '%s', got: %v", expectedError, res.Error)
	}
}

func setupMockRedisForOverwriteNonExistent(mock redismock.ClientMock) (redismock.ClientMock, error) {
	// File doesn't exist in name index
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).RedisNil()
	return mock, nil
}

// upload fails when first chunk is missing metadata
func Test_UploadFile_Failure_MissingMetadata(t *testing.T) {
	// create a mock redis client
	db, _ := redismock.NewClientMock()

	// create a channel to imitate the incoming chunks - first chunk has no metadata
	c := make(chan UploadRequest)
	go func() {
		c <- UploadRequest{ChunkData: []byte("some data")} // No metadata
	}()

	// create the file service server
	testDir := t.TempDir()
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || res.Error.Error() != "processing metadata: first chunk missing metadata" {
		t.Errorf("Expected 'processing metadata: first chunk missing metadata', got: %v", res.Error)
	}
}

// upload fails when metadata creation fails due to redis error
func Test_UploadFile_Failure_MetadataCreationError(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	mock, err = setupMockRedisForMetadataCreationError(mock)
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
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || !contains(res.Error.Error(), "creating file metadata") {
		t.Errorf("Expected error containing 'creating file metadata', got: %v", res.Error)
	}
}

func setupMockRedisForMetadataCreationError(mock redismock.ClientMock) (redismock.ClientMock, error) {
	// File doesn't exist
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).RedisNil()

	info := metadata.FileInfo{
		UploadId: FILE_UUID,
		Id:       FILE_UUID,
		Status:   metadata.STATUS_UPLOADING,
		Name:     FILE_NAME,
		Size:     35,
		MimeType: "jpeg",
	}
	bInfo, err := json.Marshal(info)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	// Metadata creation fails
	mock.ExpectSet(getKey(FILE_UUID), string(bInfo), 0).SetErr(fmt.Errorf("redis connection error"))

	// Expect cleanup call for failed upload
	mock.ExpectDel(getKey(FILE_UUID)).SetVal(1)

	return mock, nil
}

// upload fails when redis lookup for existing file fails (not redis.Nil)
func Test_UploadFile_Failure_RedisLookupError(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	mock, err = setupMockRedisForLookupError(mock)
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
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || !contains(res.Error.Error(), "checking existing file") {
		t.Errorf("Expected error containing 'checking existing file', got: %v", res.Error)
	}
}

func setupMockRedisForLookupError(mock redismock.ClientMock) (redismock.ClientMock, error) {
	// Redis lookup fails with non-Nil error
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).SetErr(fmt.Errorf("redis connection timeout"))
	return mock, nil
}

// upload fails when trying to overwrite but locking existing file fails
func Test_UploadFile_Failure_OverwriteLockError(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	mock, err = setupMockRedisForOverwriteLockError(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
	c := make(chan UploadRequest)
	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: 35, Overwrite: true}
		c <- UploadRequest{Metadata: meta}
	}()

	// create the file service server
	testDir := t.TempDir()
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || !contains(res.Error.Error(), "locking existing file") {
		t.Errorf("Expected error containing 'locking existing file', got: %v", res.Error)
	}
}

func setupMockRedisForOverwriteLockError(mock redismock.ClientMock) (redismock.ClientMock, error) {
	existingFileID := "existing_file_id"

	// File exists
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).SetVal(existingFileID)

	// Get existing file info
	existingInfo := metadata.FileInfo{
		Id:       existingFileID,
		Name:     FILE_NAME,
		Status:   metadata.STATUS_ACTIVE,
		UploadId: existingFileID,
	}
	bInfo, err := json.Marshal(existingInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling existing info")
	}
	mock.ExpectGet(getKey(existingFileID)).SetVal(string(bInfo))

	// Locking fails - this would be called by SetFileInfoStatus
	mock.ExpectGet(getKey(existingFileID)).SetErr(fmt.Errorf("redis lock error"))

	// Expect cleanup call to restore old file status - SetFileInfoStatus will be called during cleanup
	mock.ExpectGet(getKey(existingFileID)).SetVal(string(bInfo))

	info := metadata.FileInfo{
		UploadId: existingFileID,
		Status:   metadata.STATUS_ACTIVE,
		Name:     FILE_NAME,
		Id:       existingFileID,
	}
	bInfo, err = json.Marshal(info)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling expected info")
	}
	mock.ExpectSet(getKey(existingFileID), string(bInfo), 0).SetVal("OK")

	return mock, nil
}

// upload successful overwrite case
func Test_UploadFile_Success_Overwrite(t *testing.T) {
	test_data := []byte("new file content for overwrite")

	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	mock, err = setupMockRedisForOverwriteSuccess(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
	c := make(chan UploadRequest)
	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: int64(len(test_data)), Overwrite: true}
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

	// Create existing file to be overwritten
	existingFileID := "existing_file_123"
	existingFilePath := path.Join(testDir, existingFileID)
	err = os.WriteFile(existingFilePath, []byte("old content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create existing file: %v", err)
	}

	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: testDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error != nil {
		t.Errorf("Error processing overwrite upload: %v", res.Error)
	}

	// Check that file exists with existing ID (not new UUID)
	if res.FileID != existingFileID {
		t.Errorf("Expected file ID to be existing ID %s, got %s", existingFileID, res.FileID)
	}

	// Check file content was updated
	content, err := os.ReadFile(existingFilePath)
	if err != nil {
		t.Errorf("Error reading overwritten file: %v", err)
	}
	if string(content) != string(test_data) {
		t.Errorf("File content not updated correctly. Expected %s, got %s", test_data, content)
	}

	// Verify temp file was cleaned up
	_, err = os.Stat(path.Join(testDir, s.getTempPath(FILE_UUID)))
	if !os.IsNotExist(err) {
		t.Errorf("Expected temp file to be cleaned up. Got: %v", err)
	}
}

func setupMockRedisForOverwriteSuccess(mock redismock.ClientMock) (redismock.ClientMock, error) {
	existingFileID := "existing_file_123"

	// File exists in index
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).SetVal(existingFileID)

	// Get existing file info
	existingInfo := metadata.FileInfo{
		Id:        existingFileID,
		Name:      FILE_NAME,
		Status:    metadata.STATUS_ACTIVE,
		CreatedAt: timePtr(time.Date(2025, time.June, 1, 10, 0, 0, 0, time.UTC)),
	}
	bInfo, err := json.Marshal(existingInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling existing info")
	}
	mock.ExpectGet(getKey(existingFileID)).SetVal(string(bInfo))

	// Lock existing file for overwrite - SetFileInfoStatus calls
	mock.ExpectGet(getKey(existingFileID)).SetVal(string(bInfo))
	lockedInfo := existingInfo
	lockedInfo.Status = metadata.STATUS_OVERWRITING
	bLockedInfo, err := json.Marshal(lockedInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling locked info")
	}
	mock.ExpectSet(getKey(existingFileID), string(bLockedInfo), 0).SetVal("OK")

	// Create new upload metadata
	newInfo := metadata.FileInfo{
		UploadId: FILE_UUID,
		Status:   metadata.STATUS_UPLOADING,
		Name:     FILE_NAME,
		MimeType: "jpeg",
		Size:     30, // length of test data
		Id:       FILE_UUID,
	}
	bNewInfo, err := json.Marshal(newInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling new info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bNewInfo), 0).SetVal("OK")

	// Update to temp status with existing creation time
	newInfo.Status = metadata.STATUS_TEMP_UPLOAD
	now := mockTime()
	newInfo.UpdatedAt = &now
	newInfo.CreatedAt = existingInfo.CreatedAt // Should preserve original creation time
	bTempInfo, err := json.Marshal(newInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling temp info")
	}
	mock.ExpectSet(getKey(FILE_UUID), string(bTempInfo), 0).SetVal("OK")

	// Final activation with existing file ID
	finalInfo := newInfo
	finalInfo.Id = existingFileID
	finalInfo.Status = metadata.STATUS_ACTIVE
	bFinalInfo, err := json.Marshal(finalInfo)
	if err != nil {
		return mock, fmt.Errorf("Error marshaling final info")
	}
	mock.ExpectSet(getKey(existingFileID), string(bFinalInfo), 0).SetVal("OK")

	return mock, nil
}

// upload fails during chunk processing due to storage error
func Test_UploadFile_Failure_ChunkProcessingError(t *testing.T) {
	// create a mock redis client
	db, mock := redismock.NewClientMock()
	var err error
	mock, err = setupMockRedisForChunkProcessingError(mock)
	if err != nil {
		t.Errorf("Error setting up mock redis: %v", err)
	}

	// create a channel to imitate the incoming chunks
	c := make(chan UploadRequest)
	go func() {
		meta := &proto.FileMetadata{Name: FILE_NAME, MimeType: "jpeg", Size: 35, Overwrite: false}
		c <- UploadRequest{Metadata: meta}

		// Send chunk that will cause processing error
		c <- UploadRequest{ChunkData: []byte("chunk data")}
		c <- UploadRequest{IsEOF: true}
	}()

	// create the file service server with invalid storage path to trigger error
	invalidDir := "/invalid/readonly/path"
	s := FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 20, MaxFileSize: 20000, StoragePath: invalidDir}),
		logger:  newTestLogger(t),
		genUUID: mockUUID,
		now:     mockTime,
	}

	ctx := context.Background()
	// process the uploaded data
	res := s.ProcessUpload(ctx, c)
	if res.Error == nil || !contains(res.Error.Error(), "error creating directories") {
		t.Errorf("Expected error containing 'error creating directories', got: %v", res.Error)
	}
}

func setupMockRedisForChunkProcessingError(mock redismock.ClientMock) (redismock.ClientMock, error) {
	// File doesn't exist
	mock.ExpectHGet(metadata.NAME_INDEX, FILE_NAME).RedisNil()

	// Create metadata successfully
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

	// Cleanup expectations for failed upload
	mock.ExpectDel(getKey(FILE_UUID)).SetVal(1)

	return mock, nil
}

// Helper function to check if string contains substring
func contains(str, substr string) bool {
	return len(str) >= len(substr) && (str == substr ||
		(len(str) > len(substr) &&
			(str[:len(substr)] == substr ||
				str[len(str)-len(substr):] == substr ||
				containsSubstring(str, substr))))
}

func containsSubstring(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
