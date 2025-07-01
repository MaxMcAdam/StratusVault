package backend

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
)

// Test constants following existing pattern
const (
	TEST_FILE_ID_1 = "test-file-123"
	TEST_FILE_ID_2 = "test-file-456"
	TEST_FILE_NAME = "test-document.pdf"
	TEST_MIME_TYPE = "application/pdf"
	TEST_FILE_SIZE = int64(1024)
)

// Test helper functions following existing patterns
func createTestFileServiceServer(t *testing.T, db *redis.Client, tempDir string) *FileServiceServer {
	return &FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 512, MaxFileSize: 50000, StoragePath: tempDir}),
		logger:  newTestLogger(t),
		genUUID: func() string { return TEST_FILE_ID_1 },
		now:     mockTime,
	}
}

func createTestFile(t *testing.T, dir, filename string, content []byte) {
	err := os.WriteFile(path.Join(dir, filename), content, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
}

func createTestFileInfo(id, name string, status string) metadata.FileInfo {
	now := mockTime()
	return metadata.FileInfo{
		Id:        id,
		Name:      name,
		Status:    status,
		MimeType:  TEST_MIME_TYPE,
		Size:      TEST_FILE_SIZE,
		CreatedAt: &now,
		UpdatedAt: &now,
		Events: &metadata.EventInfo{
			LastUpdated: uint64(now.Unix()),
			LastRenamed: uint64(now.Unix()),
		},
	}
}

// Mock stream implementation for download testing
type mockDownloadStream struct {
	responses []*proto.DownloadFileResponse
}

func (m *mockDownloadStream) Send(resp *proto.DownloadFileResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockDownloadStream) Context() context.Context {
	return context.Background()
}

func (m *mockDownloadStream) SendMsg(msg interface{}) error {
	return nil
}

func Test_HandleDownloadRequest_Success_ByFileName(t *testing.T) {
	testData := []byte("Test content for download by filename")

	// Create mock redis client
	db, mock := redismock.NewClientMock()
	mock.ExpectHGet(metadata.NAME_INDEX, TEST_FILE_NAME).SetVal(TEST_FILE_ID_1)

	testDir := t.TempDir()
	createTestFile(t, testDir, TEST_FILE_ID_1, testData)

	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(testDir)

	s := &FileServiceServer{
		metaDB: metadata.NewWithClient(db),
		logger: newTestLogger(t),
	}

	req := &proto.DownloadFileRequest{
		FileName: TEST_FILE_NAME,
		Limit:    1024,
	}

	c := make(chan *proto.DownloadFileResponse, 10)

	err := s.HandleDownloadRequest(context.Background(), req, c)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_HandleDownloadRequest_Failure_FileNotFound(t *testing.T) {
	s := &FileServiceServer{
		logger: newTestLogger(t),
	}

	req := &proto.DownloadFileRequest{
		FileId: "nonexistent-file",
		Limit:  1024,
	}

	c := make(chan *proto.DownloadFileResponse, 10)

	err := s.HandleDownloadRequest(context.Background(), req, c)
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

// GetFileInfo Tests
func Test_GetFileInfo_Success_ByFileId(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bInfo))

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileInfoRequest{
		FileId: TEST_FILE_ID_1,
	}

	ctx := context.Background()
	resp, err := s.GetFileInfo(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp.Id != TEST_FILE_ID_1 {
		t.Errorf("Expected file ID %s, got %s", TEST_FILE_ID_1, resp.Id)
	}

	if resp.Name != TEST_FILE_NAME {
		t.Errorf("Expected file name %s, got %s", TEST_FILE_NAME, resp.Name)
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_GetFileInfo_Success_ByFileName(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations
	mock.ExpectHGet(metadata.NAME_INDEX, TEST_FILE_NAME).SetVal(TEST_FILE_ID_1)
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bInfo))

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileInfoRequest{
		FileName: TEST_FILE_NAME,
	}

	ctx := context.Background()
	resp, err := s.GetFileInfo(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp.Name != TEST_FILE_NAME {
		t.Errorf("Expected file name %s, got %s", TEST_FILE_NAME, resp.Name)
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_GetFileInfo_Failure_FileNotFound(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup mock expectations - file not found
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).RedisNil()

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileInfoRequest{
		FileId: TEST_FILE_ID_1,
	}

	ctx := context.Background()
	resp, err := s.GetFileInfo(ctx, req)

	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}

	if resp != nil {
		t.Error("Expected nil response for non-existent file")
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

// DeleteFile Tests
func Test_DeleteFile_Success_ByFileId(t *testing.T) {
	testData := []byte("Test file to be deleted")

	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations for delete operation
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bInfo))

	// Expect status update to DELETING
	deletingInfo := testInfo
	deletingInfo.Status = metadata.STATUS_DELETING
	bDeletingInfo, _ := json.Marshal(deletingInfo)
	mock.ExpectSet(getKey(TEST_FILE_ID_1), string(bDeletingInfo), 0).SetVal("OK")

	// Expect final deletion of metadata
	mock.ExpectDel(getKey(TEST_FILE_ID_1)).SetVal(1)
	mock.ExpectHDel(metadata.NAME_INDEX, TEST_FILE_NAME).SetVal(1)

	// Create test directory and file
	testDir := t.TempDir()
	createTestFile(t, testDir, TEST_FILE_ID_1, testData)

	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.DeleteFileRequest{
		FileId: TEST_FILE_ID_1,
	}

	ctx := context.Background()
	resp, err := s.DeleteFile(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp == nil {
		t.Error("Expected non-nil response")
	}

	// Verify file was deleted
	_, err = os.Stat(path.Join(testDir, TEST_FILE_ID_1))
	if !os.IsNotExist(err) {
		t.Error("Expected file to be deleted from filesystem")
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_DeleteFile_Success_ByFileName(t *testing.T) {
	testData := []byte("Test file to be deleted by name")

	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup mock expectations - lookup by filename
	mock.ExpectHGet(metadata.NAME_INDEX, TEST_FILE_NAME).SetVal(TEST_FILE_ID_1)

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations for delete operation
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bInfo))

	// Expect status update to DELETING
	deletingInfo := testInfo
	deletingInfo.Status = metadata.STATUS_DELETING
	bDeletingInfo, _ := json.Marshal(deletingInfo)
	mock.ExpectSet(getKey(TEST_FILE_ID_1), string(bDeletingInfo), 0).SetVal("OK")

	// Expect final deletion of metadata
	mock.ExpectDel(getKey(TEST_FILE_ID_1)).SetVal(1)
	mock.ExpectHDel(metadata.NAME_INDEX, TEST_FILE_NAME).SetVal(1)

	// Create test directory and file
	testDir := t.TempDir()
	createTestFile(t, testDir, TEST_FILE_ID_1, testData)

	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.DeleteFileRequest{
		FileName: TEST_FILE_NAME,
	}

	ctx := context.Background()
	resp, err := s.DeleteFile(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp == nil {
		t.Error("Expected non-nil response")
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_DeleteFile_Failure_FileNotFound(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup mock expectations - file not found
	mock.ExpectHGet(metadata.NAME_INDEX, TEST_FILE_NAME).RedisNil()

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.DeleteFileRequest{
		FileName: TEST_FILE_NAME,
	}

	ctx := context.Background()
	resp, err := s.DeleteFile(ctx, req)

	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}

	// Response should still be returned even on error
	if resp == nil {
		t.Error("Expected non-nil response even on error")
	}
}

func Test_DeleteFile_Failure_StorageDeleteError(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bInfo))

	// Expect status update to DELETING
	deletingInfo := testInfo
	deletingInfo.Status = metadata.STATUS_DELETING
	bDeletingInfo, _ := json.Marshal(deletingInfo)
	mock.ExpectSet(getKey(TEST_FILE_ID_1), string(bDeletingInfo), 0).SetVal("OK")

	// Expect rollback to ACTIVE status after storage failure
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).SetVal(string(bDeletingInfo))
	mock.ExpectSet(getKey(TEST_FILE_ID_1), string(bInfo), 0).SetVal("OK")

	// Use invalid directory to trigger storage error
	invalidDir := "/invalid/readonly/path"
	s := &FileServiceServer{
		metaDB:  metadata.NewWithClient(db),
		storage: storage.NewWithConfig(&storage.Config{ChunkSize: 512, MaxFileSize: 50000, StoragePath: invalidDir}),
		logger:  newTestLogger(t),
		genUUID: func() string { return TEST_FILE_ID_1 },
		now:     mockTime,
	}

	req := &proto.DeleteFileRequest{
		FileId: TEST_FILE_ID_1,
	}

	ctx := context.Background()
	resp, err := s.DeleteFile(ctx, req)

	if err == nil {
		t.Error("Expected error for storage deletion failure, got nil")
	}

	if resp == nil {
		t.Error("Expected non-nil response even on error")
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_GetFileEvents_Success_CreatedEvent(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_ACTIVE)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations
	mock.ExpectGet(getKey(TEST_FILE_NAME)).SetVal(string(bInfo))

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileEventsRequest{
		Tokens: []*proto.FileIdToken{
			{
				Id:        TEST_FILE_ID_1,
				Name:      TEST_FILE_NAME,
				LastToken: 0, // Zero token indicates new file
			},
		},
	}

	ctx := context.Background()
	resp, err := s.GetFileEvents(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp == nil {
		t.Fatalf("Expected 1 event, got nil")
	}

	if len(resp.Events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(resp.Events))
	}

	if resp.Events[0].EventType != proto.FileEvent_EVENT_TYPE_CREATED {
		t.Errorf("Expected CREATED event, got %v", resp.Events[0].EventType)
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_GetFileEvents_Success_DeletedEvent(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup test file info with deleted status
	testInfo := createTestFileInfo(TEST_FILE_ID_1, TEST_FILE_NAME, metadata.STATUS_DELETED)
	bInfo, _ := json.Marshal(testInfo)

	// Setup mock expectations
	mock.ExpectGet(getKey(TEST_FILE_NAME)).SetVal(string(bInfo))

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileEventsRequest{
		Tokens: []*proto.FileIdToken{
			{
				Id:        TEST_FILE_ID_1,
				Name:      TEST_FILE_NAME,
				LastToken: 8,
			},
		},
	}

	ctx := context.Background()
	resp, err := s.GetFileEvents(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if resp == nil {
		t.Fatalf("Expected 1 event, got nil")
	}

	if len(resp.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(resp.Events))
	}

	if resp.Events[0].EventType != proto.FileEvent_EVENT_TYPE_DELETED {
		t.Errorf("Expected DELETED event, got %v", resp.Events[0].EventType)
	}

	// Verify expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Redis expectations not met: %v", err)
	}
}

func Test_GetFileEvents_Failure_FileNotFound(t *testing.T) {
	// Create mock redis client
	db, mock := redismock.NewClientMock()

	// Setup mock expectations - file not found
	mock.ExpectGet(getKey(TEST_FILE_ID_1)).RedisNil()

	testDir := t.TempDir()
	s := createTestFileServiceServer(t, db, testDir)

	req := &proto.GetFileEventsRequest{
		Tokens: []*proto.FileIdToken{
			{
				Id:        TEST_FILE_ID_1,
				Name:      TEST_FILE_NAME,
				LastToken: 11,
			},
		},
	}

	ctx := context.Background()
	resp, err := s.GetFileEvents(ctx, req)

	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}

	if resp != nil {
		t.Error("Expected nil response for non-existent file")
	}
}

// Edge case and error handling tests
func Test_GetFileEvents_EmptyTokens(t *testing.T) {
	testDir := t.TempDir()
	s := createTestFileServiceServer(t, nil, testDir)

	req := &proto.GetFileEventsRequest{
		Tokens: []*proto.FileIdToken{},
	}

	ctx := context.Background()
	resp, err := s.GetFileEvents(ctx, req)

	if err != nil {
		t.Errorf("Expected no error for empty tokens, got: %v", err)
	}

	if resp == nil {
		t.Fatalf("Expected 1 event, got nil")
	}

	if len(resp.Events) != 0 {
		t.Errorf("Expected 0 events for empty tokens, got %d", len(resp.Events))
	}
}

// Test utility functions
func Test_generateFileID(t *testing.T) {
	id1 := generateFileID()
	id2 := generateFileID()

	if id1 == id2 {
		t.Error("Expected different UUIDs, got same")
	}

	if len(id1) == 0 {
		t.Error("Expected non-empty UUID")
	}
}

func Test_temp_function(t *testing.T) {
	testID := "test-id-123"
	expected := "temp/test-id-123"

	result := temp(testID)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func Test_now_function(t *testing.T) {
	before := time.Now()
	result := now()
	after := time.Now()

	if result.Before(before) || result.After(after) {
		t.Error("now() should return current time")
	}
}
