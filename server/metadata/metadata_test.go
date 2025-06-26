package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test setup helpers
func setupTestDB(t *testing.T) (*MetadataDB, *miniredis.Miniredis) {
	// Use miniredis for isolated testing
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return &MetadataDB{client: client}, mr
}

func createTestFileInfo(name string) *FileInfo {
	now := time.Now()
	return &FileInfo{
		Name:      name,
		Id:        "test-file-id-123",
		UploadId:  "upload-id-456",
		MimeType:  "text/plain",
		Size:      1024,
		Status:    STATUS_ACTIVE,
		CreatedAt: &now,
		UpdatedAt: &now,
	}
}

// Core CRUD Operations Tests
func TestSetFileInfo_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()

	err := db.SetFileInfo(ctx, fileInfo)
	assert.NoError(t, err)

	// Verify storage in Redis
	key := getKey(fileInfo.Id)
	stored, err := db.client.Get(ctx, key).Result()
	require.NoError(t, err)

	var retrieved FileInfo
	err = json.Unmarshal([]byte(stored), &retrieved)
	require.NoError(t, err)

	assert.Equal(t, fileInfo.Name, retrieved.Name)
	assert.Equal(t, fileInfo.Id, retrieved.Id)
	assert.Equal(t, fileInfo.Size, retrieved.Size)
}

func TestGetFileInfo_ByFileId_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()

	// Setup: Store file info
	err := db.SetFileInfo(ctx, fileInfo)
	require.NoError(t, err)

	// Test: Retrieve by file ID
	retrieved, err := db.GetFileInfo(ctx, fileInfo.Id, "")
	assert.NoError(t, err)
	assert.Equal(t, fileInfo.Name, retrieved.Name)
	assert.Equal(t, fileInfo.Id, retrieved.Id)
}

func TestGetFileInfo_ByFileName_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()

	// Setup: Store file info and name index
	err := db.SetFileInfo(ctx, fileInfo)
	require.NoError(t, err)
	err = db.SetFileIdInIndex(ctx, fileInfo.Name, fileInfo.Id)
	require.NoError(t, err)

	// Test: Retrieve by filename
	retrieved, err := db.GetFileInfo(ctx, "", fileInfo.Name)
	assert.NoError(t, err)
	assert.Equal(t, fileInfo.Name, retrieved.Name)
	assert.Equal(t, fileInfo.Id, retrieved.Id)
}

func TestGetFileInfo_NotFound(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()

	// Test: Try to get non-existent file
	_, err := db.GetFileInfo(ctx, "non-existent-id", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis: nil")
}

func TestDeleteFileInfo_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()

	// Setup: Store file info and name index
	err := db.SetFileInfo(ctx, fileInfo)
	fmt.Printf("Creating file %s", fileInfo.Id)
	require.NoError(t, err)
	err = db.SetFileIdInIndex(ctx, fileInfo.Name, fileInfo.Id)
	require.NoError(t, err)

	// Test: Delete file info
	err = db.DeleteFileInfo(ctx, fileInfo.Id, fileInfo.Name)
	fmt.Printf("Deleted file %s", fileInfo.Id)
	assert.NoError(t, err)

	// Verify: File info deleted
	_, err = db.GetFileInfo(ctx, fileInfo.Id, "")
	assert.Error(t, err)

	// Verify: Name index entry deleted
	_, err = db.GetFileIdInIndex(ctx, fileInfo.Name)
	assert.Error(t, err)
}

// Name Index Tests
func TestSetFileIdInIndex_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	fileName := "test.txt"
	fileId := "test-id-123"

	err := db.SetFileIdInIndex(ctx, fileName, fileId)
	assert.NoError(t, err)

	// Verify storage
	retrieved, err := db.GetFileIdInIndex(ctx, fileName)
	assert.NoError(t, err)
	assert.Equal(t, fileId, retrieved)
}

func TestGetFileIdInIndex_NotFound(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()

	_, err := db.GetFileIdInIndex(ctx, "non-existent.txt")
	assert.Error(t, err)
}

func TestDeleteFileIdInIndex_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	fileName := "test.txt"
	fileId := "test-id-123"

	// Setup
	err := db.SetFileIdInIndex(ctx, fileName, fileId)
	require.NoError(t, err)

	// Test deletion
	err = db.DeleteFileIdInIndex(ctx, fileName)
	assert.NoError(t, err)

	// Verify deletion
	_, err = db.GetFileIdInIndex(ctx, fileName)
	assert.Error(t, err)
}

// Status Management Tests
func TestSetFileInfoStatus_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Setup: Store initial file info
	err := db.SetFileInfo(ctx, fileInfo)
	require.NoError(t, err)

	// Test: Update status
	updatedInfo, err := db.SetFileInfoStatus(ctx, fileInfo.Id, STATUS_UPLOADING, false, logger)
	assert.NoError(t, err)
	assert.Equal(t, STATUS_UPLOADING, updatedInfo.Status)

	// Verify: Status was updated in storage
	retrieved, err := db.GetFileInfo(ctx, fileInfo.Id, "")
	assert.NoError(t, err)
	assert.Equal(t, STATUS_UPLOADING, retrieved.Status)
}

func TestSetFileInfoStatus_SilentMode(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Test: Try to update status for non-existent file in silent mode
	updatedInfo, err := db.SetFileInfoStatus(ctx, "non-existent", STATUS_UPLOADING, true, logger)
	assert.NoError(t, err) // Should not return error in silent mode
	assert.Nil(t, updatedInfo)
}

// Overwrite Functionality Tests
func TestOverwriteFileInfo_NewFile(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()

	// Test: Overwrite with no existing file
	result, err := db.OverwriteFileInfo(ctx, fileInfo, "")
	assert.NoError(t, err)
	assert.Equal(t, fileInfo.UploadId, result.Id) // Should use UploadId as Id
}

func TestOverwriteFileInfo_ExistingFile(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	// Setup: Create existing file
	existingFile := createTestFileInfo("existing.txt")
	existingFile.Id = "existing-id"
	existingFile.Size = 512
	ctx := context.Background()

	err := db.SetFileInfo(ctx, existingFile)
	require.NoError(t, err)

	// Test: Overwrite existing file
	newFileInfo := createTestFileInfo("existing.txt")
	newFileInfo.Size = 2048
	newFileInfo.UploadId = "new-upload-id"

	result, err := db.OverwriteFileInfo(ctx, newFileInfo, existingFile.Id)
	assert.NoError(t, err)

	// Should preserve existing ID but update other fields
	assert.Equal(t, existingFile.Id, result.Id)
	assert.Equal(t, newFileInfo.UploadId, result.UploadId)
	assert.Equal(t, newFileInfo.Size, result.Size)
	assert.Equal(t, STATUS_TEMP_UPLOAD, result.Status)
}

// List Files Tests
func TestListProtoFiles_EmptyDatabase(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	req := &proto.ListFilesRequest{
		PageSize:  10,
		PageToken: 0,
		Filter:    "*",
	}

	response, err := db.ListProtoFiles(ctx, req)
	assert.NoError(t, err)
	assert.Empty(t, response.Files)
	assert.Equal(t, int32(0), response.TotalCount)
}

func TestListProtoFiles_WithData(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()

	// Setup: Add multiple files
	files := []*FileInfo{
		createTestFileInfo("file1.txt"),
		createTestFileInfo("file2.txt"),
		createTestFileInfo("file3.txt"),
	}
	files[0].Id = "id1"
	files[1].Id = "id2"
	files[2].Id = "id3"

	for _, file := range files {
		err := db.SetFileInfo(ctx, file)
		require.NoError(t, err)
	}

	// Test: List files
	req := &proto.ListFilesRequest{
		PageSize:  10,
		PageToken: 0,
		Filter:    "*",
	}

	response, err := db.ListProtoFiles(ctx, req)
	assert.NoError(t, err)
	assert.Len(t, response.Files, 3)
	assert.Equal(t, int32(3), response.TotalCount)
}

// Proto Conversion Tests
func TestFromProto_Success(t *testing.T) {
	protoFile := &proto.FileInfo{
		Name:      "test.txt",
		Id:        "test-id",
		MimeType:  "text/plain",
		Size:      1024,
		Status:    STATUS_ACTIVE,
		CreatedAt: nil, // Will be set by timestamppb
		UpdatedAt: nil, // Will be set by timestamppb
	}

	result := FromProto(protoFile)
	assert.Equal(t, protoFile.Name, result.Name)
	assert.Equal(t, protoFile.Id, result.Id)
	assert.Equal(t, protoFile.MimeType, result.MimeType)
	assert.Equal(t, protoFile.Size, result.Size)
	assert.Equal(t, protoFile.Status, result.Status)
}

func TestToProto_Success(t *testing.T) {
	now := time.Now()
	fileInfo := &FileInfo{
		Name:      "test.txt",
		Id:        "test-id",
		MimeType:  "text/plain",
		Size:      1024,
		Status:    STATUS_ACTIVE,
		CreatedAt: &now,
		UpdatedAt: &now,
	}

	result := ToProto(fileInfo)
	assert.Equal(t, fileInfo.Name, result.Name)
	assert.Equal(t, fileInfo.Id, result.Id)
	assert.Equal(t, fileInfo.MimeType, result.MimeType)
	assert.Equal(t, fileInfo.Size, result.Size)
	assert.Equal(t, fileInfo.Status, result.Status)
	assert.NotNil(t, result.CreatedAt)
	assert.NotNil(t, result.UpdatedAt)
}

// Cleanup and Error Handling Tests
func TestCleanupFailedUpload_Success(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("test.txt")
	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Setup: Store file info
	err := db.SetFileInfo(ctx, fileInfo)
	require.NoError(t, err)

	// Test: Cleanup should not return error even if deletion fails
	db.CleanupFailedUpload(ctx, fileInfo.Id, logger)

	// This test mainly verifies the function doesn't panic
	// and logs appropriately (hard to test logging in unit tests)
}

// Edge Cases and Error Conditions
func TestGetFileId_PreferDirectId(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	directId := "direct-id"
	fileName := "test.txt"

	// Test: When both ID and name provided, should prefer direct ID
	result, err := db.GetFileId(ctx, fileName, directId)
	assert.NoError(t, err)
	assert.Equal(t, directId, result)
}

func TestGetFileId_FallbackToName(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	ctx := context.Background()
	fileName := "test.txt"
	expectedId := "name-resolved-id"

	// Setup name index
	err := db.SetFileIdInIndex(ctx, fileName, expectedId)
	require.NoError(t, err)

	// Test: When no direct ID provided, should resolve from name
	result, err := db.GetFileId(ctx, fileName, "")
	assert.NoError(t, err)
	assert.Equal(t, expectedId, result)
}

// Key Generation Test
func TestGetKey_Format(t *testing.T) {
	fileId := "test-file-123"
	expected := fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)

	result := getKey(fileId)
	assert.Equal(t, expected, result)
}

// Integration-style Tests
func TestFileLifecycle_CompleteFlow(t *testing.T) {
	db, mr := setupTestDB(t)
	defer mr.Close()

	fileInfo := createTestFileInfo("lifecycle-test.txt")
	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// 1. Create file with uploading status
	fileInfo.Status = STATUS_UPLOADING
	err := db.SetFileInfo(ctx, fileInfo)
	require.NoError(t, err)

	// 2. Set name index
	err = db.SetFileIdInIndex(ctx, fileInfo.Name, fileInfo.Id)
	require.NoError(t, err)

	// 3. Update status to active
	_, err = db.SetFileInfoStatus(ctx, fileInfo.Id, STATUS_ACTIVE, false, logger)
	require.NoError(t, err)

	// 4. Verify file is active and retrievable by name
	retrieved, err := db.GetFileInfo(ctx, "", fileInfo.Name)
	require.NoError(t, err)
	assert.Equal(t, STATUS_ACTIVE, retrieved.Status)

	// 5. Mark for deletion
	_, err = db.SetFileInfoStatus(ctx, fileInfo.Id, STATUS_DELETING, false, logger)
	require.NoError(t, err)

	// 6. Complete deletion
	err = db.DeleteFileInfo(ctx, fileInfo.Id, fileInfo.Name)
	require.NoError(t, err)

	// 7. Verify file is gone
	_, err = db.GetFileInfo(ctx, fileInfo.Id, "")
	assert.Error(t, err)
}
