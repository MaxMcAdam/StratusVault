package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/MaxMcAdam/StratusVault/proto"
	redis "github.com/redis/go-redis/v9"
)

const (
	NAME_INDEX  = "name_index"
	FILE_PREFIX = "file_metadata"

	STATUS_UPLOADING = "uploading"
	STATUS_DELETING  = "deleting"
	STATUS_ACTIVE    = "active"
)

type MetadataDB struct {
	client *redis.Client
}

func Init() *MetadataDB {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection.
	pong, err := c.Ping(context.Background()).Result()
	if err != nil {
		fmt.Println("Could not connect to Redis:", err)
		return nil
	}

	fmt.Println("Redis connected:", pong)

	return &MetadataDB{client: c}
}

func getKey(fileId string) string {
	return fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)
}

// List file metadata matching the filter in the request
// Returns one page of file metadata starting with the cursor
func (m *MetadataDB) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	// Scan to get a list of file keys
	keys, cursor, err := m.client.Scan(ctx, req.GetPageToken(), getKey(req.GetFilter()), int64(req.GetPageSize())).Result()
	if err != nil {
		return nil, fmt.Errorf("Error scanning for keys: %v", err)
	}

	if len(keys) == 0 {
		fileInfoSlice := make([]*proto.FileInfo, 0)
		return &proto.ListFilesResponse{Files: fileInfoSlice, NextPageToken: cursor, TotalCount: 0}, nil
	}

	// Bulk get of the keys returned by the scan
	fileInfoInterfSlice, err := m.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("Error getting file metadata: %v", err)
	}

	fileInfoSlice := make([]*proto.FileInfo, len(keys))

	// Convert the list of interfaces returned to a list of file metadata types
	for _, fileInfoInter := range fileInfoInterfSlice {
		str, ok := fileInfoInter.(string)
		if !ok {
			return nil, fmt.Errorf("Invalid redis response type.")
		}

		fileInfo := proto.FileInfo{}
		if err := json.Unmarshal([]byte(str), &fileInfo); err != nil {
			return nil, fmt.Errorf("Error unmarshalling file metadata: %v", err)
		}

		fileInfoSlice = append(fileInfoSlice, &fileInfo)
	}

	return &proto.ListFilesResponse{Files: fileInfoSlice, NextPageToken: cursor, TotalCount: int32(len(keys))}, nil
}

// Get the single file with the given filename
func (m *MetadataDB) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	var err error
	fileId := req.GetFileId()
	if fileId == "" {
		fileId, err = m.GetFileIdInIndex(ctx, req.GetFileName())
		if err != nil {
			return nil, fmt.Errorf("Failed to get file id from index: %v", err)
		}
	}

	return m.getFileInfo(ctx, fileId)
}

// Get the single file with the given file id
func (m *MetadataDB) getFileInfo(ctx context.Context, fileId string) (*proto.FileInfo, error) {
	str, err := m.client.Get(ctx, getKey(fileId)).Result()
	if err != nil {
		return nil, err
	}

	fileInfo := proto.FileInfo{}
	if err := json.Unmarshal([]byte(str), &fileInfo); err != nil {
		return nil, err
	}

	return &fileInfo, nil
}

func (m *MetadataDB) SetFileInfo(ctx context.Context, info *proto.FileInfo) error {
	bInfo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	fileId := info.Id
	if fileId == "" {
		if fileId, err = m.GetFileIdInIndex(ctx, info.Name); err != nil {
			return err
		}
	}
	if _, err = m.client.Set(ctx, getKey(fileId), string(bInfo), 0).Result(); err != nil {
		return err
	}
	return nil
}

// If silent then log errors instead of returning
func (m *MetadataDB) SetFileInfoStatus(ctx context.Context, fileId, status string, silent bool, l *log.Logger) (*proto.FileInfo, error) {
	info, err := m.getFileInfo(ctx, fileId)
	if err != nil {
		if silent {
			l.Printf("Error setting status in file metdata: %v", err)
			return nil, nil
		}
		return nil, err
	}

	info.Status = status

	err = m.SetFileInfo(ctx, info)
	if err != nil {
		if silent {
			l.Printf("Error setting status in file metdata: %v", err)
			return nil, nil
		}
		return nil, err
	}

	return info, nil
}

func (m *MetadataDB) DeleteFileInfo(ctx context.Context, fileId string, fileName string) error {
	if fileId, err := m.GetFileIdInIndex(ctx, fileName); err != nil {
		return err
	} else if _, err := m.client.Del(ctx, getKey(fileId)).Result(); err != nil {
		return err
	} else if err = m.DeleteFileIdInIndex(ctx, fileName); err != nil {
		return err
	}
	return nil
}

func (m *MetadataDB) GetFileIdInIndex(ctx context.Context, fileName string) (string, error) {
	return m.client.HGet(ctx, NAME_INDEX, fileName).Result()
}

func (m *MetadataDB) SetFileIdInIndex(ctx context.Context, fileName string, fileId string) error {
	if _, err := m.client.HSet(ctx, NAME_INDEX, fileName, fileId).Result(); err != nil {
		return err
	}
	return nil
}

func (m *MetadataDB) DeleteFileIdInIndex(ctx context.Context, fileName string) error {
	if _, err := m.client.HDel(ctx, NAME_INDEX, fileName).Result(); err != nil {
		return err
	}
	return nil
}

// Delete the metadata record of failed upload
// Return no error since this is a cleanup function
func (m *MetadataDB) CleanupFailedUpload(ctx context.Context, fileId string, l *log.Logger) {
	if _, err := m.client.Del(ctx, fileId).Result(); err != nil {
		log.Printf("Error deleting record from failed upload: %v", err)
	}
}
