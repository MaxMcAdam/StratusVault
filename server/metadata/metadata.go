package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/go-redis/redis/v8"
)

type MetadataDB struct {
	client *redis.Client
}

func Init() *MetadataDB {
	c := redis.NewClient(&redis.Options{})

	return &MetadataDB{client: c}
}

// List file metadata matching the filter in the request
// Returns one page of file metadata starting with the cursor
func (m *MetadataDB) ListFiles(c context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	// Scan to get a list of file keys
	keys, cursor, err := m.client.Scan(c, req.GetPageToken(), req.GetFilter(), int64(req.GetPageSize())).Result()
	if err != nil {
		return nil, err
	}

	// Bulk get of the keys returned by the scan
	fileInfoInterfSlice, err := m.client.MGet(c, keys...).Result()
	if err != nil {
		return nil, err
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
			return nil, err
		}

		fileInfoSlice = append(fileInfoSlice, &fileInfo)
	}

	return &proto.ListFilesResponse{Files: fileInfoSlice, NextPageToken: cursor, TotalCount: int32(len(keys))}, nil
}

// Get the single file with the given filename
func (m *MetadataDB) GetFileInfo(c context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	str, err := m.client.Get(c, req.GetFileId()).Result()
	if err != nil {
		return nil, err
	}

	fileInfo := proto.FileInfo{}
	if err := json.Unmarshal([]byte(str), &fileInfo); err != nil {
		return nil, err
	}

	return &fileInfo, nil
}
