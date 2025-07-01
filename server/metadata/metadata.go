package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	NAME_INDEX  = "name_index"
	FILE_PREFIX = "file_metadata"

	STATUS_UPLOADING   = "uploading"
	STATUS_UPDATING    = "updating"
	STATUS_DELETING    = "deleting"
	STATUS_DELETED     = "deleted"
	STATUS_ACTIVE      = "active"
	STATUS_TEMP_UPLOAD = "temp_upload"
	STATUS_OVERWRITING = "overwriting"
)

type FileInfo struct {
	Name      string
	Id        string
	UploadId  string // for internal use only
	MimeType  string
	Size      int64
	Status    string
	CreatedAt *time.Time
	UpdatedAt *time.Time
	Events    *EventInfo
}

// version is the most recent version of the file
// deletion time marks when the file metadata will become invalid
type EventInfo struct {
	LastUpdated  uint64
	LastRenamed  uint64
	DeletionTime *time.Time
}

type Config struct {
	Addr string
}

func FromProto(p *proto.FileInfo) *FileInfo {
	if p == nil {
		return nil
	}
	created := p.CreatedAt.AsTime()
	updated := p.UpdatedAt.AsTime()
	return &FileInfo{
		Name:      p.Name,
		Id:        p.Id,
		MimeType:  p.MimeType,
		Size:      p.Size,
		Status:    p.Status,
		CreatedAt: &created,
		UpdatedAt: &updated,
	}
}

func ToProto(f *FileInfo) *proto.FileInfo {
	if f == nil {
		return nil
	}
	return &proto.FileInfo{
		Name:      f.Name,
		Id:        f.Id,
		MimeType:  f.MimeType,
		Size:      f.Size,
		Status:    f.Status,
		CreatedAt: timestamppb.New(*f.CreatedAt),
		UpdatedAt: timestamppb.New(*f.UpdatedAt),
	}
}

type MetadataDB struct {
	client *redis.Client
}

func New() (*MetadataDB, error) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection.
	pong, err := c.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("Could not connect to Redis: %v", err)
	}

	fmt.Println("Redis connected:", pong)

	return &MetadataDB{client: c}, nil
}

func NewWithConfig(config *Config) (*MetadataDB, error) {
	c := redis.NewClient(&redis.Options{Addr: config.Addr,
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection.
	pong, err := c.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("Could not connect to Redis: %v", err)
	}

	fmt.Println("Redis connected:", pong)

	return &MetadataDB{client: c}, nil
}

func NewWithClient(c *redis.Client) *MetadataDB {
	return &MetadataDB{client: c}
}

func getKey(fileId string) string {
	return fmt.Sprintf("%s:%s", FILE_PREFIX, fileId)
}

// List file metadata matching the filter in the request
// Returns one page of file metadata starting with the cursor
func (m *MetadataDB) ListProtoFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
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

	fileInfoSlice := []*proto.FileInfo{}

	// Convert the list of interfaces returned to a list of file metadata types
	for _, fileInfoInter := range fileInfoInterfSlice {
		str, ok := fileInfoInter.(string)
		if !ok {
			return nil, fmt.Errorf("Invalid redis response type.")
		}

		fileInfo := &FileInfo{}
		if err := json.Unmarshal([]byte(str), &fileInfo); err != nil {
			return nil, fmt.Errorf("Error unmarshalling file metadata: %v", err)
		}

		fileInfoSlice = append(fileInfoSlice, ToProto(fileInfo))
	}

	return &proto.ListFilesResponse{Files: fileInfoSlice, NextPageToken: cursor, TotalCount: int32(len(keys))}, nil
}

// Get the single file with the given filename
func (m *MetadataDB) GetFileInfo(ctx context.Context, fileId, fileName string) (*FileInfo, error) {
	var err error
	if fileId == "" {
		fileId, err = m.GetFileIdInIndex(ctx, fileName)
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Failed to get file id from index: %v", err)
		}
	}

	return m.getFileInfo(ctx, fileId)
}

// Get the single file with the given file id
func (m *MetadataDB) getFileInfo(ctx context.Context, fileId string) (*FileInfo, error) {
	str, err := m.client.Get(ctx, getKey(fileId)).Result()
	if err != nil {
		return nil, err
	}

	fileInfo := FileInfo{}
	if err := json.Unmarshal([]byte(str), &fileInfo); err != nil {
		return nil, err
	}

	return &fileInfo, nil
}

func (m *MetadataDB) SetFileInfo(ctx context.Context, info *FileInfo) error {
	fileId := info.Id
	if fileId == "" {
		fileId = info.UploadId
	}
	fileId, err := m.GetFileId(ctx, info.Name, fileId)
	if err != nil {
		return fmt.Errorf("Error getting file ID: %v", err)
	}

	bInfo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	if _, err = m.client.Set(ctx, getKey(fileId), string(bInfo), 0).Result(); err != nil {
		return err
	}
	return nil
}

// If silent then log errors instead of returning
func (m *MetadataDB) SetFileInfoStatus(ctx context.Context, fileId, status string, silent bool, l *log.Logger) (*FileInfo, error) {
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

func (m *MetadataDB) DeleteFileInfo(ctx context.Context, id string, name string) error {
	if fileId, err := m.GetFileId(ctx, name, id); err != nil {
		return err
	} else if _, err := m.client.Del(ctx, getKey(fileId)).Result(); err != nil {
		return err
	} else if err = m.DeleteFileIdInIndex(ctx, name); err != nil {
		return err
	}
	return nil
}

func (m *MetadataDB) OverwriteFileInfo(ctx context.Context, fileInfo *FileInfo, existingId string) (*FileInfo, error) {
	if existingId == "" {
		fileInfo.Id = fileInfo.UploadId
		return fileInfo, nil
	}

	existingInfo, err := m.getFileInfo(ctx, existingId)
	if err != nil {
		return nil, err
	}

	existingInfo.UploadId = fileInfo.UploadId
	existingInfo.Size = fileInfo.Size
	existingInfo.MimeType = fileInfo.MimeType
	existingInfo.Status = STATUS_TEMP_UPLOAD

	return existingInfo, m.SetFileInfo(ctx, existingInfo)
}

func (m *MetadataDB) GetFileId(ctx context.Context, name, id string) (string, error) {
	if id != "" {
		return id, nil
	}
	return m.GetFileIdInIndex(ctx, name)
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
	if _, err := m.client.Del(ctx, getKey(fileId)).Result(); err != nil {
		log.Printf("Error deleting record from failed upload: %v", err)
	}
}
