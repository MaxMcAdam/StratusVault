package backend

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/MaxMcAdam/StratusVault/proto"
	"github.com/MaxMcAdam/StratusVault/server/metadata"
	"github.com/MaxMcAdam/StratusVault/server/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type FileServiceServer struct {
	storage *storage.StorageBackend
	metaDB  *metadata.MetadataDB
	logger  *log.Logger
	genUUID func() string
	now     func() time.Time

	proto.UnimplementedFileServiceServer
}

func New() (*FileServiceServer, error) {
	metaDB, err := metadata.New()
	if err != nil {
		return nil, err
	}

	return &FileServiceServer{metaDB: metaDB, storage: storage.New(), logger: log.New(os.Stdout, "", 1), genUUID: generateFileID}, nil
}

func (s *FileServiceServer) DownloadFile(req *proto.DownloadFileRequest, stream grpc.ServerStreamingServer[proto.DownloadFileResponse]) error {
	fileId := req.GetFileId()

	var err error
	if fileId == "" {
		fileId, err = s.metaDB.GetFileIdInIndex(stream.Context(), req.GetFileName())
		if err != nil {
			return err
		}
	}

	f, err := os.Open(fileId)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	isLast := false
	bytesToRead := req.GetLimit()

	if bytesToRead == 0 {
		bytesToRead = info.Size()
	}

	offset := int64(0)

	for !isLast {
		// io.ReadFull will return an error if it does not fill the buffer
		if info.Size()-offset >= req.GetLimit() {
			isLast = true
			bytesToRead = info.Size() - offset
		}

		buf := make([]byte, bytesToRead)

		if _, err = f.Seek(offset, 0); err != nil {
			return err
		}

		if _, err = io.ReadFull(f, buf); err != nil {
			return err
		}

		err = stream.Send(&proto.DownloadFileResponse{
			Chunk: &proto.FileChunk{
				Data:   buf,
				Offset: offset,
				IsLast: isLast,
			}})

		if err != nil {
			s.logger.Printf("Error sending file chunk: %v", err)
		}
	}

	return nil
}

func (s *FileServiceServer) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	fmt.Printf("Recieved request %v", req)
	return (*s.metaDB).ListProtoFiles(ctx, req)
}

func (s *FileServiceServer) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileInfo, error) {
	fmt.Printf("Recieved request %v", req)
	if info, err := (*s.metaDB).GetFileInfo(ctx, req.FileId, req.FileName); err != nil {
		return nil, err
	} else {
		return metadata.ToProto(info), nil
	}

}

func (s *FileServiceServer) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (*emptypb.Empty, error) {
	var err error
	fileId := req.FileId
	if fileId == "" {
		fileId, err = s.metaDB.GetFileIdInIndex(ctx, req.FileName)
	}

	// Mark file metadata as being currently deleted
	info, err := s.metaDB.SetFileInfoStatus(ctx, fileId, metadata.STATUS_DELETING, false, s.logger)
	if err != nil {
		return &emptypb.Empty{}, err
	}

	// Delete file from filesystem
	if err = s.storage.Delete(ctx, fileId); err != nil {
		// Update the file status to active
		s.metaDB.SetFileInfoStatus(ctx, fileId, metadata.STATUS_ACTIVE, true, s.logger)
		return &emptypb.Empty{}, err
	}

	// Delete file metadata
	fmt.Printf("Deleting: %s %s", fileId, info.Name)
	if err := s.metaDB.DeleteFileInfo(ctx, fileId, info.Name); err != nil {
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}

func (s *FileServiceServer) GetFileEvents(ctx context.Context, req *proto.GetFileEventsRequest) (*proto.GetFileEventsResponse, error) {
	resp := &proto.GetFileEventsResponse{Events: []*proto.FileEvent{}}

	for _, fileEventReq := range req.Tokens {
		info, err := s.metaDB.GetFileInfo(ctx, fileEventReq.Name, fileEventReq.Id)
		if err != nil {
			return nil, err
		}
		if info.Status == metadata.STATUS_DELETED {
			resp.Events = append(resp.Events,
				&proto.FileEvent{EventType: proto.FileEvent_EVENT_TYPE_DELETED,
					Id:   info.Id,
					Name: info.Name,
				})
			continue
		}
		if fileEventReq.LastToken == 0 {
			resp.Events = append(resp.Events,
				&proto.FileEvent{EventType: proto.FileEvent_EVENT_TYPE_CREATED,
					Id:   info.Id,
					Name: info.Name,
				})
		}
		if info.Id != "" && info.Events.LastRenamed > fileEventReq.LastToken {
			resp.Events = append(resp.Events,
				&proto.FileEvent{EventType: proto.FileEvent_EVENT_TYPE_RENAMED,
					Id:   info.Id,
					Name: info.Name,
				})
		}
		if info.Events.LastUpdated > fileEventReq.LastToken {
			resp.Events = append(resp.Events,
				&proto.FileEvent{EventType: proto.FileEvent_EVENT_TYPE_UPDATED,
					Id:   info.Id,
					Name: info.Name,
				})
		}
	}

	return resp, nil
}

func generateFileID() string {
	return uuid.New().String()
}

func temp(id string) string {
	return fmt.Sprintf("temp/%s", id)
}

func now() time.Time {
	return time.Now()
}
