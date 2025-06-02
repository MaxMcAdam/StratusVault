package metadata

import (
	"context"

	"github.com/MaxMcAdam/StratusVault/proto"
)

type MetadataStore interface {
	ListFiles(context.Context, *proto.ListFilesRequest) (*proto.ListFilesResponse, error)
	GetFileInfo(context.Context, *proto.GetFileInfoRequest) (*proto.FileInfo, error)
}
