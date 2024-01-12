package azio

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// Stat returns information about the specified file path.
func Stat(ctx context.Context, p *BlobPath) (fs.FileInfo, error) {
	client, err := p.Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("get blob size: %s: %w", p, err)
	}
	pager := client.NewListBlobsFlatPager(p.Container, &azblob.ListBlobsFlatOptions{
		Prefix: &p.Path,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", p, err)
		}
		for _, blob := range page.Segment.BlobItems {
			if *blob.Name == p.Path {
				return &fileInfo{
					name:    p.Base(),
					size:    *blob.Properties.ContentLength,
					mode:    0755,
					modTime: *blob.Properties.LastModified,
					isDir:   false,
				}, nil
			} else {
				// We must be seeing some sub-path of this directory.
				return &fileInfo{
					name:    p.Base(),
					size:    0,
					mode:    0755,
					modTime: time.Time{},
					isDir:   true,
				}, nil
			}
		}
	}
	return nil, fmt.Errorf("could not find stats for %s: %w", p, os.ErrNotExist)
}

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (f *fileInfo) Name() string {
	return f.name
}

func (f *fileInfo) Size() int64 {
	return f.size
}

func (f *fileInfo) Mode() fs.FileMode {
	return f.mode
}

func (f *fileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *fileInfo) IsDir() bool {
	return f.isDir
}

// Sys returns the underlying data source (can return nil).
func (f *fileInfo) Sys() interface{} {
	return nil // Or appropriate system-specific value
}
