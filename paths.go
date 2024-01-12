package azio

import (
	"context"
	"fmt"
	"path"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

var (
	azPathExpr  = regexp.MustCompilePOSIX("^az:\\/\\/([^\\/]*)\\/([^\\/]*)(\\/(.*))?$")
	urlPathExpr = regexp.MustCompilePOSIX("^https:\\/\\/([^\\/]*).blob.core.windows.net\\/([^\\/]*)(\\/(.*))?$")
)

// A BlobPath points to a blob on an Azure blob storage account.
type BlobPath struct {
	Account   string
	Container string
	Path      string
}

// ParseBlobPath processes "az://" and "https://" URLs into Azure blob paths.
func ParseBlobPath(s string) (*BlobPath, error) {
	for _, expr := range []*regexp.Regexp{azPathExpr, urlPathExpr} {
		if match := expr.FindStringSubmatch(s); match != nil {
			return &BlobPath{
				Account:   match[1],
				Container: match[2],
				Path:      match[4],
			}, nil
		}
	}
	return nil, fmt.Errorf("invalid blob path: \"%s\"", s)
}

// Client constructs a blob client that can work for this path.
// The context may be used to override the ClientStore.
func (b *BlobPath) Client(ctx context.Context) (*azblob.Client, error) {
	cs := GetClientStore(ctx)
	return cs.Client(b.Account)
}

// String gets a human-readable form of the path, compatible with
// ParseBlobPath.
func (b *BlobPath) String() string {
	if b.Path == "" {
		return fmt.Sprintf("az://%s/%s", b.Account, b.Container)
	} else {
		return fmt.Sprintf("az://%s/%s/%s", b.Account, b.Container, b.Path)
	}
}

func (b *BlobPath) Base() string {
	if b.Path == "" {
		return b.Container
	} else {
		return path.Base(b.Path)
	}
}
