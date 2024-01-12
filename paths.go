package azio

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

var (
	azPathExpr  = regexp.MustCompilePOSIX("^az:\\/\\/([^\\/]*)\\/([^\\/]*)(\\/(.*))?$")
	urlPathExpr = regexp.MustCompilePOSIX("^https:\\/\\/([^\\/]*).blob.core.windows.net\\/([^\\/]*)(\\/(.*))?$")
)

type BlobPath struct {
	Account   string
	Container string
	Path      string
}

func ParseBlobPath(s string) (*BlobPath, error) {
	for _, expr := range []*regexp.Regexp{azPathExpr, urlPathExpr} {
		if match := expr.FindStringSubmatch(s); match != nil {
			return &BlobPath{
				Account:   match[0],
				Container: match[1],
				Path:      match[3],
			}, nil
		}
	}
	return nil, fmt.Errorf("invalid blob path: \"%s\"", s)
}

// Client constructs a blob client that can work for this path.
// The context may be used to override the ClientStore.
func (b *BlobPath) Client(ctx context.Context) (*azblob.Client, error) {
	cs := GetClientStore(ctx)
	return cs.GetClient(b.Account)
}
