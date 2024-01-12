package azio

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func TestStat(t *testing.T) {
	path, err := ParseBlobPath("az://openaipublic/diffusion/dec-2021/upsample.pt")
	if err != nil {
		t.Fatal(err)
	}
	var options azblob.ClientOptions
	options.Retry.MaxRetries = 1
	store := NewClientStore(options)
	ctx := context.WithValue(context.Background(), ContextKeyClientStore, store)

	stats, err := Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	const expSize = 1593693639
	if stats.Size() != expSize {
		t.Errorf("expected size %d but got %d", expSize, stats.Size())
	}
}
