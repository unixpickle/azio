package azio

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func testingContext() context.Context {
	var options azblob.ClientOptions
	options.Retry.MaxRetries = 1
	store := NewClientStore(options)
	return context.WithValue(context.Background(), ContextKeyClientStore, store)
}
