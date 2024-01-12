package azio

import (
	"context"
)

const (
	DefaultMinBytes       = 1 << 24
	DefaultMaxConcurrency = 64
)

type ContextKey int

const (
	ContextKeyClientStore ContextKey = iota
	ContextKeyBlobConcurrency
)

// BlobConcurrency configures how many concurrent reads to perform at once
// while reading a blob.
type BlobConcurrency struct {
	// MinBytes is the minimum number of bytes to read per concurrent request.
	MinBytes int64

	// MaxConcurrency is the maximum number of concurrent requests.
	MaxConcurrency uint16
}

// DefaultBlobConcurrency creates a BlobConcurrency configuration with sane
// defaults.
func DefaultBlobConcurrency() BlobConcurrency {
	return BlobConcurrency{
		MinBytes:       DefaultMinBytes,
		MaxConcurrency: DefaultMaxConcurrency,
	}
}

// GetClientStore reads the ContextKeyClientStore from the context, or returns
// the default, global context store.
func GetClientStore(ctx context.Context) *ClientStore {
	store, ok := ctx.Value(ContextKeyClientStore).(*ClientStore)
	if ok {
		return store
	} else {
		return GlobalClientStore()
	}
}

// GetBlobConcurrency reads the ContextKeyBlobConcurrency from the context, or
// returns a default value.
func GetBlobConcurrency(ctx context.Context) BlobConcurrency {
	concur, ok := ctx.Value(ContextKeyBlobConcurrency).(BlobConcurrency)
	if ok {
		return concur
	} else {
		return DefaultBlobConcurrency()
	}
}

// CopyContextConfig copies the configuration from src into a new context based
// on dst.
func CopyContextConfig(dst, src context.Context) context.Context {
	return context.WithValue(
		context.WithValue(
			dst,
			ContextKeyBlobConcurrency,
			GetBlobConcurrency(src),
		),
		ContextKeyClientStore,
		GetClientStore(src),
	)
}
