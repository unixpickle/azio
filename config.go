package azio

import (
	"context"
)

type ContextKey int

const (
	ContextKeyClientStore ContextKey = iota
)

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
