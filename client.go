package azio

import (
	"fmt"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const (
	DefaultMaxRetries = 50
)

var globalClientStore *ClientStore = NewDefaultClientStore()

func GlobalClientStore() *ClientStore {
	return globalClientStore
}

// A ClientStore manages separate blob clients for different storage accounts.
type ClientStore struct {
	lock    sync.RWMutex
	clients map[string]*azblob.Client

	options azblob.ClientOptions
}

// NewDefaultClientStore creates a ClientStore with the default options.
func NewDefaultClientStore() *ClientStore {
	var options azblob.ClientOptions
	options.Retry.MaxRetries = DefaultMaxRetries
	return NewClientStore(options)
}

// NewClientStore creates an empty account-to-client mapping which will use
// the given
func NewClientStore(options azblob.ClientOptions) *ClientStore {
	return &ClientStore{
		clients: map[string]*azblob.Client{},
		options: options,
	}
}

// GetClient creates or reuses a client for the Azure storage account.
func (c *ClientStore) GetClient(account string) (*azblob.Client, error) {
	c.lock.RLock()
	if match, ok := c.clients[account]; ok {
		return match, nil
	}
	c.lock.RUnlock()

	// Note that we will block all client usage until we are done.
	// This is not ideal, but we prioritize not authenticating with Azure
	// many times in a race scenario.
	c.lock.Lock()
	defer c.lock.Unlock()

	if match, ok := c.clients[account]; ok {
		// May happen due to a race condition
		return match, nil
	}
	accountURL := fmt.Sprintf("https://%s.blob.core.windows.net", account)
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("create blob client for account: %s: %w", account, err)
	}
	client, err := azblob.NewClient(accountURL, credential, &c.options)
	if err != nil {
		return nil, fmt.Errorf("create blob client for account: %s: %w", account, err)
	}
	c.clients[account] = client
	return client, nil
}
