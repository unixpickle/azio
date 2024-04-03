package azio

import (
	"context"
	"fmt"
	"io"
)

// WriteBlob writes a file to blob store.
func WriteBlob(ctx context.Context, p *BlobPath, r io.Reader) error {
	client, err := p.Client(ctx)
	if err != nil {
		return fmt.Errorf("write blob %s: %w", p, err)
	}

	_, err = client.UploadStream(ctx, p.Container, p.Path, r, nil)
	if err != nil {
		return fmt.Errorf("write blob %s: %w", p, err)
	}
	return nil
}
