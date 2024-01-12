package azio

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

const (
	DefaultBufferSize = 1 << 23
)

// ReadBlob reads an entire blob into memory.
func ReadBlob(ctx context.Context, p *BlobPath) ([]byte, error) {
	info, err := Stat(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", p, err)
	}
	if int64(int(info.Size())) != info.Size() {
		return nil, fmt.Errorf("read blob %s: size %d not fit for this machine architecture",
			p, info.Size())
	}
	results := make([]byte, int(info.Size()))
	n, err := ReadBlobRange(ctx, p, 0, results)
	if err == nil && n != len(results) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return results[:n], fmt.Errorf("read blob %s: %w", p, io.ErrUnexpectedEOF)
	}
	return results, nil
}

// ReadBlobRange reads a range of bytes from the blob into memory.
// May return fewer bytes if the range is out of bounds.
func ReadBlobRange(ctx context.Context, p *BlobPath, start int64, out []byte) (int, error) {
	concur := GetBlobConcurrency(ctx)

	var options azblob.DownloadBufferOptions
	if int64(len(out)) > concur.MinBytes {
		c := int64(len(out)) / concur.MinBytes
		if c > int64(concur.MaxConcurrency) {
			c = int64(concur.MaxConcurrency)
		}
		options.Concurrency = uint16(c)
	} else {
		options.Concurrency = 1
	}

	options.Range = blob.HTTPRange{
		Offset: start,
		Count:  int64(len(out)),
	}

	client, err := p.Client(ctx)
	if err != nil {
		return 0, fmt.Errorf("read blob %s: %w", p, err)
	}

	realSize, err := client.DownloadBuffer(ctx, p.Container, p.Path, out, &options)
	if err != nil {
		return int(realSize), fmt.Errorf("read blob %s: %w", p, err)
	}
	return int(realSize), nil
}

// OpenBlob creates an io.ReadSeeker to access the blob.
// Sequential reads are optimized by using a buffer.
//
// When the file is seeked to a different offset, the buffer may be cleared, so
// it should not be excessively large if a lot of seeking is intended.
func OpenBlob(
	ctx context.Context,
	p *BlobPath,
	bufSize int,
) (io.ReadSeeker, error) {
	client, err := p.Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("open blob %s: %w", p, err)
	}
	info, err := Stat(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("open blob %s: %w", p, err)
	}
	return &bufferedSeekableBlobReader{
		ctx:     ctx,
		client:  client,
		path:    p,
		offset:  0,
		size:    info.Size(),
		bufSize: bufSize,
	}, nil
}

type bufferedSeekableBlobReader struct {
	ctx     context.Context
	client  *azblob.Client
	path    *BlobPath
	offset  int64
	size    int64
	bufSize int

	curBufReader *bufio.Reader
}

func (b *bufferedSeekableBlobReader) Read(buf []byte) (n int, err error) {
	if b.curBufReader == nil {
		b.createBufReader()
	}
	n, err = b.curBufReader.Read(buf)
	b.offset += int64(n)
	return
}

func (b *bufferedSeekableBlobReader) Seek(offset int64, whence int) (int64, error) {
	oldOffset := b.offset
	switch whence {
	case io.SeekStart:
		b.offset = offset
	case io.SeekEnd:
		b.offset = offset + b.size
	case io.SeekCurrent:
		b.offset += offset
	default:
		panic("unknown seek whence")
	}
	if b.offset < 0 {
		b.offset = oldOffset
		return b.offset, errors.New("seek went past beginning of file")
	} else if b.offset > b.size {
		b.offset = b.size
	}
	if b.offset != oldOffset {
		b.curBufReader = nil
	}
	return b.offset, nil
}

func (b *bufferedSeekableBlobReader) createBufReader() {
	rawReader := &rawBlobReader{
		ctx:    b.ctx,
		client: b.client,
		path:   b.path,
		offset: b.offset,
		size:   b.size,
	}
	b.curBufReader = bufio.NewReaderSize(rawReader, b.bufSize)
}

type rawBlobReader struct {
	ctx    context.Context
	client *azblob.Client
	path   *BlobPath
	offset int64
	size   int64
}

func (r *rawBlobReader) Read(buf []byte) (n int, err error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	maxSize := len(buf)
	readSize := r.size - r.offset
	if readSize > int64(maxSize) {
		readSize = int64(maxSize)
	}

	n, err = ReadBlobRange(r.ctx, r.path, r.offset, buf)
	r.offset += int64(n)
	return
}
