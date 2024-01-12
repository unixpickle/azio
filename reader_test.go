package azio

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestReadBlob(t *testing.T) {
	ctx := testingContext()

	path, err := ParseBlobPath("az://openaipublic/diffusion/dec-2021/v1.pdf")
	if err != nil {
		t.Fatal(err)
	}

	data, err := ReadBlob(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	hasher := sha1.New()
	hasher.Write(data)
	sha := hex.EncodeToString(hasher.Sum(nil))
	expHash := "cad1077b7c3373d03106cbdecbde5dda98a99725"
	if sha != expHash {
		t.Fatalf("expected hash %s but got %s", expHash, sha)
	}
}

func TestOpenBlob(t *testing.T) {
	ctx := testingContext()

	path, err := ParseBlobPath("az://openaipublic/diffusion/dec-2021/upsample.pt")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := OpenBlob(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := reader.Seek(-100, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	const expOffset = 1593693639 - 100
	if offset != expOffset {
		t.Fatalf("expected offset %d but got %d", expOffset, offset)
	}
	data := make([]byte, 50)
	if n, err := io.ReadFull(reader, data); n != 50 || err != nil {
		t.Fatalf("unexpected read size or err (size=%d, err=%s)", n, err)
	}
	expected := []byte("onPK\x06\x06,\x00\x00\x00\x00\x00\x00\x00\x1e\x03-\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1f\x03\x00\x00\x00\x00\x00\x00\x1f\x03\x00\x00\x00\x00\x00\x00\x13\xc1\x00\x00\x00\x00\x00\x00")
	if !bytes.Equal(data, expected) {
		t.Fatalf("unexpected bytes from file")
	}

	data = make([]byte, 150)
	if n, err := io.ReadFull(reader, data); n != 50 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("unexpected read size or err (size=%d, err=%s)", n, err)
	}
	expected = []byte("R\x14\xfd^\x00\x00\x00\x00PK\x06\x07\x00\x00\x00\x00e\xd5\xfd^\x00\x00\x00\x00\x01\x00\x00\x00PK\x05\x06\x00\x00\x00\x00\x1f\x03\x1f\x03\x13\xc1\x00\x00R\x14\xfd^\x00\x00")
	if !bytes.Equal(data[:len(expected)], expected) {
		t.Fatalf("unexpected bytes from file")
	}
}
