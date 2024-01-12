package azio

import (
	"testing"
)

func TestStat(t *testing.T) {
	path, err := ParseBlobPath("az://openaipublic/diffusion/dec-2021/upsample.pt")
	if err != nil {
		t.Fatal(err)
	}
	ctx := testingContext()
	stats, err := Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	const expSize = 1593693639
	if stats.Size() != expSize {
		t.Errorf("expected size %d but got %d", expSize, stats.Size())
	}
}
