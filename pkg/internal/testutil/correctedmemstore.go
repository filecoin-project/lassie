package testutil

import (
	"context"
	"io"

	format "github.com/ipfs/go-ipld-format"
)

// TODO: remove when this is fixed in IPLD prime
type CorrectedMemStore struct {
	ParentStore
}

func (cms *CorrectedMemStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := cms.ParentStore.Get(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return data, err
}

func (cms *CorrectedMemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := cms.ParentStore.GetStream(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return rc, err
}
