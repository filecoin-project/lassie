package internal

import (
	"context"
	"io"

	"github.com/ipld/go-ipld-prime/storage"
)

type ReadableWritableStorage interface {
	storage.StreamingReadableStorage
	storage.ReadableStorage
	storage.WritableStorage
}

// putCbStore simply calls a callback on each put(), with the number of blocks put
var _ ReadableWritableStorage = (*putCbStore)(nil)

type putCbStore struct {
	// parentOpener lazily opens the parent Store upon first call to this Store.
	// This avoids Store instantiation until there is some interaction from the retriever.
	// In the case of CARv2 Stores, this will avoid creation of empty .car files should
	// the retriever fail to find any candidates.
	parentOpener func() (ReadableWritableStorage, error)
	// parent is lazily instantiated and should not be directly used; use parentStore instead.
	parent ReadableWritableStorage
	cb     func(putCount int, putBytes int)
}

func NewPutCbStore(parentOpener func() (ReadableWritableStorage, error), cb func(putCount int, putBytes int)) *putCbStore {
	return &putCbStore{parentOpener: parentOpener, cb: cb}
}

func (pcb *putCbStore) Has(ctx context.Context, key string) (bool, error) {
	pbs, err := pcb.parentStore()
	if err != nil {
		return false, err
	}
	return pbs.Has(ctx, key)
}

func (pcb *putCbStore) Get(ctx context.Context, key string) ([]byte, error) {
	pbs, err := pcb.parentStore()
	if err != nil {
		return nil, err
	}
	return pbs.Get(ctx, key)
}

func (pcb *putCbStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	pbs, err := pcb.parentStore()
	if err != nil {
		return nil, err
	}
	return pbs.GetStream(ctx, key)
}

func (pcb *putCbStore) Put(ctx context.Context, key string, data []byte) error {
	pbs, err := pcb.parentStore()
	if err != nil {
		return err
	}
	pcb.cb(1, len(data))
	return pbs.Put(ctx, key, data)
}

func (pcb *putCbStore) parentStore() (ReadableWritableStorage, error) {
	if pcb.parent == nil {
		var err error
		if pcb.parent, err = pcb.parentOpener(); err != nil {
			return nil, err
		}
	}
	return pcb.parent, nil
}
