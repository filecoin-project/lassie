package internal

import (
	"context"
	"io"

	carstore "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/storage"
)

// putCbStore simply calls a callback on each put(), with the number of blocks put
var _ storage.StreamingReadableStorage = (*putCbStore)(nil)
var _ storage.ReadableStorage = (*putCbStore)(nil)
var _ storage.WritableStorage = (*putCbStore)(nil)

type putCbStore struct {
	// parentOpener lazily opens the parent Store upon first call to this Store.
	// This avoids Store instantiation until there is some interaction from the retriever.
	// In the case of CARv2 Stores, this will avoid creation of empty .car files should
	// the retriever fail to find any candidates.
	parentOpener func() (*carstore.StorageCar, error)
	// parent is lazily instantiated and should not be directly used; use parentStore instead.
	parent *carstore.StorageCar
	cb     func(putCount int, putBytes int)
}

func NewPutCbStore(parentOpener func() (*carstore.StorageCar, error), cb func(putCount int, putBytes int)) *putCbStore {
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
func (pcb *putCbStore) Finalize() error {
	if pbs, err := pcb.parentStore(); err != nil {
		return err
	} else {
		return pbs.Finalize()
	}
}
func (pcb *putCbStore) parentStore() (*carstore.StorageCar, error) {
	if pcb.parent == nil {
		var err error
		if pcb.parent, err = pcb.parentOpener(); err != nil {
			return nil, err
		}
	}
	return pcb.parent, nil
}
