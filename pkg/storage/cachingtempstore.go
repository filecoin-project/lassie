package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ types.ReadableWritableStorage = (*CachingTempStore)(nil)

var errClosed = errors.New("store closed")

// CachingTempStore is a ReadableWritableStorage that is intended for
// temporary use. It uses DeferredStorageCar as a backing store, so the
// underlying CAR file is lazily created on the first write (none will be
// created if there are no writes).
//
// A provided BlockWriteOpener will receive blocks for each Put operation, this
// is intended to be used to write a properly ordered CARv1 file.
type CachingTempStore struct {
	store     *DeferredStorageCar
	outWriter linking.BlockWriteOpener
}

func NewCachingTempStore(outWriter linking.BlockWriteOpener, store *DeferredStorageCar) *CachingTempStore {
	return &CachingTempStore{
		store:     store,
		outWriter: outWriter,
	}
}

func (ttrw *CachingTempStore) Has(ctx context.Context, key string) (bool, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if rw, err := ttrw.store.readWrite(); err != nil {
		return false, err
	} else {
		return rw.Has(ctx, key)
	}
}

func (ttrw *CachingTempStore) Get(ctx context.Context, key string) ([]byte, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if rw, err := ttrw.store.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.Get(ctx, key)
	}
}

func (ttrw *CachingTempStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if rw, err := ttrw.store.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.GetStream(ctx, key)
	}
}

// Put writes both to temporary readwrite caching storage (available for read
// operations) and to the underlying write-only CARv1 output at the same time.
func (ttrw *CachingTempStore) Put(ctx context.Context, key string, data []byte) error {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()
	return ttrw.teePut(ctx, key, data)
}

// Close will clean up any temporary resources used by the storage.
func (ttrw *CachingTempStore) Close() error {
	// we need to ensure that the writer receives no more data, so swap
	// it out with a no-op writer that returns an error
	ttrw.store.lk.Lock()
	ttrw.outWriter = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		return nil, nil, errClosed
	}
	ttrw.store.lk.Unlock()
	return ttrw.store.Close()
}

func (ttrw *CachingTempStore) teePut(ctx context.Context, key string, data []byte) error {
	// use the store directly so we don't have lock contention
	rw, err := ttrw.store.readWrite()
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	var err1, err2 error
	go func() {
		defer wg.Done()
		err1 = rw.Put(ctx, key, data)
	}()
	go func() {
		defer wg.Done()
		err2 = writeTo(ctx, ttrw.outWriter, key, data)
	}()
	wg.Wait()
	if err1 != nil {
		return err1
	}
	return err2
}

func writeTo(ctx context.Context, outWriter linking.BlockWriteOpener, key string, data []byte) error {
	cid, err := cid.Cast([]byte(key))
	if err != nil {
		return err
	}
	w, c, err := outWriter(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	n, err := bytes.NewBuffer(data).WriteTo(w)
	if err != nil {
		return err
	}
	if n != int64(len(data)) {
		return io.ErrShortWrite
	}
	return c(cidlink.Link{Cid: cid})
}
