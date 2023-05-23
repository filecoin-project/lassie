package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ types.ReadableWritableStorage = (*CachingTempStore)(nil)
var _ types.ReadableWritableStorage = (*preloadStore)(nil)

var errClosed = errors.New("store closed")

// CachingTempStore is a ReadableWritableStorage that is intended for
// temporary use. It uses DeferredStorageCar as a backing store, so the
// underlying CAR file is lazily created on the first write (none will be
// created if there are no writes).
//
// A provided BlockWriteOpener will receive blocks for each Put operation, this
// is intended to be used to write a properly ordered CARv1 file.
//
// PreloadStore returns a secondary ReadableWritableStorage that can be used
// by a traversal preloader to optimistically load blocks into the temporary
// store. Blocks loaded via the PreloadStore will not be written to the
// provided BlockWriteOpener until they appear in a Put operation on the parent
// store. In this way, the BlockWriteOpener will receive blocks in the order
// that they appear in the traversal.
type CachingTempStore struct {
	store     *DeferredStorageCar
	outWriter linking.BlockWriteOpener

	preloadKeys map[string]struct{}
}

func NewCachingTempStore(outWriter linking.BlockWriteOpener, store *DeferredStorageCar) *CachingTempStore {
	return &CachingTempStore{
		store:       store,
		outWriter:   outWriter,
		preloadKeys: make(map[string]struct{}),
	}
}

func (ttrw *CachingTempStore) Has(ctx context.Context, key string) (bool, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if _, ok := ttrw.preloadKeys[key]; ok {
		// if it's in the preload list, then it's not in the store proper
		return false, nil
	}

	if rw, err := ttrw.store.readWrite(); err != nil {
		return false, err
	} else {
		return rw.Has(ctx, key)
	}
}

func (ttrw *CachingTempStore) Get(ctx context.Context, key string) ([]byte, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if _, ok := ttrw.preloadKeys[key]; ok {
		// if it's in the preload list, then it's not in the store proper
		c, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, err
		}
		return nil, carstorage.ErrNotFound{Cid: c}
	}

	if rw, err := ttrw.store.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.Get(ctx, key)
	}
}

func (ttrw *CachingTempStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	ttrw.store.lk.Lock()
	defer ttrw.store.lk.Unlock()

	if _, ok := ttrw.preloadKeys[key]; ok {
		// if it's in the preload list, then it's not in the store proper
		c, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, err
		}
		return nil, carstorage.ErrNotFound{Cid: c}
	}

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

	if _, ok := ttrw.preloadKeys[key]; ok {
		// already in preload, just write to the outWriter
		delete(ttrw.preloadKeys, key)
		return writeTo(ctx, ttrw.outWriter, key, data)
	}
	// not in preload, write to local and outWriter
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

func (ttrw *CachingTempStore) PreloadStore() types.ReadableWritableStorage {
	return &preloadStore{ttrw: ttrw}
}

type preloadStore struct {
	ttrw *CachingTempStore
}

func (ps *preloadStore) Has(ctx context.Context, key string) (bool, error) {
	ps.ttrw.store.lk.Lock()
	defer ps.ttrw.store.lk.Unlock()
	_, has := ps.ttrw.preloadKeys[key]
	return has, nil
}

func (ps *preloadStore) Get(ctx context.Context, key string) ([]byte, error) {
	ps.ttrw.store.lk.Lock()
	defer ps.ttrw.store.lk.Unlock()
	if _, ok := ps.ttrw.preloadKeys[key]; !ok {
		c, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, err
		}
		return nil, carstorage.ErrNotFound{Cid: c}
	}
	if rw, err := ps.ttrw.store.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.Get(ctx, key)
	}
}

func (ps *preloadStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	ps.ttrw.store.lk.Lock()
	defer ps.ttrw.store.lk.Unlock()
	if _, ok := ps.ttrw.preloadKeys[key]; !ok {
		c, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, err
		}
		return nil, carstorage.ErrNotFound{Cid: c}
	}
	if rw, err := ps.ttrw.store.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.GetStream(ctx, key)
	}
}

func (ps *preloadStore) Put(ctx context.Context, key string, data []byte) error {
	ps.ttrw.store.lk.Lock()
	defer ps.ttrw.store.lk.Unlock()
	// is it already in the preload list?
	if _, ok := ps.ttrw.preloadKeys[key]; ok {
		return nil
	}
	if rw, err := ps.ttrw.store.readWrite(); err != nil {
		return err
	} else {
		// do we already have it in the store?
		if has, err := rw.Has(ctx, key); err != nil {
			return err
		} else if has {
			return nil
		}
		if err := rw.Put(ctx, key, data); err != nil {
			return err
		}
		ps.ttrw.preloadKeys[key] = struct{}{}
		return nil
	}
}
