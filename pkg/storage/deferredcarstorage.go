package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
)

var _ ReadableWritableStorage = (*DeferredCarStorage)(nil)

type DeferredCarStorage struct {
	tempDir string

	lk     sync.Mutex
	closed bool
	f      *os.File
	rw     *carstorage.StorageCar
}

func NewDeferredCarStorage(tempDir string) *DeferredCarStorage {
	return &DeferredCarStorage{
		tempDir: tempDir,
	}
}

// Close will clean up any temporary resources used by the storage.
func (dcs *DeferredCarStorage) Close() error {
	if dcs.closed {
		return nil
	}
	dcs.closed = true
	if dcs.f != nil {
		for _, err := range []error{dcs.f.Close(), os.Remove(dcs.f.Name())} {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Has returns true if the underlying CARv1 has the key.
func (dcs *DeferredCarStorage) Has(ctx context.Context, key string) (bool, error) {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.rw == nil { // not initialised, so we certainly don't have it
		return false, nil
	}

	if rw, err := dcs.readWrite(); err != nil {
		return false, err
	} else {
		return rw.Has(ctx, key)
	}
}

// Get returns data from the underlying CARv1.
func (dcs *DeferredCarStorage) Get(ctx context.Context, key string) ([]byte, error) {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.rw == nil { // not initialised, so we certainly don't have it
		keyCid, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("bad CID key: %w", err)
		}
		return nil, carstorage.ErrNotFound{Cid: keyCid}
	}

	if rw, err := dcs.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.Get(ctx, key)
	}
}

// GetStream returns data from the underlying CARv1.
func (dcs *DeferredCarStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.rw == nil { // not initialised, so we certainly don't have it
		keyCid, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("bad CID key: %w", err)
		}
		return nil, carstorage.ErrNotFound{Cid: keyCid}
	}

	if rw, err := dcs.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.GetStream(ctx, key)
	}
}

// Put writes data to the underlying CARv1 which will be initialised on the
// first call to Put.
func (dcs *DeferredCarStorage) Put(ctx context.Context, key string, data []byte) error {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if rw, err := dcs.readWrite(); err != nil {
		return err
	} else {
		return rw.Put(ctx, key, data)
	}
}

// readWrite returns a ReadableWritableStorage which is lazily initialised. It
// is not synchronized so calls that need thread safety should be wrapped in a
// mutex. This can be used to directly access the underlying CARv1 and cause it
// to be initialised.
func (dcs *DeferredCarStorage) readWrite() (ReadableWritableStorage, error) {
	if dcs.closed {
		return nil, errClosed
	}
	if dcs.rw == nil {
		var err error
		if dcs.f, err = os.CreateTemp(dcs.tempDir, "lassie_carstorage"); err != nil {
			return nil, err
		}
		rw, err := carstorage.NewReadableWritable(dcs.f, []cid.Cid{}, carv2.WriteAsCarV1(true))
		if err != nil {
			return nil, err
		}
		dcs.rw = rw
	}
	return dcs.rw, nil
}
