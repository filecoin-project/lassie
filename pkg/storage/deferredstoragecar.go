package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/multiformats/go-multihash"
)

var _ ReadableWritableStorage = (*DeferredStorageCar)(nil)

// DeferredStorageCar is a wrapper around
// github.com/ipld/go-car/v2/storage.StorageCar that defers creating the CAR
// until the first Put() operation. In this way it can be optimistically
// instantiated and no file will be created if it is never written to (such as
// in the case of an error).
type DeferredStorageCar struct {
	tempDir string
	root    cid.Cid

	lk     sync.Mutex
	closed bool
	f      *os.File
	rw     *carstorage.StorageCar
}

// NewDeferredStorageCar creates a new DeferredStorageCar.
func NewDeferredStorageCar(tempDir string, root cid.Cid) *DeferredStorageCar {
	return &DeferredStorageCar{
		tempDir: tempDir,
		root:    root,
	}
}

// Close will clean up any temporary resources used by the storage.
func (dcs *DeferredStorageCar) Close() error {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

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
func (dcs *DeferredStorageCar) Has(ctx context.Context, key string) (bool, error) {
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
func (dcs *DeferredStorageCar) Get(ctx context.Context, key string) ([]byte, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return digest, nil
	} else if err != nil {
		return nil, err
	}

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
func (dcs *DeferredStorageCar) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return io.NopCloser(bytes.NewReader(digest)), nil
	} else if err != nil {
		return nil, err
	}

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
func (dcs *DeferredStorageCar) Put(ctx context.Context, key string, data []byte) error {
	if _, ok, err := AsIdentity(key); ok {
		return nil
	} else if err != nil {
		return err
	}

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
func (dcs *DeferredStorageCar) readWrite() (ReadableWritableStorage, error) {
	if dcs.closed {
		return nil, errClosed
	}
	if dcs.rw == nil {
		var err error
		if dcs.f, err = os.CreateTemp(dcs.tempDir, "lassie_carstorage"); err != nil {
			return nil, err
		}
		rw, err := carstorage.NewReadableWritable(
			dcs.f,
			[]cid.Cid{dcs.root},
			carv2.WriteAsCarV1(true),
			carv2.StoreIdentityCIDs(false),
			carv2.UseWholeCIDs(false),
		)
		if err != nil {
			return nil, err
		}
		dcs.rw = rw
	}
	return dcs.rw, nil
}

func AsIdentity(key string) (digest []byte, ok bool, err error) {
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, false, err
	}
	dmh, err := multihash.Decode(keyCid.Hash())
	if err != nil {
		return nil, false, err
	}
	ok = dmh.Code == multihash.IDENTITY
	digest = dmh.Digest
	return digest, ok, nil
}
