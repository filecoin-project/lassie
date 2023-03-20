package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
)

var _ ipldstorage.StreamingReadableStorage = (*TeeingTempReadWrite)(nil)
var _ ipldstorage.ReadableStorage = (*TeeingTempReadWrite)(nil)
var _ ipldstorage.WritableStorage = (*TeeingTempReadWrite)(nil)

var errClosed = errors.New("store closed")

// TeeingTempReadWrite is storage wrapper that takes a write-only storage and
// returns a read-write storage. It will only create the temporary read-write
// storage when the first read or write operation is performed. This allows
// the caller to defer the creation of the temporary storage until it is
// actually needed—which may be never.
//
// Put() operations will write to both storage systems, and will block until
// both are written.
type TeeingTempReadWrite struct {
	*DeferredCarStorage
	outWriter linking.BlockWriteOpener
}

func NewTeeingTempReadWrite(outWriter linking.BlockWriteOpener, tempDir string) *TeeingTempReadWrite {
	return &TeeingTempReadWrite{
		DeferredCarStorage: NewDeferredCarStorage(tempDir),
		outWriter:          outWriter,
	}
}

// Put writes both to temporary readwrite caching storage (available for read
// operations) and to the underlying write-only CARv1 output at the same time.
func (ttrw *TeeingTempReadWrite) Put(ctx context.Context, key string, data []byte) error {
	ttrw.DeferredCarStorage.lk.Lock()
	defer ttrw.DeferredCarStorage.lk.Unlock()

	return ttrw.teePut(ctx, key, data)
}

func (ttrw *TeeingTempReadWrite) teePut(ctx context.Context, key string, data []byte) error {
	// use the store directly so we don't have lock contention
	rw, err := ttrw.DeferredCarStorage.readWrite()
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
		cid, err := cid.Cast([]byte(key))
		if err != nil {
			err2 = err
			return
		}
		w, c, err := ttrw.outWriter(linking.LinkContext{Ctx: ctx})
		if err != nil {
			err2 = err
			return
		}
		n, err := bytes.NewBuffer(data).WriteTo(w)
		if err != nil {
			err2 = err
			return
		}
		if n != int64(len(data)) {
			err2 = io.ErrShortWrite
			return
		}
		if err := c(cidlink.Link{Cid: cid}); err != nil {
			err2 = err
			return
		}
	}()
	wg.Wait()
	if err1 != nil {
		return err1
	}
	return err2
}

// Close will clean up any temporary resources used by the storage.
func (ttrw *TeeingTempReadWrite) Close() error {
	return ttrw.DeferredCarStorage.Close()
}
