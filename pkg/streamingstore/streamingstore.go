package streamingstore

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstore "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/storage"
)

var _ storage.StreamingReadableStorage = (*StreamingStore)(nil)
var _ storage.ReadableStorage = (*StreamingStore)(nil)
var _ storage.WritableStorage = (*StreamingStore)(nil)

var errClosed = errors.New("streaming store closed")

// StreamingStore is a storage that opens the underlying CAR storage only when
// required. So it should avoid creating CAR files that aren't ultimately
// touched and therefore need cleanup.
// Two storage systems are used by StreamingStore. One that can perform both
// read and write operations to serve as the block service for graphsync; and
// the other that only performs write operations, that can be used to stream
// CAR contents to the client.
// A getWriter callback is used to get a writer to the streaming storage only
// when it's needed. This can be used to trigger additional work when the
// storage is actually being setup.
// Put() operations will write to both storage systems, and will block until
// both are written.
type StreamingStore struct {
	ctx       context.Context
	roots     []cid.Cid
	getWriter func() (io.Writer, error)
	errorCb   func(error)

	lk        sync.Mutex
	closed    bool
	f         *os.File
	readWrite *carstore.StorageCar
	write     storage.WritableStorage
	tempDir   string
}

func NewStreamingStore(ctx context.Context, roots []cid.Cid, tempDir string, getWriter func() (io.Writer, error), errorCb func(error)) *StreamingStore {
	return &StreamingStore{
		ctx:       ctx,
		roots:     roots,
		getWriter: getWriter,
		errorCb:   errorCb,
		tempDir:   tempDir,
	}
}

func (ss *StreamingStore) Has(ctx context.Context, key string) (bool, error) {
	if ss.ctx.Err() != nil {
		return false, ss.ctx.Err()
	}
	reader, err := ss.lazyReadWrite()
	if err != nil {
		ss.errorCb(err)
		return false, err
	}
	return reader.Has(ctx, key)
}

func (ss *StreamingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if ss.ctx.Err() != nil {
		return nil, ss.ctx.Err()
	}
	reader, err := ss.lazyReadWrite()
	if err != nil {
		ss.errorCb(err)
		return nil, err
	}
	return reader.Get(ctx, key)
}

func (ss *StreamingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if ss.ctx.Err() != nil {
		return nil, ss.ctx.Err()
	}
	reader, err := ss.lazyReadWrite()
	if err != nil {
		ss.errorCb(err)
		return nil, err
	}
	return reader.GetStream(ctx, key)
}

func (ss *StreamingStore) Put(ctx context.Context, key string, data []byte) error {
	if ss.ctx.Err() != nil {
		return ss.ctx.Err()
	}
	writer, err := ss.lazyWriter()
	if err != nil {
		ss.errorCb(err)
		return err
	}
	return writer.Put(ctx, key, data)
}

func (ss *StreamingStore) Close() error {
	ss.lk.Lock()
	defer ss.lk.Unlock()
	// don't need to Finalize the stores because we're writing CARv1
	ss.closed = true
	if ss.f != nil {
		errs := []error{ss.f.Close(), os.Remove(ss.f.Name())}
		ss.f = nil
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ss *StreamingStore) lazyReadWrite() (*carstore.StorageCar, error) {
	ss.lk.Lock()
	defer ss.lk.Unlock()

	if ss.closed {
		return nil, errClosed
	}

	if ss.readWrite == nil {
		if err := ss.setupReadWrite(); err != nil {
			return nil, err
		}
	}

	return ss.readWrite, nil
}

// setupReadWrite is not synchronized and should only be called from the
// lazy*() methods.
func (ss *StreamingStore) setupReadWrite() error {
	var err error
	if ss.f, err = os.CreateTemp(ss.tempDir, "lassie_carstore"); err != nil {
		return err
	}
	ss.readWrite, err = carstore.NewReadableWritable(ss.f, ss.roots, carv2.WriteAsCarV1(true))
	return err
}

func (ss *StreamingStore) lazyWriter() (storage.WritableStorage, error) {
	ss.lk.Lock()
	defer ss.lk.Unlock()

	if ss.closed {
		return nil, errClosed
	}

	if ss.write == nil {
		if ss.readWrite == nil {
			if err := ss.setupReadWrite(); err != nil {
				return nil, err
			}
		}

		write, err := ss.getWriter()
		if err != nil {
			return nil, err
		}
		ss.getWriter = func() (io.Writer, error) {
			return nil, errors.New("already got writer")
		}
		writeStore, err := carstore.NewWritable(write, ss.roots, carv2.WriteAsCarV1(true))
		if err != nil {
			return nil, err
		}
		ss.write = &teeWriteStorage{
			w1: ss.readWrite,
			w2: writeStore,
		}
	}

	return ss.write, nil
}

var _ storage.WritableStorage = (*teeWriteStorage)(nil)

type teeWriteStorage struct {
	w1 storage.WritableStorage
	w2 storage.WritableStorage
}

func (tws *teeWriteStorage) Has(ctx context.Context, key string) (bool, error) {
	return tws.w1.Has(ctx, key)
}

func (tws *teeWriteStorage) Put(ctx context.Context, key string, data []byte) error {
	// put to both writers in parallel but block until complete
	wg := sync.WaitGroup{}
	wg.Add(2)
	var err1, err2 error
	go func() {
		defer wg.Done()
		err1 = tws.w1.Put(ctx, key, data)
	}()
	go func() {
		defer wg.Done()
		err2 = tws.w2.Put(ctx, key, data)
	}()
	wg.Wait()
	if err1 != nil {
		return err1
	}
	return err2
}
