package storage

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
)

type putCb struct {
	cb   func(int)
	once bool
}

var _ ipldstorage.WritableStorage = (*DeferredCarWriter)(nil)
var _ io.Closer = (*DeferredCarWriter)(nil)

type DeferredWriter interface {
	ipldstorage.WritableStorage
	io.Closer
	BlockWriteOpener() linking.BlockWriteOpener
	OnPut(cb func(int), once bool)
}

// DeferredCarWriter creates a write-only CARv1 either to an existing stream or
// to a file designated by a supplied path. CARv1 content (including header)
// only begins when the first Put() operation is performed. If the output is a
// file, it will be created when the first Put() operation is performed.
// DeferredCarWriter is threadsafe, and can be used concurrently.
// Closing the writer will close, but not delete, the underlying file. This
// writer is intended for constructing the final output CARv1 for the user.
type DeferredCarWriter struct {
	root      cid.Cid
	outPath   string
	outStream io.Writer

	lk    sync.Mutex
	f     *os.File
	w     carstorage.WritableCar
	putCb []putCb
	opts  []carv2.Option
}

// NewDeferredCarWriterForPath creates a DeferredCarWriter that will write to a
// file designated by the supplied path. The file will only be created on the
// first Put() operation.
func NewDeferredCarWriterForPath(root cid.Cid, outPath string, opts ...carv2.Option) *DeferredCarWriter {
	return &DeferredCarWriter{root: root, outPath: outPath, opts: opts}
}

// NewDeferredCarWriterForStream creates a DeferredCarWriter that will write to
// the supplied stream. The stream will only be written to on the first Put()
// operation.
func NewDeferredCarWriterForStream(root cid.Cid, outStream io.Writer, opts ...carv2.Option) *DeferredCarWriter {
	return &DeferredCarWriter{root: root, outStream: outStream, opts: opts}
}

// OnPut will call a callback when each Put() operation is started. The argument
// to the callback is the number of bytes being written. If once is true, the
// callback will be removed after the first call.
func (dcw *DeferredCarWriter) OnPut(cb func(int), once bool) {
	if dcw.putCb == nil {
		dcw.putCb = make([]putCb, 0)
	}
	dcw.putCb = append(dcw.putCb, putCb{cb: cb, once: once})
}

// Has returns false if the key was not already written to the CARv1 output.
func (dcw *DeferredCarWriter) Has(ctx context.Context, key string) (bool, error) {
	dcw.lk.Lock()
	defer dcw.lk.Unlock()

	if dcw.w == nil { // shortcut, haven't written anything, don't even initialise
		return false, nil
	}

	writer, err := dcw.writer()
	if err != nil {
		return false, err
	}

	return writer.Has(ctx, key)
}

// Put writes the given content to the CARv1 output stream, creating it if it
// doesn't exist yet.
func (dcw *DeferredCarWriter) Put(ctx context.Context, key string, content []byte) error {
	dcw.lk.Lock()
	defer dcw.lk.Unlock()

	if dcw.putCb != nil {
		// call all callbacks, remove those that were only needed once
		for i := 0; i < len(dcw.putCb); i++ {
			cb := dcw.putCb[i]
			cb.cb(len(content))
			if cb.once {
				dcw.putCb = append(dcw.putCb[:i], dcw.putCb[i+1:]...)
				i--
			}
		}
	}

	// first Put() call, initialise writer, which will write a CARv1 header
	writer, err := dcw.writer()
	if err != nil {
		return err
	}

	return writer.Put(ctx, key, content)
}

// writer()
func (dcw *DeferredCarWriter) writer() (carstorage.WritableCar, error) {
	if dcw.w == nil {
		outStream := dcw.outStream
		if outStream == nil {
			openedFile, err := os.OpenFile(dcw.outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			if err != nil {
				return nil, err
			}
			dcw.f = openedFile
			outStream = openedFile
		}
		w, err := carstorage.NewWritable(outStream, []cid.Cid{dcw.root}, append([]carv2.Option{carv2.WriteAsCarV1(true)}, dcw.opts...)...)
		if err != nil {
			return nil, err
		}
		dcw.w = w
	}
	return dcw.w, nil
}

// Close closes the underlying file, if one was created.
func (dcw *DeferredCarWriter) Close() error {
	dcw.lk.Lock()
	defer dcw.lk.Unlock()

	if dcw.f != nil {
		defer func() { dcw.f = nil }()
		return dcw.f.Close()
	}
	return nil
}

// BlockWriteOpener returns a BlockWriteOpener that operates on this storage.
func (dcw *DeferredCarWriter) BlockWriteOpener() linking.BlockWriteOpener {
	return func(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		wr, wrcommit, err := ipldstorage.PutStream(lctx.Ctx, dcw)
		return wr, func(lnk ipld.Link) error {
			return wrcommit(lnk.Binary())
		}, err
	}
}
