package storage

import (
	"context"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	"github.com/ipfs/go-cid"
	log "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	carv2 "github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var logger = log.Logger("lassie/storage")

type DuplicateAdderCar struct {
	*DeferredCarWriter
	bufferStream       io.WriteCloser
	ctx                context.Context
	streamCompletion   chan error
	streamCompletionLk sync.Mutex
}

func NewDuplicateAdderCarForStream(ctx context.Context, root cid.Cid, path string, scope types.DagScope, store *DeferredStorageCar, outStream io.Writer) *DuplicateAdderCar {
	// create an io pipe from incoming data in a regular, dedupped car file
	// to outgoing data piped to a verification that inserts dups
	incomingStream, bufferStream := io.Pipe()

	// create the dedupped car writer we'll write to first
	buffer := NewDeferredCarWriterForStream(root, bufferStream)

	// create the car writer for the final stream
	outgoing := NewDeferredCarWriterForStream(root, outStream, carv2.AllowDuplicatePuts(true))
	da := &DuplicateAdderCar{
		DeferredCarWriter: buffer,
		bufferStream:      bufferStream,
		ctx:               ctx,
	}
	// on first write to the dedupped writer, start the process of verifying and inserting dups
	buffer.OnPut(func(int) {
		da.addDupes(root, path, scope, store, outgoing, incomingStream)
	}, true)
	return da
}

func (da *DuplicateAdderCar) addDupes(root cid.Cid, path string, scope types.DagScope, store *DeferredStorageCar, outgoing *DeferredCarWriter, incomingStream io.Reader) {
	// first, check if we have a stream completion channel, and abort if this is called twice
	da.streamCompletionLk.Lock()
	if da.streamCompletion != nil {
		da.streamCompletionLk.Unlock()
		logger.Warnf("attempted to start duplicate streaming for the second time")
		return
	}
	da.streamCompletion = make(chan error, 1)
	da.streamCompletionLk.Unlock()
	// start a go routine to verify the outgoing data and insert dups
	go func() {
		var err error
		defer func() {
			select {
			case <-da.ctx.Done():
			case da.streamCompletion <- err:
			}
		}()
		sel := types.PathScopeSelector(path, scope)

		// we're going to do a verified car where we add dupes back in
		cfg := verifiedcar.Config{
			Root:               root,
			Selector:           sel,
			WriteDuplicatesOut: true,
		}

		lsys := cidlink.DefaultLinkSystem()
		// use the final car writer to write blocks
		lsys.SetWriteStorage(outgoing)
		// use the deferred storage car to read in any dups we need
		// to serve
		// TODO: we access the readWrite method interface directly to avoid
		// locking the store, which can deadlock with a simultaneous call to
		// Put because of the io.Pipe. We should find a better way to do this.
		var rw ReadableWritableStorage
		if rw, err = store.readWrite(); err != nil {
			return
		}
		lsys.SetReadStorage(rw)
		lsys.TrustedStorage = true
		unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

		// run the verification
		_, _, err = cfg.Verify(da.ctx, incomingStream, lsys)
		return
	}()
}

// Close closes the dup stream, verifying completion, if one was created.
func (da *DuplicateAdderCar) Close() error {
	// close the buffer writer
	err := da.DeferredCarWriter.Close()
	if err != nil {
		return err
	}
	// close the write side of the io.Pipe -- this should trigger finishing
	// the dupe verifcation
	err = da.bufferStream.Close()
	if err != nil {
		return err
	}

	// wait for the dupe stream to complete
	da.streamCompletionLk.Lock()
	streamCompletion := da.streamCompletion
	da.streamCompletionLk.Unlock()
	if streamCompletion == nil {
		return nil
	}
	select {
	case <-da.ctx.Done():
		return da.ctx.Err()
	case err := <-streamCompletion:
		return err
	}
}
