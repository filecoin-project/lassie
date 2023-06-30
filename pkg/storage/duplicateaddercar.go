package storage

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type DuplicateAdderCar struct {
	*DeferredCarWriter
	ctx                context.Context
	root               cid.Cid
	path               string
	scope              types.DagScope
	bytes              *types.ByteRange
	store              *DeferredStorageCar
	blockStream        *blockStream
	streamCompletion   chan error
	streamCompletionLk sync.Mutex
}

func NewDuplicateAdderCarForStream(
	ctx context.Context,
	root cid.Cid,
	path string,
	scope types.DagScope,
	bytes *types.ByteRange,
	store *DeferredStorageCar,
	outStream io.Writer,
) *DuplicateAdderCar {

	blockStream := &blockStream{ctx: ctx, seen: make(map[cid.Cid]struct{})}
	blockStream.blockBuffer = list.New()
	blockStream.cond = sync.NewCond(&blockStream.mu)

	// create the car writer for the final stream
	outgoing := NewDeferredCarWriterForStream(root, outStream, carv2.AllowDuplicatePuts(true))
	return &DuplicateAdderCar{
		DeferredCarWriter: outgoing,
		ctx:               ctx,
		root:              root,
		path:              path,
		scope:             scope,
		bytes:             bytes,
		store:             store,
		blockStream:       blockStream,
	}
}

func (da *DuplicateAdderCar) addDupes() {
	var err error
	defer func() {
		da.streamCompletion <- err
	}()
	sel := types.PathScopeSelector(da.path, da.scope, da.bytes)

	// we're going to do a verified car where we add dupes back in
	cfg := verifiedcar.Config{
		Root:               da.root,
		Selector:           sel,
		WriteDuplicatesOut: true,
	}

	lsys := cidlink.DefaultLinkSystem()
	// use the final car writer to write blocks
	lsys.SetWriteStorage(da)
	// use the deferred storage car to read in any dups we need
	// to serve
	lsys.SetReadStorage(da.store)
	lsys.TrustedStorage = true

	// run the verification
	_, _, err = cfg.VerifyBlockStream(da.ctx, da.blockStream, lsys)
}

func (da *DuplicateAdderCar) BlockWriteOpener() linking.BlockWriteOpener {
	return func(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		// first, check if we have a stream completion channel, and abort if this is called twice
		da.streamCompletionLk.Lock()
		if da.streamCompletion == nil {
			da.streamCompletion = make(chan error, 1)
			go da.addDupes()
		}
		da.streamCompletionLk.Unlock()
		var buf bytes.Buffer
		var written bool
		return &buf, func(lnk ipld.Link) error {
			if written {
				return fmt.Errorf("WriteCommitter already used")
			}
			written = true
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), lnk.(cidlink.Link).Cid)
			if err != nil {
				return err
			}
			return da.blockStream.WriteBlock(blk)
		}, nil
	}
}

// Close closes the dup stream, verifying completion, if one was created.
func (da *DuplicateAdderCar) Close() error {
	// close the block stream
	da.blockStream.Close()

	// wait for the dupe stream to complete
	da.streamCompletionLk.Lock()
	streamCompletion := da.streamCompletion
	da.streamCompletionLk.Unlock()
	if streamCompletion == nil {
		return nil
	}
	return <-streamCompletion
}

type blockStream struct {
	done        bool
	ctx         context.Context
	mu          sync.Mutex
	cond        *sync.Cond
	blockBuffer *list.List
	seen        map[cid.Cid]struct{}
}

func (bs *blockStream) Close() {
	bs.mu.Lock()
	bs.done = true
	bs.mu.Unlock()
	bs.cond.Signal()
}

func (bs *blockStream) WriteBlock(blk blocks.Block) error {
	if bs.ctx.Err() != nil {
		return bs.ctx.Err()
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.done {
		return errClosed
	}
	if _, ok := bs.seen[blk.Cid()]; ok {
		return nil
	}
	bs.seen[blk.Cid()] = struct{}{}
	bs.blockBuffer.PushBack(blk)
	bs.cond.Signal()
	return nil
}

func (bs *blockStream) Next() (blocks.Block, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	for {
		select {
		case <-bs.ctx.Done():
			return nil, bs.ctx.Err()
		default:
		}
		if e := bs.blockBuffer.Front(); e != nil {
			return bs.blockBuffer.Remove(e).(blocks.Block), nil
		}
		if bs.done {
			return nil, io.EOF
		}
		bs.cond.Wait()
	}
}
