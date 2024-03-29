package storage

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/multiformats/go-multihash"
)

type DeferredWriter interface {
	ipldstorage.WritableStorage
	io.Closer
	BlockWriteOpener() linking.BlockWriteOpener
	OnPut(cb func(int), once bool)
}

var _ DeferredWriter = (*DuplicateAdderCar)(nil)

type DuplicateAdderCar struct {
	*deferred.DeferredCarWriter
	ctx                context.Context
	root               cid.Cid
	path               string
	scope              trustlessutils.DagScope
	bytes              *trustlessutils.ByteRange
	store              *DeferredStorageCar
	blockStream        *blockStream
	streamCompletion   chan error
	streamCompletionLk sync.Mutex
}

func NewDuplicateAdderCarForStream(
	ctx context.Context,
	outStream io.Writer,
	root cid.Cid,
	path string,
	scope trustlessutils.DagScope,
	bytes *trustlessutils.ByteRange,
	store *DeferredStorageCar,
) *DuplicateAdderCar {

	outgoing := deferred.NewDeferredCarWriterForStream(
		outStream,
		[]cid.Cid{root},
		carv2.AllowDuplicatePuts(true),
		carv2.StoreIdentityCIDs(false),
		carv2.UseWholeCIDs(true),
	)

	return newDuplicateAdderCar(ctx, root, path, scope, bytes, store, outgoing)
}

func NewDuplicateAdderCarForPath(
	ctx context.Context,
	outPath string,
	root cid.Cid,
	path string,
	scope trustlessutils.DagScope,
	bytes *trustlessutils.ByteRange,
	store *DeferredStorageCar,
) *DuplicateAdderCar {

	outgoing := deferred.NewDeferredCarWriterForPath(
		outPath,
		[]cid.Cid{root},
		carv2.AllowDuplicatePuts(true),
		carv2.StoreIdentityCIDs(false),
		carv2.UseWholeCIDs(true),
	)

	return newDuplicateAdderCar(ctx, root, path, scope, bytes, store, outgoing)
}

func newDuplicateAdderCar(
	ctx context.Context,
	root cid.Cid,
	path string,
	scope trustlessutils.DagScope,
	bytes *trustlessutils.ByteRange,
	store *DeferredStorageCar,
	outgoing *deferred.DeferredCarWriter,
) *DuplicateAdderCar {
	blockStream := &blockStream{ctx: ctx, seen: make(map[cid.Cid]struct{})}
	blockStream.blockBuffer = list.New()
	blockStream.cond = sync.NewCond(&blockStream.mu)
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
	sel := trustlessutils.Request{Path: da.path, Scope: da.scope, Bytes: da.bytes}.Selector()

	// we're going to do a verified car where we add dupes back in
	cfg := traversal.Config{
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
	_, err = cfg.VerifyBlockStream(da.ctx, da.blockStream, lsys)
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
			// assume that we will never want to write out identity CIDs, and they
			// won't be expected by a traversal (i.e. go-trustless-utils won't expect
			// them in a block stream)
			if lnk.(cidlink.Link).Cid.Prefix().MhType == multihash.IDENTITY {
				return nil
			}

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
	err := <-streamCompletion
	err2 := da.DeferredCarWriter.Close()
	if err == nil {
		err = err2
	}
	return err
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

func (bs *blockStream) Next(ctx context.Context) (blocks.Block, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	for {
		select {
		case <-bs.ctx.Done():
			return nil, bs.ctx.Err()
		case <-ctx.Done():
			return nil, ctx.Err()
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
