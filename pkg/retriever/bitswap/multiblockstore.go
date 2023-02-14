package bitswap

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type contextKey string

const retrievalIDKey = contextKey("retrieval-id-key")

func RegisterRetrievalIDToContext(parentCtx context.Context, id types.RetrievalID) context.Context {
	ctx := context.WithValue(parentCtx, retrievalIDKey, id)
	return ctx
}

// ErrNotSupported indicates an operation not supported by the MultiBlockstore
var ErrNotSupported = errors.New("not supported")

// ErrIncorrectContextValue indicates a value for the retrieval id context key that wasn't a retrieval id
var ErrIncorrectContextValue = errors.New("context key does not point to a retrieval id")

// ErrAlreadyRegistered means something has already been registered for a retrieval id
var ErrAlreadyRegisterd = errors.New("already registered")

// ErrAlreadyRegistered means there is nothing registered for a retrieval id
var ErrNotRegistered = errors.New("not registered")

type linkSystemRecord struct {
	lsys   *linking.LinkSystem
	cancel context.CancelFunc
}

// MultiBlockstore creates a blockstore based on one or more linkystems, extracting the target linksystem for each request
// from the retrieval id context key
type MultiBlockstore struct {
	linkSystems   map[types.RetrievalID]*linking.LinkSystem
	linkSystemsLk sync.RWMutex
}

// NewMultiblockstore returns a new MultiBlockstore
func NewMultiblockstore() *MultiBlockstore {
	return &MultiBlockstore{
		linkSystems: make(map[types.RetrievalID]*linking.LinkSystem),
	}
}

// AddLinkSystem registers a linksystem to use for a given retrieval id
func (mbs *MultiBlockstore) AddLinkSystem(id types.RetrievalID, lsys *linking.LinkSystem) error {
	mbs.linkSystemsLk.Lock()
	defer mbs.linkSystemsLk.Unlock()
	if _, ok := mbs.linkSystems[id]; ok {
		return ErrAlreadyRegisterd
	}
	mbs.linkSystems[id] = lsys
	return nil
}

// RemoveLinkSystem unregisters the link system for a given retrieval id
func (mbs *MultiBlockstore) RemoveLinkSystem(id types.RetrievalID) {
	mbs.linkSystemsLk.Lock()
	defer mbs.linkSystemsLk.Unlock()
	delete(mbs.linkSystems, id)
}

// DeleteBlock is not supported
func (mbs *MultiBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return ErrNotSupported
}

// Has is not supported
func (mbs *MultiBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return false, ErrNotSupported
}

type byteReader interface {
	Bytes() []byte
}

// Get returns a block only if the given ctx contains a retrieval ID as a value that
// references a known linksystem. If it does, it uses that linksystem to load the block
func (mbs *MultiBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	sk := ctx.Value(retrievalIDKey)
	if sk == nil {
		return nil, format.ErrNotFound{Cid: c}
	}
	id, ok := sk.(types.RetrievalID)
	if !ok {
		return nil, ErrIncorrectContextValue
	}
	mbs.linkSystemsLk.RLock()
	lsys, ok := mbs.linkSystems[id]
	mbs.linkSystemsLk.RUnlock()
	if !ok {
		return nil, format.ErrNotFound{Cid: c}
	}
	r, err := lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	if br, ok := r.(byteReader); ok {
		return blocks.NewBlockWithCid(br.Bytes(), c)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, c)
}

// GetSize is unsupported
func (mbs *MultiBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	return 0, errors.New("not supported")
}

// Put writes a block only if the given ctx contains a retrieval ID as a value that
// references a known linksystem. If it does, it uses that linksystem to save the block
func (mbs *MultiBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	sk := ctx.Value(retrievalIDKey)
	if sk == nil {
		return ErrNotSupported
	}
	id, ok := sk.(types.RetrievalID)
	if !ok {
		return ErrIncorrectContextValue
	}
	mbs.linkSystemsLk.RLock()
	lsys, ok := mbs.linkSystems[id]
	mbs.linkSystemsLk.RUnlock()
	if !ok {
		return ErrNotSupported
	}
	w, commit, err := lsys.StorageWriteOpener(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	_, err = w.Write(blk.RawData())
	if err != nil {
		return err
	}
	return commit(cidlink.Link{Cid: blk.Cid()})
}

// PutMany puts a slice of blocks at the same time, with the same rules as Put
func (mbs *MultiBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, blk := range blks {
		err := mbs.Put(ctx, blk)
		if err != nil {
			return err
		}
	}
	return nil
}

// AllKeysChan is unsupported
func (mbs *MultiBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("not supported")
}

// HashOnRead is unsupported
func (mbs *MultiBlockstore) HashOnRead(enabled bool) {
}
