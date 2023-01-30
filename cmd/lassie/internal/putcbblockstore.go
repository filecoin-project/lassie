package internal

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
)

// putCbBlockstore simply calls a callback on each put(), with the number of blocks put
var _ blockstore.Blockstore = (*putCbBlockstore)(nil)

type putCbBlockstore struct {
	// parentOpener lazily opens the parent blockstore upon first call to this blockstore.
	// This avoids blockstore instantiation until there is some interaction from the retriever.
	// In the case of CARv2 blockstores, this will avoid creation of empty .car files should
	// the retriever fail to find any candidates.
	parentOpener func() (*carblockstore.ReadWrite, error)
	// parent is lazily instantiated and should not be directly used; use parentBlockstore instead.
	parent *carblockstore.ReadWrite
	cb     func(putCount int, putBytes int)
}

func NewPutCbBlockstore(parentOpener func() (*carblockstore.ReadWrite, error), cb func(putCount int, putBytes int)) *putCbBlockstore {
	return &putCbBlockstore{parentOpener: parentOpener, cb: cb}
}

func (pcb *putCbBlockstore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	return pbs.DeleteBlock(ctx, cid)
}
func (pcb *putCbBlockstore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return false, err
	}
	return pbs.Has(ctx, cid)
}
func (pcb *putCbBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return nil, err
	}
	return pbs.Get(ctx, cid)
}
func (pcb *putCbBlockstore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return 0, err
	}
	return pbs.GetSize(ctx, cid)
}
func (pcb *putCbBlockstore) Put(ctx context.Context, block blocks.Block) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	pcb.cb(1, len(block.RawData()))
	return pbs.Put(ctx, block)
}
func (pcb *putCbBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	var bytes int
	for _, b := range blocks {
		bytes += len(b.RawData())
	}
	pcb.cb(len(blocks), bytes)
	return pbs.PutMany(ctx, blocks)
}
func (pcb *putCbBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return nil, err
	}
	return pbs.AllKeysChan(ctx)
}
func (pcb *putCbBlockstore) HashOnRead(enabled bool) {
	if pbs, err := pcb.parentBlockstore(); err != nil {
		fmt.Printf("Failed to instantiate blockstore while setting HashOnRead: %v\n", err)
	} else {
		pbs.HashOnRead(enabled)
	}
}
func (pcb *putCbBlockstore) Finalize() error {
	if pbs, err := pcb.parentBlockstore(); err != nil {
		return err
	} else {
		return pbs.Finalize()
	}
}
func (pcb *putCbBlockstore) parentBlockstore() (*carblockstore.ReadWrite, error) {
	if pcb.parent == nil {
		var err error
		if pcb.parent, err = pcb.parentOpener(); err != nil {
			return nil, err
		}
	}
	return pcb.parent, nil
}
