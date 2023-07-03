package testpeer

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ blockstore.Blockstore = (*BackedStore)(nil)
var _ blockstore.Blockstore = (*linkSystemBlockstore)(nil)

type BackedStore struct {
	blockstore.Blockstore
}

func (bs *BackedStore) UseLinkSystem(lsys linking.LinkSystem) {
	bs.Blockstore = &linkSystemBlockstore{lsys}
}

type linkSystemBlockstore struct {
	lsys linking.LinkSystem
}

func (lsbs *linkSystemBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return errors.New("not supported")
}

func (lsbs *linkSystemBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (lsbs *linkSystemBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	rdr, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rdr)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(buf.Bytes(), c)
}

func (lsbs *linkSystemBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	rdr, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return 0, err
	}
	i, err := io.Copy(io.Discard, rdr)
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

func (lsbs *linkSystemBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	w, wc, err := lsbs.lsys.StorageWriteOpener(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, bytes.NewReader(blk.RawData())); err != nil {
		return err
	}
	return wc(cidlink.Link{Cid: blk.Cid()})
}

func (lsbs *linkSystemBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, blk := range blks {
		if err := lsbs.Put(ctx, blk); err != nil {
			return err
		}
	}
	return nil
}

func (lsbs *linkSystemBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("not supported")
}

func (lsbs *linkSystemBlockstore) HashOnRead(enabled bool) {
	lsbs.lsys.TrustedStorage = !enabled
}
