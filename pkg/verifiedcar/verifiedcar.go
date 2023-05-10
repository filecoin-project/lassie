package verifiedcar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"go.uber.org/multierr"
)

var (
	ErrMalformedCar    = errors.New("malformed CAR")
	ErrBadVersion      = errors.New("bad CAR version")
	ErrBadRoots        = errors.New("root CID mismatch")
	ErrUnexpectedBlock = errors.New("unexpected block")
	ErrExtraneousBlock = errors.New("extraneous block")
)

type Config struct {
	Root       cid.Cid           // The single root we expect to appear in the CAR and that we use to run our traversal against
	AllowCARv2 bool              // If true, allow CARv2 files to be received, otherwise strictly only allow CARv1
	Selector   selector.Selector // The selector to execute, starting at the provided Root, to verify the contents of the CAR
}

type parse struct {
	blocks uint64
	bytes  uint64
}

type nextBlock struct {
	err  error
	cid  cid.Cid
	data []byte
}

func visitNoop(traversal.Progress, datamodel.Node, traversal.VisitReason) error { return nil }

// Verify reads a CAR from the provided reader, verifies the contents are
// strictly what is specified by this Config and writes the blocks to the
// provided BlockWriteOpener. It returns the number of blocks and bytes
// written to the BlockWriteOpener.
//
// Verification is performed according to the CAR construction rules contained
// within the Trustless, and Path Gateway specifications:
//
// * https://specs.ipfs.tech/http-gateways/trustless-gateway/
//
// * https://specs.ipfs.tech/http-gateways/path-gateway/
func (cfg Config) Verify(ctx context.Context, rdr io.Reader, bwo linking.BlockWriteOpener) (uint64, uint64, error) {
	nextBlockCh := make(chan nextBlock)

	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true // we can rely on the CAR decoder to check CID integrity
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	lsys.StorageReadOpener = nextBlockReadOpener(ctx, nextBlockCh, bwo)

	cbr, err := car.NewBlockReader(rdr, car.WithTrustedCAR(false))
	if err != nil {
		// TODO: post-1.19: fmt.Errorf("%w: %w", ErrMalformedCar, err)
		return 0, 0, multierr.Combine(ErrMalformedCar, err)
	}

	switch cbr.Version {
	case 1:
	case 2:
		if !cfg.AllowCARv2 {
			return 0, 0, ErrBadVersion
		}
	default:
		return 0, 0, ErrBadVersion
	}

	if len(cbr.Roots) != 1 || cbr.Roots[0] != cfg.Root {
		return 0, 0, ErrBadRoots
	}

	// run parser in separate goroutine
	parseCh := make(chan parse, 1)
	parseCtx, parseCancel := context.WithCancel(ctx)
	defer parseCancel()
	go parseCar(parseCtx, cbr, nextBlockCh, parseCh)

	// run traversal in this goroutine
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: dagpb.AddSupportToChooser(basicnode.Chooser),
		},
	}
	rootNode, err := lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: cfg.Root}, basicnode.Prototype.Any)
	if err != nil {
		return 0, 0, err
	}
	if err := progress.WalkAdv(rootNode, cfg.Selector, visitNoop); err != nil {
		return 0, 0, traversalError(err)
	}

	// make sure we don't have any extraneous data beyond what the traversal needs
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case _, ok := <-nextBlockCh:
		if ok {
			return 0, 0, ErrExtraneousBlock
		}
	}

	// wait for parser to finish and provide errors or stats
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case parse := <-parseCh:
		return parse.blocks, parse.bytes, nil
	}
}

func nextBlockReadOpener(ctx context.Context, nextBlockCh chan nextBlock, bwo linking.BlockWriteOpener) linking.BlockReadOpener {
	return func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		cid := l.(cidlink.Link).Cid
		data, err := readNextBlock(ctx, nextBlockCh, cid)
		if err != nil {
			return nil, err
		}
		w, wc, err := bwo(lc)
		if err != nil {
			return nil, err
		}
		rdr := bytes.NewReader(data)
		if _, err := io.Copy(w, rdr); err != nil {
			return nil, err
		}
		if err := wc(l); err != nil {
			return nil, err
		}
		if _, err := rdr.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return io.NopCloser(rdr), nil
	}
}

func readNextBlock(ctx context.Context, nextBlockCh chan nextBlock, expected cid.Cid) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case blk, ok := <-nextBlockCh:
		if !ok {
			// parser ended
			return nil, format.ErrNotFound{Cid: expected}
		}
		if blk.err != nil {
			// TODO: post-1.19: fmt.Errorf("%w: %w", ErrMalformedCar, err)
			return nil, multierr.Combine(ErrMalformedCar, blk.err)
		}
		if blk.cid != expected {
			return nil, fmt.Errorf("%w: %s != %s", ErrUnexpectedBlock, blk.cid, expected)
		}
		return blk.data, nil
	}
}

func parseCar(ctx context.Context, cbr *car.BlockReader, nextBlockCh chan nextBlock, parseCh chan parse) {
	defer close(parseCh)
	defer close(nextBlockCh)
	var blockCount, byteCount uint64
	for ctx.Err() == nil {
		blk, err := cbr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			nextBlockCh <- nextBlock{err: err}
			return
		}
		nextBlockCh <- nextBlock{cid: blk.Cid(), data: blk.RawData()}
		byteCount += uint64(len(blk.RawData()))
		blockCount++
	}
	parseCh <- parse{blocks: blockCount, bytes: byteCount}
}

func traversalError(original error) error {
	err := original
	for {
		if v, ok := err.(interface{ NotFound() bool }); ok && v.NotFound() {
			// TODO: post-1.19: fmt.Errorf("%w: %w", ErrMalformedCar, err)
			return multierr.Combine(ErrMalformedCar, err)
		}
		if err = errors.Unwrap(err); err == nil {
			return original
		}
	}
}
