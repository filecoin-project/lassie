package verifiedcar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	// include all the codecs we care about
	dagpb "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
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
	ErrBadRoots        = errors.New("CAR root CID mismatch")
	ErrUnexpectedBlock = errors.New("unexpected block in CAR")
	ErrExtraneousBlock = errors.New("extraneous block in CAR")
	ErrMissingBlock    = errors.New("missing block in CAR")
)

type BlockReader interface {
	Next() (blocks.Block, error)
}

var protoChooser = dagpb.AddSupportToChooser(basicnode.Chooser)

type Config struct {
	Root               cid.Cid        // The single root we expect to appear in the CAR and that we use to run our traversal against
	AllowCARv2         bool           // If true, allow CARv2 files to be received, otherwise strictly only allow CARv1
	Selector           datamodel.Node // The selector to execute, starting at the provided Root, to verify the contents of the CAR
	CheckRootsMismatch bool           // Check if roots match expected behavior
	ExpectDuplicatesIn bool           // Handles whether the incoming stream has duplicates
	WriteDuplicatesOut bool           // Handles whether duplicates should be written a second time as blocks
	MaxBlocks          uint64         // set a budget for the traversal
}

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
func (cfg Config) VerifyCar(ctx context.Context, rdr io.Reader, lsys linking.LinkSystem) (uint64, uint64, error) {
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

	if cfg.CheckRootsMismatch && (len(cbr.Roots) != 1 || cbr.Roots[0] != cfg.Root) {
		return 0, 0, ErrBadRoots
	}
	return cfg.VerifyBlockStream(ctx, cbr, lsys)
}

func (cfg Config) VerifyBlockStream(ctx context.Context, cbr BlockReader, lsys linking.LinkSystem) (uint64, uint64, error) {
	sel, err := selector.CompileSelector(cfg.Selector)
	if err != nil {
		return 0, 0, err
	}

	cr := &carReader{
		cbr: cbr,
	}
	bt := &writeTracker{}
	lsys.TrustedStorage = true // we can rely on the CAR decoder to check CID integrity
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	nbls, lsys := NewNextBlockLinkSystem(ctx, cfg, cr, bt, lsys)

	// run traversal in this goroutine
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
		},
	}
	if cfg.MaxBlocks > 0 {
		progress.Budget = &traversal.Budget{
			LinkBudget: int64(cfg.MaxBlocks) - 1, // first block is already loaded
			NodeBudget: math.MaxInt64,
		}
	}
	lc := linking.LinkContext{Ctx: ctx}
	lnk := cidlink.Link{Cid: cfg.Root}
	proto, err := protoChooser(lnk, lc)
	if err != nil {
		return 0, 0, err
	}
	rootNode, err := lsys.Load(lc, lnk, proto)
	if err != nil {
		return 0, 0, err
	}
	if err := progress.WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		if lbn, ok := n.(datamodel.LargeBytesNode); ok {
			rdr, err := lbn.AsLargeBytes()
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, rdr)
			return err
		}
		return nil
	}); err != nil {
		return 0, 0, traversalError(err)
	}

	if nbls.Error != nil {
		// capture any errors not bubbled up through the traversal, i.e. see
		// https://github.com/ipld/go-ipld-prime/pull/524
		return 0, 0, nbls.Error
	}

	// make sure we don't have any extraneous data beyond what the traversal needs
	_, err = cbr.Next()
	if err == nil {
		return 0, 0, ErrExtraneousBlock
	} else if !errors.Is(err, io.EOF) {
		return 0, 0, err
	}

	// wait for parser to finish and provide errors or stats
	return bt.blocks, bt.bytes, nil
}

type NextBlockLinkSystem struct {
	Error error
}

func NewNextBlockLinkSystem(
	ctx context.Context,
	cfg Config,
	cr *carReader,
	bt *writeTracker,
	lsys linking.LinkSystem,
) (*NextBlockLinkSystem, linking.LinkSystem) {
	nbls := &NextBlockLinkSystem{}
	seen := make(map[cid.Cid]struct{})
	storageReadOpener := lsys.StorageReadOpener

	nextBlockReadOpener := func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		cid := l.(cidlink.Link).Cid
		var data []byte
		var err error
		if _, ok := seen[cid]; ok {
			if cfg.ExpectDuplicatesIn {
				// duplicate block, but in this case we are expecting the stream to have it
				data, err = cr.readNextBlock(ctx, cid)
				if err != nil {
					return nil, err
				}
				if !cfg.WriteDuplicatesOut {
					return bytes.NewReader(data), nil
				}
			} else {
				// duplicate block, rely on the supplied LinkSystem to have stored this
				rdr, err := storageReadOpener(lc, l)
				if !cfg.WriteDuplicatesOut {
					return rdr, err
				}
				data, err = io.ReadAll(rdr)
				if err != nil {
					return nil, err
				}
			}
		} else {
			seen[cid] = struct{}{}
			data, err = cr.readNextBlock(ctx, cid)
			if err != nil {
				return nil, err
			}
		}
		bt.recordBlock(data)
		w, wc, err := lsys.StorageWriteOpener(lc)
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

	// wrap nextBlockReadOpener in one that captures errors on `nbls`
	lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		rdr, err := nextBlockReadOpener(lc, l)
		if err != nil {
			nbls.Error = err
			return nil, err
		}
		return rdr, nil
	}

	return nbls, lsys
}

type carReader struct {
	cbr BlockReader
}

func (cr *carReader) readNextBlock(ctx context.Context, expected cid.Cid) ([]byte, error) {
	blk, err := cr.cbr.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, format.ErrNotFound{Cid: expected}
		}
		return nil, multierr.Combine(ErrMalformedCar, err)
	}

	if blk.Cid() != expected {
		return nil, fmt.Errorf("%w: %s != %s", ErrUnexpectedBlock, blk.Cid(), expected)
	}
	return blk.RawData(), nil
}

type writeTracker struct {
	blocks uint64
	bytes  uint64
}

func (bt *writeTracker) recordBlock(data []byte) {
	bt.blocks++
	bt.bytes += uint64(len(data))
}

func traversalError(original error) error {
	err := original
	for {
		if v, ok := err.(interface{ NotFound() bool }); ok && v.NotFound() {
			// TODO: post-1.19: fmt.Errorf("%w: %w", ErrMissingBlock, err)
			return multierr.Combine(ErrMissingBlock, err)
		}
		if err = errors.Unwrap(err); err == nil {
			return original
		}
	}
}
