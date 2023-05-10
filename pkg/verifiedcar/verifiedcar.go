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
	ErrBadRoots        = errors.New("CAR root CID mismatch")
	ErrUnexpectedBlock = errors.New("unexpected block in CAR")
	ErrExtraneousBlock = errors.New("extraneous block in CAR")
	ErrMissingBlock    = errors.New("missing block in CAR")
)

var protoChooser = dagpb.AddSupportToChooser(basicnode.Chooser)

type Config struct {
	Root       cid.Cid           // The single root we expect to appear in the CAR and that we use to run our traversal against
	AllowCARv2 bool              // If true, allow CARv2 files to be received, otherwise strictly only allow CARv1
	Selector   selector.Selector // The selector to execute, starting at the provided Root, to verify the contents of the CAR
}

func visitNoop(p traversal.Progress, n datamodel.Node, r traversal.VisitReason) error { return nil }

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
func (cfg Config) Verify(ctx context.Context, rdr io.Reader, lsys linking.LinkSystem) (uint64, uint64, error) {

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
	cr := &carReader{
		cbr: cbr,
	}

	lsys.TrustedStorage = true // we can rely on the CAR decoder to check CID integrity
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	lsys.StorageReadOpener = nextBlockReadOpener(ctx, cr, lsys)

	// run traversal in this goroutine
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
		},
	}
	lc := linking.LinkContext{}
	lnk := cidlink.Link{Cid: cfg.Root}
	proto, err := protoChooser(lnk, lc)
	if err != nil {
		return 0, 0, err
	}
	rootNode, err := lsys.Load(lc, lnk, proto)
	if err != nil {
		return 0, 0, err
	}
	if err := progress.WalkAdv(rootNode, cfg.Selector, visitNoop); err != nil {
		return 0, 0, traversalError(err)
	}

	// make sure we don't have any extraneous data beyond what the traversal needs
	_, err = cbr.Next()
	if !errors.Is(err, io.EOF) {
		return 0, 0, ErrExtraneousBlock
	}

	// wait for parser to finish and provide errors or stats
	return cr.blocks, cr.bytes, nil
}

func nextBlockReadOpener(ctx context.Context, cr *carReader, lsys linking.LinkSystem) linking.BlockReadOpener {
	seen := make(map[cid.Cid]struct{})
	return func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		cid := l.(cidlink.Link).Cid

		if _, ok := seen[cid]; ok {
			// duplicate block, rely on the supplied LinkSystem to have stored this
			return lsys.StorageReadOpener(lc, l)
		}

		seen[cid] = struct{}{}
		data, err := cr.readNextBlock(ctx, cid)
		if err != nil {
			return nil, err
		}
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
}

type carReader struct {
	cbr    *car.BlockReader
	blocks uint64
	bytes  uint64
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

	cr.bytes += uint64(len(blk.RawData()))
	cr.blocks++
	return blk.RawData(), nil
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
