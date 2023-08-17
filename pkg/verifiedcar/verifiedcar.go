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
	"github.com/ipld/go-ipld-prime/traversal/selector"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
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

type BlockStream interface {
	Next(ctx context.Context) (blocks.Block, error)
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
	ExpectPath         datamodel.Path // sets the expected IPLD path that the traversal should take, if set, this is used to determine whether the full expected traversal occurred
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
	return cfg.VerifyBlockStream(ctx, blockReaderStream{cbr}, lsys)
}

func (cfg Config) VerifyBlockStream(ctx context.Context, bs BlockStream, lsys linking.LinkSystem) (uint64, uint64, error) {
	cr := &carReader{bs}
	bt := &writeTracker{}
	lsys.TrustedStorage = true // we can rely on the CAR decoder to check CID integrity
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	lsys.StorageReadOpener = cfg.nextBlockReadOpener(ctx, cr, bt, lsys)

	// perform the traversal
	if err := cfg.Traverse(ctx, lsys, nil); err != nil {
		return 0, 0, err
	}

	// make sure we don't have any extraneous data beyond what the traversal needs
	_, err := bs.Next(ctx)
	if err == nil {
		return 0, 0, ErrExtraneousBlock
	} else if !errors.Is(err, io.EOF) {
		return 0, 0, err
	}

	// wait for parser to finish and provide errors or stats
	return bt.blocks, bt.bytes, nil
}

// Traverse performs a traversal using the Config's Selector, starting at the
// Config's Root, using the provided LinkSystem and optional Preloader.
//
// The traversal will capture any errors that occur during traversal, block
// loading and will account for the Config's ExpectPath property, if set, to
// ensure that the full path-based traversal has occurred.
func (cfg Config) Traverse(
	ctx context.Context,
	lsys linking.LinkSystem,
	preloader preload.Loader,
) error {
	sel, err := selector.CompileSelector(cfg.Selector)
	if err != nil {
		return err
	}

	lsys, ecr := NewErrorCapturingReader(lsys)

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
			Preloader:                      preloader,
		},
	}
	if cfg.MaxBlocks > 0 {
		progress.Budget = &traversal.Budget{
			LinkBudget: int64(cfg.MaxBlocks) - 1, // first block is already loaded
			NodeBudget: math.MaxInt64,
		}
	}

	rootNode, err := loadNode(ctx, cfg.Root, lsys)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}
	progress.LastBlock.Link = cidlink.Link{Cid: cfg.Root}

	var lastPath datamodel.Path
	visitor := func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error {
		lastPath = p.Path
		if vr == traversal.VisitReason_SelectionMatch {
			return unixfsnode.BytesConsumingMatcher(p, n)
		}
		return nil
	}

	if err := progress.WalkAdv(rootNode, sel, visitor); err != nil {
		return traversalError(err)
	}

	if err := CheckTraversalPath(cfg.ExpectPath, lastPath); err != nil {
		return err
	}

	if ecr.Error != nil {
		return fmt.Errorf("block load failed during traversal: %w", ecr.Error)
	}
	return nil
}

func CheckTraversalPath(expectPath datamodel.Path, lastTraversalPath datamodel.Path) error {
	for expectPath.Len() > 0 {
		if lastTraversalPath.Len() == 0 {
			return fmt.Errorf("failed to traverse full path, missed: [%s]", expectPath.String())
		}
		var seg, lastSeg datamodel.PathSegment
		seg, expectPath = expectPath.Shift()
		lastSeg, lastTraversalPath = lastTraversalPath.Shift()
		if seg != lastSeg {
			return fmt.Errorf("unexpected path segment visit, got [%s], expected [%s]", lastSeg.String(), seg.String())
		}
	}
	// having lastTraversalPath.Len()>0 is fine, it may be due to an "all" or
	// "entity" doing an explore-all on the remainder of the DAG after the path;
	// or it could be because ExpectPath was empty.
	return nil
}

func loadNode(ctx context.Context, rootCid cid.Cid, lsys linking.LinkSystem) (datamodel.Node, error) {
	lnk := cidlink.Link{Cid: rootCid}
	lnkCtx := linking.LinkContext{Ctx: ctx}
	proto, err := protoChooser(lnk, lnkCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to choose prototype for CID %s: %w", rootCid.String(), err)
	}
	rootNode, err := lsys.Load(lnkCtx, lnk, proto)
	if err != nil {
		return nil, fmt.Errorf("failed to load root CID: %w", err)
	}
	return rootNode, nil
}

func (cfg *Config) nextBlockReadOpener(ctx context.Context, cr *carReader, bt *writeTracker, lsys linking.LinkSystem) linking.BlockReadOpener {
	seen := make(map[cid.Cid]struct{})
	return func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
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
				rdr, err := lsys.StorageReadOpener(lc, l)
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
}

type carReader struct {
	bs BlockStream
}

func (cr *carReader) readNextBlock(ctx context.Context, expected cid.Cid) ([]byte, error) {
	blk, err := cr.bs.Next(ctx)
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

// ErrorCapturingReader captures any errors that occur during block loading
// and makes them available via the Error property.
//
// This is useful for capturing errors that occur during traversal, which are
// not currently surfaced by the traversal package, see:
//
//	https://github.com/ipld/go-ipld-prime/pull/524
type ErrorCapturingReader struct {
	sro   linking.BlockReadOpener
	Error error
}

func NewErrorCapturingReader(lsys linking.LinkSystem) (linking.LinkSystem, *ErrorCapturingReader) {
	ecr := &ErrorCapturingReader{sro: lsys.StorageReadOpener}
	lsys.StorageReadOpener = ecr.StorageReadOpener
	return lsys, ecr
}

func (ecr *ErrorCapturingReader) StorageReadOpener(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
	r, err := ecr.sro(lc, l)
	if err != nil {
		ecr.Error = err
	}
	return r, err
}

type blockReaderStream struct {
	cbr *car.BlockReader
}

func (brs blockReaderStream) Next(ctx context.Context) (blocks.Block, error) {
	return brs.cbr.Next()
}
