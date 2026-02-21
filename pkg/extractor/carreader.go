package extractor

import (
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

// ExtractingCarReader reads a CAR stream and extracts UnixFS content to disk,
// tracking expected blocks via a frontier to detect incomplete streams.
type ExtractingCarReader struct {
	extractor *Extractor
	expected  map[cid.Cid]struct{}
	onBlock   func(int)
}

func NewExtractingCarReader(ext *Extractor, rootCid cid.Cid) *ExtractingCarReader {
	return &ExtractingCarReader{
		extractor: ext,
		expected:  map[cid.Cid]struct{}{rootCid: {}},
	}
}

func (r *ExtractingCarReader) OnBlock(cb func(int)) {
	r.onBlock = cb
}

func (r *ExtractingCarReader) ReadAndExtract(ctx context.Context, rdr io.Reader) (uint64, uint64, error) {
	br, err := car.NewBlockReader(rdr)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create block reader: %w", err)
	}

	var blockCount uint64
	var byteCount uint64

	for {
		if ctx.Err() != nil {
			return blockCount, byteCount, ctx.Err()
		}

		block, err := br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to read block: %w", err)
		}

		c := block.Cid()
		data := block.RawData()

		if _, ok := r.expected[c]; !ok {
			// duplicate or out-of-order block
			if r.extractor.IsProcessed(c) {
				continue
			}
			logger.Debugw("unexpected block in CAR", "cid", c)
		}
		delete(r.expected, c)

		computed, err := c.Prefix().Sum(data)
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to compute CID for block: %w", err)
		}
		if !computed.Equals(c) {
			return blockCount, byteCount, fmt.Errorf("CID mismatch: expected %s, got %s", c, computed)
		}

		blk, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to create block: %w", err)
		}

		children, err := r.extractor.ProcessBlock(ctx, blk)
		if err != nil {
			logger.Warnw("extractor failed", "cid", c, "err", err)
		}

		for _, child := range children {
			if !r.extractor.IsProcessed(child) && !isIdentityCid(child) {
				r.expected[child] = struct{}{}
			}
		}

		blockCount++
		byteCount += uint64(len(data))

		if r.onBlock != nil {
			r.onBlock(len(data))
		}
	}

	if len(r.expected) > 0 {
		missing := make([]cid.Cid, 0, len(r.expected))
		for c := range r.expected {
			missing = append(missing, c)
		}
		return blockCount, byteCount, &IncompleteError{Missing: missing}
	}

	return blockCount, byteCount, nil
}

type IncompleteError struct {
	Missing []cid.Cid
}

func (e *IncompleteError) Error() string {
	return fmt.Sprintf("incomplete CAR: missing %d blocks", len(e.Missing))
}

func IsMissing(err error) ([]cid.Cid, bool) {
	if ie, ok := err.(*IncompleteError); ok {
		return ie.Missing, true
	}
	return nil, false
}
