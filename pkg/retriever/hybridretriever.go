package retriever

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/blockbroker"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/extractor"
	"github.com/filecoin-project/lassie/pkg/types"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-trustless-utils/traversal"
)

const DefaultRawLeafConcurrency = 8

// HybridRetriever tries a full CAR fetch first, then falls back to
// per-block frontier traversal when the initial response is incomplete.
type HybridRetriever struct {
	inner                 types.Retriever
	candidateSource       types.CandidateSource
	httpClient            *http.Client
	clock                 clock.Clock
	skipBlockVerification bool
}

func NewHybridRetriever(
	inner types.Retriever,
	candidateSource types.CandidateSource,
	httpClient *http.Client,
	skipBlockVerification bool,
) *HybridRetriever {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HybridRetriever{
		inner:                 inner,
		candidateSource:       candidateSource,
		httpClient:            httpClient,
		clock:                 clock.New(),
		skipBlockVerification: skipBlockVerification,
	}
}

type phaseOneStats struct {
	bytesReceived  uint64
	blocksReceived uint64
}

func (hr *HybridRetriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
	startTime := hr.clock.Now()

	if eventsCallback == nil {
		eventsCallback = func(types.RetrievalEvent) {}
	}

	var p1Stats phaseOneStats
	wrappedCallback := func(event types.RetrievalEvent) {
		if br, ok := event.(interface{ ByteCount() uint64 }); ok {
			p1Stats.bytesReceived += br.ByteCount()
			p1Stats.blocksReceived++
		}
		eventsCallback(event)
	}

	stats, err := hr.inner.Retrieve(ctx, request, wrappedCallback)
	if err == nil {
		return stats, nil
	}

	missingCid, ok := extractMissingCid(err)
	if !ok {
		return nil, err
	}

	logger.Infow("switching to per-block retrieval (missing blocks in initial response)",
		"root", request.Root,
		"missingCid", missingCid,
		"phase1Blocks", p1Stats.blocksReceived,
		"phase1Bytes", p1Stats.bytesReceived)

	stats, err = hr.continuePerBlock(ctx, request, eventsCallback, startTime, p1Stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (hr *HybridRetriever) continuePerBlock(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
	startTime time.Time,
	p1Stats phaseOneStats,
) (*types.RetrievalStats, error) {
	session := blockbroker.NewSession(hr.candidateSource, hr.httpClient, hr.skipBlockVerification)
	defer session.Close()

	session.SeedProviders(ctx, request.Root)

	var p2BlocksOut uint64
	var p2BytesOut uint64

	if request.MaxBlocks > 0 && p1Stats.blocksReceived >= request.MaxBlocks {
		logger.Infow("max blocks limit already reached in phase 1", "limit", request.MaxBlocks)
		duration := hr.clock.Since(startTime)
		var speed uint64
		if duration.Seconds() > 0 {
			speed = uint64(float64(p1Stats.bytesReceived) / duration.Seconds())
		}
		return &types.RetrievalStats{
			RootCid:      request.Root,
			Size:         p1Stats.bytesReceived,
			Blocks:       p1Stats.blocksReceived,
			Duration:     duration,
			AverageSpeed: speed,
			Providers:    session.UsedProviders(),
		}, nil
	}

	var maxBlocks uint64
	if request.MaxBlocks > 0 {
		maxBlocks = request.MaxBlocks - p1Stats.blocksReceived
	}

	err := hr.streamingTraverse(ctx, request, session, &p2BytesOut, &p2BlocksOut, maxBlocks)
	if err != nil {
		return nil, fmt.Errorf("per-block traversal failed: %w", err)
	}

	totalBytes := p1Stats.bytesReceived + atomic.LoadUint64(&p2BytesOut)
	totalBlocks := p1Stats.blocksReceived + atomic.LoadUint64(&p2BlocksOut)
	duration := hr.clock.Since(startTime)

	var speed uint64
	if duration.Seconds() > 0 {
		speed = uint64(float64(totalBytes) / duration.Seconds())
	}

	return &types.RetrievalStats{
		RootCid:      request.Root,
		Size:         totalBytes,
		Blocks:       totalBlocks,
		Duration:     duration,
		AverageSpeed: speed,
		Providers:    session.UsedProviders(),
	}, nil
}

// streamingTraverse fetches blocks via frontier DFS, writing each to output
// immediately. Raw leaves are batched and fetched in parallel.
func (hr *HybridRetriever) streamingTraverse(
	ctx context.Context,
	request types.RetrievalRequest,
	session blockbroker.BlockSession,
	bytesOut *uint64,
	blocksOut *uint64,
	maxBlocks uint64,
) error {
	frontier := NewFrontier(request.Root)
	baseLsys := request.LinkSystem

	var blockCount uint64

	for !frontier.Empty() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c := frontier.Pop()

		if frontier.Seen(c) {
			continue
		}

		// check phase 1 cache
		if baseLsys.StorageReadOpener != nil {
			rdr, err := baseLsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
			if err == nil {
				data, readErr := io.ReadAll(rdr)
				if readErr != nil {
					logger.Warnw("cached block read failed, fetching from network", "cid", c, "err", readErr)
				} else {
					block, blockErr := blocks.NewBlockWithCid(data, c)
					if blockErr != nil {
						logger.Warnw("cached block parse failed, fetching from network", "cid", c, "err", blockErr)
					} else {
						logger.Debugw("using cached block from phase 1", "cid", c)
						links, _ := ExtractLinks(block)
						frontier.PushAll(links)
						frontier.MarkSeen(c)
						continue
					}
				}
			}
		}

		block, err := hr.fetchBlock(ctx, c, session, baseLsys)
		if err != nil {
			return fmt.Errorf("failed to fetch block %s: %w", c, err)
		}

		links, err := ExtractLinks(block)
		if err != nil {
			logger.Warnw("failed to extract links", "cid", c, "err", err)
		} else {
			rawLeaves, dagNodes := separateLinksByCodec(ctx, links, frontier, baseLsys)
			frontier.PushAll(dagNodes)

			if maxBlocks > 0 && uint64(len(rawLeaves)) > maxBlocks-blockCount {
				rawLeaves = rawLeaves[:maxBlocks-blockCount]
			}
			if len(rawLeaves) > 0 {
				fetchedBlocks, fetchErr := hr.parallelFetchRawLeaves(ctx, rawLeaves, session)
				if fetchErr != nil {
					return fmt.Errorf("parallel fetch failed: %w", fetchErr)
				}

				for _, b := range fetchedBlocks {
					if err := hr.writeBlock(ctx, b, baseLsys); err != nil {
						return fmt.Errorf("failed to write block %s: %w", b.Cid(), err)
					}
					atomic.AddUint64(bytesOut, uint64(len(b.RawData())))
					atomic.AddUint64(blocksOut, 1)
					frontier.MarkSeen(b.Cid())
					blockCount++

					if maxBlocks > 0 && blockCount >= maxBlocks {
						logger.Infow("reached max blocks limit", "limit", maxBlocks)
						return nil
					}
				}
			}
		}

		if err := hr.writeBlock(ctx, block, baseLsys); err != nil {
			return fmt.Errorf("failed to write block %s: %w", c, err)
		}

		blockData := block.RawData()
		atomic.AddUint64(bytesOut, uint64(len(blockData)))
		atomic.AddUint64(blocksOut, 1)

		frontier.MarkSeen(c)
		blockCount++

		if maxBlocks > 0 && blockCount >= maxBlocks {
			logger.Infow("reached max blocks limit", "limit", maxBlocks)
			break
		}
	}

	return nil
}

// separateLinksByCodec splits CIDs into raw leaves (codec 0x55, no children,
// safe to fetch in parallel) and dag nodes. Skips already-seen CIDs and
// raw leaves already in storage.
func separateLinksByCodec(ctx context.Context, links []cid.Cid, frontier *Frontier, lsys linking.LinkSystem) (rawLeaves, dagNodes []cid.Cid) {
	for _, c := range links {
		if frontier.Seen(c) {
			continue
		}
		if c.Prefix().Codec == cid.Raw {
			if lsys.StorageReadOpener != nil {
				_, err := lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
				if err == nil {
					frontier.MarkSeen(c)
					logger.Debugw("raw leaf already in storage, skipping fetch", "cid", c)
					continue
				}
			}
			rawLeaves = append(rawLeaves, c)
		} else {
			dagNodes = append(dagNodes, c)
		}
	}
	return
}

func (hr *HybridRetriever) parallelFetchRawLeaves(
	ctx context.Context,
	cids []cid.Cid,
	session blockbroker.BlockSession,
) ([]blocks.Block, error) {
	if len(cids) == 0 {
		return nil, nil
	}

	logger.Debugw("parallel fetching raw leaves", "count", len(cids))

	results := make([]blocks.Block, len(cids))
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	sem := make(chan struct{}, DefaultRawLeafConcurrency)

	for i, c := range cids {
		wg.Add(1)
		go func(idx int, c cid.Cid) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errOnce.Do(func() { firstErr = ctx.Err() })
				return
			}

			block, err := session.Get(ctx, c)
			if err != nil {
				errOnce.Do(func() { firstErr = fmt.Errorf("fetch %s: %w", c, err) })
				return
			}
			results[idx] = block
		}(i, c)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	logger.Debugw("parallel fetch complete", "fetched", len(results))
	return results, nil
}

// fetchBlock prefers CAR subgraph for dag-pb/dag-cbor (may fetch children too),
// falls back to raw single-block fetch. Raw codec leaves skip CAR entirely.
func (hr *HybridRetriever) fetchBlock(
	ctx context.Context,
	c cid.Cid,
	session blockbroker.BlockSession,
	baseLsys linking.LinkSystem,
) (blocks.Block, error) {
	if c.Prefix().Codec == cid.Raw {
		block, err := session.Get(ctx, c)
		if err != nil {
			return nil, err
		}
		logger.Debugw("fetched raw leaf block", "cid", c, "bytes", len(block.RawData()))
		return block, nil
	}

	if baseLsys.StorageReadOpener != nil {
		blocksFromCAR, carErr := session.GetSubgraph(ctx, c, baseLsys)
		if carErr == nil && blocksFromCAR > 0 {
			logger.Debugw("fetched subgraph via CAR", "cid", c, "blocks", blocksFromCAR)
			rdr, err := baseLsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
			if err == nil {
				data, err := io.ReadAll(rdr)
				if err == nil {
					return blocks.NewBlockWithCid(data, c)
				}
				logger.Warnw("CAR readback failed after successful fetch", "cid", c, "err", err)
			} else {
				logger.Warnw("CAR storage read failed after successful fetch", "cid", c, "err", err)
			}
		} else if carErr != nil {
			logger.Debugw("CAR subgraph unavailable, trying single block", "cid", c, "reason", carErr)
		}
	}

	block, err := session.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	logger.Debugw("fetched single block", "cid", c, "bytes", len(block.RawData()))
	return block, nil
}

func (hr *HybridRetriever) writeBlock(
	ctx context.Context,
	block blocks.Block,
	baseLsys linking.LinkSystem,
) error {
	if baseLsys.StorageWriteOpener == nil {
		return nil
	}

	w, wc, err := baseLsys.StorageWriteOpener(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, bytes.NewReader(block.RawData())); err != nil {
		return err
	}

	return wc(cidlink.Link{Cid: block.Cid()})
}

func extractMissingCid(err error) (cid.Cid, bool) {
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return notFound.Cid, true
	}

	if errors.Is(err, traversal.ErrMissingBlock) {
		var nf format.ErrNotFound
		if errors.As(err, &nf) {
			return nf.Cid, true
		}
		return cid.Undef, true
	}

	return cid.Undef, false
}

// RetrieveAndExtract retrieves content and extracts UnixFS to disk in a
// single pass without buffering entire files in memory.
func (hr *HybridRetriever) RetrieveAndExtract(
	ctx context.Context,
	rootCid cid.Cid,
	ext *extractor.Extractor,
	eventsCallback func(types.RetrievalEvent),
	onBlock func(int),
) (*types.RetrievalStats, error) {
	startTime := hr.clock.Now()
	if eventsCallback == nil {
		eventsCallback = func(types.RetrievalEvent) {}
	}

	session := blockbroker.NewSession(hr.candidateSource, hr.httpClient, hr.skipBlockVerification)
	defer session.Close()

	session.SeedProviders(ctx, rootCid)

	carReader := extractor.NewExtractingCarReader(ext, rootCid)
	if onBlock != nil {
		carReader.OnBlock(onBlock)
	}

	var totalBlocks uint64
	var totalBytes uint64
	var primaryProvider string

	rdr, provider, err := session.GetSubgraphStream(ctx, rootCid)
	if err == nil {
		primaryProvider = provider
		eventsCallback(events.ExtractionStarted(hr.clock.Now(), rootCid, provider))
		logger.Infow("streaming CAR extraction", "root", rootCid, "provider", provider)

		blocks, bytes, extractErr := carReader.ReadAndExtract(ctx, rdr)
		rdr.Close()

		totalBlocks = blocks
		totalBytes = bytes

		if extractErr == nil {
			duration := hr.clock.Since(startTime)
			var speed uint64
			if duration.Seconds() > 0 {
				speed = uint64(float64(totalBytes) / duration.Seconds())
			}
			eventsCallback(events.ExtractionSucceeded(hr.clock.Now(), rootCid, provider, totalBytes, totalBlocks, duration))
			return &types.RetrievalStats{
				RootCid:      rootCid,
				Size:         totalBytes,
				Blocks:       totalBlocks,
				Duration:     duration,
				AverageSpeed: speed,
				Providers:    session.UsedProviders(),
			}, nil
		}

		missing, isMissing := extractor.IsMissing(extractErr)
		if !isMissing {
			return nil, fmt.Errorf("extraction failed: %w", extractErr)
		}

		logger.Infow("CAR incomplete, falling back to per-block",
			"root", rootCid,
			"blocksFromCAR", totalBlocks,
			"missing", len(missing))

		p2Blocks, p2Bytes, err := hr.extractPerBlock(ctx, ext, session, missing, onBlock)
		if err != nil {
			return nil, fmt.Errorf("per-block extraction failed: %w", err)
		}
		totalBlocks += p2Blocks
		totalBytes += p2Bytes
	} else {
		logger.Infow("CAR unavailable, using per-block extraction", "root", rootCid, "err", err)
		eventsCallback(events.ExtractionStarted(hr.clock.Now(), rootCid, "per-block"))
		primaryProvider = "per-block"

		p2Blocks, p2Bytes, err := hr.extractPerBlock(ctx, ext, session, []cid.Cid{rootCid}, onBlock)
		if err != nil {
			return nil, fmt.Errorf("per-block extraction failed: %w", err)
		}
		totalBlocks = p2Blocks
		totalBytes = p2Bytes
	}

	duration := hr.clock.Since(startTime)
	var speed uint64
	if duration.Seconds() > 0 {
		speed = uint64(float64(totalBytes) / duration.Seconds())
	}

	eventsCallback(events.ExtractionSucceeded(hr.clock.Now(), rootCid, primaryProvider, totalBytes, totalBlocks, duration))
	return &types.RetrievalStats{
		RootCid:      rootCid,
		Size:         totalBytes,
		Blocks:       totalBlocks,
		Duration:     duration,
		AverageSpeed: speed,
		Providers:    session.UsedProviders(),
	}, nil
}

func (hr *HybridRetriever) extractPerBlock(
	ctx context.Context,
	ext *extractor.Extractor,
	session blockbroker.BlockSession,
	startCids []cid.Cid,
	onBlock func(int),
) (uint64, uint64, error) {
	frontier := NewFrontier(cid.Undef)
	frontier.pending = append(frontier.pending, startCids...)

	var totalBlocks uint64
	var totalBytes uint64

	for !frontier.Empty() {
		if ctx.Err() != nil {
			return totalBlocks, totalBytes, ctx.Err()
		}

		c := frontier.Pop()
		if frontier.Seen(c) {
			continue
		}

		block, err := session.Get(ctx, c)
		if err != nil {
			return totalBlocks, totalBytes, fmt.Errorf("failed to fetch block %s: %w", c, err)
		}

		children, err := ext.ProcessBlock(ctx, block)
		if err != nil {
			logger.Warnw("extraction failed for block", "cid", c, "err", err)
		}

		frontier.PushAll(children)
		frontier.MarkSeen(c)

		blockSize := len(block.RawData())
		totalBlocks++
		totalBytes += uint64(blockSize)

		if onBlock != nil {
			onBlock(blockSize)
		}
	}

	return totalBlocks, totalBytes, nil
}
