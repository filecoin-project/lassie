package retriever

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers/groupworkpool"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/client/traceability"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"go.uber.org/multierr"
)

// IndexerRouting are the required methods to track indexer routing
type IndexerRouting interface {
	AddProviders(types.RetrievalID, []types.RetrievalCandidate)
	RemoveProviders(types.RetrievalID)
}

// MultiBlockstore are the require methods to track linksystems
type MultiBlockstore interface {
	AddLinkSystem(id types.RetrievalID, lsys *linking.LinkSystem) error
	RemoveLinkSystem(id types.RetrievalID)
}

type InProgressCids interface {
	Inc(cid.Cid, types.RetrievalID)
	Dec(cid.Cid, types.RetrievalID)
}

// BitswapRetriever uses bitswap to retrieve data
// BitswapRetriever retrieves using a combination of a go-bitswap client specially configured per retrieval,
// underneath a blockservice and a go-fetcher Fetcher.
// Selectors are used to travers the dag to make sure the CARs for bitswap match graphsync
// Note: this is a tradeoff over go-merkledag for traversal, cause selector execution is slow. But the solution
// is to improve selector execution, not introduce unpredictable encoding.
type BitswapRetriever struct {
	bstore         MultiBlockstore
	inProgressCids InProgressCids
	routing        IndexerRouting
	blockService   blockservice.BlockService
	clock          clock.Clock
	cfg            BitswapConfig
	// this is purely for testing purposes, to ensure that we receive all candidates
	awaitReceivedCandidates chan<- struct{}
	groupWorkPool           groupworkpool.GroupWorkPool
}

const shortenedDelay = 4 * time.Millisecond

// BitswapConfig contains configurable parameters for bitswap fetching
type BitswapConfig struct {
	BlockTimeout            time.Duration
	Concurrency             int
	ConcurrencyPerRetrieval int
}

// NewBitswapRetrieverFromHost constructs a new bitswap retriever for the given libp2p host
func NewBitswapRetrieverFromHost(
	ctx context.Context,
	host host.Host,
	cfg BitswapConfig,
) *BitswapRetriever {
	bstore := bitswaphelpers.NewMultiblockstore()
	inProgressCids := bitswaphelpers.NewInProgressCids()
	routing := bitswaphelpers.NewIndexerRouting(inProgressCids.Get)
	bsnet := network.NewFromIpfsHost(host, routing)
	bitswap := client.New(ctx, bsnet, bstore, client.ProviderSearchDelay(shortenedDelay))
	bsnet.Start(bitswap)
	bsrv := blockservice.New(bstore, bitswap)
	go func() {
		<-ctx.Done()
		bsnet.Stop()
	}()
	return NewBitswapRetrieverFromDeps(ctx, bsrv, routing, inProgressCids, bstore, cfg, clock.New(), nil)
}

// NewBitswapRetrieverFromDeps is primarily for testing, constructing behavior from direct dependencies
func NewBitswapRetrieverFromDeps(
	ctx context.Context,
	bsrv blockservice.BlockService,
	routing IndexerRouting,
	inProgressCids InProgressCids,
	bstore MultiBlockstore,
	cfg BitswapConfig,
	clock clock.Clock,
	awaitReceivedCandidates chan<- struct{},
) *BitswapRetriever {
	gwp := groupworkpool.New(cfg.Concurrency, cfg.ConcurrencyPerRetrieval)
	gwp.Start(ctx)

	return &BitswapRetriever{
		bstore:                  bstore,
		inProgressCids:          inProgressCids,
		routing:                 routing,
		blockService:            bsrv,
		clock:                   clock,
		cfg:                     cfg,
		awaitReceivedCandidates: awaitReceivedCandidates,
		groupWorkPool:           gwp,
	}
}

// Retrieve initializes a new bitswap session
func (br *BitswapRetriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	events func(types.RetrievalEvent),
) types.CandidateRetrieval {
	return &bitswapRetrieval{
		BitswapRetriever: br,
		bsGetter:         blockservice.NewSession(ctx, br.blockService),
		ctx:              ctx,
		request:          request,
		events:           events,
	}
}

type bitswapRetrieval struct {
	*BitswapRetriever
	bsGetter blockservice.BlockGetter
	ctx      context.Context
	request  types.RetrievalRequest
	events   func(types.RetrievalEvent)
}

// RetrieveFromCandidates retrieves via bitswap, with providers fed from the
// given asyncCandidates.
func (br *bitswapRetrieval) RetrieveFromAsyncCandidates(ayncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(br.ctx)
	defer cancelCtx()

	// Perform the retrieval in a goroutine and use retrievalShared{} to handle
	// event collection and dispatching on this goroutine; a bitswap traversal
	// can involve many, separate goroutines, all emitting events that need to
	// be collected and dispatched on this goroutine.
	shared := newRetrievalShared(nil)
	go br.runRetrieval(ctx, ayncCandidates, shared)
	return collectResults(ctx, shared, br.events)
}

func (br *bitswapRetrieval) runRetrieval(ctx context.Context, ayncCandidates types.InboundAsyncCandidates, shared *retrievalShared) {
	selector := br.request.GetSelector()
	startTime := br.clock.Now()
	// this is a hack cause we aren't able to track bitswap fetches per peer for now, so instead we just create a single peer for all events
	bitswapCandidate := types.NewRetrievalCandidate(peer.ID(""), nil, br.request.Root, metadata.Bitswap{})

	logger.Debugw("Starting bitswap retrieval", "retrievalID", br.request.RetrievalID, "root", br.request.Root)

	// setup the linksystem to record bytes & blocks written -- since this isn't automatic w/o go-data-transfer
	retrievalCtx, cancel := context.WithCancel(ctx)
	var lastBytesReceivedTimer *clock.Timer
	var doneLk sync.Mutex
	var timedOut bool
	if br.cfg.BlockTimeout != 0 {
		lastBytesReceivedTimer = br.clock.AfterFunc(br.cfg.BlockTimeout, func() {
			cancel()
			doneLk.Lock()
			timedOut = true
			doneLk.Unlock()
		})
	}

	totalWritten := atomic.Uint64{}
	blockCount := atomic.Uint64{}
	bytesWrittenCb := func(bytesWritten uint64) {
		// record first byte received
		if totalWritten.Load() == 0 {
			shared.sendEvent(ctx, events.FirstByte(br.clock.Now(), br.request.RetrievalID, bitswapCandidate, br.clock.Since(startTime), multicodec.TransportBitswap))
		}
		totalWritten.Add(bytesWritten)
		blockCount.Add(1)
		// reset the timer
		if bytesWritten > 0 && lastBytesReceivedTimer != nil {
			lastBytesReceivedTimer.Reset(br.cfg.BlockTimeout)
		}
	}

	// setup providers for this retrieval
	hasCandidates, nextCandidates, err := ayncCandidates.Next(retrievalCtx)
	if !hasCandidates || err != nil {
		cancel()
		// we never received any candidates, so we give up on bitswap retrieval
		logger.Debugw("No bitswap candidates received", "retrievalID", br.request.RetrievalID, "root", br.request.Root)
		shared.sendResult(ctx, retrievalResult{Err: ErrNoCandidates, AllFinished: true})
		return
	}

	shared.sendEvent(ctx, events.StartedRetrieval(br.clock.Now(), br.request.RetrievalID, bitswapCandidate, multicodec.TransportBitswap))

	// set initial providers, then start a goroutine to add more as they come in
	br.routing.AddProviders(br.request.RetrievalID, nextCandidates)
	logger.Debugf("Adding %d initial bitswap provider(s)", len(nextCandidates))
	go func() {
		for {
			hasCandidates, nextCandidates, err := ayncCandidates.Next(retrievalCtx)
			if !hasCandidates || err != nil {
				if br.awaitReceivedCandidates != nil {
					select {
					case <-ctx.Done():
					case br.awaitReceivedCandidates <- struct{}{}:
					}
				}
				return
			}
			br.routing.AddProviders(br.request.RetrievalID, nextCandidates)
			logger.Debugf("Adding %d more bitswap provider(s)", len(nextCandidates))
		}
	}()

	// set up the storage system, including the preloader if configured
	var preloader preload.Loader
	traversalLinkSys := br.request.LinkSystem

	loader := br.loader(ctx, shared)

	if br.request.HasPreloadLinkSystem() {
		var err error
		storage, err := bitswaphelpers.NewPreloadCachingStorage(
			br.request.LinkSystem,
			br.request.PreloadLinkSystem,
			loader,
			br.groupWorkPool.AddGroup(retrievalCtx),
		)
		if err != nil {
			cancel()
			shared.sendResult(ctx, retrievalResult{Err: err, AllFinished: true})
			return
		}
		preloader = storage.Preloader
		traversalLinkSys = *storage.TraversalLinkSystem

		br.bstore.AddLinkSystem(
			br.request.RetrievalID,
			bitswaphelpers.NewByteCountingLinkSystem(storage.BitswapLinkSystem, bytesWrittenCb),
		)
	} else {
		br.bstore.AddLinkSystem(
			br.request.RetrievalID,
			bitswaphelpers.NewByteCountingLinkSystem(&br.request.LinkSystem, bytesWrittenCb),
		)
		traversalLinkSys.StorageReadOpener = loader
	}

	// run the retrieval
	_, err = traversal.Config{
		Root:      br.request.Root,
		Selector:  selector,
		MaxBlocks: br.request.MaxBlocks,
	}.Traverse(retrievalCtx, traversalLinkSys, preloader)

	cancel()

	// unregister relevant provider records & LinkSystem
	br.routing.RemoveProviders(br.request.RetrievalID)
	br.bstore.RemoveLinkSystem(br.request.RetrievalID)
	if err != nil {
		logger.Debugw("Traversal/retrieval error, cleaning up", "err", err)
		// check for timeout on the local context
		// TODO: post 1.19 replace timedOut and doneLk with WithCancelCause and use DeadlineExceeded cause:
		// if errors.Is(retrievalCtx.Err(), context.Canceled) && errors.Is(context.Cause(retrievalCtx), context.DeadlineExceeded) {
		doneLk.Lock()
		if timedOut {
			// TODO: replace with %w: %w after 1.19
			err = multierr.Append(ErrRetrievalFailed,
				fmt.Errorf(
					"%w after %s",
					ErrRetrievalTimedOut,
					br.cfg.BlockTimeout,
				),
			)
		}
		doneLk.Unlock()
		// exclude the case where the context was cancelled by the parent, which likely means that
		// another protocol has succeeded.
		if br.ctx.Err() == nil {
			shared.sendEvent(ctx, events.FailedRetrieval(br.clock.Now(), br.request.RetrievalID, bitswapCandidate, multicodec.TransportBitswap, err.Error()))
			shared.sendResult(ctx, retrievalResult{Err: err, AllFinished: true})
		}
		return
	}
	duration := br.clock.Since(startTime)
	speed := uint64(float64(totalWritten.Load()) / duration.Seconds())

	logger.Debugw("Bitswap retrieval success", "retrievalID", br.request.RetrievalID, "root", br.request.Root, "duration", duration, "bytes", totalWritten.Load(), "blocks", blockCount.Load(), "speed", speed)

	// record success
	shared.sendEvent(ctx, events.Success(
		br.clock.Now(),
		br.request.RetrievalID,
		bitswapCandidate,
		totalWritten.Load(),
		blockCount.Load(),
		duration,
		multicodec.TransportBitswap,
	))

	shared.sendResult(ctx, retrievalResult{Stats: &types.RetrievalStats{
		StorageProviderId: peer.ID(""),
		RootCid:           br.request.Root,
		Size:              totalWritten.Load(),
		Blocks:            blockCount.Load(),
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
	}})
}

func (br *bitswapRetrieval) loader(ctx context.Context, shared *retrievalShared) func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
	return func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}
		br.inProgressCids.Inc(cidLink.Cid, br.request.RetrievalID)
		select {
		case <-lctx.Ctx.Done():
			return nil, lctx.Ctx.Err()
		default:
		}
		blk, err := br.blockService.GetBlock(lctx.Ctx, cidLink.Cid)
		br.inProgressCids.Dec(cidLink.Cid, br.request.RetrievalID)
		if err != nil {
			return nil, err
		}
		logger.Debugw("Got block from bitswap", "retrievalID", br.request.RetrievalID, "root", br.request.Root, "block", cidLink.Cid, "size", len(blk.RawData()))
		if traceableBlock, ok := blk.(traceability.Block); ok {
			shared.sendEvent(ctx, events.BlockReceived(
				br.clock.Now(),
				br.request.RetrievalID,
				types.RetrievalCandidate{RootCid: br.request.Root, MinerPeer: peer.AddrInfo{ID: traceableBlock.From}},
				multicodec.TransportBitswap,
				uint64(len(blk.RawData()))),
			)
		} else {
			logger.Warn("Got untraceable block from bitswap")
		}
		return bytes.NewReader(blk.RawData()), nil
	}
}
