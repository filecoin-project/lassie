package retriever

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/metrics"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers/preloadstore"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/bitswap/client"
	"github.com/ipfs/go-libipfs/bitswap/network"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opencensus.io/stats"
)

const DefaultConcurrency = 6

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
// BitswapRetriever retieves using a combination of a go-bitswap client specially configured per retrieval,
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
	// this is purely for testing purposes, to insure that we receive all candidates
	awaitReceivedCandidates chan<- struct{}
}

const shortenedDelay = 4 * time.Millisecond

// BitswapConfig contains configurable parameters for bitswap fetching
type BitswapConfig struct {
	BlockTimeout time.Duration
	TempDir      string
	Concurrency  int
}

// NewBitswapRetrieverFromHost constructs a new bitswap retriever for the given libp2p host
func NewBitswapRetrieverFromHost(ctx context.Context, host host.Host, cfg BitswapConfig) *BitswapRetriever {
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
	return NewBitswapRetrieverFromDeps(bsrv, routing, inProgressCids, bstore, cfg, clock.New(), nil)
}

// NewBitswapRetrieverFromDeps is primarily for testing, constructing behavior from direct dependencies
func NewBitswapRetrieverFromDeps(bsrv blockservice.BlockService, routing IndexerRouting, inProgressCids InProgressCids, bstore MultiBlockstore, cfg BitswapConfig, clock clock.Clock, awaitReceivedCandidates chan<- struct{}) *BitswapRetriever {
	return &BitswapRetriever{
		bstore:                  bstore,
		inProgressCids:          inProgressCids,
		routing:                 routing,
		blockService:            bsrv,
		clock:                   clock,
		cfg:                     cfg,
		awaitReceivedCandidates: awaitReceivedCandidates,
	}
}

// Retrieve initializes a new bitswap session
func (br *BitswapRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {
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

// RetrieveFromCandidates retrieves via go-bitswap backed with the given candidates, under the auspices of a fetcher.Fetcher
func (br *bitswapRetrieval) RetrieveFromAsyncCandidates(ayncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	selector := br.request.GetSelector()
	phaseStartTime := br.clock.Now()
	// this is a hack cause we aren't able to track bitswap fetches per peer for now, so instead we just create a single peer for all events
	bitswapCandidate := types.NewRetrievalCandidate(peer.ID(""), br.request.Cid, metadata.Bitswap{})

	// setup the linksystem to record bytes & blocks written -- since this isn't automatic w/o go-data-transfer
	ctx, cancel := context.WithCancel(br.ctx)
	var lastBytesReceivedTimer *clock.Timer
	if br.cfg.BlockTimeout != 0 {
		lastBytesReceivedTimer = br.clock.AfterFunc(br.cfg.BlockTimeout, cancel)
	}

	totalWritten := atomic.Uint64{}
	blockCount := atomic.Uint64{}
	cb := func(bytesWritten uint64) {
		// record first byte received
		if totalWritten.Load() == 0 {
			br.events(events.FirstByte(br.clock.Now(), br.request.RetrievalID, phaseStartTime, bitswapCandidate))
		}
		totalWritten.Add(bytesWritten)
		blockCount.Add(1)
		// reset the timer
		if bytesWritten > 0 && lastBytesReceivedTimer != nil {
			lastBytesReceivedTimer.Reset(br.cfg.BlockTimeout)
		}
	}

	// setup providers for this retrieval
	hasCandidates, nextCandidates, err := ayncCandidates.Next(ctx)
	if !hasCandidates || err != nil {
		cancel()
		// we never received any candidates, so we give up on bitswap retrieval
		return nil, nil
	}

	br.events(events.Started(br.clock.Now(), br.request.RetrievalID, phaseStartTime, types.RetrievalPhase, bitswapCandidate))

	// set initial providers, then start a goroutine to add more as they come in
	br.routing.AddProviders(br.request.RetrievalID, nextCandidates)
	go func() {
		for {
			hasCandidates, nextCandidates, err := ayncCandidates.Next(ctx)
			if !hasCandidates || err != nil {
				if br.awaitReceivedCandidates != nil {
					select {
					case <-br.ctx.Done():
					case br.awaitReceivedCandidates <- struct{}{}:
					}
				}
				return
			}
			br.routing.AddProviders(br.request.RetrievalID, nextCandidates)
		}
	}()

	// loader that can work with bitswap just for this session
	loader := loaderForSession(br.request.RetrievalID, br.inProgressCids, br.bsGetter)

	// set up the storage system, including the preloader
	concurrency := br.BitswapRetriever.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	cacheLinkSys := br.request.PreloadLinkSystem
	if cacheLinkSys.StorageReadOpener == nil || cacheLinkSys.StorageWriteOpener == nil {
		// An uninitialised LinkSystem, i.e. likely the request didn't provide a
		// PreloadLinkSystem. So we'll create a new one. This will result in a new
		// temporary file being created for the retrieval.
		tmpDir := br.BitswapRetriever.cfg.TempDir
		if tmpDir == "" {
			tmpDir = os.TempDir()
		}
		cacheStore := storage.NewDeferredStorageCar(tmpDir)
		cacheLinkSys = cidlink.DefaultLinkSystem()
		cacheLinkSys.SetReadStorage(cacheStore)
		cacheLinkSys.SetWriteStorage(cacheStore)
		cacheLinkSys.TrustedStorage = true
		defer cacheStore.Close()
	}
	opts := []preloadstore.Option{}
	if br.request.MaxBlocks > 0 {
		opts = append(opts, preloadstore.WithMaxBlocks(br.request.MaxBlocks-1))
	}

	storage, err := preloadstore.NewPreloadCachingStorage(
		br.request.LinkSystem,
		cacheLinkSys,
		loader,
		concurrency,
		opts...,
	)
	if err == nil {
		err = storage.Start(ctx)
	}
	if err != nil {
		cancel()
		return nil, err
	}

	// setup providers linksystem for this retrieval
	br.bstore.AddLinkSystem(
		br.request.RetrievalID,
		bitswaphelpers.NewByteCountingLinkSystem(storage.BitswapLinkSystem, cb),
	)

	// run the retrieval
	err = easyTraverse(
		ctx,
		cidlink.Link{Cid: br.request.Cid},
		selector,
		storage.TraversalLinkSystem,
		storage.Preloader,
		br.request.MaxBlocks,
	)
	storage.Stop()
	cancel()

	// unregister relevant provider records & LinkSystem
	br.routing.RemoveProviders(br.request.RetrievalID)
	br.bstore.RemoveLinkSystem(br.request.RetrievalID)
	if err != nil {
		// record failure
		br.events(events.Failed(br.clock.Now(), br.request.RetrievalID, phaseStartTime, types.RetrievalPhase, bitswapCandidate, err.Error()))
		return nil, err
	}
	duration := br.clock.Since(phaseStartTime)
	speed := uint64(float64(totalWritten.Load()) / duration.Seconds())
	preloadedPercent := storage.GetStats().PreloadedPercent()

	// record success
	br.events(events.Success(
		br.clock.Now(),
		br.request.RetrievalID,
		phaseStartTime,
		bitswapCandidate,
		totalWritten.Load(),
		blockCount.Load(),
		duration,
		big.Zero(),
		preloadedPercent,
	))

	stats.Record(ctx, metrics.BitswapPreloadedPercent.M(int64(preloadedPercent)))

	// return stats
	return &types.RetrievalStats{
		StorageProviderId: peer.ID(""),
		RootCid:           br.request.Cid,
		Size:              totalWritten.Load(),
		Blocks:            blockCount.Load(),
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
	}, nil
}

func loaderForSession(retrievalID types.RetrievalID, inProgressCids InProgressCids, bs blockservice.BlockGetter) linking.BlockReadOpener {
	return func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}
		inProgressCids.Inc(cidLink.Cid, retrievalID)
		select {
		case <-lctx.Ctx.Done():
			return nil, lctx.Ctx.Err()
		default:
		}
		blk, err := bs.GetBlock(lctx.Ctx, cidLink.Cid)
		inProgressCids.Dec(cidLink.Cid, retrievalID)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
}

func easyTraverse(
	ctx context.Context,
	root datamodel.Link,
	traverseSelector datamodel.Node,
	lsys *linking.LinkSystem,
	preloader preload.Loader,
	maxBlocks uint64,
) error {

	protoChooser := dagpb.AddSupportToChooser(basicnode.Chooser)

	// retrieve first node
	prototype, err := protoChooser(root, linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	node, err := lsys.Load(linking.LinkContext{Ctx: ctx}, root, prototype)
	if err != nil {
		return err
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     *lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
			Preloader:                      preloader,
		},
	}
	if maxBlocks > 0 {
		progress.Budget = &traversal.Budget{
			LinkBudget: int64(maxBlocks) - 1, // first block is already loaded
			NodeBudget: math.MaxInt64,
		}
	}
	progress.LastBlock.Link = root
	compiledSelector, err := selector.ParseSelector(traverseSelector)
	if err != nil {
		return err
	}
	return progress.WalkAdv(node, compiledSelector, func(prog traversal.Progress, n datamodel.Node, reason traversal.VisitReason) error { return nil })
}
