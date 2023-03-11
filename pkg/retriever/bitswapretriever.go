package retriever

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
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
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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

	totalWritten := uint64(0)
	blockCount := uint64(0)
	cb := func(bytesWritten uint64) {
		// record first byte received
		if totalWritten == 0 {
			br.events(events.FirstByte(br.clock.Now(), br.request.RetrievalID, phaseStartTime, bitswapCandidate))
		}
		atomic.AddUint64(&totalWritten, bytesWritten)
		atomic.AddUint64(&blockCount, 1)
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
	// setup providers linksystem for this retrieval
	br.bstore.AddLinkSystem(br.request.RetrievalID, bitswaphelpers.NewByteCountingLinkSystem(&br.request.LinkSystem, cb))

	loader := loaderForSession(br.request.RetrievalID, br.inProgressCids, br.bsGetter)
	// TODO: configurable tmpdir
	// TODO: configurable parallelism
	// TODO: move streamingstore down to the graphsync retriever so we are only
	// passed either a linking.BlockWriteOpener or a storage.WritableStorage
	// which generates the CARv1, letting bitswap and graphsync deal with the
	// custom caching they need. Currently we're wrapping streamingstore in
	// preloadstorage which means we have two temporary CARs, one of which is
	// never read from.
	storage, err := bitswaphelpers.NewPreloadStorage(loader, os.TempDir(), 5)
	if err != nil {
		cancel()
		return nil, err
	}
	storage.Start(ctx)

	// copy the link system
	wrappedLsys := br.request.LinkSystem
	// replace the opener with a blockservice wrapper (we still want any known adls + reifiers, hence the copy)
	wrappedLsys.StorageReadOpener = storage.Loader
	// run the retrieval
	err = easyTraverse(ctx, cidlink.Link{Cid: br.request.Cid}, selector, &wrappedLsys, storage.Preloader)
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
	speed := uint64(float64(totalWritten) / duration.Seconds())

	// record success
	br.events(events.Success(br.clock.Now(),
		br.request.RetrievalID,
		phaseStartTime,
		bitswapCandidate,
		totalWritten,
		blockCount,
		duration,
		big.Zero()))

	// return stats
	return &types.RetrievalStats{
		StorageProviderId: peer.ID(""),
		RootCid:           br.request.Cid,
		Size:              totalWritten,
		Blocks:            blockCount,
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
		fmt.Println("bs.GetBlock()", cidLink.Cid.String(), retrievalID)
		blk, err := bs.GetBlock(lctx.Ctx, cidLink.Cid)
		inProgressCids.Dec(cidLink.Cid, retrievalID)
		if err != nil {
			return nil, err
		}
		fmt.Println("bs.GetBlock() done", cidLink.Cid.String(), retrievalID)
		return bytes.NewReader(blk.RawData()), nil
	}
}

func easyTraverse(
	ctx context.Context,
	root datamodel.Link,
	traverseSelector datamodel.Node,
	lsys *linking.LinkSystem,
	preloader preload.Loader,
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
	progress.LastBlock.Link = root
	compiledSelector, err := selector.ParseSelector(traverseSelector)
	if err != nil {
		return err
	}
	return progress.WalkAdv(node, compiledSelector, func(prog traversal.Progress, n datamodel.Node, reason traversal.VisitReason) error { return nil })
}
