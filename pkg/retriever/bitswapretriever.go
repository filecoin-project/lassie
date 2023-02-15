package retriever

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/index-provider/metadata"
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
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
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
}

const shortenedDelay = 5 * time.Millisecond

// NewBitswapRetrieverFromHost constructs a new bitswap retriever for the given libp2p host
func NewBitswapRetrieverFromHost(ctx context.Context, host host.Host) *BitswapRetriever {
	bstore := bitswaphelpers.NewMultiblockstore()
	inProgressCids := bitswaphelpers.NewInProgressCids()
	routing := bitswaphelpers.NewIndexerRouting(inProgressCids.Get)
	bsnet := network.NewFromIpfsHost(host, routing)
	bitswap := client.New(ctx, bsnet, bstore, client.ProviderSearchDelay(shortenedDelay))
	bsnet.Start(bitswap)
	bsrv := blockservice.New(bstore, bitswap)
	return NewBitswapRetrieverFromDeps(bsrv, routing, inProgressCids, bstore, clock.New())
}

// NewBitswapRetrieverFromDeps is primarily for testing, constructing behavior from direct dependencies
func NewBitswapRetrieverFromDeps(bsrv blockservice.BlockService, routing IndexerRouting, inProgressCids InProgressCids, bstore MultiBlockstore, clock clock.Clock) *BitswapRetriever {
	return &BitswapRetriever{
		bstore:         bstore,
		inProgressCids: inProgressCids,
		routing:        routing,
		blockService:   bsrv,
		clock:          clock,
	}
}

// Retrieve initializes a new bitswap session
func (br *BitswapRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {
	return &bitswapRetrieval{br, blockservice.NewSession(ctx, br.blockService), ctx, request, events}
}

type bitswapRetrieval struct {
	*BitswapRetriever
	bsGetter blockservice.BlockGetter
	ctx      context.Context
	request  types.RetrievalRequest
	events   func(types.RetrievalEvent)
}

// RetrieveFromCandidates retrieves via go-bitswap backed with the given candidates, under the auspices of a fetcher.Fetcher
func (br *bitswapRetrieval) RetrieveFromCandidates(candidates []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	phaseStartTime := br.clock.Now()
	// this is a hack cause we aren't able to track bitswap fetches per peer for now, so instead we just create a single peer for all events
	bitswapCandidate := types.NewRetrievalCandidate(peer.ID(""), br.request.Cid, metadata.Bitswap{})
	br.events(events.Started(br.request.RetrievalID, phaseStartTime, types.RetrievalPhase, bitswapCandidate))

	// setup the linksystem to record bytes & blocks written -- since this isn't automatic w/o go-data-transfer
	totalWritten := uint64(0)
	blockCount := uint64(0)
	cb := func(bytesWritten uint64) {
		// record first byte received
		if totalWritten == 0 {
			br.events(events.FirstByte(br.request.RetrievalID, phaseStartTime, bitswapCandidate))
		}
		atomic.AddUint64(&totalWritten, bytesWritten)
		atomic.AddUint64(&blockCount, 1)
	}
	// setup providers for this retrieval
	br.routing.AddProviders(br.request.RetrievalID, candidates)
	// setup providers linksystem for this retrieval
	br.bstore.AddLinkSystem(br.request.RetrievalID, bitswaphelpers.NewByteCountingLinkSystem(&br.request.LinkSystem, cb))

	// copy the link system
	wrappedLsys := br.request.LinkSystem
	// replace the opener with a blockservice wrapper (we still want any known adls + reifiers, hence the copy)
	wrappedLsys.StorageReadOpener = loaderForSession(br.request.RetrievalID, br.inProgressCids, br.bsGetter)
	// run the retrieval
	err := easyTraverse(br.ctx, cidlink.Link{Cid: br.request.Cid}, selectorparse.CommonSelector_ExploreAllRecursively, &wrappedLsys)

	// unregister relevant provider records & LinkSystem
	br.routing.RemoveProviders(br.request.RetrievalID)
	br.bstore.RemoveLinkSystem(br.request.RetrievalID)
	if err != nil {
		// record failure
		br.events(events.Failed(br.request.RetrievalID, phaseStartTime, types.RetrievalPhase, bitswapCandidate, err.Error()))
		return nil, err
	}
	duration := br.clock.Since(phaseStartTime)
	speed := uint64(float64(totalWritten) / duration.Seconds())

	// record success
	br.events(events.Success(
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
		blk, err := bs.GetBlock(lctx.Ctx, cidLink.Cid)
		inProgressCids.Dec(cidLink.Cid, retrievalID)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}

func easyTraverse(ctx context.Context, root datamodel.Link, traverseSelector datamodel.Node, lsys *linking.LinkSystem) error {

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
		},
	}
	progress.LastBlock.Link = root
	compiledSelector, err := selector.ParseSelector(traverseSelector)
	if err != nil {
		return err
	}
	return progress.WalkAdv(node, compiledSelector, func(prog traversal.Progress, n datamodel.Node, reason traversal.VisitReason) error { return nil })
}
