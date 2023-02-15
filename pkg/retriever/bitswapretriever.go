package retriever

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-blockservice"
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

// BitswapRetriever uses bitswap to retrieve data
// BitswapRetriever retieves using a combination of a go-bitswap client specially configured per retrieval,
// underneath a blockservice and a go-fetcher Fetcher.
// Selectors are used to travers the dag to make sure the CARs for bitswap match graphsync
// Note: this is a tradeoff over go-merkledag for traversal, cause selector execution is slow. But the solution
// is to improve selector execution, not introduce unpredictable encoding.
type BitswapRetriever struct {
	bstore       MultiBlockstore
	routing      IndexerRouting
	blockService blockservice.BlockService
}

// NewBitswapRetrieverFromHost constructs a new bitswap retriever for the given libp2p host
func NewBitswapRetrieverFromHost(ctx context.Context, host host.Host) *BitswapRetriever {
	bstore := bitswaphelpers.NewMultiblockstore()
	routing := bitswaphelpers.NewIndexerRouting()
	bsnet := network.NewFromIpfsHost(host, routing)
	bitswap := client.New(ctx, bsnet, bstore)
	bsnet.Start(bitswap)
	bsrv := blockservice.New(bstore, bitswap)
	return NewBitswapRetrieverFromDeps(bsrv, routing, bstore)
}

// NewBitswapRetrieverFromDeps is primarily for testing, constructing behavior from direct dependencies
func NewBitswapRetrieverFromDeps(bsrv blockservice.BlockService, routing IndexerRouting, bstore MultiBlockstore) *BitswapRetriever {
	return &BitswapRetriever{
		bstore:       bstore,
		routing:      routing,
		blockService: bsrv,
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
	phaseStartTime := time.Now()
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
		totalWritten += bytesWritten
		blockCount++
	}
	// setup providers for this retrieval
	br.BitswapRetriever.routing.AddProviders(br.request.RetrievalID, candidates)
	// setup providers linksystem for this retrieval
	br.BitswapRetriever.bstore.AddLinkSystem(br.request.RetrievalID, bitswaphelpers.NewByteCountingLinkSystem(&br.request.LinkSystem, cb))

	// copy the link system
	wrappedLsys := br.request.LinkSystem
	// replace the opener with a blockservice wrapper (we still want any known adls + reifiers, hence the copy)
	wrappedLsys.StorageReadOpener = loaderForSession(br.bsGetter)
	// run the retrieval
	err := easyTraverse(br.ctx, cidlink.Link{br.request.Cid}, selectorparse.CommonSelector_ExploreAllRecursively, &wrappedLsys)

	// unregister relevant provider records & LinkSystem
	br.BitswapRetriever.routing.RemoveProviders(br.request.RetrievalID)
	br.BitswapRetriever.bstore.RemoveLinkSystem(br.request.RetrievalID)
	if err != nil {
		// record failure
		br.events(events.Failed(br.request.RetrievalID, phaseStartTime, types.RetrievalPhase, bitswapCandidate, err.Error()))
		return nil, err
	}
	duration := time.Since(phaseStartTime)
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
		Size:              totalWritten,
		Blocks:            blockCount,
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
	}, nil
}

func loaderForSession(bs blockservice.BlockGetter) linking.BlockReadOpener {
	return func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := bs.GetBlock(lctx.Ctx, cidLink.Cid)
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
