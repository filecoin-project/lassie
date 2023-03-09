package retriever

import (
	"context"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
)

type RetrievalClient interface {
	RetrievalQueryToPeer(
		ctx context.Context,
		minerPeer peer.AddrInfo,
		cid cid.Cid,
		onConnected func(),
	) (*retrievaltypes.QueryResponse, error)

	RetrieveFromPeer(
		ctx context.Context,
		linkSystem ipld.LinkSystem,
		peerID peer.ID,
		minerWallet address.Address,
		proposal *retrievaltypes.DealProposal,
		selector ipld.Node,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*types.RetrievalStats, error)
}
