package retriever

import (
	"context"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
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
	) (*types.QueryResponse, error)

	RetrieveFromPeer(
		ctx context.Context,
		linkSystem ipld.LinkSystem,
		peerID peer.ID,
		minerWallet address.Address,
		proposal *types.DealProposal,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*types.RetrievalStats, error)
}
