package retriever

import (
	"context"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
)

type RetrievalClient interface {
	Connect(ctx context.Context, peerAddr peer.AddrInfo) error
	RetrieveFromPeer(
		ctx context.Context,
		linkSystem ipld.LinkSystem,
		peerID peer.ID,
		proposal *retrievaltypes.DealProposal,
		selector ipld.Node,
		maxLinks uint64,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*types.RetrievalStats, error)
}
