package retriever

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type RetrievalClient interface {
	RetrievalQueryToPeer(
		ctx context.Context,
		minerPeer peer.AddrInfo,
		cid cid.Cid,
		onConnected func(),
	) (*retrievalmarket.QueryResponse, error)

	RetrieveFromPeer(
		ctx context.Context,
		peerID peer.ID,
		minerWallet address.Address,
		proposal *retrievalmarket.DealProposal,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*RetrievalStats, error)
}

type RetrievalStats struct {
	StorageProviderId peer.ID
	RootCid           cid.Cid
	Size              uint64
	Blocks            uint64
	Duration          time.Duration
	AverageSpeed      uint64
	TotalPayment      abi.TokenAmount
	NumPayments       int
	AskPrice          abi.TokenAmount

	// TODO: we should be able to get this if we hook into the graphsync event stream
	//TimeToFirstByte time.Duration
}
