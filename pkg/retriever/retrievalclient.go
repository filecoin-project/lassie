package retriever

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type RetrievalClient interface {
	RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid) (*retrievalmarket.QueryResponse, error)

	RetrieveContentFromPeerAsync(
		ctx context.Context,
		peerID peer.ID,
		minerWallet address.Address,
		proposal *retrievalmarket.DealProposal,
	) (<-chan RetrievalResult, <-chan uint64, func())

	SubscribeToRetrievalEvents(subscriber eventpublisher.RetrievalSubscriber)
}

type RetrievalResult struct {
	*RetrievalStats
	Err error
}

type RetrievalStats struct {
	StorageProviderId peer.ID
	RootCid           cid.Cid
	Size              uint64
	Duration          time.Duration
	AverageSpeed      uint64
	TotalPayment      abi.TokenAmount
	NumPayments       int
	AskPrice          abi.TokenAmount

	// TODO: we should be able to get this if we hook into the graphsync event stream
	//TimeToFirstByte time.Duration
}

type RetrievalSubscriber interface {
	OnRetrievalEvent(eventpublisher.RetrievalEvent)
}
