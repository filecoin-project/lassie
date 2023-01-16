package testutil

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type DelayedQueryReturn struct {
	QueryResponse *retrievalmarket.QueryResponse
	Err           error
	Delay         time.Duration
}

type DelayedRetrievalReturn struct {
	ResultStats *types.RetrievalStats
	ResultErr   error
	Delay       time.Duration
}

type MockClient struct {
	lk                      sync.Mutex
	Received_queriedPeers   []peer.ID
	Received_retrievedPeers []peer.ID

	Returns_queries    map[string]DelayedQueryReturn
	Returns_retrievals map[string]DelayedRetrievalReturn
}

func (dfc *MockClient) RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid, onConnected func()) (*retrievalmarket.QueryResponse, error) {
	dfc.lk.Lock()
	dfc.Received_queriedPeers = append(dfc.Received_queriedPeers, minerPeer.ID)
	dfc.lk.Unlock()

	if dqr, ok := dfc.Returns_queries[string(minerPeer.ID)]; ok {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-time.After(dqr.Delay):
		}
		if dqr.Err == nil {
			onConnected()
		}
		return dqr.QueryResponse, dqr.Err
	}
	return &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseUnavailable}, nil
}

func (dfc *MockClient) RetrieveFromPeer(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
	eventsCallback datatransfer.Subscriber,
	gracefulShutdownRequested <-chan struct{},
) (*types.RetrievalStats, error) {
	dfc.lk.Lock()
	dfc.Received_retrievedPeers = append(dfc.Received_retrievedPeers, peerID)
	dfc.lk.Unlock()
	if drr, ok := dfc.Returns_retrievals[string(peerID)]; ok {
		time.Sleep(drr.Delay)
		eventsCallback(datatransfer.Event{Code: datatransfer.Open}, nil)
		return drr.ResultStats, drr.ResultErr
	}
	return nil, errors.New("nope")
}

func (*MockClient) SubscribeToRetrievalEvents(subscriber eventpublisher.RetrievalSubscriber) {}
