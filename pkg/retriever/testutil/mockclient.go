package testutil

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
)

var ch1 = datatransfer.ChannelID{
	Initiator: "initiator",
	Responder: "responder",
	ID:        1,
}

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
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-gracefulShutdownRequested:
			return nil, context.Canceled
		case <-time.After(drr.Delay):
		}
		eventsCallback(datatransfer.Event{Code: datatransfer.Open}, nil)
		if drr.ResultStats != nil {
			acceptedResponse := &retrievalmarket.DealResponse{
				Status: retrievalmarket.DealStatusAccepted,
			}
			acceptedResponseNode := retrievalmarket.BindnodeRegistry.TypeToNode(acceptedResponse)
			channelState := &mockChannelState{acceptedResponseNode, 100}
			eventsCallback(datatransfer.Event{Code: datatransfer.NewVoucherResult}, channelState)
			eventsCallback(datatransfer.Event{Code: datatransfer.DataReceivedProgress}, channelState)
		}
		return drr.ResultStats, drr.ResultErr
	}
	return nil, errors.New("nope")
}

type mockChannelState struct {
	lastVoucherResult datamodel.Node
	received          uint64
}

func (m *mockChannelState) TransferID() datatransfer.TransferID {
	panic("not implemented")
}
func (m *mockChannelState) BaseCID() cid.Cid {
	panic("not implemented")
}
func (m *mockChannelState) Selector() datamodel.Node {
	panic("not implemented")
}
func (m *mockChannelState) Voucher() datatransfer.TypedVoucher {
	panic("not implemented")
}
func (m *mockChannelState) Sender() peer.ID {
	panic("not implemented")
}
func (m *mockChannelState) Recipient() peer.ID {
	panic("not implemented")
}
func (m *mockChannelState) TotalSize() uint64 {
	panic("not implemented")
}
func (m *mockChannelState) IsPull() bool {
	panic("not implemented")
}
func (m *mockChannelState) ChannelID() datatransfer.ChannelID {
	panic("not implemented")
}
func (m *mockChannelState) OtherPeer() peer.ID {
	panic("not implemented")
}
func (m *mockChannelState) SelfPeer() peer.ID {
	panic("not implemented")
}
func (m *mockChannelState) Status() datatransfer.Status {
	panic("not implemented")
}
func (m *mockChannelState) Sent() uint64 {
	panic("not implemented")
}
func (m *mockChannelState) Received() uint64 {
	return m.received
}
func (m *mockChannelState) Message() string {
	panic("not implemented")
}
func (m *mockChannelState) Vouchers() []datatransfer.TypedVoucher {
	panic("not implemented")
}
func (m *mockChannelState) VoucherResults() []datatransfer.TypedVoucher {
	panic("not implemented")
}
func (m *mockChannelState) LastVoucher() datatransfer.TypedVoucher {
	panic("not implemented")
}
func (m *mockChannelState) LastVoucherResult() datatransfer.TypedVoucher {
	return datatransfer.TypedVoucher{Voucher: m.lastVoucherResult, Type: retrievalmarket.DealResponseType}
}
func (m *mockChannelState) ReceivedCidsTotal() int64 {
	panic("not implemented")
}
func (m *mockChannelState) QueuedCidsTotal() int64 {
	panic("not implemented")
}
func (m *mockChannelState) SentCidsTotal() int64 {
	panic("not implemented")
}
func (m *mockChannelState) Queued() uint64 {
	panic("not implemented")
}
func (m *mockChannelState) DataLimit() uint64 {
	panic("not implemented")
}
func (m *mockChannelState) RequiresFinalization() bool {
	panic("not implemented")
}
func (m *mockChannelState) InitiatorPaused() bool {
	panic("not implemented")
}
func (m *mockChannelState) ResponderPaused() bool {
	panic("not implemented")
}
func (m *mockChannelState) BothPaused() bool {
	panic("not implemented")
}
func (m *mockChannelState) SelfPaused() bool {
	panic("not implemented")
}
func (m *mockChannelState) Stages() *datatransfer.ChannelStages {
	panic("not implemented")
}
