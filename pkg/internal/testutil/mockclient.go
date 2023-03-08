package testutil

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
)

type DelayedQueryReturn struct {
	QueryResponse *retrievaltypes.QueryResponse
	Err           error
	Delay         time.Duration
}

type DelayedRetrievalReturn struct {
	ResultStats *types.RetrievalStats
	ResultErr   error
	Delay       time.Duration
}

type RetrievalRequest struct {
	Peer     peer.ID
	Proposal *retrievaltypes.DealProposal
	Selector ipld.Node
}

type MockClient struct {
	lk                            sync.Mutex
	received_queriedPeers         []peer.ID
	received_retrievals           []RetrievalRequest
	received_retrievedLinkSystems []ipld.LinkSystem

	returns_queries    map[string]DelayedQueryReturn
	returns_retrievals map[string]DelayedRetrievalReturn
}

func NewMockClient(queryReturns map[string]DelayedQueryReturn, retrievalReturns map[string]DelayedRetrievalReturn) *MockClient {
	return &MockClient{
		returns_queries:    queryReturns,
		returns_retrievals: retrievalReturns,
	}
}

func (mc *MockClient) GetReceivedQueries() []peer.ID {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	return append([]peer.ID{}, mc.received_queriedPeers...)
}

func (mc *MockClient) GetReceivedRetrievalFrom(peer peer.ID) *RetrievalRequest {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	for _, r := range mc.received_retrievals {
		if r.Peer == peer {
			return &r
		}
	}
	return nil
}

func (mc *MockClient) GetReceivedRetrievals() []RetrievalRequest {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	return append([]RetrievalRequest{}, mc.received_retrievals...)
}

func (mc *MockClient) GetReceivedLinkSystems() []ipld.LinkSystem {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	return append([]ipld.LinkSystem{}, mc.received_retrievedLinkSystems...)
}

func (mc *MockClient) GetRetrievalReturns() map[string]DelayedRetrievalReturn {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	ret := make(map[string]DelayedRetrievalReturn, 0)
	for k, v := range mc.returns_retrievals {
		ret[k] = v
	}
	return ret
}

func (mc *MockClient) GetQueryReturns() map[string]DelayedQueryReturn {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	ret := make(map[string]DelayedQueryReturn, 0)
	for k, v := range mc.returns_queries {
		ret[k] = v
	}
	return ret
}

func (mc *MockClient) SetQueryReturns(queryReturns map[string]DelayedQueryReturn) {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	mc.returns_queries = queryReturns
}

func (mc *MockClient) SetRetrievalReturns(retrievalReturns map[string]DelayedRetrievalReturn) {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	mc.returns_retrievals = retrievalReturns
}

func (mc *MockClient) RetrievalQueryToPeer(
	ctx context.Context,
	minerPeer peer.AddrInfo,
	pcid cid.Cid,
	onConnected func(),
) (*retrievaltypes.QueryResponse, error) {

	mc.lk.Lock()
	mc.received_queriedPeers = append(mc.received_queriedPeers, minerPeer.ID)
	dqr, has := mc.returns_queries[string(minerPeer.ID)]
	mc.lk.Unlock()

	if has {
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
	return &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseUnavailable}, nil
}

func (mc *MockClient) RetrieveFromPeer(
	ctx context.Context,
	linkSystem ipld.LinkSystem,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievaltypes.DealProposal,
	selector ipld.Node,
	eventsCallback datatransfer.Subscriber,
	gracefulShutdownRequested <-chan struct{},
) (*types.RetrievalStats, error) {
	mc.lk.Lock()
	mc.received_retrievals = append(mc.received_retrievals, RetrievalRequest{
		Peer:     peerID,
		Proposal: proposal,
		Selector: selector,
	})
	mc.received_retrievedLinkSystems = append(mc.received_retrievedLinkSystems, linkSystem)
	drr, has := mc.returns_retrievals[string(peerID)]
	mc.lk.Unlock()

	if has {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-gracefulShutdownRequested:
			return nil, context.Canceled
		case <-time.After(drr.Delay):
		}
		eventsCallback(datatransfer.Event{Code: datatransfer.Open}, nil)
		if drr.ResultStats != nil {
			acceptedResponse := &retrievaltypes.DealResponse{
				Status: retrievaltypes.DealStatusAccepted,
			}
			acceptedResponseNode := retrievaltypes.BindnodeRegistry.TypeToNode(acceptedResponse)
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
	return datatransfer.TypedVoucher{Voucher: m.lastVoucherResult, Type: retrievaltypes.DealResponseType}
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
