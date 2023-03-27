package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type DelayedConnectReturn struct {
	Err   error
	Delay time.Duration
}

type DelayedClientReturn struct {
	ResultStats *types.RetrievalStats
	ResultErr   error
	Delay       time.Duration
}

type ClientRetrievalRequest struct {
	Peer     peer.ID
	Proposal *retrievaltypes.DealProposal
	Selector ipld.Node
}

type MockClient struct {
	lk                            sync.Mutex
	received_connections          chan peer.ID
	received_retrievals           chan ClientRetrievalRequest
	received_retrievedLinkSystems []ipld.LinkSystem

	returns_connections map[string]DelayedConnectReturn
	returns_retrievals  map[string]DelayedClientReturn
	clock               clock.Clock
}

func NewMockClient(connectReturns map[string]DelayedConnectReturn, retrievalReturns map[string]DelayedClientReturn, clock clock.Clock) *MockClient {
	return &MockClient{
		returns_retrievals:   retrievalReturns,
		returns_connections:  connectReturns,
		clock:                clock,
		received_connections: make(chan peer.ID, 16),
		received_retrievals:  make(chan ClientRetrievalRequest, 16),
	}
}

func (mc *MockClient) VerifyConnectionsReceived(ctx context.Context, t *testing.T, expectedConnections []peer.ID) {
	connections := make([]peer.ID, 0, len(expectedConnections))
	for i := 0; i < len(expectedConnections); i++ {
		select {
		case connection := <-mc.received_connections:
			t.Logf("connecting to peer: %s", connection)
			connections = append(connections, connection)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected connections", "expected %d, received %d", len(expectedConnections), i)
		}
	}
	require.ElementsMatch(t, expectedConnections, connections)
}

func (mc *MockClient) VerifyRetrievalsReceived(ctx context.Context, t *testing.T, expectedRetrievals []peer.ID) {
	retrievals := make([]peer.ID, 0, len(expectedRetrievals))
	for i := 0; i < len(expectedRetrievals); i++ {
		select {
		case retrieval := <-mc.received_retrievals:
			retrievals = append(retrievals, retrieval.Peer)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected retrievals", "expected %d, received %d", len(expectedRetrievals), i)
		}
	}
	require.ElementsMatch(t, expectedRetrievals, retrievals)
}

func (mc *MockClient) VerifyReceivedRetrievalFrom(ctx context.Context, t *testing.T, p peer.ID) ClientRetrievalRequest {
	for {
		select {
		case retrieval := <-mc.received_retrievals:
			if retrieval.Peer == p {
				return retrieval
			}
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive retrieval from peer", "peer %s", p)
		}
	}
}

func (mc *MockClient) GetReceivedLinkSystems() []ipld.LinkSystem {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	return append([]ipld.LinkSystem{}, mc.received_retrievedLinkSystems...)
}

func (mc *MockClient) GetRetrievalReturns() map[string]DelayedClientReturn {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	ret := make(map[string]DelayedClientReturn, 0)
	for k, v := range mc.returns_retrievals {
		ret[k] = v
	}
	return ret
}

func (mc *MockClient) SetRetrievalReturns(retrievalReturns map[string]DelayedClientReturn) {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	mc.returns_retrievals = retrievalReturns
}

func (mc *MockClient) GetConnectReturns() map[string]DelayedConnectReturn {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	ret := make(map[string]DelayedConnectReturn, 0)
	for k, v := range mc.returns_connections {
		ret[k] = v
	}
	return ret
}

func (mc *MockClient) SetConnectReturns(connectReturns map[string]DelayedConnectReturn) {
	mc.lk.Lock()
	defer mc.lk.Unlock()
	mc.returns_connections = connectReturns
}

func (mc *MockClient) Connect(
	ctx context.Context,
	minerPeer peer.AddrInfo,
) error {

	mc.lk.Lock()
	dqr, has := mc.returns_connections[string(minerPeer.ID)]
	mc.lk.Unlock()
	var timer *clock.Timer
	if has {
		timer = mc.clock.Timer(dqr.Delay)
	}
	select {
	case mc.received_connections <- minerPeer.ID:
	case <-ctx.Done():
		return context.Canceled
	}

	if has {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-timer.C:
		}
		return dqr.Err
	}
	return nil
}

func (mc *MockClient) RetrieveFromPeer(
	ctx context.Context,
	linkSystem ipld.LinkSystem,
	peerID peer.ID,
	proposal *retrievaltypes.DealProposal,
	selector ipld.Node,
	eventsCallback datatransfer.Subscriber,
	gracefulShutdownRequested <-chan struct{},
) (*types.RetrievalStats, error) {
	mc.lk.Lock()
	mc.received_retrievedLinkSystems = append(mc.received_retrievedLinkSystems, linkSystem)
	drr, has := mc.returns_retrievals[string(peerID)]
	mc.lk.Unlock()
	var timer *clock.Timer
	if has {
		timer = mc.clock.Timer(drr.Delay)
	}
	select {
	case <-ctx.Done():
		return nil, context.Canceled
	case mc.received_retrievals <- ClientRetrievalRequest{
		Peer:     peerID,
		Proposal: proposal,
		Selector: selector,
	}:
	}
	if has {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-gracefulShutdownRequested:
			return nil, context.Canceled
		case <-timer.C:
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
