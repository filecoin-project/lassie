package retriever

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"

	qt "github.com/frankban/quicktest"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

func TestQueryFiltering(t *testing.T) {
	testCases := []struct {
		name           string
		paid           bool
		queryResponses map[string]retrievalmarket.QueryResponse
		expectedPeers  []string
	}{
		{
			name: "PaidRetrievals: true",
			paid: true,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false",
			paid: false,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"baz"},
		},
		{
			name: "PaidRetrievals: true, /w only paid",
			paid: true,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, /w only paid",
			paid: false,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{},
		},
		{
			name: "PaidRetrievals: true, w/ no paid",
			paid: true,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, w/ no paid",
			paid: false,
			queryResponses: map[string]retrievalmarket.QueryResponse{
				"foo": {Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockClient{returns_queryResponses: tc.queryResponses}
			r, err := NewRetriever(context.Background(), RetrieverConfig{PaidRetrievals: tc.paid}, mockClient, &DummyEndpoint{}, DummyBlockConfirmer)
			qt.Assert(t, err, qt.IsNil)

			candidates := []RetrievalCandidate{}
			for p := range tc.queryResponses {
				candidates = append(candidates, RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}

			// setup activeRetrievals to avoid an "unknown retrieval" error state
			id, err := r.activeRetrievals.New(cid.Undef, len(candidates), 1)
			qt.Assert(t, err, qt.IsNil)

			// run retrieval as if `candidates` was the list the indexer provided
			r.retrieveFromBestCandidate(context.Background(), id, cid.Undef, candidates)

			// expected all queries
			qt.Assert(t, len(mockClient.received_queriedPeers), qt.Equals, len(tc.queryResponses))
			for p := range tc.queryResponses {
				qt.Assert(t, mockClient.received_queriedPeers, qt.Contains, peer.ID(p))
			}

			// verify that the list of retrievals matches the expected filtered list
			qt.Assert(t, len(mockClient.received_retrievedPeers), qt.Equals, len(tc.expectedPeers))
			for _, p := range tc.expectedPeers {
				qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, peer.ID(p))
			}
		})
	}
}

var _ RetrievalClient = (*MockClient)(nil)
var _ Endpoint = (*DummyEndpoint)(nil)
var _ BlockConfirmer = DummyBlockConfirmer
var testDealIdGen = shared.NewTimeCounter()

type MockClient struct {
	lk                      sync.Mutex
	received_queriedPeers   []peer.ID
	received_retrievedPeers []peer.ID

	returns_queryResponses map[string]retrievalmarket.QueryResponse
}

func (mc *MockClient) RetrievalProposalForAsk(ask *retrievalmarket.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*retrievalmarket.DealProposal, error) {
	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_ExploreAllRecursively
	}

	params, err := retrievalmarket.NewParamsV1(
		ask.MinPricePerByte,
		ask.MaxPaymentInterval,
		ask.MaxPaymentIntervalIncrease,
		optionalSelector,
		nil,
		ask.UnsealPrice,
	)
	if err != nil {
		return nil, err
	}
	return &retrievalmarket.DealProposal{
		PayloadCID: c,
		ID:         retrievalmarket.DealID(testDealIdGen.Next()),
		Params:     params,
	}, nil
}

func (mc *MockClient) RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	mc.lk.Lock()
	mc.received_queriedPeers = append(mc.received_queriedPeers, minerPeer.ID)
	mc.lk.Unlock()

	if qr, ok := mc.returns_queryResponses[string(minerPeer.ID)]; ok {
		return &qr, nil
	}
	return &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseUnavailable}, nil
}

func (mc *MockClient) RetrieveContentFromPeerAsync(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
) (<-chan RetrievalResult, <-chan uint64, func()) {
	mc.lk.Lock()
	mc.received_retrievedPeers = append(mc.received_retrievedPeers, peerID)
	mc.lk.Unlock()
	resChan := make(chan RetrievalResult, 1)
	resChan <- RetrievalResult{RetrievalStats: nil, Err: errors.New("nope")}
	return resChan, nil, func() {}
}

func (*MockClient) SubscribeToRetrievalEvents(subscriber eventpublisher.RetrievalSubscriber) {}

type DummyEndpoint struct{}

func (*DummyEndpoint) FindCandidates(context.Context, cid.Cid) ([]RetrievalCandidate, error) {
	return []RetrievalCandidate{}, nil
}

func DummyBlockConfirmer(c cid.Cid) (bool, error) {
	return true, nil
}
