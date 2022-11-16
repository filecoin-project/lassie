package filecoin

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	qt "github.com/frankban/quicktest"
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
			mockFilc := &MockFilClient{returns_queryResponses: tc.queryResponses}
			r, err := NewRetriever(context.Background(), RetrieverConfig{PaidRetrievals: tc.paid}, mockFilc, &DummyEndpoint{}, DummyBlockConfirmer)
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
			qt.Assert(t, len(mockFilc.received_queriedPeers), qt.Equals, len(tc.queryResponses))
			for p := range tc.queryResponses {
				qt.Assert(t, mockFilc.received_queriedPeers, qt.Contains, peer.ID(p))
			}

			// verify that the list of retrievals matches the expected filtered list
			qt.Assert(t, len(mockFilc.received_retrievedPeers), qt.Equals, len(tc.expectedPeers))
			for _, p := range tc.expectedPeers {
				qt.Assert(t, mockFilc.received_retrievedPeers, qt.Contains, peer.ID(p))
			}
		})
	}
}

var _ FilClient = (*MockFilClient)(nil)
var _ Endpoint = (*DummyEndpoint)(nil)
var _ BlockConfirmer = DummyBlockConfirmer

type MockFilClient struct {
	lk                      sync.Mutex
	received_queriedPeers   []peer.ID
	received_retrievedPeers []peer.ID

	returns_queryResponses map[string]retrievalmarket.QueryResponse
}

func (dfc *MockFilClient) RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	dfc.lk.Lock()
	dfc.received_queriedPeers = append(dfc.received_queriedPeers, minerPeer.ID)
	dfc.lk.Unlock()

	if qr, ok := dfc.returns_queryResponses[string(minerPeer.ID)]; ok {
		return &qr, nil
	}
	return &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseUnavailable}, nil
}

func (dfc *MockFilClient) RetrieveContentFromPeerAsync(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
) (<-chan filclient.RetrievalResult, <-chan uint64, func()) {
	dfc.lk.Lock()
	dfc.received_retrievedPeers = append(dfc.received_retrievedPeers, peerID)
	dfc.lk.Unlock()
	resChan := make(chan filclient.RetrievalResult, 1)
	resChan <- filclient.RetrievalResult{nil, errors.New("nope")}
	return resChan, nil, func() {}
}

func (*MockFilClient) SubscribeToRetrievalEvents(subscriber rep.RetrievalSubscriber) {}

type DummyEndpoint struct{}

func (*DummyEndpoint) FindCandidates(context.Context, cid.Cid) ([]RetrievalCandidate, error) {
	return []RetrievalCandidate{}, nil
}

func DummyBlockConfirmer(c cid.Cid) (bool, error) {
	return true, nil
}
