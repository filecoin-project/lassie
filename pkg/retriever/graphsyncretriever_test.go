package retriever

import (
	"context"
	"errors"
	mbig "math/big"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"

	qt "github.com/frankban/quicktest"
)

func TestQueryFiltering(t *testing.T) {
	testCases := []struct {
		name           string
		paid           bool
		queryResponses map[string]*retrievalmarket.QueryResponse
		expectedPeers  []string
	}{
		{
			name: "PaidRetrievals: true",
			paid: true,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false",
			paid: false,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"baz"},
		},
		{
			name: "PaidRetrievals: true, /w only paid",
			paid: true,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, /w only paid",
			paid: false,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{},
		},
		{
			name: "PaidRetrievals: true, w/ no paid",
			paid: true,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, w/ no paid",
			paid: false,
			queryResponses: map[string]*retrievalmarket.QueryResponse{
				"foo": {Message: "foo", Status: retrievalmarket.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			retrievalId := types.RetrievalID(uuid.New())
			dqr := make(map[string]testutil.DelayedQueryReturn, 0)
			for p, qr := range tc.queryResponses {
				dqr[p] = testutil.DelayedQueryReturn{QueryResponse: qr, Err: nil, Delay: time.Millisecond * 50}
			}
			mockClient := testutil.NewMockClient(dqr, nil)
			var candidates []types.RetrievalCandidate
			for p := range tc.queryResponses {
				candidates = append(candidates, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}

			cfg := &GraphSyncRetriever{
				GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
				IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievalmarket.QueryResponse) bool {
					return tc.paid || big.Add(big.Mul(queryResponse.MinPricePerByte, big.NewIntUnsigned(queryResponse.Size)), queryResponse.UnsealPrice).Equals(big.Zero())
				},
				Client: mockClient,
			}

			retrievingPeers := make([]peer.ID, 0)
			candidateQueries := make([]candidateQuery, 0)
			candidateQueriesFiltered := make([]candidateQuery, 0)

			// perform retrieval and test top-level results, we should only error in this test
			stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid.Undef,
				RetrievalID: retrievalId,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, func(event types.RetrievalEvent) {
				qt.Assert(t, event.RetrievalId(), qt.Equals, retrievalId)
				switch ret := event.(type) {
				case events.RetrievalEventQueryAsked:
					candidateQueries = append(candidateQueries, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
				case events.RetrievalEventQueryAskedFiltered:
					candidateQueriesFiltered = append(candidateQueriesFiltered, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
				case events.RetrievalEventStarted:
					if ret.Phase() == types.RetrievalPhase {
						retrievingPeers = append(retrievingPeers, event.StorageProviderId())
					}
				}
			}).RetrieveFromCandidates(candidates)
			qt.Assert(t, stats, qt.IsNil)
			qt.Assert(t, err, qt.IsNotNil)

			// expected all queries
			qt.Assert(t, len(mockClient.GetReceivedQueries()), qt.Equals, len(tc.queryResponses))
			qt.Assert(t, len(candidateQueries), qt.Equals, len(tc.queryResponses))
			for p, qr := range tc.queryResponses {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.GetReceivedQueries(), qt.Contains, pid)
				found := false
				for _, rqfc := range candidateQueries {
					if rqfc.peer == pid {
						found = true
						qt.Assert(t, rqfc.queryResponse, qt.CmpEquals(cmp.AllowUnexported(address.Address{}, mbig.Int{})), *qr)
					}
				}
				qt.Assert(t, found, qt.IsTrue)
			}

			// verify that the list of retrievals matches the expected filtered list
			qt.Assert(t, len(mockClient.GetReceivedRetrievals()), qt.Equals, len(tc.expectedPeers))
			qt.Assert(t, len(candidateQueriesFiltered), qt.Equals, len(tc.expectedPeers))
			qt.Assert(t, len(retrievingPeers), qt.Equals, len(tc.expectedPeers))
			for _, p := range tc.expectedPeers {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.GetReceivedRetrievals(), qt.Contains, pid)
				qt.Assert(t, retrievingPeers, qt.Contains, pid)
				found := false
				for _, rqfc := range candidateQueriesFiltered {
					if rqfc.peer == pid {
						found = true
						qr := tc.queryResponses[p]
						qt.Assert(t, rqfc.queryResponse, qt.CmpEquals(cmp.AllowUnexported(address.Address{}, mbig.Int{})), *qr)
					}
				}
				qt.Assert(t, found, qt.IsTrue)
			}
		})
	}
}

func TestRetrievalRacing(t *testing.T) {
	successfulQueryResponse := retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	testCases := []struct {
		name                      string
		queryReturns              map[string]testutil.DelayedQueryReturn
		expectedQueryReturns      []string
		retrievalReturns          map[string]testutil.DelayedRetrievalReturn
		expectedRetrievalAttempts []string
		expectedRetrieval         string
	}{
		{
			name: "single fast",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
				"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500},
				"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500},
			},
			expectedQueryReturns: []string{"foo"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond * 20},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrievalAttempts: []string{"foo"},
			expectedRetrieval:         "foo",
		},
		{
			name: "all queries finished",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
				"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 50},
				"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 50},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond * 500},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrievalAttempts: []string{"foo"},
			expectedRetrieval:         "foo",
		},
		{
			name: "all queries failed",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond * 20},
				"bar": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond * 20},
				"baz": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond * 20},
			},
			expectedQueryReturns: []string{},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond},
			},
			expectedRetrievalAttempts: []string{},
		},
		{
			name: "first retrieval failed",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
				"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 50},
				"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500},
			},
			expectedQueryReturns: []string{"foo", "bar"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 20},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 20},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "bar"},
			expectedRetrieval:         "bar",
		},
		{
			name: "all retrievals failed",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
				"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
				"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"bar": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"baz": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
			},
			expectedRetrievalAttempts: []string{"foo", "bar", "baz"},
		},
		// quickest query ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which is the smallest (they're all free)
		{
			name: "racing chooses best",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 20},
				"bar":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 40},
				"baz":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 40},
				"bang": {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 4, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 40},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "baz"},
			expectedRetrieval:         "baz",
		},
		// quickest query ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which in this case is the fastest to return
		// from query (they are all free and the same size)
		{
			name: "racing chooses fastest query",
			queryReturns: map[string]testutil.DelayedQueryReturn{
				"foo":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 10},
				"bar":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 110},
				"baz":  {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
				"bang": {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "bang"},
			expectedRetrieval:         "bang",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			retrievalId := types.RetrievalID(uuid.New())
			mockClient := testutil.NewMockClient(tc.queryReturns, tc.retrievalReturns)
			candidates := []types.RetrievalCandidate{}
			for p := range tc.queryReturns {
				candidates = append(candidates, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}
			cfg := &GraphSyncRetriever{
				GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
				IsAcceptableQueryResponse:   func(peer peer.ID, req types.RetrievalRequest, qr *retrievalmarket.QueryResponse) bool { return true },
				Client:                      mockClient,
			}

			retrievingPeers := make([]peer.ID, 0)
			candidateQueries := make([]candidateQuery, 0)
			candidateQueriesFiltered := make([]candidateQuery, 0)

			// perform retrieval and make sure we got a result
			stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid.Undef,
				RetrievalID: retrievalId,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, func(event types.RetrievalEvent) {
				qt.Assert(t, event.RetrievalId(), qt.Equals, retrievalId)
				switch ret := event.(type) {
				case events.RetrievalEventQueryAsked:
					candidateQueries = append(candidateQueries, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
				case events.RetrievalEventQueryAskedFiltered:
					candidateQueriesFiltered = append(candidateQueriesFiltered, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
				case events.RetrievalEventStarted:
					if ret.Phase() == types.RetrievalPhase {
						retrievingPeers = append(retrievingPeers, event.StorageProviderId())
					}
				}
			}).RetrieveFromCandidates(candidates)
			if tc.expectedRetrieval != "" {
				qt.Assert(t, stats, qt.IsNotNil)
				qt.Assert(t, err, qt.IsNil)
				// make sure we got the final retrieval we wanted
				qt.Assert(t, stats, qt.Equals, tc.retrievalReturns[tc.expectedRetrieval].ResultStats)
			} else {
				qt.Assert(t, stats, qt.IsNil)
				qt.Assert(t, err, qt.IsNotNil)
			}
			waitStart := time.Now()
			cfg.wait()
			waited := time.Since(waitStart)
			// make sure we didn't have to wait long to have the goroutines cleaned up, they should
			// return very quickly from the mockClient#RetrievalQueryToPeer after a context cancel
			qt.Assert(t, waited < 5*time.Millisecond, qt.IsTrue, qt.Commentf("wait took %s", waited))

			// make sure we handled the queries we expected
			var expectedQueryFailures int
			for _, r := range tc.queryReturns {
				if r.Err != nil {
					expectedQueryFailures++
				}
			}
			qt.Assert(t, len(mockClient.GetReceivedQueries()), qt.Equals, len(tc.queryReturns))
			for _, p := range tc.expectedQueryReturns {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.GetReceivedQueries(), qt.Contains, pid)
			}
			// make sure we only returned the queries we expected (may be a subset of the above if a retrieval was fast enough)
			qt.Assert(t, len(candidateQueries), qt.Equals, len(tc.expectedQueryReturns))
			qt.Assert(t, len(candidateQueriesFiltered), qt.Equals, len(tc.expectedQueryReturns))

			// make sure we performed the retrievals we expected
			var expectedRetrievalFailures int
			for _, r := range tc.retrievalReturns {
				if r.ResultErr != nil {
					expectedRetrievalFailures++
				}
			}
			qt.Assert(t, len(mockClient.GetReceivedRetrievals()), qt.Equals, len(tc.expectedRetrievalAttempts))
			qt.Assert(t, len(retrievingPeers), qt.Equals, len(tc.expectedRetrievalAttempts))
			for _, p := range tc.expectedRetrievalAttempts {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.GetReceivedRetrievals(), qt.Contains, pid)
			}
		})
	}
}

// run two retrievals simultaneously on a single CidRetrieval
func TestMultipleRetrievals(t *testing.T) {
	retrievalId := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	cid2 := cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")
	successfulQueryResponse := retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedQueryReturn{
			"foo":  {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 20},
			"bar":  {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 50},
			"baz":  {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			"bang": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			"boom": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond * 20},
			"bing": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 50},
		},
		map[string]testutil.DelayedRetrievalReturn{
			"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 20},
			"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 100},
			"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 100},
			"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 3}, Delay: time.Millisecond * 100},
			"boom": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("boom"), Size: 3}, Delay: time.Millisecond * 100},
			"bing": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bing"), Size: 3}, Delay: time.Millisecond * 100},
		},
	)

	cfg := &GraphSyncRetriever{
		GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
		IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
		IsAcceptableQueryResponse:   func(peer peer.ID, req types.RetrievalRequest, qr *retrievalmarket.QueryResponse) bool { return true },
		Client:                      mockClient,
	}

	candidateQueries := make([]candidateQuery, 0)
	candidateQueriesFiltered := make([]candidateQuery, 0)
	retrievingPeers := make([]peer.ID, 0)
	var lk sync.Mutex
	evtCb := func(event types.RetrievalEvent) {
		switch ret := event.(type) {
		case events.RetrievalEventQueryAsked:
			lk.Lock()
			candidateQueries = append(candidateQueries, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
			lk.Unlock()
		case events.RetrievalEventQueryAskedFiltered:
			lk.Lock()
			candidateQueriesFiltered = append(candidateQueriesFiltered, candidateQuery{ret.StorageProviderId(), ret.QueryResponse()})
			lk.Unlock()
		case events.RetrievalEventStarted:
			if ret.Phase() == types.RetrievalPhase {
				lk.Lock()
				retrievingPeers = append(retrievingPeers, event.StorageProviderId())
				lk.Unlock()
			}
		}

	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
			Cid:         cid1,
			RetrievalID: retrievalId,
			LinkSystem:  cidlink.DefaultLinkSystem(),
		}, evtCb).RetrieveFromCandidates([]types.RetrievalCandidate{
			{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}},
		})
		qt.Assert(t, stats, qt.IsNotNil)
		qt.Assert(t, err, qt.IsNil)
		// make sure we got the final retrieval we wanted
		qt.Assert(t, stats, qt.Equals, mockClient.GetRetrievalReturns()["bar"].ResultStats)
		wg.Done()
	}()

	stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid2,
		RetrievalID: retrievalId,
		LinkSystem:  cidlink.DefaultLinkSystem(),
	}, evtCb).RetrieveFromCandidates([]types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}},
	})
	qt.Assert(t, stats, qt.IsNotNil)
	qt.Assert(t, err, qt.IsNil)
	// make sure we got the final retrieval we wanted
	qt.Assert(t, stats, qt.Equals, mockClient.GetRetrievalReturns()["bing"].ResultStats)

	// both retrievals should be ~ 50+100ms

	waitStart := time.Now()
	cfg.wait() // internal goroutine cleanup
	qt.Assert(t, time.Since(waitStart) < time.Millisecond*20, qt.IsTrue, qt.Commentf("wait took %s", time.Since(waitStart)))
	wg.Wait() // make sure we're done with our own goroutine
	qt.Assert(t, time.Since(waitStart) < time.Millisecond*20, qt.IsTrue, qt.Commentf("wg wait took %s", time.Since(waitStart)))

	// make sure we handled the queries we expected
	qt.Assert(t, len(mockClient.GetReceivedQueries()), qt.Equals, 6)
	for _, p := range mockClient.GetReceivedQueries() {
		pid := peer.ID(p)
		qt.Assert(t, mockClient.GetReceivedQueries(), qt.Contains, pid)
	}
	// make sure we only returned the queries we expected, in this case 2 were too slow and 1 errored so we only get 4
	qt.Assert(t, len(candidateQueries), qt.Equals, 3)
	qt.Assert(t, len(candidateQueriesFiltered), qt.Equals, 3)

	// make sure we performed the retrievals we expected
	qt.Assert(t, len(mockClient.GetReceivedRetrievals()), qt.Equals, 3)
	qt.Assert(t, len(retrievingPeers), qt.Equals, 3)
	qt.Assert(t, mockClient.GetReceivedRetrievals(), qt.Contains, peer.ID("foo")) // errored
	qt.Assert(t, mockClient.GetReceivedRetrievals(), qt.Contains, peer.ID("bar"))
	qt.Assert(t, mockClient.GetReceivedRetrievals(), qt.Contains, peer.ID("bing"))
}

type candidateQuery struct {
	peer          peer.ID
	queryResponse retrievalmarket.QueryResponse
}
