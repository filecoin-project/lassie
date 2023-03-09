package retriever

import (
	"context"
	"errors"
	mbig "math/big"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestQueryFiltering(t *testing.T) {
	testCases := []struct {
		name           string
		paid           bool
		queryResponses map[string]*retrievaltypes.QueryResponse
		expectedPeers  []string
	}{
		{
			name: "PaidRetrievals: true",
			paid: true,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false",
			paid: false,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"baz"},
		},
		{
			name: "PaidRetrievals: true, /w only paid",
			paid: true,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, /w only paid",
			paid: false,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.NewInt(1), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{},
		},
		{
			name: "PaidRetrievals: true, w/ no paid",
			paid: true,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
		{
			name: "PaidRetrievals: false, w/ no paid",
			paid: false,
			queryResponses: map[string]*retrievaltypes.QueryResponse{
				"foo": {Message: "foo", Status: retrievaltypes.QueryResponseUnavailable},
				"bar": {Message: "bar", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
				"baz": {Message: "baz", Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()},
			},
			expectedPeers: []string{"bar", "baz"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			retrievalID := types.RetrievalID(uuid.New())
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
				GetStorageProviderTimeout: func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievaltypes.QueryResponse) bool {
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
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, func(event types.RetrievalEvent) {
				require.Equal(t, retrievalID, event.RetrievalId())
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
			}).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, candidates))
			require.Nil(t, stats)
			require.Error(t, err)

			// expected all queries
			require.Len(t, mockClient.GetReceivedQueries(), len(tc.queryResponses))
			require.Len(t, candidateQueries, len(tc.queryResponses))
			for p, qr := range tc.queryResponses {
				pid := peer.ID(p)
				require.Contains(t, mockClient.GetReceivedQueries(), pid)
				found := false
				for _, rqfc := range candidateQueries {
					if rqfc.peer == pid {
						found = true
						require.True(t, cmp.Equal(rqfc.queryResponse, *qr, cmp.AllowUnexported(address.Address{}, mbig.Int{})))
					}
				}
				require.True(t, found)
			}

			// verify that the list of retrievals matches the expected filtered list
			require.Len(t, mockClient.GetReceivedRetrievals(), len(tc.expectedPeers))
			require.Len(t, candidateQueriesFiltered, len(tc.expectedPeers))
			require.Len(t, retrievingPeers, len(tc.expectedPeers))
			for _, p := range tc.expectedPeers {
				pid := peer.ID(p)
				require.NotNil(t, mockClient.GetReceivedRetrievalFrom(pid))
				require.Contains(t, retrievingPeers, pid)
				found := false
				for _, rqfc := range candidateQueriesFiltered {
					if rqfc.peer == pid {
						found = true
						qr := tc.queryResponses[p]
						require.True(t, cmp.Equal(rqfc.queryResponse, *qr, cmp.AllowUnexported(address.Address{}, mbig.Int{})))
					}
				}
				require.True(t, found)
			}
		})
	}
}

func TestRetrievalRacing(t *testing.T) {
	successfulQueryResponse := retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

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
				"foo":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond},
				"bar":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
				"baz":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
				"bang": {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 4, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 200},
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
				"foo":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 0},
				"bar":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 220},
				"baz":  {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 200},
				"bang": {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]testutil.DelayedRetrievalReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 400},
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "bang"},
			expectedRetrieval:         "bang",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			retrievalID := types.RetrievalID(uuid.New())
			mockClient := testutil.NewMockClient(tc.queryReturns, tc.retrievalReturns)
			candidates := []types.RetrievalCandidate{}
			for p := range tc.queryReturns {
				candidates = append(candidates, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}
			cfg := &GraphSyncRetriever{
				GetStorageProviderTimeout: func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, qr *retrievaltypes.QueryResponse) bool { return true },
				Client:                    mockClient,
			}

			retrievingPeers := make([]peer.ID, 0)
			candidateQueries := make([]candidateQuery, 0)
			candidateQueriesFiltered := make([]candidateQuery, 0)

			// perform retrieval and make sure we got a result
			stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid.Undef,
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, func(event types.RetrievalEvent) {
				require.Equal(t, retrievalID, event.RetrievalId())
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
			}).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, candidates))
			if tc.expectedRetrieval != "" {
				require.NotNil(t, stats)
				require.NoError(t, err)
				// make sure we got the final retrieval we wanted
				require.Equal(t, tc.retrievalReturns[tc.expectedRetrieval].ResultStats, stats)
			} else {
				require.Nil(t, stats)
				require.Error(t, err)
			}

			// make sure we handled the queries we expected
			var expectedQueryFailures int
			for _, r := range tc.queryReturns {
				if r.Err != nil {
					expectedQueryFailures++
				}
			}
			require.Len(t, mockClient.GetReceivedQueries(), len(tc.queryReturns))
			for _, p := range tc.expectedQueryReturns {
				pid := peer.ID(p)
				require.Contains(t, mockClient.GetReceivedQueries(), pid)
			}
			// make sure we only returned the queries we expected (may be a subset of the above if a retrieval was fast enough)
			require.Len(t, candidateQueries, len(tc.expectedQueryReturns))
			require.Len(t, candidateQueriesFiltered, len(tc.expectedQueryReturns))

			// make sure we performed the retrievals we expected
			var expectedRetrievalFailures int
			for _, r := range tc.retrievalReturns {
				if r.ResultErr != nil {
					expectedRetrievalFailures++
				}
			}
			require.Len(t, mockClient.GetReceivedRetrievals(), len(tc.expectedRetrievalAttempts))
			require.Len(t, retrievingPeers, len(tc.expectedRetrievalAttempts))
			for _, p := range tc.expectedRetrievalAttempts {
				pid := peer.ID(p)
				rr := mockClient.GetReceivedRetrievalFrom(pid)
				require.NotNil(t, rr)
				require.Same(t, selectorparse.CommonSelector_ExploreAllRecursively, rr.Selector) // default selector
			}
		})
	}
}

// run two retrievals simultaneously on a single CidRetrieval
func TestMultipleRetrievals(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	cid2 := cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")
	successfulQueryResponse := retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedQueryReturn{
			// group a
			"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond},
			"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 100},
			"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			// group b
			"bang": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			"boom": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond},
			"bing": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 100},
		},
		map[string]testutil.DelayedRetrievalReturn{
			// group a
			"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond},
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 200},
			// group b
			"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 3}, Delay: time.Millisecond * 200},
			"boom": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("boom"), Size: 3}, Delay: time.Millisecond * 200},
			"bing": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bing"), Size: 3}, Delay: time.Millisecond * 200},
		},
	)

	cfg := &GraphSyncRetriever{
		GetStorageProviderTimeout: func(peer peer.ID) time.Duration { return time.Second },
		IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, qr *retrievaltypes.QueryResponse) bool { return true },
		Client:                    mockClient,
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
			RetrievalID: retrievalID,
			LinkSystem:  cidlink.DefaultLinkSystem(),
		}, evtCb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
			{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}},
		}))
		require.NoError(t, err)
		require.NotNil(t, stats)
		// make sure we got the final retrieval we wanted
		require.Equal(t, mockClient.GetRetrievalReturns()["bar"].ResultStats, stats)
		wg.Done()
	}()

	stats, err := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid2,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
	}, evtCb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}},
	}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bing"].ResultStats, stats)

	// both retrievals should be ~ 100+200ms

	waitStart := time.Now()
	wg.Wait() // make sure we're done with our own goroutine
	require.Less(t, time.Since(waitStart), time.Millisecond*100, "wg wait took %s", time.Since(waitStart))

	// make sure we handled the queries we expected
	require.Len(t, mockClient.GetReceivedQueries(), 6)
	for _, p := range mockClient.GetReceivedQueries() {
		pid := peer.ID(p)
		require.Contains(t, mockClient.GetReceivedQueries(), pid)
	}
	// make sure we only returned the queries we expected, in this case 2 were too slow and 1 errored so we only get 4
	require.Len(t, candidateQueries, 3)
	require.Len(t, candidateQueriesFiltered, 3)

	// make sure we performed the retrievals we expected
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("foo"))) // errored
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("bar")))
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("bing")))
	require.Contains(t, retrievingPeers, peer.ID("foo")) // errored
	require.Contains(t, retrievingPeers, peer.ID("bar"))
	require.Contains(t, retrievingPeers, peer.ID("bing"))
	require.Len(t, mockClient.GetReceivedRetrievals(), 3)
	require.Len(t, retrievingPeers, 3)
}

// Verify we can use a single retrieval multiple times with different candidates (so it can be used in the future with a stream)
func TestRetrievalReuse(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	successfulQueryResponse := retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedQueryReturn{
			// group a
			"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond},       // should attempt this
			"bar": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 100}, // should attempt this
			"baz": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			// group b
			"bang": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			"boom": {QueryResponse: nil, Err: errors.New("Nope"), Delay: time.Millisecond * 100},       // should not finish this
			"bing": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: time.Millisecond * 100}, // should attempt this
		},
		map[string]testutil.DelayedRetrievalReturn{
			// group a
			"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond},
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 200},
			// group b
			"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 3}, Delay: time.Millisecond * 200},
			"boom": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("boom"), Size: 3}, Delay: time.Millisecond * 200},
			"bing": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bing"), Size: 3}, Delay: time.Millisecond * 200},
		},
	)

	cfg := &GraphSyncRetriever{
		GetStorageProviderTimeout: func(peer peer.ID) time.Duration { return time.Second },
		IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, qr *retrievaltypes.QueryResponse) bool { return true },
		Client:                    mockClient,
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

	retrieval := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid1,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
	}, evtCb)

	stats, err := retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}},
	}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bar"].ResultStats, stats)

	stats, err = retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}},
		{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}},
	}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bing"].ResultStats, stats)

	// make sure we handled the queries we expected
	require.Len(t, mockClient.GetReceivedQueries(), 6)
	for _, p := range mockClient.GetReceivedQueries() {
		pid := peer.ID(p)
		require.Contains(t, mockClient.GetReceivedQueries(), pid)
	}
	// make sure we only returned the queries we expected, in this case 2 were too slow and 1 errored so we only get 4
	require.Len(t, candidateQueries, 3)
	require.Len(t, candidateQueriesFiltered, 3)

	// make sure we performed the retrievals we expected
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("foo"))) // errored
	require.Contains(t, retrievingPeers, peer.ID("foo"))                   // errored
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("bar")))
	require.Contains(t, retrievingPeers, peer.ID("bar")) // errored
	require.NotNil(t, mockClient.GetReceivedRetrievalFrom(peer.ID("bing")))
	require.Contains(t, retrievingPeers, peer.ID("bing")) // errored
	require.Len(t, mockClient.GetReceivedRetrievals(), 3)
	require.Len(t, retrievingPeers, 3)
}

func TestRetrievalSelector(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	successfulQueryResponse := retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedQueryReturn{"foo": {QueryResponse: &successfulQueryResponse, Err: nil, Delay: 0}},
		map[string]testutil.DelayedRetrievalReturn{"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: 0}},
	)

	cfg := &GraphSyncRetriever{
		GetStorageProviderTimeout: func(peer peer.ID) time.Duration { return time.Second },
		IsAcceptableQueryResponse: func(peer peer.ID, req types.RetrievalRequest, qr *retrievaltypes.QueryResponse) bool { return true },
		Client:                    mockClient,
	}

	selector := selectorparse.CommonSelector_MatchPoint

	retrieval := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid1,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
		Selector:    selector,
	}, nil)
	stats, err := retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, mockClient.GetRetrievalReturns()["foo"].ResultStats, stats)

	// make sure we performed the retrievals we expected
	rr := mockClient.GetReceivedRetrievalFrom(peer.ID("foo"))
	require.NotNil(t, rr)
	require.Same(t, selector, rr.Selector)
	require.Len(t, mockClient.GetReceivedRetrievals(), 1)
}

type candidateQuery struct {
	peer          peer.ID
	queryResponse retrievaltypes.QueryResponse
}

func MakeAsyncCandidates(t *testing.T, candidates []types.RetrievalCandidate) types.InboundAsyncCandidates {
	incoming, outgoing := types.MakeAsyncCandidates(len(candidates))
	for _, candidate := range candidates {
		err := outgoing.SendNext(context.Background(), []types.RetrievalCandidate{candidate})
		require.NoError(t, err)
	}
	close(outgoing)
	return incoming
}
