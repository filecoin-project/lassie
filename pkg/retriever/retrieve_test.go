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
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/google/go-cmp/cmp"
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
			dqr := make(map[string]delayedQueryReturn, 0)
			for p, qr := range tc.queryResponses {
				dqr[p] = delayedQueryReturn{qr, nil, time.Millisecond * 50}
			}
			mockClient := &mockClient{returns_queries: dqr}
			mockInstrumentation := &mockInstrumentation{}
			candidates := []RetrievalCandidate{}
			for p := range tc.queryResponses {
				candidates = append(candidates, RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}
			mockEndpoint := &mockEndpoint{map[cid.Cid][]RetrievalCandidate{cid.Undef: candidates}}
			retriever := &Retriever{config: RetrieverConfig{PaidRetrievals: tc.paid}} // used for isAcceptableQueryResponse() only

			cfg := &RetrievalConfig{
				Instrumentation:             mockInstrumentation,
				GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
				IsAcceptableQueryResponse:   retriever.isAcceptableQueryResponse,
			}

			// perform retrieval and test top-level results, we should only error in this test
			stats, err := Retrieve(context.Background(), cfg, mockEndpoint, mockClient, cid.Undef)
			qt.Assert(t, stats, qt.IsNil)
			qt.Assert(t, err, qt.IsNotNil)

			// expected all queries
			qt.Assert(t, mockInstrumentation.errorQueryingRetrievalCandidate, qt.IsNil)
			qt.Assert(t, len(mockClient.received_queriedPeers), qt.Equals, len(tc.queryResponses))
			qt.Assert(t, len(mockInstrumentation.retrievalQueryForCandidate), qt.Equals, len(tc.queryResponses))
			for p, qr := range tc.queryResponses {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.received_queriedPeers, qt.Contains, pid)
				found := false
				for _, rqfc := range mockInstrumentation.retrievalQueryForCandidate {
					if rqfc.candidate.MinerPeer.ID == pid {
						found = true
						qt.Assert(t, rqfc.queryResponse, qt.CmpEquals(cmp.AllowUnexported(address.Address{}, mbig.Int{})), qr)
					}
				}
				qt.Assert(t, found, qt.IsTrue)
			}

			// verify that the list of retrievals matches the expected filtered list
			qt.Assert(t, len(mockClient.received_retrievedPeers), qt.Equals, len(tc.expectedPeers))
			qt.Assert(t, len(mockInstrumentation.filteredRetrievalQueryForCandidate), qt.Equals, len(tc.expectedPeers))
			qt.Assert(t, len(mockInstrumentation.retrievingFromCandidate), qt.Equals, len(tc.expectedPeers))
			qt.Assert(t, len(mockInstrumentation.errorRetrievingFromCandidate), qt.Equals, len(tc.expectedPeers)) // they all error!
			for _, p := range tc.expectedPeers {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, pid)
				qt.Assert(t, mockInstrumentation.retrievingFromCandidate, qt.Any(qt.CmpEquals(cmp.AllowUnexported(cid.Cid{}))), RetrievalCandidate{peer.AddrInfo{ID: pid}, cid.Undef})
				found := false
				for _, rqfc := range mockInstrumentation.filteredRetrievalQueryForCandidate {
					if rqfc.candidate.MinerPeer.ID == pid {
						found = true
						qr := tc.queryResponses[p]
						qt.Assert(t, rqfc.queryResponse, qt.CmpEquals(cmp.AllowUnexported(address.Address{}, mbig.Int{})), qr)
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
		queryReturns              map[string]delayedQueryReturn
		expectedQueryReturns      []string
		retrievalReturns          map[string]delayedRetrievalReturn
		expectedRetrievalAttempts []string
		expectedRetrieval         string
	}{
		{
			name: "single fast",
			queryReturns: map[string]delayedQueryReturn{
				"foo": {&successfulQueryResponse, nil, time.Millisecond * 20},
				"bar": {&successfulQueryResponse, nil, time.Millisecond * 200},
				"baz": {&successfulQueryResponse, nil, time.Millisecond * 200},
			},
			expectedQueryReturns: []string{"foo"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}}, time.Millisecond * 20},
				"bar": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}}, time.Millisecond * 200},
				"baz": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}}, time.Millisecond * 200},
			},
			expectedRetrievalAttempts: []string{"foo"},
			expectedRetrieval:         "foo",
		},
		{
			name: "all queries finished",
			queryReturns: map[string]delayedQueryReturn{
				"foo": {&successfulQueryResponse, nil, time.Millisecond * 20},
				"bar": {&successfulQueryResponse, nil, time.Millisecond * 50},
				"baz": {&successfulQueryResponse, nil, time.Millisecond * 50},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}}, time.Millisecond * 200},
				"bar": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}}, time.Millisecond * 200},
				"baz": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}}, time.Millisecond * 200},
			},
			expectedRetrievalAttempts: []string{"foo"},
			expectedRetrieval:         "foo",
		},
		{
			name: "all queries failed",
			queryReturns: map[string]delayedQueryReturn{
				"foo": {nil, errors.New("Nope"), time.Millisecond * 20},
				"bar": {nil, errors.New("Nope"), time.Millisecond * 20},
				"baz": {nil, errors.New("Nope"), time.Millisecond * 20},
			},
			expectedQueryReturns: []string{},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}}, time.Millisecond},
				"bar": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}}, time.Millisecond},
				"baz": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}}, time.Millisecond},
			},
			expectedRetrievalAttempts: []string{},
		},
		{
			name: "first retrieval failed",
			queryReturns: map[string]delayedQueryReturn{
				"foo": {&successfulQueryResponse, nil, time.Millisecond * 20},
				"bar": {&successfulQueryResponse, nil, time.Millisecond * 50},
				"baz": {&successfulQueryResponse, nil, time.Millisecond * 200},
			},
			expectedQueryReturns: []string{"foo", "bar"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo": {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 20},
				"bar": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}}, time.Millisecond * 20},
				"baz": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}}, time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "bar"},
			expectedRetrieval:         "bar",
		},
		{
			name: "all retrievals failed",
			queryReturns: map[string]delayedQueryReturn{
				"foo": {&successfulQueryResponse, nil, time.Millisecond * 20},
				"bar": {&successfulQueryResponse, nil, time.Millisecond * 20},
				"baz": {&successfulQueryResponse, nil, time.Millisecond * 20},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo": {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 100},
				"bar": {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 100},
				"baz": {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 100},
			},
			expectedRetrievalAttempts: []string{"foo", "bar", "baz"},
		},
		// quickest query ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which is the smallest (they're all free)
		{
			name: "racing chooses best",
			queryReturns: map[string]delayedQueryReturn{
				"foo":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, nil, time.Millisecond * 20},
				"bar":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, nil, time.Millisecond * 40},
				"baz":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, nil, time.Millisecond * 40},
				"bang": {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 4, UnsealPrice: big.Zero()}, nil, time.Millisecond * 40},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo":  {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 100},
				"bar":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}}, time.Millisecond * 20},
				"baz":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}}, time.Millisecond * 20},
				"bang": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}}, time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "baz"},
			expectedRetrieval:         "baz",
		},
		// quickest query ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which in this case is the fastest to return
		// from query (they are all free and the same size)
		{
			name: "racing chooses fastest query",
			queryReturns: map[string]delayedQueryReturn{
				"foo":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, nil, time.Millisecond * 20},
				"bar":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, nil, time.Millisecond * 50},
				"baz":  {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, nil, time.Millisecond * 60},
				"bang": {&retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 3, UnsealPrice: big.Zero()}, nil, time.Millisecond * 40},
			},
			expectedQueryReturns: []string{"foo", "bar", "baz", "bang"},
			retrievalReturns: map[string]delayedRetrievalReturn{
				"foo":  {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 100},
				"bar":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}}, time.Millisecond * 20},
				"baz":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}}, time.Millisecond * 20},
				"bang": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}}, time.Millisecond * 20},
			},
			expectedRetrievalAttempts: []string{"foo", "bang"},
			expectedRetrieval:         "bang",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &mockClient{
				returns_queries:    tc.queryReturns,
				returns_retrievals: tc.retrievalReturns,
			}
			mockInstrumentation := &mockInstrumentation{}
			candidates := []RetrievalCandidate{}
			for p := range tc.queryReturns {
				candidates = append(candidates, RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(p)}})
			}
			mockEndpoint := &mockEndpoint{map[cid.Cid][]RetrievalCandidate{cid.Undef: candidates}}

			cfg := &RetrievalConfig{
				Instrumentation:             mockInstrumentation,
				GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
				IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
				IsAcceptableQueryResponse:   func(qr *retrievalmarket.QueryResponse) bool { return true },
			}

			// perform retrieval and make sure we got a result
			stats, err := Retrieve(context.Background(), cfg, mockEndpoint, mockClient, cid.Undef)
			if tc.expectedRetrieval != "" {
				qt.Assert(t, stats, qt.IsNotNil)
				qt.Assert(t, err, qt.IsNil)
				// make sure we got the final retrieval we wanted
				qt.Assert(t, stats, qt.Equals, tc.retrievalReturns[tc.expectedRetrieval].retrievalResult.RetrievalStats)
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
				if r.err != nil {
					expectedQueryFailures++
				}
			}
			qt.Assert(t, len(mockInstrumentation.errorQueryingRetrievalCandidate), qt.Equals, expectedQueryFailures)
			qt.Assert(t, len(mockClient.received_queriedPeers), qt.Equals, len(tc.queryReturns))
			for _, p := range tc.expectedQueryReturns {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.received_queriedPeers, qt.Contains, pid)
			}
			// make sure we only returned the queries we expected (may be a subset of the above if a retrieval was fast enough)
			qt.Assert(t, len(mockInstrumentation.retrievalQueryForCandidate), qt.Equals, len(tc.expectedQueryReturns))
			qt.Assert(t, len(mockInstrumentation.filteredRetrievalQueryForCandidate), qt.Equals, len(tc.expectedQueryReturns))

			// make sure we performed the retrievals we expected
			var expectedRetrievalFailures int
			for _, r := range tc.retrievalReturns {
				if r.retrievalResult.Err != nil {
					expectedRetrievalFailures++
				}
			}
			qt.Assert(t, len(mockInstrumentation.errorRetrievingFromCandidate), qt.Equals, expectedRetrievalFailures)
			qt.Assert(t, len(mockClient.received_retrievedPeers), qt.Equals, len(tc.expectedRetrievalAttempts))
			qt.Assert(t, len(mockInstrumentation.retrievingFromCandidate), qt.Equals, len(tc.expectedRetrievalAttempts))
			for _, p := range tc.expectedRetrievalAttempts {
				pid := peer.ID(p)
				qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, pid)
			}
		})
	}
}

// run two retrievals simultaneously on a single CidRetrieval
func TestMultipleRetrievals(t *testing.T) {
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	cid2 := cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")
	successfulQueryResponse := retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}

	mockClient := &mockClient{
		returns_queries: map[string]delayedQueryReturn{
			"foo":  {&successfulQueryResponse, nil, time.Millisecond * 20},
			"bar":  {&successfulQueryResponse, nil, time.Millisecond * 50},
			"baz":  {&successfulQueryResponse, nil, time.Millisecond * 200}, // should not finish this
			"bang": {&successfulQueryResponse, nil, time.Millisecond * 200}, // should not finish this
			"boom": {nil, errors.New("Nope"), time.Millisecond * 20},
			"bing": {&successfulQueryResponse, nil, time.Millisecond * 50},
		},
		returns_retrievals: map[string]delayedRetrievalReturn{
			"foo":  {RetrievalResult{Err: errors.New("Nope")}, time.Millisecond * 20},
			"bar":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}}, time.Millisecond * 100},
			"baz":  {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}}, time.Millisecond * 100},
			"bang": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 3}}, time.Millisecond * 100},
			"boom": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("boom"), Size: 3}}, time.Millisecond * 100},
			"bing": {RetrievalResult{RetrievalStats: &RetrievalStats{StorageProviderId: peer.ID("bing"), Size: 3}}, time.Millisecond * 100},
		},
	}
	mockInstrumentation := &mockInstrumentation{}
	mockEndpoint := &mockEndpoint{map[cid.Cid][]RetrievalCandidate{
		cid1: {
			{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}},
		},
		cid2: {
			{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}},
			{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}},
		},
	}}

	cfg := &RetrievalConfig{
		Instrumentation:             mockInstrumentation,
		GetStorageProviderTimeout:   func(peer peer.ID) time.Duration { return time.Second },
		IsAcceptableStorageProvider: func(peer peer.ID) bool { return true },
		IsAcceptableQueryResponse:   func(qr *retrievalmarket.QueryResponse) bool { return true },
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stats, err := Retrieve(context.Background(), cfg, mockEndpoint, mockClient, cid1)
		qt.Assert(t, stats, qt.IsNotNil)
		qt.Assert(t, err, qt.IsNil)
		// make sure we got the final retrieval we wanted
		qt.Assert(t, stats, qt.Equals, mockClient.returns_retrievals["bar"].retrievalResult.RetrievalStats)
		wg.Done()
	}()

	stats, err := Retrieve(context.Background(), cfg, mockEndpoint, mockClient, cid2)
	qt.Assert(t, stats, qt.IsNotNil)
	qt.Assert(t, err, qt.IsNil)
	// make sure we got the final retrieval we wanted
	qt.Assert(t, stats, qt.Equals, mockClient.returns_retrievals["bing"].retrievalResult.RetrievalStats)

	// both retrievals should be ~ 50+100ms

	waitStart := time.Now()
	cfg.wait() // internal goroutine cleanup
	qt.Assert(t, time.Since(waitStart) < time.Millisecond*20, qt.IsTrue, qt.Commentf("wait took %s", time.Since(waitStart)))
	wg.Wait() // make sure we're done with our own goroutine
	qt.Assert(t, time.Since(waitStart) < time.Millisecond*20, qt.IsTrue, qt.Commentf("wg wait took %s", time.Since(waitStart)))

	// make sure we handled the queries we expected
	qt.Assert(t, len(mockInstrumentation.errorQueryingRetrievalCandidate), qt.Equals, 1)
	qt.Assert(t, len(mockClient.received_queriedPeers), qt.Equals, 6)
	for _, p := range mockClient.received_queriedPeers {
		pid := peer.ID(p)
		qt.Assert(t, mockClient.received_queriedPeers, qt.Contains, pid)
	}
	// make sure we only returned the queries we expected, in this case 2 were too slow and 1 errored so we only get 4
	qt.Assert(t, len(mockInstrumentation.retrievalQueryForCandidate), qt.Equals, 3)
	qt.Assert(t, len(mockInstrumentation.filteredRetrievalQueryForCandidate), qt.Equals, 3)

	// make sure we performed the retrievals we expected
	qt.Assert(t, len(mockInstrumentation.errorRetrievingFromCandidate), qt.Equals, 1)
	qt.Assert(t, len(mockClient.received_retrievedPeers), qt.Equals, 3)
	qt.Assert(t, len(mockInstrumentation.retrievingFromCandidate), qt.Equals, 3)
	qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, peer.ID("foo")) // errored
	qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, peer.ID("bar"))
	qt.Assert(t, mockClient.received_retrievedPeers, qt.Contains, peer.ID("bing"))
}

var _ RetrievalClient = (*mockClient)(nil)
var _ Endpoint = (*mockEndpoint)(nil)
var _ BlockConfirmer = dummyBlockConfirmer
var _ Instrumentation = (*mockInstrumentation)(nil)
var testDealIdGen = shared.NewTimeCounter()

type delayedQueryReturn struct {
	queryResponse *retrievalmarket.QueryResponse
	err           error
	delay         time.Duration
}

type delayedRetrievalReturn struct {
	retrievalResult RetrievalResult
	delay           time.Duration
}

type mockClient struct {
	lk                      sync.Mutex
	received_queriedPeers   []peer.ID
	received_retrievedPeers []peer.ID

	returns_queries    map[string]delayedQueryReturn
	returns_retrievals map[string]delayedRetrievalReturn
}

func (dfc *mockClient) RetrievalProposalForAsk(ask *retrievalmarket.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*retrievalmarket.DealProposal, error) {
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

func (dfc *mockClient) RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	dfc.lk.Lock()
	dfc.received_queriedPeers = append(dfc.received_queriedPeers, minerPeer.ID)
	dfc.lk.Unlock()

	if dqr, ok := dfc.returns_queries[string(minerPeer.ID)]; ok {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-time.After(dqr.delay):
		}
		return dqr.queryResponse, dqr.err
	}
	return &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseUnavailable}, nil
}

func (dfc *mockClient) RetrieveContentFromPeerAsync(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
) (<-chan RetrievalResult, <-chan uint64, func()) {
	dfc.lk.Lock()
	dfc.received_retrievedPeers = append(dfc.received_retrievedPeers, peerID)
	dfc.lk.Unlock()
	resChan := make(chan RetrievalResult, 1)
	if drr, ok := dfc.returns_retrievals[string(peerID)]; ok {
		time.Sleep(drr.delay)
		resChan <- drr.retrievalResult
	} else {
		resChan <- RetrievalResult{RetrievalStats: nil, Err: errors.New("nope")}
	}
	return resChan, nil, func() {}
}

func (*mockClient) SubscribeToRetrievalEvents(subscriber eventpublisher.RetrievalSubscriber) {}

type mockEndpoint struct {
	candidates map[cid.Cid][]RetrievalCandidate
}

func (me *mockEndpoint) FindCandidates(ctx context.Context, cid cid.Cid) ([]RetrievalCandidate, error) {
	return me.candidates[cid], nil
}

func dummyBlockConfirmer(c cid.Cid) (bool, error) {
	return true, nil
}

type instrumentationCandidateError struct {
	candidate RetrievalCandidate
	err       error
}
type instrumentationCandidateQuery struct {
	candidate     RetrievalCandidate
	queryResponse *retrievalmarket.QueryResponse
}
type mockInstrumentation struct {
	lk                                 sync.Mutex
	foundCount                         *int
	filteredCount                      *int
	errorQueryingRetrievalCandidate    []instrumentationCandidateError
	errorRetrievingFromCandidate       []instrumentationCandidateError
	retrievalQueryForCandidate         []instrumentationCandidateQuery
	filteredRetrievalQueryForCandidate []instrumentationCandidateQuery
	retrievingFromCandidate            []RetrievalCandidate
}

func (mi *mockInstrumentation) OnRetrievalCandidatesFound(foundCount int) error {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	mi.foundCount = &foundCount
	return nil
}
func (mi *mockInstrumentation) OnRetrievalCandidatesFiltered(filteredCount int) error {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	mi.filteredCount = &filteredCount
	return nil
}
func (mi *mockInstrumentation) OnErrorQueryingRetrievalCandidate(candidate RetrievalCandidate, err error) {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	if mi.errorQueryingRetrievalCandidate == nil {
		mi.errorQueryingRetrievalCandidate = make([]instrumentationCandidateError, 0)
	}
	mi.errorQueryingRetrievalCandidate = append(mi.errorQueryingRetrievalCandidate, instrumentationCandidateError{candidate, err})
}
func (mi *mockInstrumentation) OnErrorRetrievingFromCandidate(candidate RetrievalCandidate, err error) {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	if mi.errorRetrievingFromCandidate == nil {
		mi.errorRetrievingFromCandidate = make([]instrumentationCandidateError, 0)
	}
	mi.errorRetrievingFromCandidate = append(mi.errorRetrievingFromCandidate, instrumentationCandidateError{candidate, err})
}
func (mi *mockInstrumentation) OnRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse) {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	if mi.retrievalQueryForCandidate == nil {
		mi.retrievalQueryForCandidate = make([]instrumentationCandidateQuery, 0)
	}
	mi.retrievalQueryForCandidate = append(mi.retrievalQueryForCandidate, instrumentationCandidateQuery{candidate, queryResponse})
}
func (mi *mockInstrumentation) OnFilteredRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse) {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	if mi.filteredRetrievalQueryForCandidate == nil {
		mi.filteredRetrievalQueryForCandidate = make([]instrumentationCandidateQuery, 0)
	}
	mi.filteredRetrievalQueryForCandidate = append(mi.filteredRetrievalQueryForCandidate, instrumentationCandidateQuery{candidate, queryResponse})
}
func (mi *mockInstrumentation) OnRetrievingFromCandidate(candidate RetrievalCandidate) {
	mi.lk.Lock()
	defer mi.lk.Unlock()
	if mi.retrievingFromCandidate == nil {
		mi.retrievingFromCandidate = make([]RetrievalCandidate, 0)
	}
	mi.retrievingFromCandidate = append(mi.retrievingFromCandidate, candidate)
}
