package retriever_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRetrieverStart(t *testing.T) {
	config := retriever.RetrieverConfig{}
	candidateFinder := &testutil.MockCandidateFinder{}
	client := &testutil.MockClient{}
	dummyBlockConfirmer := func(cid cid.Cid) (bool, error) { return true, nil }
	ret, err := retriever.NewRetriever(context.Background(), config, client, candidateFinder, dummyBlockConfirmer)
	require.NoError(t, err)

	// --- run ---
	result, err := ret.Retrieve(context.Background(), types.RetrievalID(uuid.New()), cid.MustParse("bafkqaalb"))
	require.ErrorIs(t, err, retriever.ErrRetrieverNotStarted)
	require.Nil(t, result)
}

func TestRetriever(t *testing.T) {
	cid1 := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	blacklistedPeer := peer.ID("blacklisted")
	config := retriever.RetrieverConfig{
		MinerBlacklist: map[peer.ID]bool{blacklistedPeer: true},
	}

	tc := []struct {
		name               string
		candidates         []types.RetrievalCandidate
		returns_queries    map[string]testutil.DelayedQueryReturn
		returns_retrievals map[string]testutil.DelayedRetrievalReturn
		expectedEvents     []testutil.CollectedEvent
	}{
		{
			name: "single candidate and successful retrieval",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 20},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              1,
					TotalPayment:      abi.NewStoragePower(0),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []testutil.CollectedEvent{
				{Name: "IndexerProgress", Cid: cid1, Stage: eventpublisher.StartedCode},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFoundCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFilteredCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalSuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
			},
		},

		{
			name: "two candidates, fast one wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
				string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerB): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerB,
					Size:              10,
					TotalPayment:      abi.NewStoragePower(0),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []testutil.CollectedEvent{
				{Name: "IndexerProgress", Cid: cid1, Stage: eventpublisher.StartedCode},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFoundCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFilteredCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalSuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerB}},
			},
		},

		{
			name: "blacklisted candidate",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1},
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(blacklistedPeer): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 1, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
				string(peerA):           {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              1,
					TotalPayment:      abi.NewStoragePower(0),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []testutil.CollectedEvent{
				{Name: "IndexerProgress", Cid: cid1, Stage: eventpublisher.StartedCode},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFoundCode, StorageProviderIds: []peer.ID{blacklistedPeer, peerA}},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFilteredCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalSuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
			},
		},

		{
			name: "two candidates, fast one fails query, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(peerA): {Err: errors.New("blip"), Delay: time.Millisecond * 5},
				string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerB): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              1,
					TotalPayment:      abi.NewStoragePower(0),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []testutil.CollectedEvent{
				{Name: "IndexerProgress", Cid: cid1, Stage: eventpublisher.StartedCode},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFoundCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFilteredCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QueryFailure", Cid: cid1, StorageProviderIds: []peer.ID{peerA}, ErrorStr: "query failed: blip"},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalSuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerB}},
			},
		},

		{
			name: "two candidates, fast one fails retrieval, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
				string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerB,
					Size:              10,
					TotalPayment:      abi.NewStoragePower(0),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
				string(peerB): {ResultStats: nil, ResultErr: errors.New("bork!"), Delay: time.Millisecond * 5},
			},
			expectedEvents: []testutil.CollectedEvent{
				{Name: "IndexerProgress", Cid: cid1, Stage: eventpublisher.StartedCode},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFoundCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "IndexerCandidates", Cid: cid1, Stage: eventpublisher.CandidatesFilteredCode, StorageProviderIds: []peer.ID{peerA, peerB}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "RetrievalFailure", Cid: cid1, StorageProviderIds: []peer.ID{peerB}, ErrorStr: "retrieval failed: bork!"},
				{Name: "QueryProgress", Cid: cid1, Stage: eventpublisher.ConnectedCode, StorageProviderIds: []peer.ID{peerB}},
				{Name: "QuerySuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.StartedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalProgress", Cid: cid1, Stage: eventpublisher.ProposedCode, StorageProviderIds: []peer.ID{peerA}},
				{Name: "RetrievalSuccess", Cid: cid1, StorageProviderIds: []peer.ID{peerA}},
			},
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			// --- setup ---
			candidateFinder := &testutil.MockCandidateFinder{Candidates: map[cid.Cid][]types.RetrievalCandidate{cid1: tc.candidates}}
			client := &testutil.MockClient{
				Returns_queries:    tc.returns_queries,
				Returns_retrievals: tc.returns_retrievals,
			}
			eventsListener := testutil.NewCollectingEventsListener()
			dummyBlockConfirmer := func(cid cid.Cid) (bool, error) { return true, nil }

			// --- create ---
			ret, err := retriever.NewRetriever(context.Background(), config, client, candidateFinder, dummyBlockConfirmer)
			require.NoError(t, err)
			ret.RegisterListener(eventsListener)

			// --- start ---
			select {
			case <-ret.Start():
			case <-time.After(time.Millisecond * 50):
				require.Fail(t, "timed out waiting for retriever to start")
			}

			// --- retrieve ---
			retrievalId, err := types.NewRetrievalID()
			require.NoError(t, err)
			result, err := ret.Retrieve(context.Background(), retrievalId, cid1)
			require.NoError(t, err)
			var successfulPeer string
			for p, retrievalReturns := range tc.returns_retrievals {
				if retrievalReturns.ResultStats != nil {
					successfulPeer = p
				}
			}
			require.Equal(t, client.Returns_retrievals[successfulPeer].ResultStats, result)

			// --- stop ---
			time.Sleep(time.Millisecond * 5) // sleep to allow events to flush
			select {
			case <-ret.Stop():
			case <-time.After(time.Millisecond * 50):
				require.Fail(t, "timed out waiting for retriever to stop")
			}

			// --- verify events ---
			if len(eventsListener.CollectedEvents) != len(tc.expectedEvents) {
				for _, event := range eventsListener.CollectedEvents {
					t.Logf("event: %+v", event)
				}
			}
			require.Len(t, eventsListener.CollectedEvents, len(tc.expectedEvents))
			for i, event := range tc.expectedEvents {
				// fill in some values that are easier here than in setup
				event.RetrievalId = retrievalId
				switch event.Name {
				case "QuerySuccess":
					event.QueryResponse = client.Returns_queries[string(event.StorageProviderIds[0])].QueryResponse
				case "RetrievalSuccess":
					event.ReceivedCids = client.Returns_retrievals[string(event.StorageProviderIds[0])].ResultStats.Blocks
					event.ReceivedSize = client.Returns_retrievals[string(event.StorageProviderIds[0])].ResultStats.Size
					event.Confirmed = true
				case "QueryProgress":
					// these events can come out of order, so we can't verify it in a specific position
					testutil.VerifyContainsCollectedEvent(t, eventsListener.CollectedEvents, event)
					continue
				}

				testutil.VerifyCollectedEvent(t, eventsListener.CollectedEvents[i], event)
			}
			testutil.VerifyCollectedEventTimings(t, eventsListener.CollectedEvents)
		})
	}
}
