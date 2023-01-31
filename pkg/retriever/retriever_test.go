package retriever_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRetrieverStart(t *testing.T) {
	config := retriever.RetrieverConfig{}
	candidateFinder := &testutil.MockCandidateFinder{}
	client := &testutil.MockClient{}
	ret, err := retriever.NewRetriever(context.Background(), config, client, candidateFinder)
	require.NoError(t, err)

	// --- run ---
	result, err := ret.Retrieve(context.Background(), cidlink.DefaultLinkSystem(), types.RetrievalID(uuid.New()), cid.MustParse("bafkqaalb"))
	require.ErrorIs(t, err, retriever.ErrRetrieverNotStarted)
	require.Nil(t, result)
}

func TestRetriever(t *testing.T) {
	rid := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	blacklistedPeer := peer.ID("blacklisted")
	ist := time.Now().Add(time.Second * -10)
	qst := time.Now().Add(time.Second * -5)
	rst := time.Now().Add(time.Second)

	tc := []struct {
		name               string
		setup              func(*retriever.RetrieverConfig)
		candidates         []types.RetrievalCandidate
		returns_queries    map[string]testutil.DelayedQueryReturn
		returns_retrievals map[string]testutil.DelayedRetrievalReturn
		successfulPeer     peer.ID
		err                error
		expectedEvents     []types.RetrievalEvent
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
					Blocks:            2,
					Duration:          3 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 1, 2, 3*time.Second, big.Zero()),
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
					Blocks:            11,
					Duration:          12 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, qst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 10, 11, 12*time.Second, big.Zero()),
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
					Blocks:            2,
					Duration:          3 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, cid1), types.NewRetrievalCandidate(peerA, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 1, 2, 3*time.Second, big.Zero()),
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
					Blocks:            2,
					Duration:          3 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Failed(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1), "query failed: blip"),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 1, 2, 3*time.Second, big.Zero()),
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
					StorageProviderId: peerA,
					Size:              10,
					Blocks:            20,
					Duration:          30 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
				string(peerB): {ResultStats: nil, ResultErr: errors.New("bork!"), Delay: time.Millisecond * 5},
			},
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Failed(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1), "retrieval failed: bork!"),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 10, 20, 30*time.Second, big.Zero()),
			},
		},

		{
			name: "two candidates, first times out retrieval",
			setup: func(rc *retriever.RetrieverConfig) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 100
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
				string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              10,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 500},
				string(peerB): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerB,
					Size:              20,
					Blocks:            30,
					Duration:          40 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Millisecond * 5},
			},
			successfulPeer: peerB,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				// delay of 200ms for peerA retrieval happens here, no datatransfer.Open from DT so no ProposedCode event for peerA
				events.Failed(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1), "timeout after 100ms"),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 20, 30, 40*time.Second, big.Zero()),
			},
		},
		{
			name: "no candidates",
			setup: func(rc *retriever.RetrieverConfig) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 100
			},
			candidates:         []types.RetrievalCandidate{},
			returns_queries:    map[string]testutil.DelayedQueryReturn{},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{},
			successfulPeer:     peer.ID(""),
			err:                retriever.ErrNoCandidates,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Failed(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
			},
		},
		{
			name: "no acceptable candidates",
			setup: func(rc *retriever.RetrieverConfig) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 100
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1},
			},
			returns_queries:    map[string]testutil.DelayedQueryReturn{},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{},
			successfulPeer:     peer.ID(""),
			err:                retriever.ErrNoCandidates,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, cid1)}),
				events.Failed(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
			},
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			// --- setup ---
			candidateFinder := &testutil.MockCandidateFinder{Candidates: map[cid.Cid][]types.RetrievalCandidate{cid1: tc.candidates}}
			client := testutil.NewMockClient(tc.returns_queries, tc.returns_retrievals)
			subscriber := testutil.NewCollectingEventsListener()
			config := retriever.RetrieverConfig{
				MinerBlacklist: map[peer.ID]bool{blacklistedPeer: true},
			}
			if tc.setup != nil {
				tc.setup(&config)
			}

			// --- create ---
			ret, err := retriever.NewRetriever(context.Background(), config, client, candidateFinder)
			require.NoError(t, err)
			ret.RegisterSubscriber(subscriber.Collect)

			// --- start ---
			ret.Start()

			// --- retrieve ---
			require.NoError(t, err)
			result, err := ret.Retrieve(context.Background(), cidlink.DefaultLinkSystem(), rid, cid1)
			if tc.err == nil {
				require.NoError(t, err)
				successfulPeer := string(tc.successfulPeer)
				if successfulPeer == "" {
					for p, retrievalReturns := range tc.returns_retrievals {
						if retrievalReturns.ResultStats != nil {
							successfulPeer = p
						}
					}
				}
				require.Equal(t, client.GetRetrievalReturns()[successfulPeer].ResultStats, result)
			} else {
				require.ErrorIs(t, tc.err, err)
			}

			// --- stop ---
			time.Sleep(time.Millisecond * 5) // sleep to allow events to flush
			select {
			case <-ret.Stop():
			case <-time.After(time.Millisecond * 50):
				require.Fail(t, "timed out waiting for retriever to stop")
			}

			// --- verify events ---
			if len(subscriber.CollectedEvents) != len(tc.expectedEvents) {
				for _, event := range subscriber.CollectedEvents {
					t.Logf("event: %+v", event)
				}
			}
			require.Len(t, subscriber.CollectedEvents, len(tc.expectedEvents))
			for i, event := range tc.expectedEvents {
				if (event.Code() == types.StartedCode || event.Code() == types.ConnectedCode) && event.Phase() == types.QueryPhase {
					// these events can come out of order, so we can't verify it in a specific position
					testutil.VerifyContainsCollectedEvent(t, subscriber.CollectedEvents, event)
					continue
				}
				testutil.VerifyCollectedEvent(t, subscriber.CollectedEvents[i], event)
			}
			testutil.VerifyCollectedEventTimings(t, subscriber.CollectedEvents)
		})
	}
}

func TestLinkSystemPerRequest(t *testing.T) {
	rid := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")

	candidates := []types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1},
		{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1},
	}
	returnsQueries := map[string]testutil.DelayedQueryReturn{
		string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
		string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
	}
	returnsRetrievals := map[string]testutil.DelayedRetrievalReturn{
		string(peerA): {ResultStats: &types.RetrievalStats{
			StorageProviderId: peerA,
			Size:              1,
			Blocks:            2,
			Duration:          3 * time.Second,
			TotalPayment:      big.Zero(),
			RootCid:           cid1,
			AskPrice:          abi.NewTokenAmount(0),
		}, Delay: time.Millisecond * 5},
		string(peerB): {ResultStats: &types.RetrievalStats{
			StorageProviderId: peerB,
			Size:              10,
			Blocks:            11,
			Duration:          12 * time.Second,
			TotalPayment:      big.Zero(),
			RootCid:           cid1,
			AskPrice:          abi.NewTokenAmount(0),
		}, Delay: time.Millisecond * 5},
	}

	candidateFinder := &testutil.MockCandidateFinder{Candidates: map[cid.Cid][]types.RetrievalCandidate{cid1: candidates}}
	client := testutil.NewMockClient(returnsQueries, returnsRetrievals)
	subscriber := testutil.NewCollectingEventsListener()
	config := retriever.RetrieverConfig{}

	// --- create ---
	ret, err := retriever.NewRetriever(context.Background(), config, client, candidateFinder)
	require.NoError(t, err)
	ret.RegisterSubscriber(subscriber.Collect)

	// --- start ---
	ret.Start()

	// --- retrieve ---
	lsA := cidlink.DefaultLinkSystem()
	lsA.NodeReifier = func(lc linking.LinkContext, n datamodel.Node, ls *linking.LinkSystem) (datamodel.Node, error) {
		return basicnode.NewString("linkSystem A"), nil
	}
	lsB := cidlink.DefaultLinkSystem()
	lsB.NodeReifier = func(lc linking.LinkContext, n datamodel.Node, ls *linking.LinkSystem) (datamodel.Node, error) {
		return basicnode.NewString("linkSystem B"), nil
	}
	result, err := ret.Retrieve(context.Background(), lsA, rid, cid1)
	require.NoError(t, err)
	require.Equal(t, returnsRetrievals[string(peerA)].ResultStats, result)

	// switch them around
	returnsQueries = map[string]testutil.DelayedQueryReturn{
		string(peerA): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
		string(peerB): {QueryResponse: &retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
	}
	client.SetQueryReturns(returnsQueries)

	// --- retrieve ---
	result, err = ret.Retrieve(context.Background(), lsB, rid, cid1)
	require.NoError(t, err)
	require.Equal(t, returnsRetrievals[string(peerB)].ResultStats, result)

	// --- stop ---
	time.Sleep(time.Millisecond * 5) // sleep to allow events to flush
	select {
	case <-ret.Stop():
	case <-time.After(time.Millisecond * 50):
		require.Fail(t, "timed out waiting for retriever to stop")
	}

	// --- verify ---
	// two different linksystems for the different calls, in the order that we
	// supplied them in our call to Retrieve()
	require.Len(t, client.GetReceivedLinkSystems(), 2)
	nd, err := client.GetReceivedLinkSystems()[0].NodeReifier(linking.LinkContext{}, nil, nil)
	require.NoError(t, err)
	str, err := nd.AsString()
	require.NoError(t, err)
	require.Equal(t, "linkSystem A", str)
	nd, err = client.GetReceivedLinkSystems()[1].NodeReifier(linking.LinkContext{}, nil, nil)
	require.NoError(t, err)
	str, err = nd.AsString()
	require.NoError(t, err)
	require.Equal(t, "linkSystem B", str)
}
