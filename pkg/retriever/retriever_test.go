package retriever_test

import (
	"context"
	"errors"
	"testing"
	"time"

	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestRetrieverStart(t *testing.T) {
	config := session.Config{}
	candidateFinder := &testutil.MockCandidateFinder{}
	client := &testutil.MockClient{}
	session := session.NewSession(config, true)
	gsretriever := &retriever.GraphSyncRetriever{
		GetStorageProviderTimeout: session.GetStorageProviderTimeout,
		IsAcceptableQueryResponse: session.IsAcceptableQueryResponse,
		Client:                    client,
	}
	ret, err := retriever.NewRetriever(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
		multicodec.TransportGraphsyncFilecoinv1: gsretriever,
	})
	require.NoError(t, err)

	// --- run ---
	result, err := ret.Retrieve(context.Background(), types.RetrievalRequest{
		LinkSystem:  cidlink.DefaultLinkSystem(),
		RetrievalID: types.RetrievalID(uuid.New()),
		Cid:         cid.MustParse("bafkqaalb"),
	}, func(types.RetrievalEvent) {})
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
		setup              func(*session.Config)
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
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 20},
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
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 1, 2, 3*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},

		{
			name: "two candidates, fast one wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Second},
				string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
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
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, qst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 10, 11, 12*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},

		{
			name: "blacklisted candidate",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(blacklistedPeer): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 1, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
				string(peerA):           {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
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
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, cid1), types.NewRetrievalCandidate(peerA, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 1, 2, 3*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},
		{
			name: "two candidates, fast one fails query, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {Err: errors.New("blip"), Delay: time.Millisecond * 5},
				string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 50},
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
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Failed(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1), "query failed: blip"),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 1, 2, 3*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},

		{
			name: "two candidates, fast one fails retrieval, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 500},
				string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
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
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Failed(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1), "retrieval failed: bork!"),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerA, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerA, cid1), 10, 20, 30*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},

		{
			name: "two candidates, first times out retrieval",
			setup: func(rc *session.Config) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 200
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries: map[string]testutil.DelayedQueryReturn{
				string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond},
				string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 100},
			},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              10,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Second * 2},
				string(peerB): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerB,
					Size:              20,
					Blocks:            30,
					Duration:          40 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: 0},
			},
			successfulPeer: peerB,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.CandidatesFiltered(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, cid1), types.NewRetrievalCandidate(peerB, cid1)}),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Started(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerA, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1)),
				events.Connected(rid, qst, types.QueryPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.QueryAsked(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				events.QueryAskedFiltered(rid, qst, types.NewRetrievalCandidate(peerB, cid1), retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}),
				// delay of 200ms for peerA retrieval happens here, no datatransfer.Open from DT so no ProposedCode event for peerA
				events.Failed(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerA, cid1), "timeout after 200ms"),
				events.Started(rid, rst, types.RetrievalPhase, types.NewRetrievalCandidate(peerB, cid1)),
				events.Proposed(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Accepted(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.FirstByte(rid, rst, types.NewRetrievalCandidate(peerB, cid1)),
				events.Success(rid, rst, types.NewRetrievalCandidate(peerB, cid1), 20, 30, 40*time.Second, big.Zero()),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},
		{
			name: "no candidates",
			setup: func(rc *session.Config) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 100
			},
			candidates:         []types.RetrievalCandidate{},
			returns_queries:    map[string]testutil.DelayedQueryReturn{},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{},
			successfulPeer:     peer.ID(""),
			err:                retriever.ErrNoCandidates,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Failed(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},
		{
			name: "no acceptable candidates",
			setup: func(rc *session.Config) {
				rc.DefaultMinerConfig.RetrievalTimeout = time.Millisecond * 100
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_queries:    map[string]testutil.DelayedQueryReturn{},
			returns_retrievals: map[string]testutil.DelayedRetrievalReturn{},
			successfulPeer:     peer.ID(""),
			err:                retriever.ErrNoCandidates,
			expectedEvents: []types.RetrievalEvent{
				events.Started(rid, ist, types.FetchPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.Started(rid, ist, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}),
				events.CandidatesFound(rid, ist, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, cid1)}),
				events.Failed(rid, rst, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
				events.Finished(rid, rst, types.RetrievalCandidate{RootCid: cid1}),
			},
		},
	}

	for _, tc := range tc {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// --- setup ---
			candidateFinder := &testutil.MockCandidateFinder{Candidates: map[cid.Cid][]types.RetrievalCandidate{cid1: tc.candidates}}
			client := testutil.NewMockClient(tc.returns_queries, tc.returns_retrievals)
			subscriber := testutil.NewCollectingEventsListener()
			config := session.Config{
				ProviderBlockList: map[peer.ID]bool{blacklistedPeer: true},
			}
			if tc.setup != nil {
				tc.setup(&config)
			}
			session := session.NewSession(config, true)
			gsretriever := &retriever.GraphSyncRetriever{
				GetStorageProviderTimeout: session.GetStorageProviderTimeout,
				IsAcceptableQueryResponse: session.IsAcceptableQueryResponse,
				Client:                    client,
			}
			// --- create ---
			ret, err := retriever.NewRetriever(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
				multicodec.TransportGraphsyncFilecoinv1: gsretriever,
			})
			require.NoError(t, err)
			ret.RegisterSubscriber(subscriber.Collect)

			// --- start ---
			ret.Start()

			// --- retrieve ---
			require.NoError(t, err)
			result, err := ret.Retrieve(context.Background(), types.RetrievalRequest{
				LinkSystem:  cidlink.DefaultLinkSystem(),
				RetrievalID: rid,
				Cid:         cid1,
			}, func(types.RetrievalEvent) {})
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
			for _, event := range tc.expectedEvents {
				testutil.VerifyContainsCollectedEvent(t, subscriber.CollectedEvents, event)
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
		{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
		{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
	}
	returnsQueries := map[string]testutil.DelayedQueryReturn{
		string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
		string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 500},
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
	config := session.Config{}
	session := session.NewSession(config, true)
	gsretriever := &retriever.GraphSyncRetriever{
		GetStorageProviderTimeout: session.GetStorageProviderTimeout,
		IsAcceptableQueryResponse: session.IsAcceptableQueryResponse,
		Client:                    client,
	}
	// --- create ---
	ret, err := retriever.NewRetriever(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
		multicodec.TransportGraphsyncFilecoinv1: gsretriever,
	})
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
	result, err := ret.Retrieve(context.Background(), types.RetrievalRequest{
		LinkSystem:  lsA,
		RetrievalID: rid,
		Cid:         cid1,
	}, func(types.RetrievalEvent) {})
	require.NoError(t, err)
	require.Equal(t, returnsRetrievals[string(peerA)].ResultStats, result)

	// switch them around
	returnsQueries = map[string]testutil.DelayedQueryReturn{
		string(peerA): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 500},
		string(peerB): {QueryResponse: &retrievaltypes.QueryResponse{Status: retrievaltypes.QueryResponseAvailable, MinPricePerByte: big.Zero(), Size: 2, UnsealPrice: big.Zero()}, Err: nil, Delay: time.Millisecond * 5},
	}
	client.SetQueryReturns(returnsQueries)

	// --- retrieve ---
	result, err = ret.Retrieve(context.Background(), types.RetrievalRequest{
		LinkSystem:  lsB,
		RetrievalID: rid,
		Cid:         cid1,
	}, func(types.RetrievalEvent) {})
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
