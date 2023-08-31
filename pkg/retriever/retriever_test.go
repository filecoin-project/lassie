package retriever

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestRetrieverStart(t *testing.T) {
	candidateFinder := &testutil.MockCandidateFinder{}
	client := &testutil.MockClient{}
	session := session.NewSession(nil, true)
	gsretriever := NewGraphsyncRetriever(session, client)
	ret, err := NewRetriever(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
		multicodec.TransportGraphsyncFilecoinv1: gsretriever,
	})
	require.NoError(t, err)

	// --- run ---
	result, err := ret.Retrieve(context.Background(), types.RetrievalRequest{
		LinkSystem:  cidlink.DefaultLinkSystem(),
		RetrievalID: types.RetrievalID(uuid.New()),
		Request:     trustlessutils.Request{Root: cid.MustParse("bafkqaalb")},
	}, func(types.RetrievalEvent) {})
	require.ErrorIs(t, err, ErrRetrieverNotStarted)
	require.Nil(t, result)
}

func TestRetriever(t *testing.T) {
	rid := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	initialPause := time.Millisecond * 5
	blacklistedPeer := peer.ID("blacklisted")
	startTime := time.Now().Add(time.Hour)
	tc := []struct {
		name               string
		setup              func(*testutil.MockSession)
		candidates         []types.RetrievalCandidate
		path               string
		dups               bool
		scope              trustlessutils.DagScope
		returns_connected  map[string]testutil.DelayedConnectReturn
		returns_retrievals map[string]testutil.DelayedClientReturn
		cancelAfter        time.Duration
		successfulPeer     peer.ID
		err                error
		expectedSequence   []testutil.ExpectedActionsAtTime
	}{
		{
			name: "single candidate and successful retrieval",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Millisecond * 20},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 20 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(20*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: 20 * time.Millisecond},
					},
				},
				{
					AfterStart:         20*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerA},
				},
				{
					AfterStart: 25*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.Accepted(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.FirstByte(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 1, 2, 3*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(25*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerA, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerA, Value: math.Trunc(1.0 / float64((3 * time.Second).Milliseconds()))},
					},
				},
			},
		},
		{
			name: "single candidate and successful retrieval, w/ path & dups & scope",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			path:  "some/path/to/request",
			dups:  true,
			scope: trustlessutils.DagScopeBlock,
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Millisecond * 20},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "/some/path/to/request?dag-scope=block&dups=y", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 20 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(20*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: 20 * time.Millisecond},
					},
				},
				{
					AfterStart:         20*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerA},
				},
				{
					AfterStart: 25*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.Accepted(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.FirstByte(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(25*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 1, 2, 3*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(25*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerA, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerA, Value: math.Trunc(1.0 / float64((3 * time.Second).Milliseconds()))},
					},
				},
			},
		},
		{
			name: "two candidates, fast one wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Second},
				string(peerB): {Err: nil, Delay: time.Millisecond * 5},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA, peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 5 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(5*time.Millisecond), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerB, Duration: 5 * time.Millisecond},
					},
				},
				{
					AfterStart:         5*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerB},
				},
				{
					AfterStart: 10*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.Accepted(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.FirstByte(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 10, 11, 12*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(10*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerB, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerB, Value: math.Trunc(10.0 / float64((12 * time.Second).Milliseconds()))},
					},
				},
			},
		},
		{
			name: "blacklisted candidate",
			setup: func(ms *testutil.MockSession) {
				ms.SetBlockList(map[peer.ID]bool{blacklistedPeer: true})
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				// fastest is blacklisted, shouldn't even touch it
				string(blacklistedPeer): {Err: nil, Delay: time.Millisecond * 5},
				string(peerA):           {Err: nil, Delay: time.Millisecond * 50},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, nil, cid1), types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 50 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(50*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: 50 * time.Millisecond},
					},
				},
				{
					AfterStart:         50*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerA},
				},
				{
					AfterStart: 55*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.Accepted(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.FirstByte(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 1, 2, 3*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(55*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerA, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerA, Value: math.Trunc(1.0 / float64((3 * time.Second).Milliseconds()))},
					},
				},
			},
		},

		{
			name: "two candidates, fast one fails connect, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: errors.New("blip"), Delay: time.Millisecond * 5},
				string(peerB): {Err: nil, Delay: time.Millisecond * 50},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA, peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 5 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.FailedRetrieval(startTime.Add(5*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1, "unable to connect to provider: blip"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerA},
					},
				},
				{
					AfterStart: 50 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(50*time.Millisecond), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerB, Duration: 50 * time.Millisecond},
					},
				},
				{
					AfterStart:         50*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerB},
				},
				{
					AfterStart: 55*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.Accepted(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.FirstByte(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(55*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 1, 2, 3*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(55*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerB, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerB, Value: math.Trunc(1.0 / float64((3 * time.Second).Milliseconds()))},
					},
				},
			},
		},

		{
			name: "two candidates, fast one fails retrieval, slow wins",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Millisecond * 500},
				string(peerB): {Err: nil, Delay: time.Millisecond * 5},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA, peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 5 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(5*time.Millisecond), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerB, Duration: 5 * time.Millisecond},
					},
				},
				{
					AfterStart:         5*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerB},
				},
				{
					AfterStart: 10*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.FailedRetrieval(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1, "retrieval failed: bork!"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerB},
					},
				},
				{
					AfterStart:         500 * time.Millisecond,
					ReceivedRetrievals: []peer.ID{peerA},
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(500*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: 500 * time.Millisecond},
					},
				},
				{
					AfterStart: 505 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(505*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.Accepted(startTime.Add(505*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
						events.FirstByte(startTime.Add(505*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(505*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 10, 20, 30*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(505*time.Millisecond), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerA, Duration: 5 * time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerA, Value: math.Trunc(10.0 / float64((30 * time.Second).Milliseconds()))},
					},
				},
			},
		},

		{
			name: "two candidates, first times out retrieval",
			setup: func(ms *testutil.MockSession) {
				ms.SetProviderTimeout(time.Millisecond * 200)
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Millisecond},
				string(peerB): {Err: nil, Delay: time.Millisecond * 100},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
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
				}, Delay: time.Millisecond},
			},
			successfulPeer: peerB,
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA, peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 1 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(1*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: time.Millisecond},
					},
				},
				{
					AfterStart:         1*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerA},
				},
				{
					AfterStart: 100 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(100*time.Millisecond), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerB, Duration: 100 * time.Millisecond},
					},
				},
				{
					AfterStart:         201*time.Millisecond + initialPause,
					ReceivedRetrievals: []peer.ID{peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.FailedRetrieval(startTime.Add(201*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1, "timeout after 200ms"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerA},
					},
				},
				{
					AfterStart: 202*time.Millisecond + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(202*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.Accepted(startTime.Add(202*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
						events.FirstByte(startTime.Add(202*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 1*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
						events.Success(startTime.Add(202*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 20, 30, 40*time.Second, multicodec.TransportGraphsyncFilecoinv1),
						events.Finished(startTime.Add(202*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerB, Duration: time.Millisecond},
						{Type: testutil.SessionMetric_Success, Provider: peerB, Value: math.Trunc(20.0 / float64((34 * time.Second).Milliseconds()))},
					},
				},
			},
		},

		{
			name: "no candidates",
			setup: func(ms *testutil.MockSession) {
				ms.SetProviderTimeout(time.Millisecond * 100)
			},
			candidates:         []types.RetrievalCandidate{},
			returns_connected:  map[string]testutil.DelayedConnectReturn{},
			returns_retrievals: map[string]testutil.DelayedClientReturn{},
			successfulPeer:     peer.ID(""),
			err:                ErrNoCandidates,
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:           0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.Failed(startTime, rid, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
						events.Finished(startTime, rid, types.RetrievalCandidate{RootCid: cid1}),
					},
				},
			},
		},
		{
			name: "no acceptable candidates",
			setup: func(ms *testutil.MockSession) {
				ms.SetProviderTimeout(time.Millisecond * 100)
				ms.SetBlockList(map[peer.ID]bool{blacklistedPeer: true})
			},
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected:  map[string]testutil.DelayedConnectReturn{},
			returns_retrievals: map[string]testutil.DelayedClientReturn{},
			successfulPeer:     peer.ID(""),
			err:                ErrNoCandidates,
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: blacklistedPeer}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(blacklistedPeer, nil, cid1)}),
						events.Failed(startTime, rid, types.RetrievalCandidate{RootCid: cid1}, "no candidates"),
						events.Finished(startTime, rid, types.RetrievalCandidate{RootCid: cid1}),
					},
				},
			},
		},
		{
			// testing context cancellation, but this doesn't quite test all the
			// way into the protocol (graphsync) retriever itself since the
			// Retriever sits in between and has multiple paths to handling
			// context cancellation. A leaky goroutine can still occur deeper
			// in. A test with the same name directly against the
			// GraphsyncRetriever gets more accurately in.
			name: "context cancelled before completed",
			candidates: []types.RetrievalCandidate{
				{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
				{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
			},
			returns_connected: map[string]testutil.DelayedConnectReturn{
				string(peerA): {Err: nil, Delay: time.Millisecond * 1},
				string(peerB): {Err: nil, Delay: time.Millisecond * 5},
			},
			returns_retrievals: map[string]testutil.DelayedClientReturn{
				string(peerA): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerA,
					Size:              10,
					Blocks:            11,
					Duration:          12 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Second * 1},
				string(peerB): {ResultStats: &types.RetrievalStats{
					StorageProviderId: peerB,
					Size:              10,
					Blocks:            11,
					Duration:          12 * time.Second,
					TotalPayment:      big.Zero(),
					RootCid:           cid1,
					AskPrice:          abi.NewTokenAmount(0),
				}, Delay: time.Second * 1},
			},
			cancelAfter: time.Millisecond * 100,
			err:         context.Canceled,
			expectedSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					CandidatesDiscovered: []testutil.DiscoveredCandidate{
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
						{
							Cid:       cid1,
							Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
						},
					},
					ReceivedConnections: []peer.ID{peerA, peerB},
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
						events.StartedFindingCandidates(startTime, rid, cid1),
						events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
						events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
				},
				{
					AfterStart: 5 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(1*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerA, Duration: 1 * time.Millisecond},
					},
				},
				{
					AfterStart: 5 * time.Millisecond,
					ExpectedEvents: []types.RetrievalEvent{
						events.ConnectedToProvider(startTime.Add(5*time.Millisecond), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerB, Duration: 5 * time.Millisecond},
					},
				},
				{
					AfterStart: 5*time.Millisecond + initialPause,
				},
				{
					AfterStart: 100 * time.Millisecond,
					// context cancellation here should cause the retrieval to fail and not emit more events
				},
				{
					AfterStart: 1*time.Millisecond + 1*time.Second + initialPause,
				},
			},
		},
	}

	ctx := context.Background()
	for _, tc := range tc {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			retCtx, retCancel := context.WithCancel(ctx)
			defer retCancel()

			clock := clock.NewMock()
			clock.Set(startTime)
			// --- setup ---
			candidateFinder := testutil.NewMockCandidateFinder(nil, map[cid.Cid][]types.RetrievalCandidate{cid1: tc.candidates})
			client := testutil.NewMockClient(tc.returns_connected, tc.returns_retrievals, clock)
			session := testutil.NewMockSession(ctx)
			if tc.setup != nil {
				tc.setup(session)
			}
			gsretriever := NewGraphsyncRetrieverWithConfig(session, client, clock, initialPause, true)

			// --- create ---
			ret, err := NewRetrieverWithClock(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
				multicodec.TransportGraphsyncFilecoinv1: gsretriever,
			}, clock)
			require.NoError(t, err)

			// --- start ---
			ret.Start()

			// --- retrieve ---
			require.NoError(t, err)
			results := testutil.RetrievalVerifier{
				ExpectedSequence: tc.expectedSequence,
			}.RunWithVerification(
				ctx,
				t,
				clock,
				client,
				candidateFinder,
				session,
				retCancel,
				tc.cancelAfter,
				[]testutil.RunRetrieval{func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
					return ret.Retrieve(retCtx, types.RetrievalRequest{
						LinkSystem:  cidlink.DefaultLinkSystem(),
						RetrievalID: rid,
						Request: trustlessutils.Request{
							Root:       cid1,
							Path:       tc.path,
							Scope:      tc.scope,
							Duplicates: tc.dups,
						},
					}, cb)
				}},
			)

			// --- stop ---
			ret.Stop()

			// -- verify --
			require.Len(t, results, 1)
			result, err := results[0].Stats, results[0].Err
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
		})
	}
}

func TestLinkSystemPerRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	startTime := time.Now().Add(5 * time.Hour)
	clock := clock.NewMock()
	clock.Set(startTime)
	initialPause := time.Millisecond * 2
	rid := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")

	candidates := []types.RetrievalCandidate{
		{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
		{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
	}
	returnsConnected := map[string]testutil.DelayedConnectReturn{
		string(peerA): {Err: nil, Delay: time.Millisecond * 5},
		string(peerB): {Err: nil, Delay: time.Millisecond * 500},
	}
	returnsRetrievals := map[string]testutil.DelayedClientReturn{
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

	candidateFinder := testutil.NewMockCandidateFinder(nil, map[cid.Cid][]types.RetrievalCandidate{cid1: candidates})
	client := testutil.NewMockClient(returnsConnected, returnsRetrievals, clock)
	session := session.NewSession(nil, true)
	gsretriever := NewGraphsyncRetrieverWithConfig(session, client, clock, initialPause, true)

	// --- create ---
	ret, err := NewRetrieverWithClock(context.Background(), session, candidateFinder, map[multicodec.Code]types.CandidateRetriever{
		multicodec.TransportGraphsyncFilecoinv1: gsretriever,
	}, clock)
	require.NoError(t, err)

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
	results := testutil.RetrievalVerifier{
		ExpectedSequence: []testutil.ExpectedActionsAtTime{
			{
				AfterStart: 0,
				CandidatesDiscovered: []testutil.DiscoveredCandidate{
					{
						Cid:       cid1,
						Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
					},
					{
						Cid:       cid1,
						Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
					},
				},
				ReceivedConnections: []peer.ID{peerA, peerB},
				ExpectedEvents: []types.RetrievalEvent{
					events.StartedFetch(startTime, rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
					events.StartedFindingCandidates(startTime, rid, cid1),
					events.CandidatesFound(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
					events.CandidatesFiltered(startTime, rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
					events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					events.StartedRetrieval(startTime, rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
				},
			},
			{
				AfterStart: 5 * time.Millisecond,
				ExpectedEvents: []types.RetrievalEvent{
					events.ConnectedToProvider(startTime.Add(5*time.Millisecond), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
				},
			},
			{
				AfterStart:         5*time.Millisecond + initialPause,
				ReceivedRetrievals: []peer.ID{peerA},
			},
			{
				AfterStart: 10*time.Millisecond + initialPause,
				ExpectedEvents: []types.RetrievalEvent{
					events.Proposed(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
					events.Accepted(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1)),
					events.FirstByte(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
					events.Success(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), 1, 2, 3*time.Second, multicodec.TransportGraphsyncFilecoinv1),
					events.Finished(startTime.Add(10*time.Millisecond+initialPause), rid, types.RetrievalCandidate{RootCid: cid1})},
			},
		},
	}.RunWithVerification(ctx, t, clock, client, candidateFinder, nil, nil, 0, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return ret.Retrieve(context.Background(), types.RetrievalRequest{
				LinkSystem:  lsA,
				RetrievalID: rid,
				Request:     trustlessutils.Request{Root: cid1},
			}, cb)
		},
	})
	require.Len(t, results, 1)
	result, err := results[0].Stats, results[0].Err
	require.NoError(t, err)
	require.Equal(t, returnsRetrievals[string(peerA)].ResultStats, result)

	// switch them around
	returnsConnected = map[string]testutil.DelayedConnectReturn{
		string(peerA): {Err: nil, Delay: time.Millisecond * 500},
		string(peerB): {Err: nil, Delay: time.Millisecond * 5},
	}
	client.SetConnectReturns(returnsConnected)

	// --- retrieve ---
	results = testutil.RetrievalVerifier{
		ExpectedSequence: []testutil.ExpectedActionsAtTime{
			{
				AfterStart: 0,
				CandidatesDiscovered: []testutil.DiscoveredCandidate{
					{
						Cid:       cid1,
						Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
					},
					{
						Cid:       cid1,
						Candidate: types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid1, Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{})},
					},
				},
				ReceivedConnections: []peer.ID{peerA, peerB},
				ExpectedEvents: []types.RetrievalEvent{
					events.StartedFetch(startTime.Add(10*time.Millisecond+initialPause), rid, cid1, "?dag-scope=all&dups=n", multicodec.TransportGraphsyncFilecoinv1),
					events.StartedFindingCandidates(startTime.Add(10*time.Millisecond+initialPause), rid, cid1),
					events.CandidatesFound(startTime.Add(10*time.Millisecond+initialPause), rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
					events.CandidatesFiltered(startTime.Add(10*time.Millisecond+initialPause), rid, cid1, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerA, nil, cid1), types.NewRetrievalCandidate(peerB, nil, cid1)}),
					events.StartedRetrieval(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerA, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
					events.StartedRetrieval(startTime.Add(10*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
				},
			},
			{
				AfterStart: 5 * time.Millisecond,
				ExpectedEvents: []types.RetrievalEvent{
					events.ConnectedToProvider(startTime.Add(15*time.Millisecond+initialPause), rid, types.NewRetrievalCandidate(peerB, nil, cid1), multicodec.TransportGraphsyncFilecoinv1),
				},
			},
			{
				AfterStart:         5*time.Millisecond + initialPause,
				ReceivedRetrievals: []peer.ID{peerB},
			},
			{
				AfterStart: 10*time.Millisecond + initialPause,
				ExpectedEvents: []types.RetrievalEvent{
					events.Proposed(startTime.Add((10*time.Millisecond+initialPause)*2), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
					events.Accepted(startTime.Add((10*time.Millisecond+initialPause)*2), rid, types.NewRetrievalCandidate(peerB, nil, cid1)),
					events.FirstByte(startTime.Add((10*time.Millisecond+initialPause)*2), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 5*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1),
					events.Success(startTime.Add((10*time.Millisecond+initialPause)*2), rid, types.NewRetrievalCandidate(peerB, nil, cid1), 10, 11, 12*time.Second, multicodec.TransportGraphsyncFilecoinv1),
					events.Finished(startTime.Add((10*time.Millisecond+initialPause)*2), rid, types.RetrievalCandidate{RootCid: cid1})},
			},
		},
	}.RunWithVerification(ctx, t, clock, client, candidateFinder, nil, nil, 0, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return ret.Retrieve(context.Background(), types.RetrievalRequest{
				LinkSystem:  lsB,
				RetrievalID: rid,
				Request:     trustlessutils.Request{Root: cid1},
			}, cb)
		},
	})
	require.Len(t, results, 1)
	result, err = results[0].Stats, results[0].Err

	require.NoError(t, err)
	require.Equal(t, returnsRetrievals[string(peerB)].ResultStats, result)

	// --- stop ---
	ret.Stop()

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
