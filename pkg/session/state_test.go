package session

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspend(t *testing.T) {
	ret := types.RetrievalID(uuid.New())
	cid := cid.MustParse("bafkqaalb")
	selector := selectorparse.CommonSelector_ExploreAllRecursively
	testSPA := peer.ID("A")
	testSPB := peer.ID("B")
	testSPC := peer.ID("C")

	cfg := &Config{
		MaxFailuresBeforeSuspend: 3,
		FailureHistoryDuration:   time.Millisecond * 50,
		SuspensionDuration:       time.Millisecond * 50,
	}

	state := NewSessionState(cfg)

	assert.True(t, state.RegisterRetrieval(ret, cid, selector))

	// Must have max failures + 1 logged and be marked as suspended... and then
	// no longer be marked as suspended after the suspension duration is up
	for i := uint(0); i < cfg.MaxFailuresBeforeSuspend+1; i++ {
		require.NoError(t, state.AddToRetrieval(ret, []peer.ID{testSPA}))
		require.NoError(t, state.RecordFailure(testSPA, ret))
	}
	require.Len(t, state.spm[testSPA].failures, int(cfg.MaxFailuresBeforeSuspend+1))
	require.True(t, state.IsSuspended(testSPA))
	require.Eventually(
		t,
		func() bool { return !state.IsSuspended(testSPA) },
		cfg.SuspensionDuration*time.Duration(3),
		time.Millisecond,
	)

	// Must have max failures and not be marked as suspended
	for i := uint(0); i < cfg.MaxFailuresBeforeSuspend; i++ {
		require.NoError(t, state.AddToRetrieval(ret, []peer.ID{testSPB}))
		require.NoError(t, state.RecordFailure(testSPB, ret))
	}
	require.Len(t, state.spm[testSPB].failures, int(cfg.MaxFailuresBeforeSuspend))
	require.False(t, state.IsSuspended(testSPB))
	require.False(t, state.IsSuspended(testSPC)) // one we've never registered
}

func TestSPConcurrency(t *testing.T) {
	state := NewSessionState(DefaultConfig())
	selector := selectorparse.CommonSelector_ExploreAllRecursively
	ret1 := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	ret2 := types.RetrievalID(uuid.New())
	cid2 := cid.MustParse("bafkqaalc")
	ret3 := types.RetrievalID(uuid.New())
	cid3 := cid.MustParse("bafkqaald")
	p1 := peer.ID("A")
	p2 := peer.ID("B")
	p3 := peer.ID("C")

	assert.True(t, state.RegisterRetrieval(ret1, cid1, selector))

	require.Equal(t, uint(0), state.GetConcurrency(p1))
	require.Equal(t, uint(0), state.GetConcurrency(p2))
	require.Equal(t, uint(0), state.GetConcurrency(p3))

	require.NoError(t, state.AddToRetrieval(ret1, []peer.ID{p1, p2, p3}))
	require.Error(t, state.AddToRetrieval(ret2, []peer.ID{p1, p2, p3})) // no such retrieval (yet)

	require.Equal(t, uint(1), state.GetConcurrency(p1))
	require.Equal(t, uint(1), state.GetConcurrency(p2))
	require.Equal(t, uint(1), state.GetConcurrency(p3))

	assert.True(t, state.RegisterRetrieval(ret2, cid2, selector))
	require.NoError(t, state.AddToRetrieval(ret2, []peer.ID{p1, p2}))

	require.Equal(t, uint(2), state.GetConcurrency(p1))
	require.Equal(t, uint(2), state.GetConcurrency(p2))
	require.Equal(t, uint(1), state.GetConcurrency(p3))

	assert.True(t, state.RegisterRetrieval(ret3, cid3, selector))
	require.NoError(t, state.AddToRetrieval(ret3, []peer.ID{p1}))

	require.Equal(t, uint(3), state.GetConcurrency(p1))
	require.Equal(t, uint(2), state.GetConcurrency(p2))
	require.Equal(t, uint(1), state.GetConcurrency(p3))

	assert.NoError(t, state.EndRetrieval(ret1))
	require.Equal(t, uint(2), state.GetConcurrency(p1))
	require.Equal(t, uint(1), state.GetConcurrency(p2))
	require.Equal(t, uint(0), state.GetConcurrency(p3))

	assert.NoError(t, state.EndRetrieval(ret2))
	require.Equal(t, uint(1), state.GetConcurrency(p1))
	require.Equal(t, uint(0), state.GetConcurrency(p2))
	require.Equal(t, uint(0), state.GetConcurrency(p3))

	assert.NoError(t, state.EndRetrieval(ret3))
	require.Equal(t, uint(0), state.GetConcurrency(p1))
	require.Equal(t, uint(0), state.GetConcurrency(p2))
	require.Equal(t, uint(0), state.GetConcurrency(p3))

	// test failures reducing concurrency
	assert.True(t, state.RegisterRetrieval(ret1, cid1, selector))
	assert.True(t, state.RegisterRetrieval(ret2, cid2, selector))
	require.NoError(t, state.AddToRetrieval(ret1, []peer.ID{p1, p2, p3}))
	require.NoError(t, state.AddToRetrieval(ret2, []peer.ID{p1, p2, p3}))

	require.Equal(t, uint(2), state.GetConcurrency(p1))
	require.Equal(t, uint(2), state.GetConcurrency(p2))
	require.Equal(t, uint(2), state.GetConcurrency(p3))

	require.NoError(t, state.RecordFailure(p1, ret1))
	require.Equal(t, uint(1), state.GetConcurrency(p1))
	require.Equal(t, uint(2), state.GetConcurrency(p2))
	require.Equal(t, uint(2), state.GetConcurrency(p3))
	require.ErrorContains(t, state.RecordFailure(p1, ret1), "no such storage provider")

	assert.NoError(t, state.EndRetrieval(ret2))
	require.Equal(t, uint(0), state.GetConcurrency(p1))
	require.Equal(t, uint(1), state.GetConcurrency(p2))
	require.Equal(t, uint(1), state.GetConcurrency(p3))

	assert.NoError(t, state.EndRetrieval(ret1))
	require.Equal(t, uint(0), state.GetConcurrency(p1))
	require.Equal(t, uint(0), state.GetConcurrency(p2))
	require.Equal(t, uint(0), state.GetConcurrency(p3))
}

func TestRetrievalUniqueness(t *testing.T) {
	state := NewSessionState(DefaultConfig())
	ret1 := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	ret2 := types.RetrievalID(uuid.New())
	cid2 := cid.MustParse("bafkqaalc")
	ret3 := types.RetrievalID(uuid.New())
	all := selectorparse.CommonSelector_ExploreAllRecursively
	matcher := selectorparse.CommonSelector_MatchPoint

	// unique RetrievalID and unique CID; i.e. can't retrieve the same CID simultaneously
	assert.True(t, state.RegisterRetrieval(ret1, cid1, all))
	assert.False(t, state.RegisterRetrieval(ret1, cid1, all))
	assert.False(t, state.RegisterRetrieval(ret1, cid2, all))
	assert.False(t, state.RegisterRetrieval(ret2, cid1, all))

	// unique Retrieval ID can register for a different selector w/ same cid
	assert.True(t, state.RegisterRetrieval(ret3, cid1, matcher))

	require.NoError(t, state.EndRetrieval(ret1))
	require.Error(t, state.EndRetrieval(ret1))
	require.Error(t, state.EndRetrieval(ret2))
	require.NoError(t, state.EndRetrieval(ret3))

	assert.True(t, state.RegisterRetrieval(ret2, cid1, all))
	assert.False(t, state.RegisterRetrieval(ret2, cid1, all))
	assert.True(t, state.RegisterRetrieval(ret1, cid2, all))
	assert.False(t, state.RegisterRetrieval(ret2, cid1, all))

	require.NoError(t, state.EndRetrieval(ret1))
	require.NoError(t, state.EndRetrieval(ret2))
	require.Error(t, state.EndRetrieval(ret1))
	require.Error(t, state.EndRetrieval(ret2))
}

type actionType int

const (
	connectAction actionType = iota
)

type action struct {
	p   peer.ID
	typ actionType
	d   time.Duration
}

func (a action) execute(s *SessionState) {
	switch a.typ {
	case connectAction:
		s.RegisterConnectTime(a.p, a.d)
	default:
		panic("unrecognized action type")
	}
}

func TestCandidateComparison(t *testing.T) {
	peers := make([]peer.ID, 10)
	for i := 0; i < 10; i++ {
		peers[i] = peer.ID(fmt.Sprintf("peer%d", i))
	}

	testCases := []struct {
		name          string
		protocol      multicodec.Code
		actions       []action
		metadata      []metadata.Protocol
		expectedOrder []peer.ID
	}{
		{
			name: "http peers, different connect speeds",
			actions: []action{
				{peers[0], connectAction, time.Second},
				{peers[1], connectAction, 2 * time.Second},
				{peers[2], connectAction, 3 * time.Second},
				{peers[3], connectAction, 4 * time.Second},
				{peers[4], connectAction, 5 * time.Second},
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "graphsync peers, different connect speeds",
			actions: []action{
				{peers[0], connectAction, time.Second},
				{peers[1], connectAction, 2 * time.Second},
				{peers[2], connectAction, 3 * time.Second},
				{peers[3], connectAction, 4 * time.Second},
				{peers[4], connectAction, 5 * time.Second},
			},
			metadata:      []metadata.Protocol{&metadata.GraphsyncFilecoinV1{}, &metadata.GraphsyncFilecoinV1{}, &metadata.GraphsyncFilecoinV1{}, &metadata.GraphsyncFilecoinV1{}, &metadata.GraphsyncFilecoinV1{}},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name:     "graphsync chooses best",
			protocol: multicodec.TransportGraphsyncFilecoinv1,
			actions: []action{
				{peers[0], connectAction, time.Second},
				{peers[1], connectAction, time.Second},
				{peers[2], connectAction, 2 * time.Second},
				{peers[3], connectAction, 2 * time.Second},
				{peers[4], connectAction, 3 * time.Second},
			},
			metadata: []metadata.Protocol{
				&metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: false},
				&metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				&metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				&metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				&metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false}, // same as prev, slower connect
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		// NOTE: use `ema = (a, r, ...v) => { for (const i of v) { r = (1-a)*i + a*r }; return r }`
		// to calculate the expected averages in a Node.js repl: ema(0.5, 10, 11, 13) = 11.75
		{
			name: "multiple connect, averages don't cross",
			actions: []action{
				{peers[0], connectAction, 10 * time.Second}, // 10
				{peers[0], connectAction, 11 * time.Second}, // 10.5
				{peers[0], connectAction, 13 * time.Second}, // 11.75
				{peers[1], connectAction, 12 * time.Second}, // 12
				{peers[1], connectAction, 12 * time.Second}, // 12
				{peers[1], connectAction, 12 * time.Second}, // 12
			},
			expectedOrder: []peer.ID{peers[0], peers[1]},
		},
		{
			name: "multiple connect, averages cross",
			actions: []action{
				{peers[0], connectAction, 10 * time.Second}, // 10
				{peers[0], connectAction, 11 * time.Second}, // 10.5
				{peers[0], connectAction, 13 * time.Second}, // 11.75
				{peers[0], connectAction, 13 * time.Second}, // 12.375
				{peers[1], connectAction, 12 * time.Second}, // 12
				{peers[1], connectAction, 12 * time.Second}, // 12
				{peers[1], connectAction, 12 * time.Second}, // 12
			},
			expectedOrder: []peer.ID{peers[1], peers[0]},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := NewSessionState(DefaultConfig())
			for _, action := range tc.actions {
				action.execute(state)
			}
			p := multicodec.TransportIpfsGatewayHttp
			if tc.protocol != multicodec.Code(0) {
				p = tc.protocol
			}
			for i := 1; i < len(tc.expectedOrder); i++ {
				var mda metadata.Protocol = metadata.IpfsGatewayHttp{}
				var mdb metadata.Protocol = metadata.IpfsGatewayHttp{}
				if len(tc.metadata) > 0 {
					mda = tc.metadata[i-1]
					mdb = tc.metadata[i]
				}
				require.True(t, state.CompareStorageProviders(p, tc.expectedOrder[i-1], tc.expectedOrder[i], mda, mdb), "%d: expected %s to be better than %s", i, string(tc.expectedOrder[i-1]), string(tc.expectedOrder[i]))
				require.False(t, state.CompareStorageProviders(p, tc.expectedOrder[i], tc.expectedOrder[i-1], mdb, mda), "%d: expected %s to be worse than %s", i, string(tc.expectedOrder[i]), string(tc.expectedOrder[i-1]))
			}
		})
	}
}
