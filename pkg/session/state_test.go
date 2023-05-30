package session

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	require.NoError(t, state.RecordFailure(ret1, p1))
	require.Equal(t, uint(1), state.GetConcurrency(p1))
	require.Equal(t, uint(2), state.GetConcurrency(p2))
	require.Equal(t, uint(2), state.GetConcurrency(p3))
	require.ErrorContains(t, state.RecordFailure(ret1, p1), "no such storage provider")

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

var retrievalId = types.RetrievalID(uuid.New())

type actionType int

const (
	connectAction actionType = iota
	successAction
	failureAction
	ttfbAction
)

type action struct {
	p   peer.ID
	typ actionType
	d   time.Duration
	v   uint64
}

func (a action) execute(t *testing.T, s *SessionState) {
	switch a.typ {
	case connectAction:
		s.RecordConnectTime(a.p, a.d)
	case successAction:
		s.RecordSuccess(a.p, a.v)
	case failureAction:
		require.NoError(t, s.AddToRetrieval(retrievalId, []peer.ID{a.p}))
		require.NoError(t, s.RecordFailure(retrievalId, a.p))
	case ttfbAction:
		s.RecordFirstByteTime(a.p, a.d)
	default:
		panic("unrecognized action type")
	}
}

/*
The following tests use an implicit understanding of the way scoring works
within the State. By controlling a sequence of actions and estimating the
internal scoring, we then used a fixed dice-roll to make sure that each two
providers compare in the predicted way.

To approximate the expected scoring components, use a Node.js repl, creating an
`ema` function:
	ema=(a,r,...v)=>v.reduce((p,c)=>(1-a)*c+a*p,r)
Then use it to calculate the per-score, per-provider, and across-provider
components:
 - connect time (seconds->milliseconds, truncated to ints), `a` of 0.5:
   Math.floor(ema(0.5, 10000, 11000, 13000)) = 1175
 - to calculate overall (oema) of connect time, use an `a` of 0.8, for all
   values, e.g.:
   Math.floor(ema(0.8, 10000, 11000, 13000, 13000, 12000, 12000, 12000))
 - Note that internally, the ema function in Go will truncate the millisecond
   integers per calculation, so the results won't be quite the same in JS. An
   int ema calculator would be more accurate would be:
   emaI=(a,r,...v)=>v.reduce((p,c)=>Math.floor((1-a)*c+a*p),r)
To calculate the decayed normalised form of connect time:
 - Math.exp(-(1/oema)*ema)
*/

func TestCandidateComparison(t *testing.T) {
	peers := make([]peer.ID, 10)
	for i := 0; i < 10; i++ {
		peers[i] = peer.ID(fmt.Sprintf("peer%d", i))
	}

	testCases := []struct {
		name          string
		actions       []action
		metadata      map[peer.ID]metadata.Protocol
		expectedOrder []peer.ID
	}{
		{
			name: "http peers, different connect speeds",
			actions: []action{
				{p: peers[0], typ: connectAction, d: time.Second},
				{p: peers[1], typ: connectAction, d: 2 * time.Second},
				{p: peers[2], typ: connectAction, d: 3 * time.Second},
				{p: peers[3], typ: connectAction, d: 4 * time.Second},
				{p: peers[4], typ: connectAction, d: 5 * time.Second},
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "graphsync peers, different connect speeds",
			actions: []action{
				{p: peers[0], typ: connectAction, d: time.Second},
				{p: peers[1], typ: connectAction, d: 2 * time.Second},
				{p: peers[2], typ: connectAction, d: 3 * time.Second},
				{p: peers[3], typ: connectAction, d: 4 * time.Second},
				{p: peers[4], typ: connectAction, d: 5 * time.Second},
			},
			metadata: map[peer.ID]metadata.Protocol{
				peers[0]: &metadata.GraphsyncFilecoinV1{},
				peers[1]: &metadata.GraphsyncFilecoinV1{},
				peers[2]: &metadata.GraphsyncFilecoinV1{},
				peers[3]: &metadata.GraphsyncFilecoinV1{},
				peers[4]: &metadata.GraphsyncFilecoinV1{},
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "graphsync chooses best, w/ connect time",
			actions: []action{
				{p: peers[0], typ: connectAction, d: time.Second},
				{p: peers[1], typ: connectAction, d: time.Second},
				{p: peers[2], typ: connectAction, d: 2 * time.Second},
				{p: peers[3], typ: connectAction, d: 2 * time.Second},
				{p: peers[4], typ: connectAction, d: 3 * time.Second},
			},
			metadata: map[peer.ID]metadata.Protocol{
				peers[0]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: false},
				peers[1]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[2]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[3]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				peers[4]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false}, // same as prev, slower connect
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "multiple connect, averages don't cross",
			actions: []action{
				{p: peers[0], typ: connectAction, d: 10 * time.Second}, // c-ema: 10000
				{p: peers[0], typ: connectAction, d: 11 * time.Second}, // c-ema: 10500
				{p: peers[0], typ: connectAction, d: 13 * time.Second}, // c-ema: 11750
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				// connect oema: 11364
			},
			expectedOrder: []peer.ID{peers[0], peers[1]},
		},
		{
			name: "multiple connect, averages cross",
			actions: []action{
				{p: peers[0], typ: connectAction, d: 10 * time.Second}, // c-ema: 10000
				{p: peers[0], typ: connectAction, d: 11 * time.Second}, // c-ema: 10500
				{p: peers[0], typ: connectAction, d: 13 * time.Second}, // c-ema: 11750
				{p: peers[0], typ: connectAction, d: 13 * time.Second}, // c-ema: 12375
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				// connect oema: 11593
			},
			expectedOrder: []peer.ID{peers[1], peers[0]},
		},
		{
			// a peer without connect time data should use the oema
			name: "no connect data, first",
			actions: []action{
				{p: peers[0], typ: connectAction, d: 10 * time.Second}, // c-ema: 10000
				{p: peers[0], typ: connectAction, d: 11 * time.Second}, // c-ema: 10500
				{p: peers[0], typ: connectAction, d: 13 * time.Second}, // c-ema: 11750
				{p: peers[0], typ: connectAction, d: 13 * time.Second}, // c-ema: 12375
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				// connect oema: 11593, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[2], peers[1], peers[0]},
		},
		{
			// same as previous but we're expecting it to slot in later
			name: "no connect data, not first",
			actions: []action{
				{p: peers[0], typ: connectAction, d: 10 * time.Second}, // c-ema: 10000
				{p: peers[0], typ: connectAction, d: 11 * time.Second}, // c-ema: 10500
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				{p: peers[1], typ: connectAction, d: 12 * time.Second}, // c-ema: 12000
				// connect oema: 11078, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[0], peers[2], peers[1]},
		},
		{
			name: "success better than failure",
			actions: []action{
				{p: peers[1], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[0], typ: failureAction, d: 0}, // s-ema: 0
			},
			expectedOrder: []peer.ID{peers[1], peers[0]},
		},
		{
			name: "success, flakies, failure",
			actions: []action{
				{p: peers[1], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[1], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[3], typ: failureAction, d: 0}, // s-ema: 0
				{p: peers[3], typ: failureAction, d: 0}, // s-ema: 0
				{p: peers[0], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[0], typ: failureAction, d: 0}, // s-ema: 0.5
				{p: peers[0], typ: successAction, v: 0}, // s-ema: 0.75
				{p: peers[0], typ: failureAction, d: 0}, // s-ema: 0.375
				{p: peers[2], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[2], typ: successAction, v: 0}, // s-ema: 1
				{p: peers[2], typ: failureAction, d: 0}, // s-ema: 0.5
			},
			expectedOrder: []peer.ID{peers[1], peers[2], peers[0], peers[3]},
		},
		{
			name: "combined metrics",
			actions: []action{
				// 0, 1, 2
				{p: peers[0], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[0], typ: connectAction, d: 1 * time.Second}, // s-ema: 1, c-ema: 10000
				{p: peers[1], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[1], typ: connectAction, d: 2 * time.Second}, // s-ema: 1, c-ema: 20000
				{p: peers[2], typ: failureAction, d: 0},               // s-ema: 0
				{p: peers[2], typ: connectAction, d: 1 * time.Second}, // s-ema: 0, c-ema: 10000

				// same pattern, better metadata: 5, 4, 3
				{p: peers[5], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[5], typ: connectAction, d: 1 * time.Second}, // s-ema: 1, c-ema: 10000
				{p: peers[4], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[4], typ: connectAction, d: 2 * time.Second}, // s-ema: 1, c-ema: 20000
				{p: peers[3], typ: failureAction, d: 0},               // s-ema: 0
				{p: peers[3], typ: connectAction, d: 1 * time.Second}, // s-ema: 0, c-ema: 10000

				// same pattern, best metadata: 7, 8, 6
				{p: peers[7], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[7], typ: connectAction, d: 1 * time.Second}, // s-ema: 1, c-ema: 10000
				{p: peers[8], typ: successAction, v: 0},               // s-ema: 1
				{p: peers[8], typ: connectAction, d: 2 * time.Second}, // s-ema: 1, c-ema: 20000
				{p: peers[6], typ: failureAction, d: 0},               // s-ema: 0
				{p: peers[6], typ: connectAction, d: 1 * time.Second}, // s-ema: 0, c-ema: 10000

				// connect oema: 1282
			},
			metadata: map[peer.ID]metadata.Protocol{
				peers[0]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				peers[1]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				peers[2]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},

				peers[3]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[4]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[5]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},

				peers[6]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: true},
				peers[7]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: true},
				peers[8]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: true},
			},
			expectedOrder: []peer.ID{peers[7], peers[8], peers[6], peers[5], peers[4], peers[3], peers[0], peers[1], peers[2]},
		},
		{
			name: "ttfb chooses best",
			actions: []action{
				{p: peers[4], typ: ttfbAction, d: time.Second},
				{p: peers[3], typ: ttfbAction, d: 2 * time.Second},
				{p: peers[2], typ: ttfbAction, d: 3 * time.Second},
				{p: peers[1], typ: ttfbAction, d: 4 * time.Second},
				{p: peers[0], typ: ttfbAction, d: 5 * time.Second},
			},
			expectedOrder: []peer.ID{peers[4], peers[3], peers[2], peers[1], peers[0]},
		},
		{
			name: "graphsync chooses best, w/ ttfb time",
			actions: []action{
				{p: peers[0], typ: ttfbAction, d: time.Second},
				{p: peers[1], typ: ttfbAction, d: time.Second},
				{p: peers[2], typ: ttfbAction, d: 2 * time.Second},
				{p: peers[3], typ: ttfbAction, d: 2 * time.Second},
				{p: peers[4], typ: ttfbAction, d: 3 * time.Second},
			},
			metadata: map[peer.ID]metadata.Protocol{
				peers[0]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: false},
				peers[1]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[2]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[3]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				peers[4]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false}, // same as prev, slower connect
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "multiple ttfb, averages don't cross",
			actions: []action{
				{p: peers[0], typ: ttfbAction, d: 10 * time.Second}, // ttfb-ema: 10000
				{p: peers[0], typ: ttfbAction, d: 11 * time.Second}, // ttfb-ema: 10500
				{p: peers[0], typ: ttfbAction, d: 13 * time.Second}, // ttfb-ema: 11750
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				// ttfb oema: 11364
			},
			expectedOrder: []peer.ID{peers[0], peers[1]},
		},
		{
			name: "multiple ttfb, averages cross",
			actions: []action{
				{p: peers[0], typ: ttfbAction, d: 10 * time.Second}, // ttfb-ema: 10000
				{p: peers[0], typ: ttfbAction, d: 11 * time.Second}, // ttfb-ema: 10500
				{p: peers[0], typ: ttfbAction, d: 13 * time.Second}, // ttfb-ema: 11750
				{p: peers[0], typ: ttfbAction, d: 13 * time.Second}, // ttfb-ema: 12375
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				// ttfb oema: 11593
			},
			expectedOrder: []peer.ID{peers[1], peers[0]},
		},
		{
			// a peer without ttfb time data should use the oema
			name: "no ttfb data, first",
			actions: []action{
				{p: peers[0], typ: ttfbAction, d: 10 * time.Second}, // ttfb-ema: 10000
				{p: peers[0], typ: ttfbAction, d: 11 * time.Second}, // ttfb-ema: 10500
				{p: peers[0], typ: ttfbAction, d: 13 * time.Second}, // ttfb-ema: 11750
				{p: peers[0], typ: ttfbAction, d: 13 * time.Second}, // ttfb-ema: 12375
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				// ttfb oema: 11593, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[2], peers[1], peers[0]},
		},
		{
			// same as previous but we're expecting it to slot in later
			name: "no ttfb data, not first",
			actions: []action{
				{p: peers[0], typ: ttfbAction, d: 10 * time.Second}, // ttfb-ema: 10000
				{p: peers[0], typ: ttfbAction, d: 11 * time.Second}, // ttfb-ema: 10500
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				{p: peers[1], typ: ttfbAction, d: 12 * time.Second}, // ttfb-ema: 12000
				// ttfb oema: 11078, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[0], peers[2], peers[1]},
		},
		{
			name: "bandwidth chooses best",
			actions: []action{
				{p: peers[4], typ: successAction, v: 1},
				{p: peers[3], typ: successAction, v: 2},
				{p: peers[2], typ: successAction, v: 3},
				{p: peers[1], typ: successAction, v: 4},
				{p: peers[0], typ: successAction, v: 5},
			},
			expectedOrder: []peer.ID{peers[4], peers[3], peers[2], peers[1], peers[0]},
		},
		{
			name: "graphsync chooses best, w/ bandwidth time",
			actions: []action{
				{p: peers[0], typ: successAction, v: 1},
				{p: peers[1], typ: successAction, v: 1},
				{p: peers[2], typ: successAction, v: 2},
				{p: peers[3], typ: successAction, v: 2},
				{p: peers[4], typ: successAction, v: 3},
			},
			metadata: map[peer.ID]metadata.Protocol{
				peers[0]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: false},
				peers[1]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[2]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: true},
				peers[3]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false},
				peers[4]: &metadata.GraphsyncFilecoinV1{VerifiedDeal: false, FastRetrieval: false}, // same as prev, slower connect
			},
			expectedOrder: []peer.ID{peers[0], peers[1], peers[2], peers[3], peers[4]},
		},
		{
			name: "multiple bandwidth, averages don't cross",
			actions: []action{
				{p: peers[0], typ: successAction, v: 1000}, // b-ema: 10000
				{p: peers[0], typ: successAction, v: 1100}, // b-ema: 10500
				{p: peers[0], typ: successAction, v: 1300}, // b-ema: 11750
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				// bandwidth oema: 11364
			},
			expectedOrder: []peer.ID{peers[0], peers[1]},
		},
		{
			name: "multiple bandwidth, averages cross",
			actions: []action{
				{p: peers[0], typ: successAction, v: 1000}, // b-ema: 10000
				{p: peers[0], typ: successAction, v: 1100}, // b-ema: 10500
				{p: peers[0], typ: successAction, v: 1300}, // b-ema: 11750
				{p: peers[0], typ: successAction, v: 1300}, // b-ema: 12375
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				// bandwidth oema: 11593
			},
			expectedOrder: []peer.ID{peers[1], peers[0]},
		},
		{
			// a peer without bandwidth time data should use the oema
			name: "no bandwidth data, first",
			actions: []action{
				{p: peers[0], typ: successAction, v: 1000}, // b-ema: 10000
				{p: peers[0], typ: successAction, v: 1100}, // b-ema: 10500
				{p: peers[0], typ: successAction, v: 1300}, // b-ema: 11750
				{p: peers[0], typ: successAction, v: 1300}, // b-ema: 12375
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				// bandwidth oema: 11593, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[2], peers[1], peers[0]},
		},
		{
			// same as previous but we're expecting it to slot in later
			name: "no bandwidth data, not first",
			actions: []action{
				{p: peers[0], typ: successAction, v: 1000}, // b-ema: 10000
				{p: peers[0], typ: successAction, v: 1100}, // b-ema: 10500
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				{p: peers[1], typ: successAction, v: 1200}, // b-ema: 12000
				// bandwidth oema: 11078, peer[2] will use this value
			},
			expectedOrder: []peer.ID{peers[0], peers[2], peers[1]},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig().WithoutRandomness()
			state := NewSessionState(cfg)
			// setup a retrieval so we don't error on "unknown retrieval"
			require.True(t, state.RegisterRetrieval(retrievalId, cid.Undef, basicnode.NewString("boop")))

			for _, action := range tc.actions {
				action.execute(t, state)
			}

			// list of peers we're working with, build it in the same order as
			// the peers array so we don't bias with 'expectedOrder' ordering
			tp := make([]peer.ID, 0, len(tc.expectedOrder))
			mda := make([]metadata.Protocol, 0, len(tc.expectedOrder))
			for len(tp) < len(tc.expectedOrder) {
				for _, p := range tc.expectedOrder {
					if p == peers[len(tp)] {
						if len(tc.metadata) > 0 {
							mda = append(mda, tc.metadata[p])
						} else {
							mda = append(mda, metadata.IpfsGatewayHttp{})
						}
						tp = append(tp, p)
						break
					}
				}
			}

			// choose next provider until we've exhausted the list
			gotOrder := make([]peer.ID, 0, len(tc.expectedOrder))
			for len(gotOrder) < len(tc.expectedOrder) {
				next := state.ChooseNextProvider(tp, mda)
				gotOrder = append(gotOrder, tp[next])
				tp = append(tp[:next], tp[next+1:]...)
			}

			require.Equal(t, tc.expectedOrder, gotOrder)
		})
	}
}
