package session

import (
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
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
