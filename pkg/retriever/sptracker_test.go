package retriever

import (
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspend(t *testing.T) {
	cfg := &spTrackerConfig{
		maxFailuresBeforeSuspend: 3,
		failureHistoryDuration:   time.Millisecond * 50,
		suspensionDuration:       time.Millisecond * 50,
	}

	tracker := newSpTracker(cfg)

	testSPA := peer.ID("A")
	testSPB := peer.ID("B")
	testSPC := peer.ID("C")

	// Must have max failures + 1 logged and be marked as suspended... and then
	// no longer be marked as suspended after the suspension duration is up
	for i := uint(0); i < cfg.maxFailuresBeforeSuspend+1; i++ {
		tracker.RecordFailure(testSPA)
	}
	require.Len(t, tracker.spm[testSPA].failures, int(cfg.maxFailuresBeforeSuspend+1))
	require.True(t, tracker.IsSuspended(testSPA))
	require.Eventually(
		t,
		func() bool { return !tracker.IsSuspended(testSPA) },
		cfg.suspensionDuration*time.Duration(3),
		time.Millisecond,
	)

	// Must have max failures and not be marked as suspended
	for i := uint(0); i < cfg.maxFailuresBeforeSuspend; i++ {
		tracker.RecordFailure(testSPB)
	}
	require.Len(t, tracker.spm[testSPB].failures, int(cfg.maxFailuresBeforeSuspend))
	require.False(t, tracker.IsSuspended(testSPB))
	require.False(t, tracker.IsSuspended(testSPC)) // one we've never registered
}

func TestSPConcurrency(t *testing.T) {
	tracker := newSpTracker(nil)
	ret1 := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	ret2 := types.RetrievalID(uuid.New())
	cid2 := cid.MustParse("bafkqaalc")
	ret3 := types.RetrievalID(uuid.New())
	cid3 := cid.MustParse("bafkqaald")
	p1 := peer.ID("A")
	p2 := peer.ID("B")
	p3 := peer.ID("C")

	assert.True(t, tracker.RegisterRetrieval(ret1, cid1))

	require.Equal(t, uint(0), tracker.GetConcurrency(p1))
	require.Equal(t, uint(0), tracker.GetConcurrency(p2))
	require.Equal(t, uint(0), tracker.GetConcurrency(p3))

	require.NoError(t, tracker.AddToRetrieval(ret1, []peer.ID{p1, p2, p3}))
	require.Error(t, tracker.AddToRetrieval(ret2, []peer.ID{p1, p2, p3})) // no such retrieval (yet)

	require.Equal(t, uint(1), tracker.GetConcurrency(p1))
	require.Equal(t, uint(1), tracker.GetConcurrency(p2))
	require.Equal(t, uint(1), tracker.GetConcurrency(p3))

	assert.True(t, tracker.RegisterRetrieval(ret2, cid2))
	require.NoError(t, tracker.AddToRetrieval(ret2, []peer.ID{p1, p2}))

	require.Equal(t, uint(2), tracker.GetConcurrency(p1))
	require.Equal(t, uint(2), tracker.GetConcurrency(p2))
	require.Equal(t, uint(1), tracker.GetConcurrency(p3))

	assert.True(t, tracker.RegisterRetrieval(ret3, cid3))
	require.NoError(t, tracker.AddToRetrieval(ret3, []peer.ID{p1}))

	require.Equal(t, uint(3), tracker.GetConcurrency(p1))
	require.Equal(t, uint(2), tracker.GetConcurrency(p2))
	require.Equal(t, uint(1), tracker.GetConcurrency(p3))

	assert.NoError(t, tracker.EndRetrieval(ret1))
	require.Equal(t, uint(2), tracker.GetConcurrency(p1))
	require.Equal(t, uint(1), tracker.GetConcurrency(p2))
	require.Equal(t, uint(0), tracker.GetConcurrency(p3))

	assert.NoError(t, tracker.EndRetrieval(ret2))
	require.Equal(t, uint(1), tracker.GetConcurrency(p1))
	require.Equal(t, uint(0), tracker.GetConcurrency(p2))
	require.Equal(t, uint(0), tracker.GetConcurrency(p3))

	assert.NoError(t, tracker.EndRetrieval(ret3))
	require.Equal(t, uint(0), tracker.GetConcurrency(p1))
	require.Equal(t, uint(0), tracker.GetConcurrency(p2))
	require.Equal(t, uint(0), tracker.GetConcurrency(p3))
}

func TestRetrievalUniqueness(t *testing.T) {
	tracker := newSpTracker(nil)
	ret1 := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafkqaalb")
	ret2 := types.RetrievalID(uuid.New())
	cid2 := cid.MustParse("bafkqaalc")

	// unique RetrievalID and unique CID; i.e. can't retrieve the same CID simultaneously
	assert.True(t, tracker.RegisterRetrieval(ret1, cid1))
	assert.False(t, tracker.RegisterRetrieval(ret1, cid1))
	assert.False(t, tracker.RegisterRetrieval(ret1, cid2))
	assert.False(t, tracker.RegisterRetrieval(ret2, cid1))

	require.NoError(t, tracker.EndRetrieval(ret1))
	require.Error(t, tracker.EndRetrieval(ret1))
	require.Error(t, tracker.EndRetrieval(ret2))

	assert.True(t, tracker.RegisterRetrieval(ret2, cid1))
	assert.False(t, tracker.RegisterRetrieval(ret2, cid1))
	assert.True(t, tracker.RegisterRetrieval(ret1, cid2))
	assert.False(t, tracker.RegisterRetrieval(ret2, cid1))

	require.NoError(t, tracker.EndRetrieval(ret1))
	require.NoError(t, tracker.EndRetrieval(ret2))
	require.Error(t, tracker.EndRetrieval(ret1))
	require.Error(t, tracker.EndRetrieval(ret2))
}
