package retriever

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
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

	// Must have max failures + 1 logged and be marked as suspended... and then
	// no longer be marked as suspended after the suspension duration is up
	for i := uint(0); i < cfg.maxFailuresBeforeSuspend+1; i++ {
		tracker.RecordFailure(testSPA)
	}
	require.Len(t, tracker.spfm[testSPA].failures, int(cfg.maxFailuresBeforeSuspend+1))
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
	require.Len(t, tracker.spfm[testSPB].failures, int(cfg.maxFailuresBeforeSuspend))
	require.False(t, tracker.IsSuspended(testSPB))
}
