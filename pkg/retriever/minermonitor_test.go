package retriever

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestSuspend(t *testing.T) {
	cfg := minerMonitorConfig{
		maxFailuresBeforeSuspend: 3,
		failureHistoryDuration:   time.Millisecond * 10,
		suspensionDuration:       time.Millisecond * 10,
	}

	monitor := newMinerMonitor(cfg)

	testMinerA := peer.ID("A")
	testMinerB := peer.ID("B")

	// Must have max failures + 1 logged and be marked as suspended... and then
	// no longer be marked as suspended after the suspension duration is up
	for i := uint(0); i < cfg.maxFailuresBeforeSuspend+1; i++ {
		monitor.recordFailure(testMinerA)
	}
	require.Len(t, monitor.statuses[testMinerA].failures, int(cfg.maxFailuresBeforeSuspend+1))
	require.True(t, monitor.suspended(testMinerA))
	require.Eventually(
		t,
		func() bool { return !monitor.suspended(testMinerA) },
		cfg.suspensionDuration*time.Duration(2),
		time.Millisecond,
	)

	// Must have max failures and not be marked as suspended
	for i := uint(0); i < cfg.maxFailuresBeforeSuspend; i++ {
		monitor.recordFailure(testMinerB)
	}
	require.Len(t, monitor.statuses[testMinerB].failures, int(cfg.maxFailuresBeforeSuspend))
	require.False(t, monitor.suspended(testMinerB))
}
