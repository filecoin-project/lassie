package filecoin

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

type minerStatus struct {
	suspensionStart    time.Time // 0 = not suspended
	suspensionDuration time.Duration

	// NOTE: failures are assumed to be in order from oldest at the beginning of
	// the array to newest at the end
	failures []time.Time
}

func (status *minerStatus) suspended() bool {
	return !status.suspensionStart.IsZero() && time.Since(status.suspensionStart) < status.suspensionDuration
}

type minerMonitorConfig struct {
	maxFailuresBeforeSuspend uint
	failureHistoryDuration   time.Duration
	suspensionDuration       time.Duration
}

// Miner monitor is a helper which assists in tracking which miners are not
// responding and decides whether they should be temporarily suspended
type minerMonitor struct {
	cfg minerMonitorConfig

	lk       sync.RWMutex
	statuses map[peer.ID]minerStatus
}

func newMinerMonitor(cfg minerMonitorConfig) *minerMonitor {
	return &minerMonitor{
		cfg: cfg,

		statuses: make(map[peer.ID]minerStatus),
	}
}

func (monitor *minerMonitor) suspended(miner peer.ID) bool {
	monitor.lk.RLock()
	defer monitor.lk.RUnlock()

	if status, ok := monitor.statuses[miner]; ok {
		return status.suspended()
	}

	return false
}

func (monitor *minerMonitor) recordFailure(miner peer.ID) {
	monitor.lk.Lock()
	defer monitor.lk.Unlock()

	status := monitor.statuses[miner]

	// Filter out expired history
	n := 0
	for _, failure := range status.failures {
		if time.Since(failure) <= monitor.cfg.failureHistoryDuration {
			status.failures[n] = failure
			n++
		}
	}
	status.failures = status.failures[:n]

	// Add new failure to history
	status.failures = append(status.failures, time.Now())

	// Decide whether to suspend miner
	if len(status.failures) > int(monitor.cfg.maxFailuresBeforeSuspend) {
		status.suspensionStart = time.Now()
		status.suspensionDuration = monitor.cfg.suspensionDuration
	}

	if status.suspended() {
		log.Warnf(
			"Suspending miner for %s after %d failures within %s: %s",
			status.suspensionDuration,
			len(status.failures),
			time.Since(status.failures[0]),
			miner,
		)
	}

	// Write updated status back to map
	monitor.statuses[miner] = status
}
