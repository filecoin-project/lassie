package retriever

import (
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type activeRetrieval struct {
	cid                cid.Cid
	storageProviderIds []peer.ID
}

type trackedSp struct {
	suspensionStart    time.Time
	suspensionDuration time.Duration
	failures           []time.Time // should be ordered, oldest to newest
	concurrency        uint
}

func (status *trackedSp) isSuspended() bool {
	return !status.suspensionStart.IsZero() && time.Since(status.suspensionStart) < status.suspensionDuration
}

type spTrackerConfig struct {
	maxFailuresBeforeSuspend uint
	failureHistoryDuration   time.Duration
	suspensionDuration       time.Duration
}

type spTracker struct {
	lk  sync.RWMutex
	cfg spTrackerConfig
	// active retrievals
	arm map[types.RetrievalID]activeRetrieval
	// failures and concurrency of storage providers
	spm map[peer.ID]trackedSp
}

// newSpTracker creates a new spTracker with the given config. If the config is
// nil, a default config will be used.
func newSpTracker(cfg *spTrackerConfig) *spTracker {
	if cfg == nil {
		cfg = &spTrackerConfig{
			maxFailuresBeforeSuspend: 5,
			suspensionDuration:       time.Minute * 10,
			failureHistoryDuration:   time.Second * 30,
		}
	}
	return &spTracker{
		cfg: *cfg,
		arm: make(map[types.RetrievalID]activeRetrieval),
		spm: make(map[peer.ID]trackedSp),
	}
}

// RegisterRetrieval registers a retrieval, returning false if the retrieval for
// this RetrievalID or CID CID already exists, or true if it is new.
func (spt *spTracker) RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid) bool {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if _, has := spt.arm[retrievalId]; has {
		return false
	}
	for rid, ar := range spt.arm {
		if rid == retrievalId || ar.cid == cid {
			return false
		}
	}
	// new
	spt.arm[retrievalId] = activeRetrieval{cid, make([]peer.ID, 0)}
	return true
}

// EndRetrieval cleans up an existing retrieval
func (spt *spTracker) EndRetrieval(retrievalId types.RetrievalID) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[retrievalId]; has {
		for _, id := range ar.storageProviderIds {
			if c, has := spt.spm[id]; has && c.concurrency > 0 {
				c.concurrency--
				spt.spm[id] = c
			} else {
				return fmt.Errorf("internal error, peer.ID not properly double-recorded %s", retrievalId)
			}
		}
		delete(spt.arm, retrievalId)
		return nil
	}
	return fmt.Errorf("no such active retrieval %s", retrievalId)
}

// AddToRetrieval adds a set of storage providers to an existing retrieval, this
// will increase the concurrency for each of the storage providers until the
// retrieval has finished
func (spt *spTracker) AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[retrievalId]; has {
		ar.storageProviderIds = append(ar.storageProviderIds, storageProviderIds...)
		for _, id := range storageProviderIds {
			ts := spt.spm[id]
			ts.concurrency++
			spt.spm[id] = ts
		}
		return nil
	}
	return fmt.Errorf("no such active retrieval %s", retrievalId)
}

// GetConcurrency returns the number of retrievals the given storage provider
// is currently recorded as being involved in
func (spt *spTracker) GetConcurrency(storageProviderId peer.ID) uint {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if c, has := spt.spm[storageProviderId]; !has {
		return c.concurrency
	}
	return 0
}

// IsSuspended determines whether a storage provider has been temporarily
// suspended due to an excessive number of recent errors
func (spt *spTracker) IsSuspended(storageProviderId peer.ID) bool {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if status, has := spt.spm[storageProviderId]; has {
		return status.isSuspended()
	}
	return false
}

// RecordFailure records a failure for a storage provider, potentially leading
// to a suspension
func (spt *spTracker) RecordFailure(storageProviderId peer.ID) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	status := spt.spm[storageProviderId]

	// Filter out expired history
	n := 0
	for _, failure := range status.failures {
		if time.Since(failure) <= spt.cfg.failureHistoryDuration {
			status.failures[n] = failure
			n++
		}
	}
	status.failures = status.failures[:n]

	// Add new failure to history
	status.failures = append(status.failures, time.Now())

	// Decide whether to suspend miner
	if len(status.failures) > int(spt.cfg.maxFailuresBeforeSuspend) {
		status.suspensionStart = time.Now()
		status.suspensionDuration = spt.cfg.suspensionDuration
	}

	if status.isSuspended() {
		log.Warnf(
			"Suspending storage provider for %s after %d failures within %s: %s",
			status.suspensionDuration,
			len(status.failures),
			time.Since(status.failures[0]),
			storageProviderId,
		)
	}

	// Write updated status back to map
	spt.spm[storageProviderId] = status
}
