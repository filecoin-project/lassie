package retriever

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type activeRetrieval struct {
	id                 uuid.UUID
	storageProviderIds []peer.ID
}

type spFailures struct {
	suspensionStart    time.Time
	suspensionDuration time.Duration
	failures           []time.Time // should be ordered, oldest to newest
}

func (status *spFailures) isSuspended() bool {
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
	arm map[cid.Cid]activeRetrieval
	// storage provider concurrency
	spcm map[peer.ID]uint
	// storage provider failures
	spfm map[peer.ID]spFailures
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
		cfg:  *cfg,
		arm:  make(map[cid.Cid]activeRetrieval),
		spcm: make(map[peer.ID]uint),
		spfm: make(map[peer.ID]spFailures),
	}
}

// RegisterRetrieval registers a retrieval, returning true if the retrieval for
// this CID already exists, or false if it is new.
func (spt *spTracker) RegisterRetrieval(cid cid.Cid) (uuid.UUID, bool) {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[cid]; has {
		return ar.id, true
	}
	// new
	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	spt.arm[cid] = activeRetrieval{id, make([]peer.ID, 0)}
	return id, false
}

// EndRetrieval cleans up an existing retrieval
func (spt *spTracker) EndRetrieval(cid cid.Cid) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[cid]; has {
		for _, id := range ar.storageProviderIds {
			if c, has := spt.spcm[id]; has {
				if c == 1 {
					delete(spt.spcm, id)
				} else {
					spt.spcm[id]--
				}
			} else {
				return fmt.Errorf("internal error, peer.ID not properly double-recorded %s", cid)
			}
		}
		delete(spt.arm, cid)
	}
	return fmt.Errorf("no such active retrieval for %s", cid)
}

// AddToRetrieval adds a set of storage providers to an existing retrieval, this
// will increase the concurrency for each of the storage providers until the
// retrieval has finished
func (spt *spTracker) AddToRetrieval(cid cid.Cid, storageProviderIds []peer.ID) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[cid]; has {
		ar.storageProviderIds = append(ar.storageProviderIds, storageProviderIds...)
		for _, id := range storageProviderIds {
			if _, has := spt.spcm[id]; !has {
				spt.spcm[id] = 0
			}
			spt.spcm[id]++
		}
		return nil
	}
	return fmt.Errorf("no such active retrieval for %s", cid)
}

// GetConcurrency returns the number of retrievals the given storage provider
// is currently recorded as being involved in
func (spt *spTracker) GetConcurrency(storageProviderId peer.ID) uint {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if c, has := spt.spcm[storageProviderId]; !has {
		return c
	}
	return 0
}

// IsSuspended determines whether a storage provider has been temporarily
// suspended due to an excessive number of recent errors
func (spt *spTracker) IsSuspended(storageProviderId peer.ID) bool {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if status, has := spt.spfm[storageProviderId]; has {
		return status.isSuspended()
	}
	return false
}

// RecordFailure records a failure for a storage provider, potentially leading
// to a suspension
func (spt *spTracker) RecordFailure(storageProviderId peer.ID) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	status := spt.spfm[storageProviderId]

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
	spt.spfm[storageProviderId] = status
}
