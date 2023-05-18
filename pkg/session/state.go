package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var _ State = (*SessionState)(nil)

// State defines an interface for the collection, monitoring and management
// of dynamic retrieval information on a per-storage provider basis.
// It augments a session with dynamic data collection to make decisions based
// on ongong interactions with storage providers.
type State interface {
	// IsSuspended determines whether a storage provider has been temporarily
	// suspended due to an excessive number of recent errors.
	IsSuspended(storageProviderId peer.ID) bool

	// GetConcurrency returns the number of retrievals the given storage
	// provider is currently recorded as being involved in.
	GetConcurrency(storageProviderId peer.ID) uint

	// RecordFailure records a failure for a storage provider, potentially
	// leading to a suspension; this is used on both query and retrieval
	// phases.
	RecordFailure(storageProviderId peer.ID, retrievalId types.RetrievalID) error

	// RegisterRetrieval registers a retrieval, returning false if the
	// retrieval for this RetrievalID or this CID already exists, or true if it
	// is new.
	RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool

	// AddToRetrieval adds a set of storage providers to an existing retrieval,
	// this will increase the concurrency for each of the storage providers
	// until the retrieval has finished.
	AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error

	// RemoveFromRetrieval removes a storage provider from a an active
	// retrieval, decreasing the concurrency for that storage provider. Used in
	// both the case of a retrieval failure (RecordFailure) and when a
	// QueryResponse is unacceptable.
	RemoveFromRetrieval(storageProviderId peer.ID, retrievalId types.RetrievalID) error

	// EndRetrieval cleans up an existing retrieval.
	EndRetrieval(retrievalId types.RetrievalID) error

	// RegisterConnectTime records the time it took to connect to a storage
	// provider. This is used for prioritisation of storage providers and is
	// recorded with a decay according to SessionStateConfig#ConnectTimeAlpha.
	RegisterConnectTime(storageProviderId peer.ID, connectTime time.Duration)

	// CompareStorageProviders compares two storage providers and returns true
	// if the first storage provider is preferred over the second storage
	// provider. This uses both historically collected state and information
	// contained within the metadata, depending on protocol.
	CompareStorageProviders(protocol multicodec.Code, a, b peer.ID, mda, mdb metadata.Protocol) bool
}

type activeRetrieval struct {
	cid                cid.Cid
	selectorString     string
	storageProviderIds []peer.ID
}

type storageProvider struct {
	suspensionStart        time.Time
	suspensionDuration     time.Duration
	failures               []time.Time // should be ordered, oldest to newest
	concurrency            uint
	connectTimeInitialized bool // whether connectTime has >0 samples so we don't include initial 0 in the average
	connectTime            time.Duration
}

func (status *storageProvider) isSuspended() bool {
	return !status.suspensionStart.IsZero() && time.Since(status.suspensionStart) < status.suspensionDuration
}

type SessionState struct {
	lk     sync.RWMutex
	config *Config
	// active retrievals
	arm map[types.RetrievalID]activeRetrieval
	// failures and concurrency of storage providers
	spm map[peer.ID]storageProvider
}

// NewSessionState creates a new SessionState with the given config. If the config is
// nil, a default config will be used.
func NewSessionState(config *Config) *SessionState {
	if config == nil {
		panic("config is required")
	}
	return &SessionState{
		config: config,
		arm:    make(map[types.RetrievalID]activeRetrieval),
		spm:    make(map[peer.ID]storageProvider),
	}
}

func (spt *SessionState) RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if _, has := spt.arm[retrievalId]; has {
		return false
	}
	jsonSelector, _ := ipld.Encode(selector, dagjson.Encode)
	for rid, ar := range spt.arm {
		if rid == retrievalId || (ar.cid == cid && ar.selectorString == string(jsonSelector)) {
			return false
		}
	}
	// new
	spt.arm[retrievalId] = activeRetrieval{cid, string(jsonSelector), make([]peer.ID, 0)}
	return true
}

func (spt *SessionState) EndRetrieval(retrievalId types.RetrievalID) error {
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

func (spt *SessionState) AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()
	if ar, has := spt.arm[retrievalId]; has {
		ar.storageProviderIds = append(ar.storageProviderIds, storageProviderIds...)
		for _, id := range storageProviderIds {
			ts := spt.spm[id]
			ts.concurrency++
			spt.spm[id] = ts
		}
		spt.arm[retrievalId] = ar
		return nil
	}
	return fmt.Errorf("no such active retrieval %s", retrievalId)
}

func (spt *SessionState) GetConcurrency(storageProviderId peer.ID) uint {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if c, has := spt.spm[storageProviderId]; has {
		return c.concurrency
	}
	return 0
}

func (spt *SessionState) IsSuspended(storageProviderId peer.ID) bool {
	spt.lk.RLock()
	defer spt.lk.RUnlock()
	if status, has := spt.spm[storageProviderId]; has {
		return status.isSuspended()
	}
	return false
}

func (spt *SessionState) RemoveFromRetrieval(storageProviderId peer.ID, retrievalId types.RetrievalID) error {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	if ar, has := spt.arm[retrievalId]; has {
		var foundSp bool
		for ii, id := range ar.storageProviderIds {
			if id == storageProviderId {
				foundSp = true
				// remove from this retrieval
				ar.storageProviderIds = append(ar.storageProviderIds[:ii], ar.storageProviderIds[ii+1:]...)
				if c, has := spt.spm[id]; has && c.concurrency > 0 {
					c.concurrency--
					spt.spm[id] = c
				} else {
					return fmt.Errorf("internal error, peer.ID not properly double-recorded %s", retrievalId)
				}
				break
			}
		}
		if !foundSp {
			return fmt.Errorf("internal error, no such storage provider %s for retrieval %s", storageProviderId, retrievalId)
		}
		spt.arm[retrievalId] = ar
	} else {
		return fmt.Errorf("internal error, no such active retrieval %s", retrievalId)
	}

	return nil
}

func (spt *SessionState) RecordFailure(storageProviderId peer.ID, retrievalId types.RetrievalID) error {
	// remove from this retrieval to free up the SP to be tried again for a future retrieval
	if err := spt.RemoveFromRetrieval(storageProviderId, retrievalId); err != nil {
		return err
	}

	spt.lk.Lock()
	defer spt.lk.Unlock()

	status := spt.spm[storageProviderId]

	// Filter out expired history
	n := 0
	for _, failure := range status.failures {
		if time.Since(failure) <= spt.config.FailureHistoryDuration {
			status.failures[n] = failure
			n++
		}
	}
	status.failures = status.failures[:n]

	// Add new failure to history
	status.failures = append(status.failures, time.Now())

	// Decide whether to suspend miner
	if len(status.failures) > int(spt.config.MaxFailuresBeforeSuspend) {
		status.suspensionStart = time.Now()
		status.suspensionDuration = spt.config.SuspensionDuration
	}

	if status.isSuspended() {
		logger.Warnf(
			"Suspending storage provider for %s after %d failures within %s: %s",
			status.suspensionDuration,
			len(status.failures),
			time.Since(status.failures[0]),
			storageProviderId,
		)
	}

	// Write updated status back to map
	spt.spm[storageProviderId] = status
	return nil
}

func (spt *SessionState) RegisterConnectTime(storageProviderId peer.ID, current time.Duration) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	status := spt.spm[storageProviderId]
	// EMA of connect time
	if !status.connectTimeInitialized {
		status.connectTimeInitialized = true
		status.connectTime = current
	} else {
		status.connectTime = time.Duration((1-spt.config.ConnectTimeAlpha)*float64(current) + spt.config.ConnectTimeAlpha*float64(status.connectTime))
	}
	spt.spm[storageProviderId] = status
}

func (spt *SessionState) CompareStorageProviders(protocol multicodec.Code, a, b peer.ID, mda, mdb metadata.Protocol) bool {
	if protocol == multicodec.TransportGraphsyncFilecoinv1 {
		gsmda := mda.(*metadata.GraphsyncFilecoinV1)
		gsmdb := mdb.(*metadata.GraphsyncFilecoinV1)
		// prioritize verified deals over not verified deals
		if gsmda.VerifiedDeal != gsmdb.VerifiedDeal {
			return gsmda.VerifiedDeal
		}

		// prioritize fast retrievel over not fast retrieval
		if gsmda.FastRetrieval != gsmdb.FastRetrieval {
			return gsmda.FastRetrieval
		}
	}

	spt.lk.Lock()
	defer spt.lk.Unlock()
	sa := spt.spm[a]
	sb := spt.spm[b]

	return sa.connectTime < sb.connectTime
}
