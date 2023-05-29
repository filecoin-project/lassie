package session

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
)

var _ State = (*SessionState)(nil)

// State defines an interface for the collection, monitoring and management
// of dynamic retrieval information on a per-storage provider basis.
// It augments a session with dynamic data collection to make decisions based
// on ongong interactions with storage providers.
type State interface {
	// GetConcurrency returns the number of retrievals the given storage
	// provider is currently recorded as being involved in.
	GetConcurrency(storageProviderId peer.ID) uint

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
	RemoveFromRetrieval(retrievalId types.RetrievalID, storageProviderId peer.ID) error

	// RecordFailure records a failure for a storage provider, contributing to
	// the success metric for this provider.
	RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error

	// RecordSuccess records a success for a storage provider, contributing to
	// the success metric for this provider.
	RecordSuccess(storageProviderId peer.ID)

	// EndRetrieval cleans up an existing retrieval.
	EndRetrieval(retrievalId types.RetrievalID) error

	// RecordConnectTime records the time it took to connect to a storage
	// provider. This is used for prioritisation of storage providers and is
	// recorded with a decay according to SessionStateConfig#ConnectTimeAlpha.
	RecordConnectTime(storageProviderId peer.ID, connectTime time.Duration)

	// ChooseNextProvider compares a list of storage providers and returns the
	// index of the next storage provider to use. This uses both historically
	// collected state and information contained within the metadata, depending
	// on protocol.
	ChooseNextProvider(peers []peer.ID, metadata []metadata.Protocol) int
}

type activeRetrieval struct {
	cid                cid.Cid
	selectorString     string
	storageProviderIds []peer.ID
}

type metric[T any] struct {
	initialized bool
	value       T
}

func (m metric[T]) getValue(def T) T {
	if m.initialized {
		return m.value
	}
	return def
}

type storageProvider struct {
	concurrency   uint
	connectTimeMs metric[uint64]
	success       metric[float64]
}

type SessionState struct {
	lk     sync.RWMutex
	config *Config
	// active retrievals
	arm map[types.RetrievalID]activeRetrieval
	// failures and concurrency of storage providers
	spm                  map[peer.ID]storageProvider
	overallConnectTimeMs metric[uint64]
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

func (spt *SessionState) RemoveFromRetrieval(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
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

func (spt *SessionState) RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
	// remove from this retrieval to free up the SP to be tried again for a future retrieval
	if err := spt.RemoveFromRetrieval(retrievalId, storageProviderId); err != nil {
		return err
	}

	spt.recordSuccess(storageProviderId, 0)
	return nil
}

func (spt *SessionState) RecordSuccess(storageProviderId peer.ID) {
	spt.recordSuccess(storageProviderId, 1)
}

func (spt *SessionState) recordSuccess(storageProviderId peer.ID, current float64) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	status := spt.spm[storageProviderId]
	// EMA of successes (0.0 & 1.0)
	if !status.success.initialized {
		status.success.initialized = true
		status.success.value = current
	} else {
		status.success.value = (1-spt.config.SuccessAlpha)*current + spt.config.SuccessAlpha*status.success.value
	}
	spt.spm[storageProviderId] = status
}

func (spt *SessionState) RecordConnectTime(storageProviderId peer.ID, current time.Duration) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	currentMs := uint64(current.Milliseconds())

	status := spt.spm[storageProviderId]
	// EMA of connect time
	if !status.connectTimeMs.initialized {
		status.connectTimeMs.initialized = true
		status.connectTimeMs.value = currentMs
	} else {
		status.connectTimeMs.value = uint64((1-spt.config.ConnectTimeAlpha)*float64(currentMs) + spt.config.ConnectTimeAlpha*float64(status.connectTimeMs.value))
	}
	spt.spm[storageProviderId] = status

	if !spt.overallConnectTimeMs.initialized {
		spt.overallConnectTimeMs.initialized = true
		spt.overallConnectTimeMs.value = currentMs
	} else {
		spt.overallConnectTimeMs.value = uint64((1-spt.config.OverallConnectTimeAlpha)*float64(currentMs) + spt.config.OverallConnectTimeAlpha*float64(spt.overallConnectTimeMs.value))
	}
}

func (spt *SessionState) ChooseNextProvider(peers []peer.ID, mda []metadata.Protocol) int {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	// score all peers, a float >=0 each, higher is better
	scores := make([]float64, len(peers))
	var tot float64
	ind := make([]int, len(peers)) // index into peers that we can sort without changing peers
	for ii, p := range peers {
		ind[ii] = ii
		gsmd, _ := mda[ii].(*metadata.GraphsyncFilecoinV1)
		scores[ii] = spt.scoreProvider(p, gsmd)
		tot += scores[ii]
	}
	// sort so that the non-random selection choose the first (best)
	sort.Slice(ind, func(i, j int) bool {
		return scores[ind[i]] > scores[ind[j]]
	})

	// choose a random peer, weighted by score
	r := tot * spt.config.roll()
	for _, pi := range ind {
		s := scores[pi]
		if r <= s {
			return pi
		}
		r -= s
	}
	panic("unreachable")
}

// scoreProvider returns a score for a given provider, higher is better.
// This method assumes the caller holds lock.
func (spt *SessionState) scoreProvider(id peer.ID, md *metadata.GraphsyncFilecoinV1) float64 {
	var score float64
	// var v, f bool

	// graphsync metadata weighting
	if md != nil {
		if md.VerifiedDeal {
			score += spt.config.GraphsyncVerifiedDealWeight
			// v = true
		}
		if md.FastRetrieval {
			score += spt.config.GraphsyncFastRetrievalWeight
			// f = true
		}
	}

	// collected metrics scoring
	sp := spt.spm[id]

	// Exponential decay of connect time `f(x) = exp(-λx)` where λ is our
	// exponential decay constant that we use to normalise the decay curve by
	// observed connect time over all providers; giving us a stronger signal
	// for relatively fast providers, but a long-tail toward a zero value for
	// slow providers.
	// https://en.wikipedia.org/wiki/Exponential_decay
	// If we have no connect time data, use the average, if we have no average
	// use 1.
	λ := 1 / float64(spt.overallConnectTimeMs.getValue(1))
	score += spt.config.ConnectTimeWeight * math.Exp(-λ*float64(sp.connectTimeMs.getValue(spt.overallConnectTimeMs.getValue(1))))

	// if we have no success data, treat it as fully successful
	score += spt.config.SuccessWeight * sp.success.getValue(1)

	// DEBUG: fmt.Printf("%s v=%v, f=%v, oconn=%d, conn=%d, success=%f, score=%f\n", string(id), v, f, spt.overallConnectTimeMs.value, sp.connectTimeMs.value, sp.success.value, score)
	return score
}
