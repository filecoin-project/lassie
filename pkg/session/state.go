package session

import (
	"fmt"
	"math"
	"sort"
	"strings"
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

	// EndRetrieval cleans up an existing retrieval.
	EndRetrieval(retrievalId types.RetrievalID) error

	// RecordConnectTime records the time it took to connect to a storage
	// provider. This is used for prioritisation of storage providers and is
	// recorded with a decay according to SessionStateConfig#ConnectTimeAlpha.
	RecordConnectTime(storageProviderId peer.ID, connectTime time.Duration)

	// RecordFirstByteTime records the time it took to receive the first byte
	// from a storage provider. This is used for prioritisation of storage
	// providers and is recorded with a decay according to
	// SessionStateConfig#FirstByteTimeAlpha.
	RecordFirstByteTime(storageProviderId peer.ID, firstByteTime time.Duration)

	// RecordFailure records a failure for a storage provider. This is used for
	// prioritisation of storage providers and is recorded as a success=0 with a
	// decay according to SessionStateConfig#SuccessAlpha.
	RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error

	// RecordSuccess records a success for a storage provider. This is used for
	// prioritisation of storage providers and is recorded as a success=1 with a
	// decay according to SessionStateConfig#SuccessAlpha. Bandwidth is also
	// used for prioritisation and is recorded with a decay according to
	// SessionStateConfig#BandwidthAlpha.
	RecordSuccess(storageProviderId peer.ID, bandwidthBytesPerSecond uint64)

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
	concurrency     uint
	connectTimeMs   metric[uint64]
	firstByteTimeMs metric[uint64]
	bandwidthBps    metric[uint64]
	success         metric[float64]
}

type SessionState struct {
	lk     sync.RWMutex
	config *Config
	// active retrievals
	arm map[types.RetrievalID]activeRetrieval
	// failures and concurrency of storage providers
	spm                    map[peer.ID]storageProvider
	overallConnectTimeMs   metric[uint64]
	overallFirstByteTimeMs metric[uint64]
	overallBandwidthBps    metric[uint64]
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

func (spt *SessionState) removeFromRetrieval(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
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
	spt.lk.Lock()
	defer spt.lk.Unlock()

	// remove from this retrieval to free up the SP to be tried again for a future retrieval
	if err := spt.removeFromRetrieval(retrievalId, storageProviderId); err != nil {
		return err
	}

	spt.recordSuccessMetric(storageProviderId, 0)
	return nil
}

func (spt *SessionState) recordSuccessMetric(storageProviderId peer.ID, current float64) {
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
func (spt *SessionState) RecordSuccess(storageProviderId peer.ID, bandwidthBytesPerSecond uint64) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	spt.recordSuccessMetric(storageProviderId, 1)

	status := spt.spm[storageProviderId]
	// EMA of bandwidth
	if !status.bandwidthBps.initialized {
		status.bandwidthBps.initialized = true
		status.bandwidthBps.value = bandwidthBytesPerSecond
	} else {
		status.bandwidthBps.value = uint64((1-spt.config.BandwidthAlpha)*float64(bandwidthBytesPerSecond) + spt.config.BandwidthAlpha*float64(status.bandwidthBps.value))
	}
	spt.spm[storageProviderId] = status

	if !spt.overallBandwidthBps.initialized {
		spt.overallBandwidthBps.initialized = true
		spt.overallBandwidthBps.value = bandwidthBytesPerSecond
	} else {
		spt.overallBandwidthBps.value = uint64((1-spt.config.OverallBandwidthAlpha)*float64(bandwidthBytesPerSecond) + spt.config.OverallBandwidthAlpha*float64(spt.overallBandwidthBps.value))
	}
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

func (spt *SessionState) RecordFirstByteTime(storageProviderId peer.ID, current time.Duration) {
	spt.lk.Lock()
	defer spt.lk.Unlock()

	currentMs := uint64(current.Milliseconds())

	status := spt.spm[storageProviderId]
	// EMA of firstByte time
	if !status.firstByteTimeMs.initialized {
		status.firstByteTimeMs.initialized = true
		status.firstByteTimeMs.value = currentMs
	} else {
		status.firstByteTimeMs.value = uint64((1-spt.config.FirstByteTimeAlpha)*float64(currentMs) + spt.config.FirstByteTimeAlpha*float64(status.firstByteTimeMs.value))
	}
	spt.spm[storageProviderId] = status

	if !spt.overallFirstByteTimeMs.initialized {
		spt.overallFirstByteTimeMs.initialized = true
		spt.overallFirstByteTimeMs.value = currentMs
	} else {
		spt.overallFirstByteTimeMs.value = uint64((1-spt.config.OverallFirstByteTimeAlpha)*float64(currentMs) + spt.config.OverallFirstByteTimeAlpha*float64(spt.overallFirstByteTimeMs.value))
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
		scores[ii] = spt.scoreProvider(p)
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
	sb := strings.Builder{}
	sb.WriteString("internal error - failed to choose a provider from: ")
	for _, pi := range ind {
		sb.WriteString(fmt.Sprintf("%s: %f, ", peers[pi], scores[pi]))
	}
	sb.WriteString(fmt.Sprintf("with roll of %f", r))
	panic(sb.String())
}

// scoreProvider returns a score for a given provider, higher is better.
// This method assumes the caller holds lock.
// HTTP-only: scoring based on collected metrics (connect time, bandwidth, success rate)
func (spt *SessionState) scoreProvider(id peer.ID) float64 {
	var score float64

	// collected metrics scoring
	sp := spt.spm[id]

	score += expDecay(spt.overallConnectTimeMs, sp.connectTimeMs, spt.config.ConnectTimeWeight)
	score += expDecay(spt.overallFirstByteTimeMs, sp.firstByteTimeMs, spt.config.FirstByteTimeWeight)
	score += expDecay(spt.overallBandwidthBps, sp.bandwidthBps, spt.config.BandwidthWeight)

	// if we have no success data, treat it as fully successful
	score += spt.config.SuccessWeight * sp.success.getValue(1)

	return score
}

// expDecay calculates the exponential decay of metric `x`: `f(x) =
// exp(-λx)` where λ is our exponential decay constant that we use to
// normalise the decay curve by observed values over all providers; giving us a
// stronger signal for providers with lower values of `x`, but a long-tail
// toward a zero value for providers with higher values of `x`.
//
// The function takes in three parameters:
// - overall: the overall `x` of all providers
// - current: the `x` of the current provider
// - weight: the weight to be applied to the exponential decay
//
// We use this for each of the metrics that we don't have a good pre-defined
// normalisation method for. Timing metrics will depend on client conditions so
// we can't use a fixed normalisation for them.
func expDecay(overall metric[uint64], current metric[uint64], weight float64) float64 {
	o := overall.getValue(1)
	if o == 0 { // avoid divide by zero
		o = 1
	}
	λ := 1 / float64(o)
	return weight * math.Exp(-λ*float64(current.getValue(o)))
}
