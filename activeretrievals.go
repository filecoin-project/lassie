package filecoin

import (
	"sync"
	"time"

	"github.com/application-research/filclient/rep"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type activeRetrieval struct {
	retrievalId              uuid.UUID
	retrievalCid             cid.Cid
	rootCid                  cid.Cid // for Estuary candidates, this can be different to retrievalCid & retrievals will report it so we have to track it
	currentStorageProviderId peer.ID // for tracking concurrent SP counts
	queryStartTime           time.Time
	queryCandidateCount      int // number we expect queriesFinished to reach before query phase ends
	queriesFinished          int
	retrievalStartTime       time.Time
	retrievalCandidateCount  int // number we expect retrievalsFinished to reach before retrieval phase ends
	retrievalsFinished       int
}

// ActiveRetrievalsManager tracks the lifecycle of an active retrieval. This
// currently includes two distinct phases: query, and retrieval. Each phase can
// involve multiple storage providers. The query phase is assumed to be a
// parallel operation while the retrieval phase is serial and we can therefore
// track which single storage provider is currently involved in the retrieval.
//
// One purpose of the retrieval manager is to track the concurrency of
// retrievals from individual storage providers. When attempting to set the
// current storage provider to perform a retrieval with using the
// SetRetrievalCandidate() call, the maximum number of concurrent retrievals
// from that storage provider can be provided. We can then scan the currently
// active retrievals and check that we don't exceed the maximum.
//
// The second purpose is to attach a UUID to each retrieval and match that to
// events we receive from filclient so we can report each retrieval (all phases)
// using a single unique identifier. Note that it is possible for the retrieval
// phase to be operating against a different CID than the originally requested,
// (rootCid vs retrievalCid - when using Estuary rather than the indexer). So
// finding the right active retrieval is not straightforward.
//
// Determining the "end" of a retrieval is non-trivial since we need to match
// the number of successes and failures of each phase against the expected
// number for that phase. See maybeFinish().
type ActiveRetrievalsManager struct {
	arMap map[cid.Cid]*activeRetrieval
	lk    sync.RWMutex
}

// NewActiveRetrievalsManager instantiates a new ActiveRetrievalsManager
func NewActiveRetrievalsManager() *ActiveRetrievalsManager {
	return &ActiveRetrievalsManager{arMap: make(map[cid.Cid]*activeRetrieval)}
}

// New registers a new retrieval, setting the expected number of storage
// provider candidates for each phase. Typically the retrievalCandidateCount
// is unknown but setting it to zero may mean that a retrieval is considered
// finished before we have a chance to set the correct number using a
// subsequent call to SetRetrievalCandidateCount().
//
// A unique ID is returned for the new retrieval that can be used to identify it
// over the entire lifecycle.
func (arm *ActiveRetrievalsManager) New(retrievalCid cid.Cid, queryCandidateCount int, retrievalCandidateCount int) (uuid.UUID, error) {
	retrievalId, err := uuid.NewRandom()
	if err != nil {
		return uuid.UUID{}, err
	}

	arm.lk.Lock()
	defer arm.lk.Unlock()

	if _, ok := arm.findActiveRetrievalFor(retrievalCid); ok {
		return uuid.UUID{}, ErrRetrievalAlreadyRunning{retrievalCid}
	}

	arm.arMap[retrievalCid] = &activeRetrieval{
		retrievalId:              retrievalId,
		retrievalCid:             retrievalCid,
		rootCid:                  retrievalCid,
		currentStorageProviderId: "",
		queryStartTime:           time.Now(),
		queryCandidateCount:      queryCandidateCount,
		queriesFinished:          0,
		retrievalStartTime:       time.Time{},
		retrievalCandidateCount:  retrievalCandidateCount,
		retrievalsFinished:       0,
	}

	log.Debugf("Registered new active retrieval for %s (%d active, %d/%d query candidates)", retrievalCid, len(arm.arMap), 0, queryCandidateCount)

	return retrievalId, nil
}

// SetRetrievalCandidateCount updates the number of storage provider candidates
// once we know the number. When the number of finished retrievals equals this
// number and the number of finished queries equals the query candidate count
// the full retrieval is considered complete and can be cleaned up.
func (arm *ActiveRetrievalsManager) SetRetrievalCandidateCount(retrievalCid cid.Cid, candidateCount int) {
	arm.lk.Lock()
	defer arm.lk.Unlock()

	ar, found := arm.findActiveRetrievalFor(retrievalCid)
	if !found {
		log.Errorf("Unexpected active retrieval SetRetrievalCandidateCount for %s", retrievalCid)
		return
	}
	ar.retrievalCandidateCount = candidateCount
	ar.retrievalStartTime = time.Now()
	log.Debugf("Updated active retrieval for %s to retrieval phase (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
	arm.maybeFinish(retrievalCid, ar)
}

// GetStatusFor fetches basic information for a retrieval, identified by the
// original retrieval CID (which may be different to the CID a storage provider
// is being asked for). The phase is provided here in order to determine which
// start time to return (query or retrieval).
func (arm *ActiveRetrievalsManager) GetStatusFor(retrievalCid cid.Cid, phase rep.Phase) (uuid.UUID, cid.Cid, time.Time, bool) {
	arm.lk.RLock()
	defer arm.lk.RUnlock()
	ar, found := arm.findActiveRetrievalFor(retrievalCid)
	if !found {
		return uuid.UUID{}, cid.Undef, time.Time{}, false
	}
	phaseStart := ar.queryStartTime
	if phase == rep.RetrievalPhase {
		phaseStart = ar.retrievalStartTime
	}
	return ar.retrievalId, ar.retrievalCid, phaseStart, true
}

// GetActiveRetrievalCountFor returns the number of active retrievals for a
// storage provider. Active retrievals only count the retrieval phase, not the
// query phase requests.
func (arm *ActiveRetrievalsManager) GetActiveRetrievalCountFor(storageProviderId peer.ID) uint {
	arm.lk.RLock()
	defer arm.lk.RUnlock()
	return arm.countRetrievalsFor(storageProviderId)
}

// countRetrievalsFor counts the number of active retrievals for a
// storageProviderId. This function requires a read lock on arm.lk!
func (arm *ActiveRetrievalsManager) countRetrievalsFor(storageProviderId peer.ID) uint {
	var currentRetrievals uint
	for _, ret := range arm.arMap {
		if ret.currentStorageProviderId == storageProviderId {
			currentRetrievals++
		}
	}
	return currentRetrievals
}

// SetRetrievalCandidate updates the current storage provider that we are
// performing a retrieval (not query) from. It also sets the root CID for this
// candidate, which may be different to the originally requested CID.
//
// If the maxConcurrent is non-zero, the number of concurrent retrievals (not
// including queries) being performed from this storage provider is checked and
// if an additional retrieval would exceed this number ErrHitRetrievalLimit is
// returned without performing the update.
func (arm *ActiveRetrievalsManager) SetRetrievalCandidate(retrievalCid, rootCid cid.Cid, storageProviderId peer.ID, maxConcurrent uint) error {
	arm.lk.Lock()
	defer arm.lk.Unlock()

	ar, found := arm.findActiveRetrievalFor(retrievalCid)
	if !found {
		log.Errorf("Unexpected active retrieval SetRetrievalCandidate for %s", retrievalCid)
		return ErrUnexpectedRetrieval
	}

	// If limit is enabled (non-zero) and we have already hit it, we can't
	// allow this retrieval to start
	if maxConcurrent > 0 {
		if arm.countRetrievalsFor(storageProviderId) >= maxConcurrent {
			ar.retrievalsFinished++ // this retrieval won't start so we won't get events for it, treat it as finished
			arm.maybeFinish(retrievalCid, ar)
			return ErrHitRetrievalLimit
		}
	}

	ar.currentStorageProviderId = storageProviderId
	ar.rootCid = rootCid

	return nil
}

// QueryCandidatedFinished registers that a query has finished with one of the
// query candidates. It's used to increment the number of finished queries and
// also check whether cleanup may be necessary.
func (arm *ActiveRetrievalsManager) QueryCandidatedFinished(retrievalCid cid.Cid) {
	arm.lk.Lock()
	defer arm.lk.Unlock()

	ar, found := arm.findActiveRetrievalFor(retrievalCid)
	if !found {
		log.Errorf("Unexpected active retrieval QueryCandidatedFinished for %s (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
		return
	}
	ar.queriesFinished++
	log.Debugf("QueryCandidatedFinished for %s (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
	arm.maybeFinish(retrievalCid, ar)
}

// RetrievalCandidatedFinished registers that a retrieval has finished with one
// of the query candidates. In the case of a failure, it's used to increment the
// number of finished so we can check the finished count against the number of
// expected candidates for possible clean-up. In the case of success, we can
// assume no more retrievals will occur so we can jump straight to clean-up.
func (arm *ActiveRetrievalsManager) RetrievalCandidatedFinished(retrievalCid cid.Cid, success bool) {
	arm.lk.Lock()
	defer arm.lk.Unlock()

	ar, found := arm.findActiveRetrievalFor(retrievalCid)
	if !found {
		log.Errorf("Unexpected active retrieval RetrievalCandidatedFinished for %s (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
		return
	}
	ar.retrievalsFinished++
	if success {
		// if success, there won't be any more retrieval attempts so we can short-circuit clean-up
		ar.retrievalCandidateCount = ar.retrievalsFinished
	}
	log.Debugf("RetrievalCandidatedFinished for %s (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
	arm.maybeFinish(retrievalCid, ar)
}

// findActiveRetrievalFor looks up a retrieval in the map by CID but will also
// search for rootCid matches as a secondary option since they can also be
// identified by that way.
// Note that one implication of looking up a CID in both places is that calls to
// New() will return ErrRetrievalAlreadyRunning if that CID is involved as
// either a primary CID or a root CID. But this is reasonable since a successful
// retrieval will yield the newly requested CID regardless.
func (arm *ActiveRetrievalsManager) findActiveRetrievalFor(cid cid.Cid) (*activeRetrieval, bool) {
	ar, ok := arm.arMap[cid]
	if ok {
		return ar, true
	}
	// look for the CID as rootCid across all running retrievals
	for _, ar := range arm.arMap {
		if cid.Equals(ar.rootCid) {
			return ar, true
		}
	}
	return nil, false
}

// maybeFinish checks whether we've completed all expected queries and
// retrievals, and if so, cleans up the record. "complete" means that we've
// recorded a finish for the expected number of queries and retrievals.
// This function expects the lock to be held by the caller!
func (arm *ActiveRetrievalsManager) maybeFinish(retrievalCid cid.Cid, ar *activeRetrieval) {
	if ar.queriesFinished >= ar.queryCandidateCount && ar.retrievalsFinished >= ar.retrievalCandidateCount {
		if ar.retrievalCid.Equals(retrievalCid) {
			delete(arm.arMap, retrievalCid)
		} else {
			for _, ar := range arm.arMap {
				if retrievalCid.Equals(ar.rootCid) {
					delete(arm.arMap, ar.retrievalCid)
					break
				}
			}
		}
		log.Debugf("Unregistered active retrieval for %s (%d active, %d/%d query candidates, %d/%d retrieval candidates)", retrievalCid, len(arm.arMap), ar.queriesFinished, ar.queryCandidateCount, ar.retrievalsFinished, ar.retrievalCandidateCount)
	}
}
