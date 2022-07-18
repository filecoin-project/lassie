package filecoin

import (
	"sync"
	"time"

	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// RetrievalEventListener defines a type that receives events fired during a
// retrieval process, including the process of querying available storage
// providers to find compatible ones to attempt retrieval from.
type RetrievalEventListener interface {
	// RetrievalQueryStart defines the start of the process of querying all
	// storage providers that are known to have this CID
	RetrievalQueryStart(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, candidateCount int)

	// RetrievalQueryProgress events occur during the query process, stages
	// include: connect and query-ask
	RetrievalQueryProgress(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode)

	// RetrievalQueryFailure events occur on the failure of querying a storage
	// provider. A query will result in either a RetrievalQueryFailure or
	// a RetrievalQuerySuccess event.
	RetrievalQueryFailure(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, err error)

	// RetrievalQueryFailure events occur on successfully querying a storage
	// provider. A query will result in either a RetrievalQueryFailure or
	// a RetrievalQuerySuccess event.
	RetrievalQuerySuccess(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse)

	// RetrievalStart events are fired at the beginning of retrieval-proper, once
	// a storage provider is selected for retrieval. Note that upon failure,
	// another storage provider may be selected so multiple RetrievalStart events
	// may occur for the same retrieval.
	RetrievalStart(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, rootCid cid.Cid, storageProviderId peer.ID)

	// RetrievalProgress events occur during the process of a retrieval. The
	// Success and failure progress event types are not reported here, but are
	// signalled via RetrievalSuccess or RetrievalFailure.
	RetrievalProgress(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode)

	// RetrievalSuccess events occur on the success of a retrieval. A retrieval
	// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
	// event.
	RetrievalSuccess(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, retrievalStats filclient.RetrievalStats)

	// RetrievalFailure events occur on the failure of a retrieval. A retrieval
	// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
	// event.
	RetrievalFailure(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, err error)
}

type eventManager struct {
	lk        sync.RWMutex
	idx       int
	listeners map[int]RetrievalEventListener
}

func newEventManager() *eventManager {
	return &eventManager{listeners: make(map[int]RetrievalEventListener)}
}

func (em *eventManager) RegisterListener(listener RetrievalEventListener) func() {
	em.lk.Lock()
	defer em.lk.Unlock()

	idx := em.idx
	em.idx++
	em.listeners[idx] = listener

	// return unregister function
	return func() {
		em.lk.Lock()
		defer em.lk.Unlock()
		delete(em.listeners, idx)
	}
}

func (em *eventManager) fireEvent(cb func(timestamp time.Time, listener RetrievalEventListener)) {
	timestamp := time.Now()
	go func() {
		em.lk.RLock()
		listeners := make([]RetrievalEventListener, 0)
		for _, listener := range em.listeners {
			listeners = append(listeners, listener)
		}
		em.lk.RUnlock()

		for _, listener := range listeners {
			cb(timestamp, listener)
		}
	}()
}

func (em *eventManager) FireRetrievalQueryStart(retrievalId uuid.UUID, requestedCid cid.Cid, candidateCount int) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQueryStart(retrievalId, timestamp, requestedCid, candidateCount)
	})
}

func (em *eventManager) FireRetrievalQueryProgress(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQueryProgress(retrievalId, timestamp, requestedCid, storageProviderId, stage)
	})
}

func (em *eventManager) FireRetrievalQueryFailure(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, err error) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQueryFailure(retrievalId, timestamp, requestedCid, storageProviderId, err)
	})
}

func (em *eventManager) FireRetrievalQuerySuccess(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQuerySuccess(retrievalId, timestamp, requestedCid, storageProviderId, queryResponse)
	})
}

func (em *eventManager) FireRetrievalStart(retrievalId uuid.UUID, requestedCid cid.Cid, rootCid cid.Cid, storageProviderId peer.ID) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalStart(retrievalId, timestamp, requestedCid, rootCid, storageProviderId)
	})
}

func (em *eventManager) FireRetrievalProgress(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalProgress(retrievalId, timestamp, requestedCid, storageProviderId, stage)
	})
}

func (em *eventManager) FireRetrievalSuccess(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, retrievalStats filclient.RetrievalStats) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalSuccess(retrievalId, timestamp, requestedCid, storageProviderId, retrievalStats)
	})
}

func (em *eventManager) FireRetrievalFailure(retrievalId uuid.UUID, requestedCid cid.Cid, storageProviderId peer.ID, err error) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalFailure(retrievalId, timestamp, requestedCid, storageProviderId, err)
	})
}
