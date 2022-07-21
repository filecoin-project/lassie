package filecoin

import (
	"sync"
	"time"

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
	// RetrievalQueryProgress events occur during the query process, stages
	// include: connect and query-ask
	RetrievalQueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode)

	// RetrievalQueryFailure events occur on the failure of querying a storage
	// provider. A query will result in either a RetrievalQueryFailure or
	// a RetrievalQuerySuccess event.
	RetrievalQueryFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)

	// RetrievalQueryFailure events occur on successfully querying a storage
	// provider. A query will result in either a RetrievalQueryFailure or
	// a RetrievalQuerySuccess event.
	RetrievalQuerySuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse)

	// RetrievalProgress events occur during the process of a retrieval. The
	// Success and failure progress event types are not reported here, but are
	// signalled via RetrievalSuccess or RetrievalFailure.
	RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode)

	// RetrievalSuccess events occur on the success of a retrieval. A retrieval
	// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
	// event.
	RetrievalSuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64)

	// RetrievalFailure events occur on the failure of a retrieval. A retrieval
	// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
	// event.
	RetrievalFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)
}

type EventManager struct {
	lk        sync.RWMutex
	idx       int
	listeners map[int]RetrievalEventListener
}

func NewEventManager() *EventManager {
	return &EventManager{listeners: make(map[int]RetrievalEventListener)}
}

func (em *EventManager) RegisterListener(listener RetrievalEventListener) func() {
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

func (em *EventManager) fireEvent(cb func(timestamp time.Time, listener RetrievalEventListener)) {
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

func (em *EventManager) FireRetrievalQueryProgress(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQueryProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

func (em *EventManager) FireRetrievalQueryFailure(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQueryFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}

func (em *EventManager) FireRetrievalQuerySuccess(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalQuerySuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, queryResponse)
	})
}

func (em *EventManager) FireRetrievalProgress(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

func (em *EventManager) FireRetrievalSuccess(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, receivedSize uint64) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalSuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, receivedSize)
	})
}

func (em *EventManager) FireRetrievalFailure(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.fireEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}
