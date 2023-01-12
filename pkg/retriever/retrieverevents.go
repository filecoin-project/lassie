package retriever

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RetrievalEventListener defines a type that receives events fired during a
// retrieval process, including the process of querying available storage
// providers to find compatible ones to attempt retrieval from.
type RetrievalEventListener interface {
	// IndexerProgress events occur during the Indexer process, stages.
	// Currently this includes a "started" event.
	IndexerProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code)

	// IndexerCandidates events occur after querying the indexer.
	// Currently this includes "candidates-found" and "candidates-filtered" events.
	IndexerCandidates(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code, storageProviderIds []peer.ID)

	// QueryProgress events occur during the query process, stages.
	// Currently this includes "started" and "connected" events.
	QueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code)

	// QueryFailure events occur on the failure of querying a storage
	// provider. A query will result in either a QueryFailure or
	// a QuerySuccess event.
	QueryFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)

	// QuerySuccess ("query-asked") events occur on successfully querying a storage
	// provider. A query will result in either a QueryFailure or
	// a QuerySuccess event.
	QuerySuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse)

	// RetrievalProgress events occur during the process of a retrieval. The
	// Success and failure progress event types are not reported here, but are
	// signalled via RetrievalSuccess or RetrievalFailure.
	RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code)

	// RetrievalSuccess events occur on the success of a retrieval. A retrieval
	// will result in either a QueryFailure or a QuerySuccess
	// event.
	RetrievalSuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool)

	// RetrievalFailure events occur on the failure of a retrieval. A retrieval
	// will result in either a QueryFailure or a QuerySuccess
	// event.
	RetrievalFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)
}

type eventExecution struct {
	ts time.Time
	cb func(timestamp time.Time, listener RetrievalEventListener)
}

type EventManager struct {
	ctx       context.Context
	lk        sync.RWMutex
	idx       int
	listeners map[int]RetrievalEventListener
	events    chan eventExecution
}

func NewEventManager(ctx context.Context) *EventManager {
	em := &EventManager{
		ctx:       ctx,
		listeners: make(map[int]RetrievalEventListener),
		events:    make(chan eventExecution, 16),
	}
	go em.loop()
	return em
}

func (em *EventManager) loop() {
	for {
		select {
		case <-em.ctx.Done():
			return
		case execution := <-em.events:
			em.lk.RLock()
			listeners := make([]RetrievalEventListener, 0, len(em.listeners))
			for _, listener := range em.listeners {
				listeners = append(listeners, listener)
			}
			em.lk.RUnlock()

			for _, listener := range listeners {
				execution.cb(execution.ts, listener)
			}
		}
	}
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

func (em *EventManager) queueEvent(cb func(timestamp time.Time, listener RetrievalEventListener)) {
	exec := eventExecution{time.Now(), cb} // time is _now_ even if we delay telling listeners
	select {
	case <-em.ctx.Done():
	case em.events <- exec:
	}
}

// FireQueryProgress calls QueryProgress for all listeners
func (em *EventManager) FireIndexerProgress(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerProgress(retrievalId, phaseStartTime, timestamp, requestedCid, stage)
	})
}

// FireIndexerCandidates calls IndexerCandidates for all listeners
func (em *EventManager) FireIndexerCandidates(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, stage eventpublisher.Code, storageProviderIds []peer.ID) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerCandidates(retrievalId, phaseStartTime, timestamp, requestedCid, stage, storageProviderIds)
	})
}

// FireQueryProgress calls QueryProgress for all listeners
func (em *EventManager) FireQueryProgress(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QueryProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

// FireQueryFailure calls QueryFailure for all listeners
func (em *EventManager) FireQueryFailure(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QueryFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}

// FireQuerySuccess calls QuerySuccess ("query-asked") for all listeners
func (em *EventManager) FireQuerySuccess(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QuerySuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, queryResponse)
	})
}

// FireRetrievalProgress calls RetrievalProgress for all listeners
func (em *EventManager) FireRetrievalProgress(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

// FireRetrievalSuccess calls RetrievalSuccess for all listeners
func (em *EventManager) FireRetrievalSuccess(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalSuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, receivedSize, receivedCids, confirmed)
	})
}

// FireRetrievalFailure calls RetrievalFailure for all listeners
func (em *EventManager) FireRetrievalFailure(retrievalId uuid.UUID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}
