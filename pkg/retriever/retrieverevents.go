package retriever

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RetrievalEventListener defines a type that receives events fired during a
// retrieval process, including the process of querying available storage
// providers to find compatible ones to attempt retrieval from.
type RetrievalEventListener interface {
	// IndexerProgress events occur during the Indexer process, stages.
	// Currently this includes a "started" event.
	IndexerProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code)

	// IndexerCandidates events occur after querying the indexer.
	// Currently this includes "candidates-found" and "candidates-filtered" events.
	IndexerCandidates(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code, storageProviderIds []peer.ID)

	// QueryProgress events occur during the query process, stages.
	// Currently this includes "started" and "connected" events.
	QueryProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code)

	// QueryFailure events occur on the failure of querying a storage
	// provider. A query will result in either a QueryFailure or
	// a QuerySuccess event.
	QueryFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)

	// QuerySuccess ("query-asked") events occur on successfully querying a storage
	// provider. A query will result in either a QueryFailure or
	// a QuerySuccess event.
	QuerySuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse)

	// RetrievalProgress events occur during the process of a retrieval. The
	// Success and failure progress event types are not reported here, but are
	// signalled via RetrievalSuccess or RetrievalFailure.
	RetrievalProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code)

	// RetrievalSuccess events occur on the success of a retrieval. A retrieval
	// will result in either a QueryFailure or a QuerySuccess
	// event.
	RetrievalSuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool)

	// RetrievalFailure events occur on the failure of a retrieval. A retrieval
	// will result in either a QueryFailure or a QuerySuccess
	// event.
	RetrievalFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string)
}

type eventExecution struct {
	ts time.Time
	cb func(timestamp time.Time, listener RetrievalEventListener)
}

type EventManager struct {
	ctx       context.Context
	lk        sync.RWMutex
	idx       int
	started   bool
	listeners map[int]RetrievalEventListener
	events    chan eventExecution
	cancel    context.CancelFunc
	stopped   chan struct{}
}

func NewEventManager(ctx context.Context) *EventManager {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	return &EventManager{
		ctx:       ctx,
		cancel:    cancel,
		listeners: make(map[int]RetrievalEventListener),
		events:    make(chan eventExecution, 16),
		stopped:   make(chan struct{}, 1),
	}
}

func (em *EventManager) Start() chan struct{} {
	startChan := make(chan struct{})
	go func() {
		em.lk.Lock()
		em.started = true
		em.lk.Unlock()
		startChan <- struct{}{}
		for {
			select {
			case <-em.ctx.Done():
				em.stopped <- struct{}{}
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
	}()
	return startChan
}

func (em *EventManager) IsStarted() bool {
	em.lk.RLock()
	defer em.lk.RUnlock()
	return em.started
}

func (em *EventManager) Stop() chan struct{} {
	em.cancel()
	return em.stopped
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
	if em.ctx.Err() != nil {
		return
	}
	select {
	case <-em.ctx.Done():
	case em.events <- exec:
	}
}

// FireQueryProgress calls QueryProgress for all listeners
func (em *EventManager) FireIndexerProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerProgress(retrievalId, phaseStartTime, timestamp, requestedCid, stage)
	})
}

// FireIndexerCandidates calls IndexerCandidates for all listeners
func (em *EventManager) FireIndexerCandidates(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, stage eventpublisher.Code, storageProviderIds []peer.ID) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerCandidates(retrievalId, phaseStartTime, timestamp, requestedCid, stage, storageProviderIds)
	})
}

// FireQueryProgress calls QueryProgress for all listeners
func (em *EventManager) FireQueryProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QueryProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

// FireQueryFailure calls QueryFailure for all listeners
func (em *EventManager) FireQueryFailure(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QueryFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}

// FireQuerySuccess calls QuerySuccess ("query-asked") for all listeners
func (em *EventManager) FireQuerySuccess(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.QuerySuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, queryResponse)
	})
}

// FireRetrievalProgress calls RetrievalProgress for all listeners
func (em *EventManager) FireRetrievalProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage eventpublisher.Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalProgress(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, stage)
	})
}

// FireRetrievalSuccess calls RetrievalSuccess for all listeners
func (em *EventManager) FireRetrievalSuccess(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalSuccess(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, receivedSize, receivedCids, confirmed)
	})
}

// FireRetrievalFailure calls RetrievalFailure for all listeners
func (em *EventManager) FireRetrievalFailure(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, errString string) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.RetrievalFailure(retrievalId, phaseStartTime, timestamp, requestedCid, storageProviderId, errString)
	})
}
