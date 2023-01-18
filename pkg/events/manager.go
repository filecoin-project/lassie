package events

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type eventExecution struct {
	ts time.Time
	cb func(timestamp time.Time, listener RetrievalEventListener)
}

// EventManager is responsible for dispatching events to registered listeners.
// Events are dispatched asynchronously, so listeners should not assume that
// events are received within the window of a blocking retriever.Retrieve()
// call.
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

// NewEventManager creates a new EventManager. Start() must be called to start
// the event loop.
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

// Start starts the event loop. Start() must be called before any events can be
// dispatched. A channel is returned that will receive a single value when the
// event loop has started.
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

// IsStarted returns true if the event loop has been started.
func (em *EventManager) IsStarted() bool {
	em.lk.RLock()
	defer em.lk.RUnlock()
	return em.started
}

// Stop stops the event loop. A channel is returned that will receive a single
// value when the event loop has stopped.
func (em *EventManager) Stop() chan struct{} {
	em.cancel()
	return em.stopped
}

// RegisterListener registers a listener to receive events. The returned
// function can be called to unregister the listener.
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
func (em *EventManager) FireIndexerProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, stage Code) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerProgress(retrievalId, phaseStartTime, timestamp, requestedCid, stage)
	})
}

// FireIndexerCandidates calls IndexerCandidates for all listeners
func (em *EventManager) FireIndexerCandidates(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, stage Code, storageProviderIds []peer.ID) {
	em.queueEvent(func(timestamp time.Time, listener RetrievalEventListener) {
		listener.IndexerCandidates(retrievalId, phaseStartTime, timestamp, requestedCid, stage, storageProviderIds)
	})
}

// FireQueryProgress calls QueryProgress for all listeners
func (em *EventManager) FireQueryProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage Code) {
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
func (em *EventManager) FireRetrievalProgress(retrievalId types.RetrievalID, requestedCid cid.Cid, phaseStartTime time.Time, storageProviderId peer.ID, stage Code) {
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
