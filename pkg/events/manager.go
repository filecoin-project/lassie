package events

import (
	"context"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
)

// EventManager is responsible for dispatching events to registered subscribers.
// Events are dispatched asynchronously, so subscribers should not assume that
// events are received within the window of a blocking retriever.Retrieve()
// call.
type EventManager struct {
	ctx         context.Context
	lk          sync.RWMutex
	idx         int
	started     bool
	subscribers map[int]types.RetrievalEventSubscriber
	events      chan types.RetrievalEvent
	cancel      context.CancelFunc
	stopped     chan struct{}
}

// NewEventManager creates a new EventManager. Start() must be called to start
// the event loop.
func NewEventManager(ctx context.Context) *EventManager {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	return &EventManager{
		ctx:         ctx,
		cancel:      cancel,
		subscribers: make(map[int]types.RetrievalEventSubscriber),
		events:      make(chan types.RetrievalEvent, 16),
		stopped:     make(chan struct{}, 1),
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
			case event := <-em.events:
				em.lk.RLock()
				subscribers := make([]types.RetrievalEventSubscriber, 0, len(em.subscribers))
				for _, subscriber := range em.subscribers {
					subscribers = append(subscribers, subscriber)
				}
				em.lk.RUnlock()

				for _, subscriber := range subscribers {
					subscriber(event)
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

// RegisterSubscriber registers a subscriber to receive events. The returned
// function can be called to unregister the subscriber.
func (em *EventManager) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	em.lk.Lock()
	defer em.lk.Unlock()

	idx := em.idx
	em.idx++
	em.subscribers[idx] = subscriber

	// return unregister function
	return func() {
		em.lk.Lock()
		defer em.lk.Unlock()
		delete(em.subscribers, idx)
	}
}

// DispatchEvent queues the event to be dispatched to all event subscribers.
// Calling the subscriber functions happens on a separate goroutine dedicated
// to this function.
func (em *EventManager) DispatchEvent(event types.RetrievalEvent) {
	if em.ctx.Err() != nil {
		return
	}
	select {
	case <-em.ctx.Done():
	case em.events <- event:
	}
}
