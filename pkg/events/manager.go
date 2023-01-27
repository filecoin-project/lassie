package events

import (
	"container/list"
	"context"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
)

type indexedSubscriber struct {
	idx        int
	subscriber types.RetrievalEventSubscriber
}

// EventManager is responsible for dispatching events to registered subscribers.
// Events are dispatched asynchronously, so subscribers should not assume that
// events are received within the window of a blocking retriever.Retrieve()
// call.
type EventManager struct {
	ctx         context.Context
	lk          sync.RWMutex
	idx         int
	started     bool
	subscribers []indexedSubscriber
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
		subscribers: make([]indexedSubscriber, 0),
		events:      make(chan types.RetrievalEvent),
		stopped:     make(chan struct{}, 1),
	}
}

// Start starts the event loop. Start() must be called before any events can be
// dispatched.
func (em *EventManager) Start() {
	queue := list.New()
	toProcess := make(chan types.RetrievalEvent)

	// process events
	go func() {
		for {
			select {
			case event := <-toProcess:
				em.lk.RLock()
				// make a copy of the subscribers slice so that we don't hold the lock
				subscribers := append([]indexedSubscriber{}, em.subscribers...)
				em.lk.RUnlock()

				for _, subscriber := range subscribers {
					subscriber.subscriber(event)
				}
			case <-em.ctx.Done():
				em.stopped <- struct{}{}
				return
			}
		}
	}()

	// receive events and queue them up for processing
	go func() {
		outgoing := func() chan<- types.RetrievalEvent {
			if queue.Len() == 0 {
				return nil // no events to process, will prevent the select from sending
			}
			return toProcess
		}
		nextEvent := func() types.RetrievalEvent {
			if queue.Len() == 0 {
				return nil
			}
			return queue.Front().Value.(types.RetrievalEvent)
		}
		for {
			select {
			case event := <-em.events:
				queue.PushBack(event)
			case outgoing() <- nextEvent(): // if events to process, setup channel and send
				queue.Remove(queue.Front())
			case <-em.ctx.Done():
				return
			}
		}
	}()

	em.lk.Lock()
	em.started = true
	em.lk.Unlock()
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
	em.subscribers = append(em.subscribers, indexedSubscriber{idx, subscriber})

	// return unregister function
	return func() {
		em.lk.Lock()
		defer em.lk.Unlock()
		for i, s := range em.subscribers {
			if s.idx == idx {
				em.subscribers = append(em.subscribers[:i], em.subscribers[i+1:]...)
				return
			}
		}
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
