package eventpublisher

import (
	"context"
	"sync"
)

type RetrievalSubscriber interface {
	OnRetrievalEvent(RetrievalEvent)
}

type RetrievalEventPublisher struct {
	closing chan struct{}
	closed  chan struct{}
	// Lock for the subscribers list
	subscribersLk sync.RWMutex
	// The list of subscribers
	subscribers map[int]RetrievalSubscriber
	idx         int
	events      chan RetrievalEvent
}

// A return function unsubscribing a subscribed Subscriber via the Subscribe() function
type UnsubscribeFn func()

func NewEventPublisher() *RetrievalEventPublisher {
	ep := &RetrievalEventPublisher{
		subscribers: make(map[int]RetrievalSubscriber, 0),
		events:      make(chan RetrievalEvent, 16),
		closing:     make(chan struct{}),
		closed:      make(chan struct{}),
	}

	return ep
}

func (ep *RetrievalEventPublisher) Start() error {
	go ep.loop()
	return nil
}

func (ep *RetrievalEventPublisher) Stop(ctx context.Context) error {
	close(ep.closing)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ep.closed:
		return nil
	}
}

func (ep *RetrievalEventPublisher) loop() {
	defer func() {
		close(ep.closed)
	}()
	for {
		select {
		case <-ep.closing:
			return
		case event := <-ep.events:
			ep.subscribersLk.RLock()
			subscribers := make([]RetrievalSubscriber, 0, len(ep.subscribers))
			for _, subscriber := range ep.subscribers {
				subscribers = append(subscribers, subscriber)
			}
			ep.subscribersLk.RUnlock()

			for _, subscriber := range subscribers {
				subscriber.OnRetrievalEvent(event)
			}
		}
	}
}

func (ep *RetrievalEventPublisher) Subscribe(subscriber RetrievalSubscriber) UnsubscribeFn {
	// Lock writes on the subscribers list
	ep.subscribersLk.Lock()
	defer ep.subscribersLk.Unlock()

	// increment the index so we can assign a unique one to this subscriber so
	// our unregister function works
	idx := ep.idx
	ep.idx++
	ep.subscribers[idx] = subscriber

	// return unregister function
	return func() {
		ep.subscribersLk.Lock()
		defer ep.subscribersLk.Unlock()
		delete(ep.subscribers, idx)
	}
}

func (ep *RetrievalEventPublisher) Publish(event RetrievalEvent) {
	select {
	case <-ep.closing:
	case ep.events <- event:
	}
}

// Returns the number of retrieval event subscribers
func (ep *RetrievalEventPublisher) SubscriberCount() int {
	ep.subscribersLk.RLock()
	defer ep.subscribersLk.RUnlock()
	return len(ep.subscribers)
}
