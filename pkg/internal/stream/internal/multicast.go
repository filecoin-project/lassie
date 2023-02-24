package internal

import (
	"sync"
	"sync/atomic"

	"github.com/filecoin-project/lassie/pkg/types"
)

type MulticastReplay[T any] struct {
	Stream[T]
	Subscriber[T]
	lk   sync.Mutex
	last *T
	err  error

	subscribers []types.StreamSubscriber[T]

	cachedSubscriberLk sync.RWMutex
	cachedSubscribers  *[]types.StreamSubscriber[T]
}

func NewMulticast[T any]() *MulticastReplay[T] {
	m := &Multicast[T]{}
	m.Stream = NewStream[T]()
}

func (m *MulticastReplay[T]) onNext(val T) {
	m.last = &val
	m.cachedSubscriberLk.Lock()
	if m.cachedSubscribers == nil {
		m.lk.Lock()
		*m.cachedSubscribers = append([]types.StreamSubscriber[T](nil), m.subscribers...)
		m.lk.Unlock()
	}
	cachedSubscribers := *m.cachedSubscribers
	m.cachedSubscriberLk.Unlock()
	for _, s := range cachedSubscribers {
		s.Next(val)
	}
}

func (m *MulticastReplay[T]) onError(err error) {
	m.err = err
	m.lk.Lock()
	defer m.lk.Unlock()
	for _, s := range m.subscribers {
		s.Error(err)
	}
}

func (m *MulticastReplay[T]) onComplete() {
	m.lk.Lock()
	defer m.lk.Unlock()
	for _, s := range m.subscribers {
		s.Complete()
	}
}

func (m *Multicast[T]) subscribe(subscriber types.StreamSubscriber[T]) types.GracefulCanceller {
	if m.err != nil {
		subscriber.Error(m.err)
		return Empty
	}
	if m.isStopped {
		subscriber.Complete()
		return Empty
	}
	atomic.C
}
