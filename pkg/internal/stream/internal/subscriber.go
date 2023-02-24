package internal

import (
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
)

type Subscriber[T any] struct {
	Subscription
	lk         sync.Mutex
	isStopped  bool
	onNext     func(T)
	onError    func(error)
	onComplete func()
}

func NewSafeSubscriber[T any](subcriber types.StreamSubscriber[T]) *Subscriber[T] {
	return &Subscriber[T]{
		onNext:     subcriber.Next,
		onError:    subcriber.Error,
		onComplete: subcriber.Complete,
	}
}

func NewSubscriber[T any](onNext func(T), onError func(error), onComplete func()) types.StreamSubscriber[T] {
	return &Subscriber[T]{
		onNext:     onNext,
		onError:    onError,
		onComplete: onComplete,
	}
}

func (o *Subscriber[T]) Next(t T) {
	o.lk.Lock()
	defer o.lk.Unlock()
	if !o.isStopped {
		o.onNext(t)
	}
}
func (o *Subscriber[T]) Error(err error) {
	o.lk.Lock()
	defer o.lk.Unlock()
	if !o.isStopped {
		o.isStopped = true
		o.onError(err)
	}
}

func (o *Subscriber[T]) Complete() {
	o.lk.Lock()
	defer o.lk.Unlock()
	if !o.isStopped {
		o.isStopped = true
		o.onComplete()
	}
}

func (o *Subscriber[T]) TearDown() error {
	o.lk.Lock()
	defer o.lk.Unlock()
	if o.Subscription.closed {
		return nil
	}
	o.isStopped = true
	return o.Subscription.TearDown()
}
