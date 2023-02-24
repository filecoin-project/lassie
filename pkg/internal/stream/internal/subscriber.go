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
	onFinalize types.GracefulCanceller
}

func NewSafeSubscriber[T any](subcriber types.StreamSubscriber[T]) *Subscriber[T] {
	return &Subscriber[T]{
		onNext:     subcriber.Next,
		onError:    subcriber.Error,
		onComplete: subcriber.Complete,
	}
}

func NewSubscriber[T any](onNext func(T), onError func(error), onComplete func(), onFinalize types.GracefulCanceller) types.StreamSubscriber[T] {
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
		o.TearDown()
	}
}

func (o *Subscriber[T]) Complete() {
	o.lk.Lock()
	defer o.lk.Unlock()
	if !o.isStopped {
		o.isStopped = true
		o.onComplete()
		o.TearDown()
	}
}

func (o *Subscriber[T]) TearDown() error {
	o.lk.Lock()
	defer o.lk.Unlock()
	o.Subscription.lock.Lock()
	closed := o.Subscription.closed
	o.Subscription.lock.Unlock()
	if closed {
		return nil
	}
	o.isStopped = true
	err := o.Subscription.TearDown()
	if err != nil {
		return err
	}
	if o.onFinalize != nil {
		return o.onFinalize.TearDown()
	}
	return nil
}
