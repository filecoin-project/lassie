package internal

import "github.com/filecoin-project/lassie/pkg/types"

type Operator[T any, U any] func(source types.Stream[T], subscriber *Subscriber[U]) types.GracefulCanceller

type Stream[T any] struct {
	subscribe func(*Subscriber[T]) types.GracefulCanceller
}

func (s Stream[T]) Subscribe(subscriber types.StreamSubscriber[T]) types.GracefulCanceller {
	internalSubscriber, ok := subscriber.(*Subscriber[T])
	if !ok {
		internalSubscriber = NewSafeSubscriber(subscriber)
	}
	internalSubscriber.Add(s.subscribe(internalSubscriber))
	return internalSubscriber
}

type derivedStream[S any, T any] struct {
	source   types.Stream[S]
	operator Operator[S, T]
}

func (s derivedStream[S, T]) subscribe(subscriber *Subscriber[T]) types.GracefulCanceller {
	return s.operator(s.source, subscriber)
}

func NewStream[T any](subscribe func(*Subscriber[T]) types.GracefulCanceller) types.Stream[T] {
	return Stream[T]{subscribe: subscribe}
}

func Lift[T any, U any](source types.Stream[T], operator Operator[T, U]) types.Stream[U] {
	return NewStream((derivedStream[T, U]{source: source, operator: operator}).subscribe)
}
