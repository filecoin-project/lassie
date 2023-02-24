package stream

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/types"
)

type internalSubscriber[T any] struct {
	isStopped  bool
	onNext     func(T)
	onError    func(error)
	onComplete func()
}

func (o internalSubscriber[T]) Next(t T) {
	if !o.isStopped {
		o.onNext(t)
	}
}
func (o internalSubscriber[T]) Error(err error) {
	if !o.isStopped {
		o.isStopped = true
		o.onError(err)
	}
}

func (o internalSubscriber[T]) Complete() {
	if !o.isStopped {
		o.isStopped = true
		o.onComplete()
	}
}

func safeSubscriber[T any](subcriber types.StreamSubscriber[T]) types.StreamSubscriber[T] {
	return &internalSubscriber[T]{
		onNext:     subcriber.Next,
		onError:    subcriber.Error,
		onComplete: subcriber.Complete,
	}
}

func NewMultiSubscriber[T any](subscribers []types.StreamSubscriber[T]) types.StreamSubscriber[T] {
	return internalSubscriber[T]{
		onNext: func(t T) {
			for _, s := range subscribers {
				s.Next(t)
			}
		},
		onError: func(err error) {
			for _, s := range subscribers {
				s.Error(err)
			}
		},
		onComplete: func() {
			for _, s := range subscribers {
				s.Complete()
			}
		},
	}
}

func operatorSubscriber[T any, U any](dest types.StreamSubscriber[U], onNext func(T)) types.StreamSubscriber[T] {
	return internalSubscriber[T]{
		onNext: func(t T) {
			onNext(t)
		},
		onError: func(err error) {
			dest.Error(err)
		},
		onComplete: func() {
			dest.Complete()
		},
	}
}

type Operator[T any, U any] func(source types.Stream[T], subscriber types.StreamSubscriber[U])

type stream[T any] struct {
	subscribe func(types.StreamSubscriber[T])
}

func (s stream[T]) Subscribe(subscriber types.StreamSubscriber[T]) {
	_, ok := subscriber.(*internalSubscriber[T])
	if !ok {
		subscriber = safeSubscriber(subscriber)
	}
	s.subscribe(subscriber)
}

type derivedStream[S any, T any] struct {
	source   types.Stream[S]
	operator Operator[S, T]
}

func (s derivedStream[S, T]) subscribe(subscriber types.StreamSubscriber[T]) {
	s.operator(s.source, subscriber)
}

func NewStream[T any](subscribe func(types.StreamSubscriber[T])) types.Stream[T] {
	return stream[T]{subscribe: subscribe}
}

func lift[T any, U any](source types.Stream[T], operator Operator[T, U]) types.Stream[U] {
	return NewStream((derivedStream[T, U]{source: source, operator: operator}).subscribe)
}

// BufferDebounce groups values after each emission from the source channel for the specified duration
func BufferDebounce[T any](ctx context.Context, source types.Stream[T], debounceTime time.Duration, clk clock.Clock) types.Stream[[]T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[[]T]) {
		var timerCancel context.CancelFunc
		var currentValues []T
		var lock sync.Mutex
		emit := func() {
			lock.Lock()
			if timerCancel != nil {
				timerCancel()
			}
			timerCancel = nil
			prevValues := currentValues
			currentValues = nil
			lock.Unlock()
			if len(prevValues) != 0 {
				subscriber.Next(prevValues)
			}
		}

		debounceSubscriber := internalSubscriber[T]{
			onNext: func(next T) {
				var timerCtx context.Context
				lock.Lock()
				currentValues = append(currentValues, next)
				timerCtx, timerCancel = context.WithCancel(ctx)
				lock.Unlock()
				timer := clk.Timer(debounceTime)
				go func() {
					select {
					case <-timerCtx.Done():
						if !timer.Stop() {
							<-timer.C
						}
					case <-timer.C:
						emit()
					}
				}()
			},
			onError: func(err error) {
				emit()
				subscriber.Error(err)
			},
			onComplete: func() {
				emit()
				subscriber.Complete()
			},
		}
		source.Subscribe(debounceSubscriber)
	})
}

// Map transforms values on a channel based on the passed in transformation function
func Map[T any, U any](stream types.Stream[T], mapFn func(T) (U, error)) types.Stream[U] {
	return lift(stream, func(source types.Stream[T], subscriber types.StreamSubscriber[U]) {
		mapSubscriber := operatorSubscriber(subscriber, func(t T) {
			next, err := mapFn(t)
			if err != nil {
				subscriber.Error(err)
				return
			}
			subscriber.Next(next)
		})
		source.Subscribe(mapSubscriber)
	})
}

// Filter filters values on a channel based on the passed in function -- if the function returns true, the value
// is emmitted, if not, the value is not emitted
func Filter[T any](source types.Stream[T], include func(T) bool) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		filterSubscriber := operatorSubscriber(subscriber, func(t T) {
			if include(t) {
				subscriber.Next(t)
			}
		})
		source.Subscribe(filterSubscriber)
	})
}

func Take[T any](source types.Stream[T], count uint64) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		seen := uint64(0)
		takeSubscriber := operatorSubscriber(subscriber, func(t T) {
			seen++
			if seen <= count {
				subscriber.Next(t)
				if count <= seen {
					subscriber.Complete()
				}
			}
		})
		source.Subscribe(takeSubscriber)
	})
}

func TakeWhile[T any](source types.Stream[T], while func(T) bool, inclusive bool) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		takeSubscriber := operatorSubscriber(subscriber, func(t T) {
			if while(t) {
				subscriber.Next(t)
				return
			}
			if inclusive {
				subscriber.Next(t)
			}
			subscriber.Complete()
		})
		source.Subscribe(takeSubscriber)
	})
}

func Last[T any](source types.Stream[T]) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		var last T
		var hasLast bool
		lastSubscriber := &internalSubscriber[T]{
			onNext: func(t T) {
				hasLast = true
				last = t
			},
			onError: func(err error) {
				subscriber.Error(err)
			},
			onComplete: func() {
				if hasLast {
					subscriber.Next(last)
				}
				subscriber.Complete()
			},
		}
		source.Subscribe(lastSubscriber)
	})
}

func Scan[T any, U any](stream types.Stream[T], scanFn func(U, T) (U, error), start U) types.Stream[U] {
	return lift(stream, func(source types.Stream[T], subscriber types.StreamSubscriber[U]) {
		accum := start
		scanSubscriber := operatorSubscriber(subscriber, func(t T) {
			var err error
			accum, err = scanFn(accum, t)
			if err != nil {
				subscriber.Error(err)
				return
			}
			subscriber.Next(accum)
		})
		source.Subscribe(scanSubscriber)
	})
}

func Observe[T any](source types.Stream[T], observer types.StreamSubscriber[T]) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		observingSubscriber := internalSubscriber[T]{
			onNext: func(next T) {
				observer.Next(next)
				subscriber.Next(next)
			},
			onError: func(err error) {
				observer.Error(err)
				subscriber.Error(err)
			},
			onComplete: func() {
				observer.Complete()
				subscriber.Complete()
			},
		}
		source.Subscribe(observingSubscriber)
	})
}

func CatchError[T any](source types.Stream[T], onError func(error)) types.Stream[T] {
	return lift(source, func(source types.Stream[T], subscriber types.StreamSubscriber[T]) {
		catchSubscriber := internalSubscriber[T]{
			onNext: func(next T) {
				subscriber.Next(next)
			},
			onError: func(err error) {
				if onError != nil {
					onError(err)
				}
				subscriber.Complete()
			},
			onComplete: func() {
				subscriber.Complete()
			},
		}
		source.Subscribe(catchSubscriber)
	})
}

const splitBufferSize = 16

type StreamName string

func SplitStream[T any](ctx context.Context, source types.Stream[T], streams map[StreamName]struct{}, splitFn func(T) (map[StreamName]T, error)) map[StreamName]types.Stream[T] {
	subscribers := make(map[StreamName]types.StreamSubscriber[T], len(streams))
	splitStreams := make(map[StreamName]types.Stream[T], len(streams))
	var once sync.Once
	for stream := range streams {
		outgoing := make(chan types.Result[T], splitBufferSize)
		subscribers[stream] = NewChannelSubscriber(ctx, outgoing)
		splitStreams[stream] = NewResultChannelStream(ctx, outgoing)
	}
	allSubscriber := NewMultiSubscriber(subscribers)
	splitSubscriber := operatorSubscriber(allSubscriber,
		func(value T) {
			splitValues, splitErr := splitFn(value)
			if splitErr != nil {
				allSubscriber.Error(splitErr)
				return
			}
			for key, subscriber := range subscribers {
				if next, ok := splitValues[key]; ok {
					subscriber.Next(next)
				}
			}
		})
	linkedStreams := make(map[StreamName]types.Stream[T], len(streams))
	for streamName, splitStream := range splitStreams {
		linkedStreams[streamName] = lift(splitStream, func(splitSource types.Stream[T], subscriber types.StreamSubscriber[T]) {
			once.Do(func() {
				go func() {
					source.Subscribe(splitSubscriber)
				}()
			})
			splitSource.Subscribe(subscriber)
		})
	}
	return linkedStreams
}

func NewChannelSubscriber[T any](ctx context.Context, outgoing chan<- types.Result[T]) types.StreamSubscriber[T] {
	return internalSubscriber[T]{
		onNext: func(t T) {
			sendContext(ctx, outgoing, types.Value(t))
		},
		onError: func(err error) {
			sendContext(ctx, outgoing, types.Error[T](err))
		},
		onComplete: func() {
			close(outgoing)
		},
	}
}

func sendContext[T any](ctx context.Context, outgoing chan<- T, next T) {
	select {
	case <-ctx.Done(): // Context's done channel has the highest priority
		return
	default:
	}
	select {
	case <-ctx.Done():
		return
	case outgoing <- next:
		return
	}
}

func ResultStream[T any](source types.Stream[types.Result[T]]) types.Stream[T] {
	return lift(source, func(source types.Stream[types.Result[T]], subscriber types.StreamSubscriber[T]) {
		resultSubscriber := operatorSubscriber(subscriber, func(next types.Result[T]) {
			if next.Err != nil {
				subscriber.Error(next.Err)
				return
			}
			subscriber.Next(next.Value)
		})
		source.Subscribe(resultSubscriber)
	})
}

func Just[T any](value T) types.Stream[T] {
	return NewStream(func(subcriber types.StreamSubscriber[T]) {
		subcriber.Next(value)
		subcriber.Complete()
	})
}

func Error[T any](err error) types.Stream[T] {
	return NewStream(func(subcriber types.StreamSubscriber[T]) {
		subcriber.Error(err)
	})
}

func Race[T any](streams []types.Stream[T]) types.Stream[T] {
	if len(streams) == 1 {
		return streams[0]
	}
	return NewStream((types.StreamSubscriber[T])))
}

func NewResultChannelStream[T any](ctx context.Context, incoming <-chan types.Result[T]) types.Stream[T] {
	return ResultStream(NewChannelStream(ctx, incoming))
}

func NewChannelStream[T any](ctx context.Context, incoming <-chan T) types.Stream[T] {
	return NewStream(func(subscriber types.StreamSubscriber[T]) {
		for {
			select {
			case <-ctx.Done():
			default:
			}
			select {
			case <-ctx.Done():
				return
			case next, ok := <-incoming:
				if !ok {
					subscriber.Complete()
					return
				}
				subscriber.Next(next)
			}
		}
	})
}
