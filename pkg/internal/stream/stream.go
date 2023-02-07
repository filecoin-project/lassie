package stream

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
)

type Result[T any] struct {
	Value T
	Err   error
}

func sendContext[T any](ctx context.Context, outgoing chan<- T, next T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outgoing <- next:
		return nil
	}
}

// BufferDebounce groups values after each emission from the source channel for the specified duration
func BufferDebounce[T any](ctx context.Context, incoming <-chan Result[T], debounceTime time.Duration, clk clock.Clock) <-chan Result[[]T] {
	outgoing := make(chan Result[[]T])
	go func() {
		var currentValues []T
		var timer *clock.Timer
		for {
			timerChan := func() <-chan time.Time {
				if timer != nil {
					return timer.C
				}
				return nil
			}
			select {
			case <-ctx.Done():
				return
			case <-timerChan():
				if err := sendContext(ctx, outgoing, Result[[]T]{Value: currentValues}); err != nil {
					return
				}
				currentValues = currentValues[:0]
				timer = nil
			case next, ok := <-incoming:
				if !ok {
					if len(currentValues) != 0 {
						if err := sendContext(ctx, outgoing, Result[[]T]{Value: currentValues}); err != nil {
							return
						}
					}
					close(outgoing)
					return
				}
				if next.Err != nil {
					if len(currentValues) != 0 {
						if err := sendContext(ctx, outgoing, Result[[]T]{Value: currentValues}); err != nil {
							return
						}
					}
					if err := sendContext(ctx, outgoing, Result[[]T]{Err: next.Err}); err != nil {
						return
					}
					close(outgoing)
					return
				}
				currentValues = append(currentValues, next.Value)
				if timer == nil {
					timer = clk.Timer(debounceTime)
				}
			}
		}
	}()
	return outgoing
}

// Map transforms values on a channel based on the passed in transformation function
func Map[T any, U any](ctx context.Context, incoming <-chan Result[T], mapFn func(T) U) <-chan Result[U] {
	outgoing := make(chan Result[U])
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case next, ok := <-incoming:
				if !ok {
					close(outgoing)
					return
				}
				if next.Err != nil {
					if err := sendContext(ctx, outgoing, Result[U]{Err: next.Err}); err != nil {
						return
					}
					close(outgoing)
					return
				}
				nextMapped := mapFn(next.Value)
				if err := sendContext(ctx, outgoing, Result[U]{Value: nextMapped}); err != nil {
					return
				}
			}
		}
	}()
	return outgoing
}

// Filter filters values on a channel based on the passed in function -- if the function returns true, the value
// is emmitted, if not, the value is not emitted
func Filter[T any](ctx context.Context, incoming <-chan Result[T], includeFilter func(T) bool) <-chan Result[T] {
	outgoing := make(chan Result[T])
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case next, ok := <-incoming:
				if !ok {
					close(outgoing)
					return
				}
				if next.Err != nil {
					if err := sendContext(ctx, outgoing, next); err != nil {
						return
					}
					close(outgoing)
					return
				}
				if includeFilter(next.Value) {
					if err := sendContext(ctx, outgoing, next); err != nil {
						return
					}
				}
			}
		}
	}()
	return outgoing
}
