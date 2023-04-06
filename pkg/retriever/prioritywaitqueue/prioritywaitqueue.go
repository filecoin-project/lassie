// Package prioritywaitqueue implements a blocking queue for prioritised
// coordination of goroutine execution.
package prioritywaitqueue

import (
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// PriorityWaitQueue is a blocking queue for coordinating goroutines, providing
// a gating mechanism such that only one goroutine may run at a time, where the
// goroutine allowed to run is chosen based on a priority comparison function.
type PriorityWaitQueue[T interface{}] interface {
	// Wait is called with with a value that can be prioritised in comparison to
	// other values of the same type. Returns a "done" function that MUST be
	// called when the work to be performed. The call to Wait will block until
	// there are other running goroutines that have also called Wait and not yet
	// called their "done" function.
	//
	// It is up to the caller to handle context cancellation, goroutines should
	// check for themselves when Wait() returns and immediately call done() if
	// a context has cancelled in order to clean up all running goroutines.
	Wait(waitWith T) func()
}

type Option[T interface{}] func(PriorityWaitQueue[T])

// WithInitialPause sets an initial pause for the first call to Wait() on the
// PriorityWaitQueue. This is useful if you want to allow goroutines to
// queue up before any of them are allowed to run. A short pause will mean that
// the initial first-in-first-run behaviour is overridden, where the first
// goroutine may have to compete with others before getting to run.
func WithInitialPause[T interface{}](duration time.Duration) Option[T] {
	return func(q PriorityWaitQueue[T]) {
		q.(*priorityWaitQueue[T]).initialPause = duration
	}
}

// WithClock sets the clock to use for the PriorityWaitQueue. This is useful
// for testing.
func WithClock[T interface{}](clock clock.Clock) Option[T] {
	return func(q PriorityWaitQueue[T]) {
		q.(*priorityWaitQueue[T]).clock = clock
	}
}

// ComparePriority should return true if a has a higher priority, and therefore
// should run, BEFORE b.
type ComparePriority[T interface{}] func(a T, b T) bool

// New creates a new PriorityWaitQueue with the provided ComparePriority
// function for type T.
func New[T interface{}](cmp ComparePriority[T], options ...Option[T]) PriorityWaitQueue[T] {
	pwq := &priorityWaitQueue[T]{
		cmp:     cmp,
		cond:    sync.NewCond(&sync.Mutex{}),
		waiters: make([]*T, 0),
		clock:   clock.New(),
	}
	for _, opt := range options {
		opt(pwq)
	}
	return pwq
}

var _ PriorityWaitQueue[int] = &priorityWaitQueue[int]{}

type priorityWaitQueue[T interface{}] struct {
	cmp              ComparePriority[T]
	cond             *sync.Cond
	waiters          []*T
	running          *T
	clock            clock.Clock
	initialPause     time.Duration
	initialPauseDone bool
	initialPauseLk   sync.Mutex
}

func (pwq *priorityWaitQueue[T]) Wait(waitWith T) func() {
	waitWithPtr := &waitWith

	pwq.cond.L.Lock()
	defer pwq.cond.L.Unlock()

	// register us as a waiter
	pwq.waiters = append(pwq.waiters, waitWithPtr)

	// if we have an initial pause, do it now and block any subsequent calls since
	// we (the first caller) hold the lock
	if pwq.initialPause > 0 {
		pwq.cond.L.Unlock()
		pwq.initialPauseLk.Lock()
		if !pwq.initialPauseDone {
			pwq.clock.Sleep(pwq.initialPause)
			pwq.initialPauseDone = true
		}
		pwq.initialPauseLk.Unlock()
		pwq.cond.L.Lock()
	}

	for {
		if pwq.running == nil { // none currently running, check if we can
			canRun := true
			// is there another waiter in the queue with higher priority than us?
			for _, waiter := range pwq.waiters {
				if waiter != waitWithPtr && pwq.cmp(*waiter, waitWith) {
					canRun = false
				}
			}
			if canRun { // didn't find a higher-priority waiter, we can run
				// remove us from the wait list
				removed := false
				for i, waiter := range pwq.waiters {
					if waiter == waitWithPtr {
						pwq.waiters = append(pwq.waiters[:i], pwq.waiters[i+1:]...)
						removed = true
						break
					}
				}
				if !removed {
					panic("didn't find current waiter in the wait list")
				}
				pwq.running = waitWithPtr
				// done() must be called when the work is complete and another job
				// can be run
				done := func() {
					pwq.cond.L.Lock()
					defer pwq.cond.L.Unlock()
					if pwq.running != waitWithPtr {
						panic(fmt.Sprintf("Done() was called with a runner that was not expected to be running: %v <> %v", pwq.running, &waitWith))
					}
					pwq.running = nil
					// notify all to check whether they are next to run
					pwq.cond.Broadcast()
				}
				return done
			}
		}

		// unlock and wait until we get a broadcast
		pwq.cond.Wait()
	}
}
