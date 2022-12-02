package prioritywaitqueue

import (
	"fmt"
	"sync"
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

// ComparePriority should return true if a has a higher priority, and therefore
// should run, BEFORE b.
type ComparePriority[T interface{}] func(a T, b T) bool

// New creates a new PriorityWaitQueue with the provided ComparePriority
// function for type T.
func New[T interface{}](cmp ComparePriority[T]) PriorityWaitQueue[T] {
	return &priorityWaitQueue[T]{
		cmp:     cmp,
		cond:    sync.NewCond(&sync.Mutex{}),
		waiters: make([]*T, 0),
	}
}

var _ PriorityWaitQueue[int] = &priorityWaitQueue[int]{}

type priorityWaitQueue[T interface{}] struct {
	cmp     ComparePriority[T]
	cond    *sync.Cond
	waiters []*T
	running *T
}

func (brq *priorityWaitQueue[T]) Wait(waitWith T) func() {
	waitWithPtr := &waitWith
	brq.cond.L.Lock()
	defer brq.cond.L.Unlock()
	// register us as a waiter
	brq.waiters = append(brq.waiters, waitWithPtr)
	for {
		if brq.running == nil { // none currently running, check if we can
			canRun := true
			// is there another waiter in the queue with higher priority than us?
			for _, waiter := range brq.waiters {
				if waiter != waitWithPtr && brq.cmp(*waiter, waitWith) {
					canRun = false
				}
			}
			if canRun { // didn't find a higher-priority waiter, we can run
				// remove us from the wait list
				removed := false
				for i, waiter := range brq.waiters {
					if waiter == waitWithPtr {
						brq.waiters = append(brq.waiters[:i], brq.waiters[i+1:]...)
						removed = true
						break
					}
				}
				if !removed {
					panic("didn't find current waiter in the wait list")
				}
				brq.running = waitWithPtr
				// done() must be called when the work is complete and another job
				// can be run
				done := func() {
					brq.cond.L.Lock()
					defer brq.cond.L.Unlock()
					if brq.running != waitWithPtr {
						panic(fmt.Sprintf("Done() was called with a runner that was not expected to be running: %v <> %v", brq.running, &waitWith))
					}
					brq.running = nil
					// notify all to check whether they are next to run
					brq.cond.Broadcast()
				}
				return done
			}
		}
		// wait until we get a broadcast
		brq.cond.Wait()
	}
}
