package coordinators

import (
	"context"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

// Async runs a series of retrieval tasks in sequence, but asynchronously from the queuing of tasks
// This keeps the queuing task unblocked.
func Async(ctx context.Context, queueOperationsFn types.QueueRetrievalsFn) (*types.RetrievalStats, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rq := &retrievalQueue{}
	rq.newRetrievalsSignal = sync.NewCond(&rq.retrievalsLk)
	go func() {
		queueOperationsFn(ctx, func(retrieval types.Retrieval) {
			rq.queue(ctx, retrieval)
		})
		rq.close()
	}()

	var totalErr error
	for {
		hasNext, next := rq.dequeue(ctx)
		if !hasNext {
			return nil, totalErr
		}
		stats, err := next.Retrieve()
		if stats != nil {
			return stats, nil
		}
		totalErr = multierr.Append(totalErr, err)
	}
}

type retrievalQueue struct {
	retrievals          []types.Retrieval
	done                bool
	retrievalsLk        sync.Mutex
	newRetrievalsSignal *sync.Cond
}

func (rq *retrievalQueue) queue(ctx context.Context, retrieval types.Retrieval) {
	rq.retrievalsLk.Lock()
	defer rq.retrievalsLk.Unlock()

	if ctx.Err() != nil {
		return
	}
	rq.retrievals = append(rq.retrievals, retrieval)

	rq.newRetrievalsSignal.Signal()
}

func (rq *retrievalQueue) close() {

	rq.retrievalsLk.Lock()
	defer rq.retrievalsLk.Unlock()
	rq.done = true
	rq.newRetrievalsSignal.Signal()
}

func (rq *retrievalQueue) dequeue(ctx context.Context) (bool, types.Retrieval) {
	rq.retrievalsLk.Lock()
	defer rq.retrievalsLk.Unlock()
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		if len(rq.retrievals) > 0 {
			var next types.Retrieval
			next, rq.retrievals = rq.retrievals[0], rq.retrievals[1:]
			return true, next
		}
		if rq.done {
			return false, nil
		}

		rq.newRetrievalsSignal.Wait()
	}
}
