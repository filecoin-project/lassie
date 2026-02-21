package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
)

// retrievalShared is the shared state and coordination between the per-SP
// retrieval goroutines.
type retrievalShared struct {
	waitQueue  prioritywaitqueue.PriorityWaitQueue[peer.ID]
	resultChan chan retrievalResult
	finishChan chan struct{}
}

func newRetrievalShared(waitQueue prioritywaitqueue.PriorityWaitQueue[peer.ID]) *retrievalShared {
	return &retrievalShared{
		resultChan: make(chan retrievalResult),
		finishChan: make(chan struct{}),
		waitQueue:  waitQueue,
	}
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (shared *retrievalShared) canSendResult() bool {
	select {
	case <-shared.finishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (shared *retrievalShared) sendResult(ctx context.Context, result retrievalResult) bool {
	select {
	case <-ctx.Done():
	case <-shared.finishChan:
		return false
	case shared.resultChan <- result:
		if result.Stats != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(shared.finishChan)
		}
	}
	return true
}

func (shared *retrievalShared) sendAllFinished(ctx context.Context) {
	select {
	case <-ctx.Done():
	case shared.resultChan <- retrievalResult{AllFinished: true}:
	}
}

func (shared *retrievalShared) sendEvent(ctx context.Context, peerID peer.ID, event types.RetrievalEvent) {
	shared.sendResult(ctx, retrievalResult{PeerID: peerID, Event: &event})
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func collectResults(ctx context.Context, shared *retrievalShared, eventsCallback func(peer.ID, types.RetrievalEvent)) (*types.RetrievalStats, error) {
	var retrievalErrors error
	for {
		select {
		case result := <-shared.resultChan:
			if result.Event != nil {
				eventsCallback(result.PeerID, *result.Event)
				break
			}
			if result.Err != nil {
				retrievalErrors = multierr.Append(retrievalErrors, result.Err)
			}
			if result.Stats != nil {
				return result.Stats, nil
			}
			// have we got all responses but no success?
			if result.AllFinished {
				// we failed, and got only retrieval errors
				retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				return nil, retrievalErrors
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}
