package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

// AsyncCandidateSplitter creates an splitter for a candidate stream from a simple function to split a set of candidates.
// Essentially, on each new candidate set from the stream, we run the splitter function, and feed the split candidates sets
// into the appropriate downstream candidate streams
type AsyncCandidateSplitter[T comparable] struct {
	Keys              []T
	CandidateSplitter types.CandidateSplitter[T]
}

// NewCandidateSplitterFn constructs a new candidate splitter for the given keys
// it's used to insurate
type NewCandidateSplitterFn[T comparable] func(keys []T) types.CandidateSplitter[T]

var _ types.AsyncCandidateSplitter[int] = AsyncCandidateSplitter[int]{}

// NewAsyncCandidateSplitter creates a new candidate splitter with the given passed in sync splitter constructor
func NewAsyncCandidateSplitter[T comparable](keys []T, newCandidateSplitter NewCandidateSplitterFn[T]) AsyncCandidateSplitter[T] {
	return AsyncCandidateSplitter[T]{
		Keys:              keys,
		CandidateSplitter: newCandidateSplitter(keys),
	}
}

// SplitRetrievalRequest splits candidate streams for a given retrieval request
func (acs AsyncCandidateSplitter[T]) SplitRetrievalRequest(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncRetrievalSplitter[T] {
	retrievalSplitter := acs.CandidateSplitter.SplitRetrievalRequest(ctx, request, events)
	return asyncRetrievalSplitter[T]{
		AsyncCandidateSplitter: acs,
		ctx:                    ctx,
		retrievalSplitter:      retrievalSplitter,
	}
}

type asyncRetrievalSplitter[T comparable] struct {
	AsyncCandidateSplitter[T]
	ctx               context.Context
	retrievalSplitter types.RetrievalSplitter[T]
}

const splitBufferSize = 16

// SplitAsyncCandidates performs the work of actually splitting streams for a given request
func (ars asyncRetrievalSplitter[T]) SplitAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (map[T]types.InboundAsyncCandidates, <-chan error) {
	incoming := make(map[T]types.InboundAsyncCandidates, len(ars.Keys))
	outgoing := make(map[T]types.OutboundAsyncCandidates, len(ars.Keys))
	for _, k := range ars.Keys {
		incoming[k], outgoing[k] = types.MakeAsyncCandidates(splitBufferSize)
	}
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			for _, outgoingCandidateStream := range outgoing {
				close(outgoingCandidateStream)
			}
			close(errChan)
		}()
		for {
			hasCandidates, candidates, err := asyncCandidates.Next(ars.ctx)
			if err != nil {
				errChan <- err
				return
			}
			if !hasCandidates {
				errChan <- nil
				return
			}
			splitCandidateSets, err := ars.retrievalSplitter.SplitCandidates(candidates)
			if err != nil {
				errChan <- err
				return
			}
			for k, candidateSet := range splitCandidateSets {
				err := outgoing[k].SendNext(ars.ctx, candidateSet)
				if err != nil {
					errChan <- err
					return
				}
			}
		}
	}()
	return incoming, errChan
}
