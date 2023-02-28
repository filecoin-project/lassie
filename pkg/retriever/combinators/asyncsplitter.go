package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

type AsyncCandidateSplitter[T comparable] struct {
	Keys              []T
	CandidateSplitter types.CandidateSplitter[T]
}

type NewCandidateSplitterFn[T comparable] func(keys []T) types.CandidateSplitter[T]

var _ types.AsyncCandidateSplitter[int] = AsyncCandidateSplitter[int]{}

func NewAsyncCandidateSplitter[T comparable](keys []T, newCandidateSplitter NewCandidateSplitterFn[T]) AsyncCandidateSplitter[T] {
	return AsyncCandidateSplitter[T]{
		Keys:              keys,
		CandidateSplitter: newCandidateSplitter(keys),
	}
}

func (acs AsyncCandidateSplitter[T]) SplitRetrieval(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncRetrievalSplitter[T] {
	retrievalSplitter := acs.CandidateSplitter.SplitRetrieval(ctx, request, events)
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

func (acr asyncRetrievalSplitter[T]) SplitAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (map[T]types.InboundAsyncCandidates, <-chan error) {
	incoming := make(map[T]types.InboundAsyncCandidates, len(acr.Keys))
	outgoing := make(map[T]types.OutboundAsyncCandidates, len(acr.Keys))
	for _, k := range acr.Keys {
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
			hasCandidates, candidates, err := asyncCandidates.Next(acr.ctx)
			if err != nil {
				errChan <- err
				return
			}
			if !hasCandidates {
				errChan <- nil
				return
			}
			splitCandidateSets, err := acr.retrievalSplitter.SplitCandidates(candidates)
			if err != nil {
				errChan <- err
				return
			}
			for k, candidateSet := range splitCandidateSets {
				err := outgoing[k].SendNext(acr.ctx, candidateSet)
				if err != nil {
					errChan <- err
					return
				}
			}
		}
	}()
	return incoming, errChan
}
