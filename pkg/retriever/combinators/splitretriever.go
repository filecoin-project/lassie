package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
)

var _ types.AsyncCandidateRetriever = SplitRetriever[int]{}

// SplitRetriever retrieves by first splitting candidates with a splitter, then passing the split sets
// to multiple retrievers using the given coordination style
type SplitRetriever[T comparable] struct {
	AsyncCandidateSplitter types.AsyncCandidateSplitter[T]
	CandidateRetrievers    map[T]types.AsyncCandidateRetriever
	CoordinationKind       types.CoordinationKind
}

func (m SplitRetriever[T]) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncCandidateRetrieval {
	candidateRetrievals := make(map[T]types.AsyncCandidateRetrieval, len(m.CandidateRetrievers))
	for key, candidateRetriever := range m.CandidateRetrievers {
		candidateRetrievals[key] = candidateRetriever.Retrieve(ctx, request, events)
	}
	return splitRetrieval[T]{
		retrievalSplitter:   m.AsyncCandidateSplitter.SplitRetrieval(ctx, request, events),
		candidateRetrievals: candidateRetrievals,
		coodinationKind:     m.CoordinationKind,
		ctx:                 ctx,
	}
}

type splitRetrieval[T comparable] struct {
	retrievalSplitter   types.AsyncRetrievalSplitter[T]
	candidateRetrievals map[T]types.AsyncCandidateRetrieval
	coodinationKind     types.CoordinationKind
	ctx                 context.Context
}

func (m splitRetrieval[T]) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	asyncSplitCandidates, errChan := m.retrievalSplitter.SplitAsyncCandidates(asyncCandidates)
	coordinator, err := coordinators.Coordinator(m.coodinationKind)
	if err != nil {
		return nil, err
	}
	return coordinator(m.ctx, func(ctx context.Context, retrievalCall func(types.Retrieval)) {
		for key, asyncCandidates := range asyncSplitCandidates {
			if asyncCandidateRetrieval, ok := m.candidateRetrievals[key]; ok {
				retrievalCall(types.AsyncCandidateRetrievalCall{
					Candidates:              asyncCandidates,
					AsyncCandidateRetrieval: asyncCandidateRetrieval,
				})
			}
		}
		retrievalCall(types.DeferredErrorRetrieval{Ctx: ctx, ErrChan: errChan})
	})
}
