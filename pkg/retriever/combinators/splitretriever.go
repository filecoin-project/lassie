package combinators

import (
	"context"
	"errors"

	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
)

var _ types.CandidateRetriever = SplitRetriever[int]{}

// SplitRetriever retrieves by first splitting candidates with a splitter, then passing the split sets
// to multiple retrievers using the given coordination style
type SplitRetriever[T comparable] struct {
	AsyncCandidateSplitter types.AsyncCandidateSplitter[T]
	CandidateRetrievers    map[T]types.CandidateRetriever
	CoordinationKind       types.CoordinationKind
}

func (m SplitRetriever[T]) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {

	return splitRetrieval[T]{
		retrievalSplitter:   m.AsyncCandidateSplitter.SplitRetrievalRequest(ctx, request, events),
		candidateRetrievers: m.CandidateRetrievers,
		coordinationKind:    m.CoordinationKind,
		ctx:                 ctx,
		request:             request,
		events:              events,
	}
}

type splitRetrieval[T comparable] struct {
	retrievalSplitter   types.AsyncRetrievalSplitter[T]
	candidateRetrievers map[T]types.CandidateRetriever
	coordinationKind    types.CoordinationKind
	ctx                 context.Context
	request             types.RetrievalRequest
	events              func(types.RetrievalEvent)
}

func (m splitRetrieval[T]) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	asyncSplitCandidates, errChan := m.retrievalSplitter.SplitAsyncCandidates(asyncCandidates)
	coordinator, err := coordinators.Coordinator(m.coordinationKind)
	if err != nil {
		return nil, err
	}
	stats, err := coordinator(m.ctx, func(ctx context.Context, retrievalCall func(types.RetrievalTask)) {
		candidateRetrievals := make(map[T]types.CandidateRetrieval, len(m.candidateRetrievers))
		for key, candidateRetriever := range m.candidateRetrievers {
			candidateRetrievals[key] = candidateRetriever.Retrieve(ctx, m.request, m.events)
		}
		for key, asyncCandidates := range asyncSplitCandidates {
			if asyncCandidateRetrieval, ok := candidateRetrievals[key]; ok {
				retrievalCall(types.AsyncRetrievalTask{
					Candidates:              asyncCandidates,
					AsyncCandidateRetrieval: asyncCandidateRetrieval,
				})
			}
		}
		retrievalCall(types.DeferredErrorTask{Ctx: ctx, ErrChan: errChan})
	})
	if stats == nil && err == nil {
		return nil, errors.New("no eligible retrievers")
	}
	return stats, err
}
