package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
)

type AsyncCandidateRetriever struct {
	CandidateRetriever types.CandidateRetriever
}

func (acr AsyncCandidateRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncCandidateRetrieval {
	candidateRetrieval := acr.CandidateRetriever.Retrieve(ctx, request, events)
	return &asyncCandidateRetrieval{
		AsyncCandidateRetriever: acr,
		ctx:                     ctx,
		candidateRetrieval:      candidateRetrieval,
	}
}

type asyncCandidateRetrieval struct {
	AsyncCandidateRetriever
	ctx                context.Context
	candidateRetrieval types.CandidateRetrieval
}

func (acr asyncCandidateRetrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	coordinator, err := coordinators.Coordinator(types.AsyncCoordination)
	if err != nil {
		return nil, err
	}
	return coordinator(acr.ctx, func(ctx context.Context, retrievalCall func(types.Retrieval)) {
		for {
			hasCandidates, nextCandidates, err := asyncCandidates.Next(ctx)
			if err != nil {
				return
			}
			if !hasCandidates {
				return
			}
			retrievalCall(types.CandidateRetrievalCall{
				Candidates:         nextCandidates,
				CandidateRetrieval: acr.candidateRetrieval,
			})
		}
	})
}
