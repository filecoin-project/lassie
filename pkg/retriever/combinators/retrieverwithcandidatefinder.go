package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

// RetrieverWithCandidateFinder retrieves from a candidate retriever after first retrieving candidates from a candidate finder
type RetrieverWithCandidateFinder struct {
	CandidateFinder    types.CandidateFinder
	CandidateRetriever types.AsyncCandidateRetriever
	CoordinationKind   types.CoordinationKind
}

var _ types.Retriever = RetrieverWithCandidateFinder{}

func (rcf RetrieverWithCandidateFinder) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	asyncCandidateRetrieval := rcf.CandidateRetriever.Retrieve(ctx, request, events)

	asyncCandidates := make(chan []types.RetrievalCandidate)
	outbound := types.OutboundAsyncCandidates(asyncCandidates)
	inbound := types.InboundAsyncCandidates(asyncCandidates)

	findErr := make(chan error, 1)
	resultChan := make(chan types.RetrievalResult, 1)
	go func() {
		defer close(findErr)
		defer close(outbound)
		err := rcf.CandidateFinder.FindCandidates(ctx, request, events, func(candidates []types.RetrievalCandidate) {
			_ = outbound.SendNext(ctx, candidates)
		})
		findErr <- err
	}()
	go func() {
		stats, err := asyncCandidateRetrieval.RetrieveFromAsyncCandidates(inbound)
		resultChan <- types.RetrievalResult{Stats: stats, Err: err}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-findErr:
			if err != nil {
				return nil, err
			}
		case result := <-resultChan:
			return result.Stats, result.Err
		}
	}
}
