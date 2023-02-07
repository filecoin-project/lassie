package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

// RetrieverWithCandidateFinder retrieves from a candidate retriever after first retrieving candidates from a candidate finder
type RetrieverWithCandidateFinder struct {
	CandidateFinder    types.RetrievalCandidateFinder
	CandidateRetriever types.CandidateRetriever
}

var _ types.Retriever = RetrieverWithCandidateFinder{}

func (rcf RetrieverWithCandidateFinder) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	candidates, err := rcf.CandidateFinder.FindCandidates(ctx, request, events)
	if err != nil {
		return nil, err
	}
	return rcf.CandidateRetriever.RetrieveFromCandidates(ctx, request, candidates, events)
}
