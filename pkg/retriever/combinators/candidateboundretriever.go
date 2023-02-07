package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

// CandidateBoundRetriever retrieves via a CandidateRetriever but with a fixed set of candidates
type CandidateBoundRetriever struct {
	Candidates         types.CandidateStream
	CandidateRetriever types.CandidateRetriever
}

var _ types.Retriever = CandidateBoundRetriever{}

func (br CandidateBoundRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	return br.CandidateRetriever.RetrieveFromCandidates(ctx, request, br.Candidates, events)
}
