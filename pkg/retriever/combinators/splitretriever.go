package combinators

import (
	"context"
	"errors"

	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
)

var _ types.CandidateRetriever = SplitRetriever{}

// SplitRetriever retrieves by first splitting candidates with a splitter, then passing the split sets
// to multiple retrievers using the given coordination style
type SplitRetriever struct {
	CandidateSplitter   types.CandidateSplitter
	CandidateRetrievers []types.CandidateRetriever
	CoordinationKind    types.CoordinationKind
}

func (m SplitRetriever) RetrieveFromCandidates(ctx context.Context, request types.RetrievalRequest, candidates types.CandidateStream, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	splitCandidates, err := m.CandidateSplitter.SplitCandidates(ctx, request, candidates, events)
	if err != nil {
		return nil, err
	}
	retrievers := make([]types.Retriever, 0, len(splitCandidates))
	for i, candidateSet := range splitCandidates {
		if len(candidateSet) > 0 && i < len(m.CandidateRetrievers) {
			retrievers = append(retrievers, CandidateBoundRetriever{
				Candidates:         candidateSet,
				CandidateRetriever: m.CandidateRetrievers[i],
			})
		}
	}
	var childRetriever types.Retriever
	switch m.CoordinationKind {
	case types.RaceCoordination:
		childRetriever = coordinators.RaceRetriever{Retrievers: retrievers}
	case types.SequentialCoordination:
		childRetriever = coordinators.SequenceRetriever{Retrievers: retrievers}
	default:
		return nil, errors.New("unrecognized retriever kind")
	}
	return childRetriever.Retrieve(ctx, request, events)
}
