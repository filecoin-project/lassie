package combinators

import (
	"context"

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

func (m SplitRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {
	candidateRetrievals := make([]types.CandidateRetrieval, 0, len(m.CandidateRetrievers))
	for _, candidateRetriever := range m.CandidateRetrievers {
		candidateRetrievals = append(candidateRetrievals, candidateRetriever.Retrieve(ctx, request, events))
	}
	return splitRetrieval{
		retrievalSplitter:   m.CandidateSplitter.SplitRetrieval(ctx, request, events),
		candidateRetrievals: candidateRetrievals,
		coodinationKind:     m.CoordinationKind,
		ctx:                 ctx,
	}
}

type splitRetrieval struct {
	retrievalSplitter   types.RetrievalSplitter
	candidateRetrievals []types.CandidateRetrieval
	coodinationKind     types.CoordinationKind
	ctx                 context.Context
}

func (m splitRetrieval) RetrieveFromCandidates(candidates []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	splitCandidates, err := m.retrievalSplitter.SplitCandidates(candidates)
	if err != nil {
		return nil, err
	}
	retrievers := make([]types.CandidateRetrievalCall, 0, len(splitCandidates))
	for i, candidateSet := range splitCandidates {
		if len(candidateSet) > 0 && i < len(m.candidateRetrievals) {
			retrievers = append(retrievers, types.CandidateRetrievalCall{
				Candidates:         candidateSet,
				CandidateRetrieval: m.candidateRetrievals[i],
			})
		}
	}
	coordinator, err := coordinators.Coordinator(m.coodinationKind)
	if err != nil {
		return nil, err
	}
	return coordinator(m.ctx, retrievers)
}
