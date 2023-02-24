package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/internal/stream"
	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

type AsyncCandidateRetriever struct {
	types.CandidateRetriever
}

func (acr AsyncCandidateRetriever) RetrieveAsync(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncCandidateRetrieval {
	return asyncCandidateRetrieval{
		AsyncCandidateRetriever: acr,
		CandidateRetrieval:      acr.Retrieve(ctx, request, events),
	}
}

type asyncCandidateRetrieval struct {
	AsyncCandidateRetriever
	types.CandidateRetrieval
}

func (acr asyncCandidateRetrieval) RetrieveFromCandidatesAsync(candidateStream types.CandidateStream) types.AsyncRetrieval {
	// map each new set of candidates to a call to the sync retriever, packaged as a Result type
	asyncResults := stream.Map(candidateStream, acr.retrieveAsResult)
	// aggregate the error values
	aggregateErrors := stream.Scan(asyncResults, func(combinedError types.RetrievalResult, next types.RetrievalResult) (types.RetrievalResult, error) {
		if next.Value != nil {
			return next, nil
		}
		return types.RetrievalResult{Err: multierr.Append(combinedError.Err, next.Err)}, nil
	}, types.RetrievalResult{})
	// keep going until we get a success
	untilSuccess := stream.TakeWhile(aggregateErrors, func(rr types.RetrievalResult) bool { return rr.Value == nil }, true)
	// take the final value on the stream, unwrap the result type
	return stream.ResultStream(stream.Last(untilSuccess))
}

func (acr asyncCandidateRetrieval) retrieveAsResult(candidates []types.RetrievalCandidate) (types.RetrievalResult, error) {
	stats, err := acr.RetrieveFromCandidates(candidates)
	return types.RetrievalResult{Value: stats, Err: err}, nil
}
