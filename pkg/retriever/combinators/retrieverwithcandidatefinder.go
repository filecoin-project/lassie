package combinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/internal/stream"
	"github.com/filecoin-project/lassie/pkg/types"
)

// RetrieverWithCandidateFinder retrieves from a candidate retriever after first retrieving candidates from a candidate finder
type RetrieverWithCandidateFinder struct {
	CandidateFinder    types.CandidateFinder
	CandidateRetriever types.AsyncCandidateRetriever
}

var _ types.AsyncRetriever = RetrieverWithCandidateFinder{}

func (rcf RetrieverWithCandidateFinder) RetrieveAsync(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.AsyncRetrieval {
	asyncCandidateRetrieval := rcf.CandidateRetriever.RetrieveAsync(ctx, request, events)
	candidates, err, asyncCandidateErrChan := rcf.CandidateFinder.FindCandidates(ctx, request, events)
	if err != nil {
		return stream.Error[*types.RetrievalStats](err)
	}
	// TODO: Improve this
	return stream.NewStream(func(subscriber types.StreamSubscriber[*types.RetrievalStats]) {
		resultChan := make(chan types.RetrievalResult, 1)
		go func() {
			stats, err := types.GetResults(asyncCandidateRetrieval.RetrieveFromCandidatesAsync(candidates))
			resultChan <- types.RetrievalResult{Value: stats, Err: err}
		}()
		select {
		case <-ctx.Done():
			subscriber.Error(ctx.Err())
		case err := <-asyncCandidateErrChan:
			subscriber.Error(err)
		case result := <-resultChan:
			if result.Err != nil {
				subscriber.Error(result.Err)
				return
			}
			subscriber.Next(result.Value)
			subscriber.Complete()
		}
	})
}
