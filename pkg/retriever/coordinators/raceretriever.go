package coordinators

import (
	"context"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

// RaceRetriever retrieves by racing one or more retrievers together, taking the first successful result or returning a combined error if all fail
type RaceRetriever struct {
	Retrievers []types.Retriever
}

var _ types.Retriever = RaceRetriever{}

func (rr RaceRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	resultChan := make(chan types.RetrievalResult)
	ctx, cancel := context.WithCancel(ctx)

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(rr.Retrievers))
	for _, retriever := range rr.Retrievers {
		retriever := retriever
		go func() {
			defer waitGroup.Done()
			stats, err := retriever.Retrieve(ctx, request, events)
			select {
			case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
			case <-ctx.Done():
			}
		}()
	}
	stats, err := collectResults(ctx, resultChan, len(rr.Retrievers))
	cancel()
	waitGroup.Wait()
	return stats, err
}

func collectResults(ctx context.Context, resultChan <-chan types.RetrievalResult, totalRetrievers int) (*types.RetrievalStats, error) {
	var totalErr error
	finishedCount := 0
	for {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				totalErr = multierr.Append(totalErr, result.Err)
			}
			if result.Stats != nil {
				return result.Stats, nil
			}
			// have we got all responses but no success?
			finishedCount++
			if finishedCount >= totalRetrievers {
				return nil, totalErr
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}
