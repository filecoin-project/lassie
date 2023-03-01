package coordinators

import (
	"context"
	"errors"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

// Race retrieves by racing one or more canditate retrievals together, taking the first successful result or returning a combined error if all fail
func Race(ctx context.Context, retrievalCalls []types.CandidateRetrievalCall) (*types.RetrievalStats, error) {
	resultChan := make(chan types.RetrievalResult)
	ctx, cancel := context.WithCancel(ctx)

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(retrievalCalls))
	for _, retrievalCall := range retrievalCalls {
		retrievalCall := retrievalCall
		go func() {
			defer waitGroup.Done()
			stats, err := retrievalCall.CandidateRetrieval.RetrieveFromCandidates(retrievalCall.Candidates)
			select {
			case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
			case <-ctx.Done():
			}
		}()
	}
	stats, err := collectResults(ctx, resultChan, len(retrievalCalls))
	cancel()
	return stats, err
}

func collectResults(ctx context.Context, resultChan <-chan types.RetrievalResult, totalRetrievers int) (*types.RetrievalStats, error) {
	var totalErr error
	finishedCount := 0
	if totalRetrievers == 0 {
		return nil, errors.New("no eligible retrievers")
	}
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
