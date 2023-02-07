package util

import (
	"context"
	"errors"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

func WithCandidates(retrievalCandidateFinder types.RetrievalCandidateFinder, childRetriever types.CandidateRetriever) types.Retriever {
	return func(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		candidates, err := retrievalCandidateFinder(ctx, request, events)
		if err != nil {
			return nil, err
		}
		return childRetriever(ctx, request, candidates, events)
	}
}

func WithFoundCandidates(candidates []types.RetrievalCandidate, candidateRetriever types.CandidateRetriever) types.Retriever {
	return func(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		return candidateRetriever(ctx, request, candidates, events)
	}
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

// TODO: seems like we should be able to adapt the GraphsyncRetriever to use this

func RaceRetrievers(retrievers []types.Retriever) types.Retriever {
	return func(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		resultChan := make(chan types.RetrievalResult)
		ctx, cancel := context.WithCancel(ctx)

		var waitGroup sync.WaitGroup
		waitGroup.Add(len(retrievers))
		for _, retriever := range retrievers {
			retriever := retriever
			go func() {
				defer waitGroup.Done()
				stats, err := retriever(ctx, request, events)
				select {
				case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
				case <-ctx.Done():
				}
			}()
		}
		stats, err := collectResults(ctx, resultChan, len(retrievers))
		cancel()
		waitGroup.Wait()
		return stats, err
	}
}

func SequenceRetrievers(retrievers []types.Retriever) types.Retriever {
	return func(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		var totalErr error
		for _, retriever := range retrievers {
			stats, err := retriever(ctx, request, events)
			if err != nil {
				totalErr = multierr.Append(totalErr, err)
			}
			if stats != nil {
				return stats, nil
			}
		}
		return nil, totalErr
	}
}

// TODO: Priority based sequencing

func MultiRetriever(candidateSplitter types.CandidateSplitter, childRetrievers []types.CandidateRetriever, coordinationKind types.CoordinationKind) types.CandidateRetriever {
	return func(ctx context.Context, request types.RetrievalRequest, candidates []types.RetrievalCandidate, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		splitCandidates, err := candidateSplitter(ctx, request, candidates, events)
		if err != nil {
			return nil, err
		}
		retrievers := make([]types.Retriever, 0, len(splitCandidates))
		for i, candidateSet := range splitCandidates {
			if len(candidateSet) > 0 && i < len(childRetrievers) {
				retrievers = append(retrievers, WithFoundCandidates(candidateSet, childRetrievers[i]))
			}
		}
		var childRetriever types.Retriever
		switch coordinationKind {
		case types.RaceCoordination:
			childRetriever = RaceRetrievers(retrievers)
		case types.SequentialCoordination:
			childRetriever = SequenceRetrievers(retrievers)
		default:
			return nil, errors.New("unrecognized retriever kind")
		}
		return childRetriever(ctx, request, events)
	}
}
