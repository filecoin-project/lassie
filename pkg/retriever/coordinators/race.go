package coordinators

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

func Race(ctx context.Context, queueOperations types.QueueRetrievalsFn) (*types.RetrievalStats, error) {
	resultChan := make(chan types.RetrievalResult)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		queueOperations(ctx, func(nextRetrieval types.RetrievalTask) {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				stats, err := nextRetrieval.Run(ctx)
				select {
				case <-ctx.Done():
				case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
				}
			}()
		})
	}()
	done := make(chan struct{}, 1)
	go func() {
		waitGroup.Wait()
		close(resultChan)
		done <- struct{}{}
	}()
	stats, err := consume(ctx, resultChan)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
	}
	return stats, err
}

func consume(ctx context.Context, resultChan <-chan types.RetrievalResult) (*types.RetrievalStats, error) {
	var totalErr error
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				return nil, totalErr
			}
			if result.Err != nil {
				totalErr = multierr.Append(totalErr, result.Err)
			}
			if result.Stats != nil {
				return result.Stats, nil
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}
