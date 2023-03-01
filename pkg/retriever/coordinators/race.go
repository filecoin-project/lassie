package coordinators

import (
	"context"
	"errors"
	"sync"

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
				stats, err := nextRetrieval.Run()
				select {
				case <-ctx.Done():
				case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
				}
			}()
		})
	}()
	go func() {
		waitGroup.Wait()
		close(resultChan)
	}()
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
