package coordinators

import (
	"context"
	"errors"

	"github.com/filecoin-project/lassie/pkg/types"
)

func Sequence(ctx context.Context, queueOperationsFn types.QueueRetrievalsFn) (*types.RetrievalStats, error) {
	var totalErr []error
	var finalStats *types.RetrievalStats
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queueOperationsFn(ctx, func(retrieval types.RetrievalTask) {
		if finalStats != nil {
			return
		}
		stats, err := retrieval.Run()
		if err != nil {
			totalErr = append(totalErr, err)
		}
		if stats != nil {
			finalStats = stats
			cancel()
		}
	})
	if finalStats == nil {
		return nil, errors.Join(totalErr...)
	}
	return finalStats, nil
}
