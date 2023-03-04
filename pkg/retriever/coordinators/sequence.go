package coordinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

func Sequence(ctx context.Context, queueOperationsFn types.QueueRetrievalsFn) (*types.RetrievalStats, error) {
	var totalErr error
	var finalStats *types.RetrievalStats
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queueOperationsFn(ctx, func(retrieval types.RetrievalTask) {
		if finalStats != nil {
			return
		}
		stats, err := retrieval.Run(ctx)
		if err != nil {
			totalErr = multierr.Append(totalErr, err)
		}
		if stats != nil {
			finalStats = stats
			cancel()
		}
	})
	if finalStats == nil {
		return nil, totalErr
	}
	return finalStats, nil
}
