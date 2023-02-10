package coordinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

func Sequence(ctx context.Context, retrievalCalls []types.CandidateRetrievalCall) (*types.RetrievalStats, error) {
	var totalErr error
	for _, retrievalCall := range retrievalCalls {
		stats, err := retrievalCall.CandidateRetrieval.RetrieveFromCandidates(retrievalCall.Candidates)
		if err != nil {
			totalErr = multierr.Append(totalErr, err)
		}
		if stats != nil {
			return stats, nil
		}
	}
	return nil, totalErr
}
