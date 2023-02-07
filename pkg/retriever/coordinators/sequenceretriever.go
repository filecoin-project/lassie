package coordinators

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

// SequenceRetriever retrieves by running one or more retrievers in sequnece, taking the first successful result or returning a combined error if all fail
type SequenceRetriever struct {
	Retrievers []types.Retriever
}

var _ types.Retriever = SequenceRetriever{}

func (sr SequenceRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	var totalErr error
	for _, retriever := range sr.Retrievers {
		stats, err := retriever.Retrieve(ctx, request, events)
		if err != nil {
			totalErr = multierr.Append(totalErr, err)
		}
		if stats != nil {
			return stats, nil
		}
	}
	return nil, totalErr
}
