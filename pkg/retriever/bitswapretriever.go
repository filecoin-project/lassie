package retriever

import (
	"context"
	"errors"

	"github.com/filecoin-project/lassie/pkg/types"
)

type BitswapRetriever struct{}

func NewBitswapRetriever() *BitswapRetriever {
	return &BitswapRetriever{}
}

func (br *BitswapRetriever) RetrieveFromCandidates(ctx context.Context, request types.RetrievalRequest, candidates types.CandidateStream, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	return nil, errors.New("not implemented")
}
