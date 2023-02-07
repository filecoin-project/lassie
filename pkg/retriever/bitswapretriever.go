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

func (br *BitswapRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, candidates []types.RetrievalCandidate, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	return nil, errors.New("not implemented")
}
