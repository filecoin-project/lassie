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

func (br *BitswapRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {
	return &bitswapRetrieval{br, ctx, request, events}
}

type bitswapRetrieval struct {
	*BitswapRetriever
	ctx     context.Context
	request types.RetrievalRequest
	events  func(types.RetrievalEvent)
}

func (br *bitswapRetrieval) RetrieveFromCandidates(candidates []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	return nil, errors.New("not implemented")
}
