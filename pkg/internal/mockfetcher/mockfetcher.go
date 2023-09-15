package mockfetcher

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
)

var _ types.Fetcher = &MockFetcher{}

type MockFetcher struct {
	FetchFunc func(ctx context.Context, request types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error)
}

func NewMockFetcher() *MockFetcher {
	return &MockFetcher{}
}

func (m *MockFetcher) Fetch(ctx context.Context, request types.RetrievalRequest, opt ...types.FetchOption) (*types.RetrievalStats, error) {
	return m.FetchFunc(ctx, request, types.NewFetchConfig(opt...).EventsCallback)
}
