package testutil

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

type MockCandidateFinder struct {
	Error      error
	Candidates map[cid.Cid][]types.RetrievalCandidate
}

func (me *MockCandidateFinder) FindCandidatesAsync(ctx context.Context, c cid.Cid) (<-chan types.FindCandidatesResult, error) {
	rs, err := me.FindCandidates(ctx, c)
	if err != nil {
		return nil, err
	}
	switch len(rs) {
	case 0:
		return nil, nil
	default:
		rch := make(chan types.FindCandidatesResult, len(rs))
		for _, r := range rs {
			rch <- types.FindCandidatesResult{
				Value: r,
			}
		}
		return rch, nil
	}
}

func (me *MockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	if me.Error != nil {
		return nil, me.Error
	}
	return me.Candidates[cid], nil
}
