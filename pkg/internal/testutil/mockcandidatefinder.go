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
	rch := make(chan types.FindCandidatesResult, len(rs))
	go func() {
		for _, r := range rs {
			select {
			case <-ctx.Done():
				return
			case rch <- types.FindCandidatesResult{
				Candidate: r,
			}:
			}
		}
		close(rch)
	}()
	return rch, nil
}

func (me *MockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	if me.Error != nil {
		return nil, me.Error
	}
	return me.Candidates[cid], nil
}
