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

func (me *MockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	if me.Error != nil {
		return nil, me.Error
	}
	return me.Candidates[cid], nil
}
