package testutil

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

type MockCandidateFinder struct {
	Candidates map[cid.Cid][]types.RetrievalCandidate
}

func (me *MockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	return me.Candidates[cid], nil
}
