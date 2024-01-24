package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

type DiscoveredCandidate struct {
	Cid       cid.Cid
	Candidate types.RetrievalCandidate
}

type MockCandidateSource struct {
	err                  error
	candidates           map[cid.Cid][]types.RetrievalCandidate
	discoveredCandidates chan DiscoveredCandidate
}

func NewMockCandidateSource(err error, candidates map[cid.Cid][]types.RetrievalCandidate) *MockCandidateSource {
	return &MockCandidateSource{
		err:                  err,
		candidates:           candidates,
		discoveredCandidates: make(chan DiscoveredCandidate, 16),
	}
}

func (me *MockCandidateSource) VerifyCandidatesDiscovered(ctx context.Context, t *testing.T, afterStart time.Duration, expectedCandidatesDiscovered []DiscoveredCandidate) {
	candidatesDiscovered := make([]DiscoveredCandidate, 0, len(expectedCandidatesDiscovered))
	for i := 0; i < len(expectedCandidatesDiscovered); i++ {
		select {
		case candidate := <-me.discoveredCandidates:
			candidatesDiscovered = append(candidatesDiscovered, candidate)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected candidates", "expected %d, received %d @ %s", len(expectedCandidatesDiscovered), i, afterStart)
		}
	}
	require.ElementsMatch(t, expectedCandidatesDiscovered, candidatesDiscovered)
}

func (me *MockCandidateSource) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	rs, err := me.findCandidates(ctx, c)
	if err != nil {
		return err
	}
	for _, r := range rs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cb(r)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case me.discoveredCandidates <- DiscoveredCandidate{Cid: c, Candidate: r}:
		}
	}
	return nil
}

func (me *MockCandidateSource) findCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	if me.err != nil {
		return nil, me.err
	}
	return me.candidates[cid], nil
}
