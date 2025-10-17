package indexerlookup_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/internal/mockindexer"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/find/model"
	"github.com/stretchr/testify/require"
)

func TestCandidateSource(t *testing.T) {
	cids := make([]cid.Cid, 0, 10)
	candidates := make(map[cid.Cid][]types.RetrievalCandidate, 10)
	binaryMetadata := make(map[cid.Cid][][]byte, 10)
	for i := 0; i < 10; i++ {
		next := testutil.GenerateRetrievalCandidates(t, 2)
		cids = append(cids, next[0].RootCid)
		candidates[next[0].RootCid] = next
		for _, candidate := range next {
			binaryMetadatum, err := candidate.Metadata.MarshalBinary()
			require.NoError(t, err)
			binaryMetadata[next[0].RootCid] = append(binaryMetadata[next[0].RootCid], binaryMetadatum)
		}
	}
	testCases := []struct {
		name            string
		cidReturns      map[cid.Cid][]model.ProviderResult
		expectedReturns map[cid.Cid][]types.RetrievalCandidate
		async           bool
	}{
		{
			name: "basic fetch",
			cidReturns: map[cid.Cid][]model.ProviderResult{
				cids[0]: {
					{
						Metadata:  binaryMetadata[cids[0]][0],
						ContextID: random.Bytes(100),
						Provider:  &candidates[cids[0]][0].MinerPeer,
					},
					{
						Metadata:  binaryMetadata[cids[0]][1],
						ContextID: random.Bytes(100),
						Provider:  &candidates[cids[0]][1].MinerPeer,
					},
				},
				cids[1]: {
					{
						Metadata:  binaryMetadata[cids[1]][0],
						ContextID: random.Bytes(100),
						Provider:  &candidates[cids[1]][0].MinerPeer,
					},
					{
						Metadata:  binaryMetadata[cids[1]][1],
						ContextID: random.Bytes(100),
						Provider:  &candidates[cids[1]][1].MinerPeer,
					},
				},
			},
			expectedReturns: map[cid.Cid][]types.RetrievalCandidate{
				cids[0]: candidates[cids[0]],
				cids[1]: candidates[cids[1]],
			},
		},
		{
			name:       "not found",
			cidReturns: map[cid.Cid][]model.ProviderResult{},
			expectedReturns: map[cid.Cid][]types.RetrievalCandidate{
				cids[0]: {},
				cids[1]: {},
			},
		},
	}
	ctx := context.Background()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			req := require.New(t)
			clock := clock.NewMock()
			connectCh := make(chan string, 1)
			mockIndexer, err := mockindexer.NewMockIndexer(ctx, "127.0.0.1", 0, testCase.cidReturns, clock, connectCh)
			req.NoError(err)
			closeErr := make(chan error, 1)
			go func() {
				err := mockIndexer.Start()
				closeErr <- err
			}()
			indexerURL, err := url.Parse("http://" + mockIndexer.Addr())
			req.NoError(err)
			candidateSource, err := indexerlookup.NewCandidateSource(indexerlookup.WithHttpEndpoint(indexerURL))
			req.NoError(err)
			for cid, expectedReturns := range testCase.expectedReturns {
				gatheredCandidates := []types.RetrievalCandidate{}
				asyncCandidatesErr := make(chan error, 1)
				go func() {
					asyncCandidatesErr <- candidateSource.FindCandidates(ctx, cid, func(candidate types.RetrievalCandidate) {
						gatheredCandidates = append(gatheredCandidates, candidate)
					})
				}()
				select {
				case <-ctx.Done():
					req.FailNow("cancelled")
				case recv := <-connectCh:
					req.Regexp("^/multihash/"+cid.Hash().B58String(), recv)
				}
				for range expectedReturns {
					clock.Add(5 * time.Second)
				}
				select {
				case <-ctx.Done():
					req.FailNow("did not receive results")
				case err = <-asyncCandidatesErr:
					require.NoError(t, err)
				}
				req.Equal(expectedReturns, gatheredCandidates)
			}
			err = mockIndexer.Close()
			req.NoError(err)
			select {
			case <-ctx.Done():
				req.FailNow("did not close server")
			case err := <-closeErr:
				req.NoError(err)
			}
		})
	}
}
