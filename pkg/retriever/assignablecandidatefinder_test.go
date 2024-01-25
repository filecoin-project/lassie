package retriever_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestAssignableCandidateFinder(t *testing.T) {
	ctx := context.Background()
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	cid2 := cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")

	testCases := []struct {
		name               string
		candidateResults   map[cid.Cid][]string
		candidateError     error
		filteredPeers      []string
		fixedPeers         map[cid.Cid][]string
		expectedEvents     map[cid.Cid][]types.EventCode
		expectedCandidates map[cid.Cid][]string
		expectedErrors     map[cid.Cid]error
	}{
		{
			name: "successful candidates, no filtering",
			candidateResults: map[cid.Cid][]string{
				cid1: {"fiz", "bang", "booz"},
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedCandidates: map[cid.Cid][]string{
				cid1: {"fiz", "bang", "booz"},
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
				cid2: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
			},
		},
		{
			name:           "candidate finder error",
			candidateError: errors.New("something went wrong"),
			expectedErrors: map[cid.Cid]error{
				cid1: fmt.Errorf("could not get retrieval candidates for %s: %w", cid1, errors.New("something went wrong")),
				cid2: fmt.Errorf("could not get retrieval candidates for %s: %w", cid2, errors.New("something went wrong")),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.FailedCode},
				cid2: {types.StartedFindingCandidatesCode, types.FailedCode},
			},
		},
		{
			name: "no candidates, indexer",
			candidateResults: map[cid.Cid][]string{
				cid1: {},
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedErrors: map[cid.Cid]error{
				cid1: retriever.ErrNoCandidates,
			},
			expectedCandidates: map[cid.Cid][]string{
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.FailedCode},
				cid2: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
			},
		},
		{
			name: "successful candidates, filtering",
			candidateResults: map[cid.Cid][]string{
				cid1: {"fiz", "bang", "booz"},
				cid2: {"apples", "oranges", "cheese"},
			},
			filteredPeers: []string{"fiz", "apples"},
			expectedCandidates: map[cid.Cid][]string{
				cid1: {"bang", "booz"},
				cid2: {"oranges", "cheese"},
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
				cid2: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
			},
		},
		{
			name: "no candidates, via filtering",
			candidateResults: map[cid.Cid][]string{
				cid1: {"fiz", "bang", "booz"},
				cid2: {"apples", "oranges", "cheese"},
			},
			filteredPeers: []string{"fiz", "bang", "booz"},
			expectedErrors: map[cid.Cid]error{
				cid1: retriever.ErrNoCandidates,
			},
			expectedCandidates: map[cid.Cid][]string{
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.FailedCode},
				cid2: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
			},
		},
		{
			name: "fixed peers",
			candidateResults: map[cid.Cid][]string{
				cid1: {"fiz", "bang", "booz"},
				cid2: {"apples", "oranges", "cheese"},
			},
			expectedCandidates: map[cid.Cid][]string{
				cid1: {"double", "trouble"},
				cid2: {"super", "duper"},
			},
			fixedPeers: map[cid.Cid][]string{
				cid1: {"double", "trouble"},
				cid2: {"super", "duper"},
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
				cid2: {types.StartedFindingCandidatesCode, types.CandidatesFoundCode, types.CandidatesFilteredCode},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			allCandidateResults := make(map[cid.Cid][]types.RetrievalCandidate, len(testCase.candidateResults))
			for c, stringResults := range testCase.candidateResults {
				candidateResults := make([]types.RetrievalCandidate, 0, len(stringResults))
				for _, stringResult := range stringResults {
					candidateResults = append(candidateResults, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID(stringResult)}})
				}
				allCandidateResults[c] = candidateResults
			}
			if testCase.fixedPeers == nil {
				testCase.fixedPeers = make(map[cid.Cid][]string)
			}
			allProviders := make(map[cid.Cid][]types.Provider, len(testCase.fixedPeers))
			for c, stringResults := range testCase.fixedPeers {
				providers := make([]types.Provider, 0, len(stringResults))
				for _, stringResult := range stringResults {
					providers = append(providers, types.Provider{
						Peer:      peer.AddrInfo{ID: peer.ID(stringResult)},
						Protocols: []metadata.Protocol{&metadata.GraphsyncFilecoinV1{}, &metadata.Bitswap{}, &metadata.IpfsGatewayHttp{}},
					})
				}
				allProviders[c] = providers
			}
			candidateSource := testutil.NewMockCandidateSource(testCase.candidateError, allCandidateResults)
			isAcceptableStorageProvider := func(candidate types.RetrievalCandidate) (bool, types.RetrievalCandidate) {
				for _, filteredPeer := range testCase.filteredPeers {
					if candidate.MinerPeer.ID == peer.ID(filteredPeer) {
						return false, types.RetrievalCandidate{}
					}
				}
				return true, candidate
			}
			receivedCandidates := make(map[cid.Cid][]string)
			appendCandidates := func(cid cid.Cid, candidates []types.RetrievalCandidate) {
				stringCandidates := make([]string, 0, len(candidates))
				for _, candidate := range candidates {
					stringCandidates = append(stringCandidates, string(candidate.MinerPeer.ID))
				}
				receivedCandidates[cid] = stringCandidates
			}
			receivedEvents := make(map[cid.Cid][]types.RetrievalEvent)
			retrievalCollector := func(evt types.RetrievalEvent) {
				receivedEvents[evt.RootCid()] = append(receivedEvents[evt.RootCid()], evt)
			}
			retrievalCandidateFinder := retriever.NewAssignableCandidateFinder(candidateSource, isAcceptableStorageProvider)
			rid1, err := types.NewRetrievalID()
			req.NoError(err)
			receivedErrors := make(map[cid.Cid]error)
			var candidates []types.RetrievalCandidate
			candidateCollector := func(incoming []types.RetrievalCandidate) {
				candidates = append(candidates, incoming...)
			}

			err = retrievalCandidateFinder.FindCandidates(ctx, types.RetrievalRequest{
				RetrievalID: rid1,
				Request:     trustlessutils.Request{Root: cid1},
				LinkSystem:  cidlink.DefaultLinkSystem(),
				Providers:   allProviders[cid1],
			}, retrievalCollector, candidateCollector)
			if err != nil {
				receivedErrors[cid1] = err
			} else {
				appendCandidates(cid1, candidates)
			}
			rid2, err := types.NewRetrievalID()
			req.NoError(err)
			candidates = nil
			err = retrievalCandidateFinder.FindCandidates(ctx, types.RetrievalRequest{
				RetrievalID: rid2,
				Request:     trustlessutils.Request{Root: cid2},
				LinkSystem:  cidlink.DefaultLinkSystem(),
				Providers:   allProviders[cid2],
			}, retrievalCollector, candidateCollector)
			if err != nil {
				receivedErrors[cid2] = err
			} else {
				appendCandidates(cid2, candidates)
			}
			expectedCandidates := testCase.expectedCandidates
			if expectedCandidates == nil {
				expectedCandidates = make(map[cid.Cid][]string)
			}
			req.Equal(expectedCandidates, receivedCandidates)
			expectedErrors := testCase.expectedErrors
			if expectedErrors == nil {
				expectedErrors = make(map[cid.Cid]error)
			}
			req.Equal(expectedErrors, receivedErrors)
			receivedCodes := make(map[cid.Cid][]types.EventCode, len(receivedEvents))
			for key, events := range receivedEvents {
				receivedCodes[key] = make([]types.EventCode, 0, len(events))
				for _, event := range events {
					receivedCodes[key] = append(receivedCodes[key], event.Code())
				}
			}
			req.Equal(testCase.expectedEvents, receivedCodes)
		})
	}

}
