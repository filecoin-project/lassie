package retriever_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/boxo/bitswap/client/traceability"
	"github.com/ipfs/boxo/blockservice"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitswapRetriever(t *testing.T) {
	ctx := context.Background()

	store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true
	tbc1 := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	tbc2 := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	var tbc1Cids []cid.Cid
	for _, blk := range tbc1.AllBlocks() {
		tbc1Cids = append(tbc1Cids, blk.Cid())
	}
	var tbc2Cids []cid.Cid
	for _, blk := range tbc2.AllBlocks() {
		tbc2Cids = append(tbc2Cids, blk.Cid())
	}
	allCids := append(tbc1Cids, tbc2Cids...)
	cid1 := tbc1.TipLink.(cidlink.Link).Cid
	cid2 := tbc2.TipLink.(cidlink.Link).Cid

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	depth10Selector := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
	remoteBlockDuration := 50 * time.Millisecond

	testCases := []struct {
		name               string
		localLinkSystems   func() map[cid.Cid]*linking.LinkSystem
		remoteLinkSystems  map[cid.Cid]*linking.LinkSystem
		selector           []ipld.Node
		expectedCandidates map[cid.Cid][]types.RetrievalCandidate
		expectedEvents     map[cid.Cid][]types.EventCode
		expectedStats      map[cid.Cid]*types.RetrievalStats
		expectedErrors     map[cid.Cid]string
		expectedCids       []cid.Cid
		expectedRemoteCids map[cid.Cid][]cid.Cid
		cfg                retriever.BitswapConfig
	}{
		{
			name: "successful full remote fetch",
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.AllBlocks(), false),
				cid2: makeLsys(tbc2.AllBlocks(), false),
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 100)...), types.SuccessCode),
				cid2: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 100)...), types.SuccessCode),
			},
			expectedCids: allCids,
			expectedRemoteCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids,
				cid2: tbc2Cids,
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:      cid1,
					Size:         sizeOf(tbc1.AllBlocks()),
					Blocks:       100,
					Duration:     remoteBlockDuration * 100,
					AverageSpeed: uint64(float64(sizeOf(tbc1.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
				cid2: {
					RootCid:      cid2,
					Size:         sizeOf(tbc2.AllBlocks()),
					Blocks:       100,
					Duration:     remoteBlockDuration * 100,
					AverageSpeed: uint64(float64(sizeOf(tbc2.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
			},
		},
		{
			name: "successful partial remote fetch",
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.Blocks(50, 100), false),
				cid2: makeLsys(append(tbc2.Blocks(25, 45), tbc2.Blocks(75, 100)...), false),
			},
			localLinkSystems: func() map[cid.Cid]*linking.LinkSystem {
				return map[cid.Cid]*linking.LinkSystem{
					cid1: makeLsys(tbc1.Blocks(0, 50), false),
					cid2: makeLsys(append(tbc2.Blocks(0, 25), tbc2.Blocks(45, 75)...), false),
				}
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 50)...), types.SuccessCode),
				cid2: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 45)...), types.SuccessCode),
			},
			// only expect to bitswap fetch the blocks we don't have
			expectedRemoteCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids[50:],
				cid2: append(append([]cid.Cid{}, tbc2Cids[25:45]...), tbc2Cids[75:]...),
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:      cid1,
					Size:         sizeOf(tbc1.Blocks(50, 100)),
					Blocks:       50,
					Duration:     remoteBlockDuration * 50,
					AverageSpeed: uint64(float64(sizeOf(tbc1.Blocks(50, 100))) / (remoteBlockDuration * 50).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
				cid2: {
					RootCid:      cid2,
					Size:         sizeOf(append(tbc2.Blocks(25, 45), tbc2.Blocks(75, 100)...)),
					Blocks:       45,
					Duration:     remoteBlockDuration * 45,
					AverageSpeed: uint64(float64(sizeOf(append(tbc2.Blocks(25, 45), tbc2.Blocks(75, 100)...))) / (remoteBlockDuration * 45).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
			},
		},
		{
			name: "successful selective remote fetch",
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.AllBlocks(), false),
				cid2: makeLsys(tbc2.AllBlocks(), false),
			},
			// recurse depth of 10 will yield the first 5 blocks *only*, because
			// the selector counts nodes and we have blocks with {Parents:[&Any]},
			// i.e. two jumps per block
			selector: []ipld.Node{depth10Selector, depth10Selector},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 5)...), types.SuccessCode),
				cid2: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 5)...), types.SuccessCode),
			},
			expectedCids: append(append([]cid.Cid{}, tbc1Cids[:5]...), tbc2Cids[:5]...),
			expectedRemoteCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids[:5],
				cid2: tbc2Cids[:5],
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:      cid1,
					Size:         sizeOf(tbc1.AllBlocks()[:5]),
					Blocks:       5,
					Duration:     remoteBlockDuration * 5,
					AverageSpeed: uint64(float64(sizeOf(tbc1.AllBlocks()[:5])) / (remoteBlockDuration * 5).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
				cid2: {
					RootCid:      cid2,
					Size:         sizeOf(tbc2.AllBlocks()[:5]),
					Blocks:       5,
					Duration:     remoteBlockDuration * 5,
					AverageSpeed: uint64(float64(sizeOf(tbc2.AllBlocks()[:5])) / (remoteBlockDuration * 5).Seconds()),
					TotalPayment: big.Zero(),
					AskPrice:     big.Zero(),
				},
			},
		},
		{
			name: "fail remote fetch about non-zero blocks",
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.Blocks(0, 50), false),
				cid2: makeLsys(tbc2.Blocks(0, 50), false),
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 50)...), types.FailedRetrievalCode),
				cid2: append(append([]types.EventCode{types.StartedRetrievalCode, types.FirstByteCode}, repeatCode(types.BlockReceivedCode, 50)...), types.FailedRetrievalCode),
			},
			expectedErrors: map[cid.Cid]string{
				cid1: "could not load link",
				cid2: "could not load link",
			},
		},
		{
			name: "fail remote fetch immediately",
			localLinkSystems: func() map[cid.Cid]*linking.LinkSystem {
				return map[cid.Cid]*linking.LinkSystem{
					cid1: makeLsys(tbc1.Blocks(0, 50), false),
					cid2: makeLsys(tbc2.Blocks(0, 50), false),
				}
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedRetrievalCode, types.FailedRetrievalCode},
				cid2: {types.StartedRetrievalCode, types.FailedRetrievalCode},
			},
			expectedErrors: map[cid.Cid]string{
				cid1: "could not load link",
				cid2: "could not load link",
			},
		},
		{
			name: "failed no candidates",
			localLinkSystems: func() map[cid.Cid]*linking.LinkSystem {
				return map[cid.Cid]*linking.LinkSystem{
					cid1: makeLsys(tbc1.Blocks(0, 50), false),
					cid2: makeLsys(tbc2.Blocks(0, 50), false),
				}
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{},
		},
		{
			name: "timeout",
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.AllBlocks(), false),
				cid2: makeLsys(tbc2.AllBlocks(), false),
			},
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: testutil.GenerateRetrievalCandidates(t, 5),
				cid2: testutil.GenerateRetrievalCandidates(t, 7),
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedRetrievalCode, types.FirstByteCode, types.BlockReceivedCode, types.FailedRetrievalCode},
				cid2: {types.StartedRetrievalCode, types.FirstByteCode, types.BlockReceivedCode, types.FailedRetrievalCode},
			},
			expectedErrors: map[cid.Cid]string{
				cid1: "retrieval failed; retrieval timed out after 10ms",
				cid2: "retrieval failed; retrieval timed out after 10ms",
			},
			cfg: retriever.BitswapConfig{
				BlockTimeout: 10 * time.Millisecond,
			},
		},
	}

	for _, withPreloader := range []bool{false, true} {
		name := "without preloader"
		if withPreloader {
			name = "with preloader"
		}
		t.Run(name, func(t *testing.T) {
			for _, testCase := range testCases {
				withPreloader := withPreloader
				testCase := testCase

				t.Run(testCase.name, func(t *testing.T) {
					t.Parallel()

					req := require.New(t)
					ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()

					// storage
					var localLinkSystems map[cid.Cid]*linking.LinkSystem
					if testCase.localLinkSystems != nil {
						localLinkSystems = testCase.localLinkSystems()
					} else {
						localLinkSystems = make(map[cid.Cid]*linking.LinkSystem)
					}
					remoteCidLoads := make(map[cid.Cid][]cid.Cid)
					remoteLinkSystems := make(map[cid.Cid]*linking.LinkSystem)
					for root, ls := range testCase.remoteLinkSystems {
						remoteCidLoads[root] = make([]cid.Cid, 0)
						// wrap the link system to record which cids are loaded
						remoteLinkSystems[root] = func(root cid.Cid, ls linking.LinkSystem, sro linking.BlockReadOpener) *linking.LinkSystem {
							ls.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
								r, err := sro(lc, l)
								if err == nil {
									remoteCidLoads[root] = append(remoteCidLoads[root], l.(cidlink.Link).Cid)
								}
								return r, err
							}
							return &ls
						}(root, *ls, ls.StorageReadOpener)
					}
					linkSystemForCid := func(c cid.Cid, lsMap map[cid.Cid]*linking.LinkSystem) *linking.LinkSystem {
						if _, ok := lsMap[c]; !ok {
							lsMap[c] = makeLsys(nil, true)
						}
						return lsMap[c]
					}
					var preloadLinkSys1, preloadLinkSys2 linking.LinkSystem
					if withPreloader {
						preloadLinkSys1 = *makeLsys(nil, true)
						preloadLinkSys2 = *makeLsys(nil, true)
					}

					// retrieval 1
					rid1, err := types.NewRetrievalID()
					req.NoError(err)
					req1Context := types.RegisterRetrievalIDToContext(ctx, rid1)
					var sel ipld.Node
					if testCase.selector != nil {
						sel = testCase.selector[0]
					}
					req1 := types.RetrievalRequest{
						RetrievalID:       rid1,
						Request:           trustlessutils.Request{Root: cid1},
						LinkSystem:        *linkSystemForCid(cid1, localLinkSystems),
						PreloadLinkSystem: preloadLinkSys1,
						Selector:          sel,
					}

					// retrieval 2
					rid2, err := types.NewRetrievalID()
					req.NoError(err)
					req2Context := types.RegisterRetrievalIDToContext(ctx, rid2)
					if testCase.selector != nil {
						sel = testCase.selector[1]
					}
					req2 := types.RetrievalRequest{
						RetrievalID:       rid2,
						Request:           trustlessutils.Request{Root: cid2},
						LinkSystem:        *linkSystemForCid(cid2, localLinkSystems),
						PreloadLinkSystem: preloadLinkSys2,
						Selector:          sel,
					}

					// bitswap && mock setup
					mbs := bitswaphelpers.NewMultiblockstore()
					clock := clock.NewMock()
					unlockExchange := make(chan struct{})
					exchange := &mockExchange{
						getLsys: func(ctx context.Context) (*linking.LinkSystem, error) {
							select {
							case <-ctx.Done():
								req.FailNow("exchange not unlocked")
							case <-unlockExchange:
							}
							clock.Add(remoteBlockDuration)
							id, err := types.RetrievalIDFromContext(ctx)
							if err != nil {
								return nil, err
							}
							switch id {
							case rid1:
								return linkSystemForCid(cid1, remoteLinkSystems), nil
							case rid2:
								return linkSystemForCid(cid2, remoteLinkSystems), nil
							default:
								return nil, errors.New("unrecognized retrieval")
							}
						},
					}
					bsrv := blockservice.New(mbs, exchange)
					mir := newMockIndexerRouting()
					mipc := &mockInProgressCids{}
					awaitReceivedCandidates := make(chan struct{}, 1)
					bsr := retriever.NewBitswapRetrieverFromDeps(ctx, bsrv, mir, mipc, mbs, testCase.cfg, clock, awaitReceivedCandidates)
					receivedEvents := make(map[cid.Cid][]types.RetrievalEvent)
					retrievalCollector := func(evt types.RetrievalEvent) {
						receivedEvents[evt.PayloadCid()] = append(receivedEvents[evt.PayloadCid()], evt)
					}

					// retrieve
					retrieval1 := bsr.Retrieve(req1Context, req1, retrievalCollector)
					retrieval2 := bsr.Retrieve(req2Context, req2, retrievalCollector)
					receivedStats := make(map[cid.Cid]*types.RetrievalStats, 2)
					receivedErrors := make(map[cid.Cid]error, 2)
					expectedCandidates := make(map[types.RetrievalID][]types.RetrievalCandidate)
					if testCase.expectedCandidates != nil {
						for key, candidates := range testCase.expectedCandidates {
							switch key {
							case cid1:
								expectedCandidates[rid1] = candidates
							case cid2:
								expectedCandidates[rid2] = candidates
							}
						}
					}
					// reset the clock
					clock.Set(time.Now())
					retrievalResult := make(chan types.RetrievalResult, 1)
					go func() {
						stats, err := retrieval1.RetrieveFromAsyncCandidates(makeAsyncCandidates(t, expectedCandidates[rid1]))
						retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
					}()
					if len(expectedCandidates[rid1]) > 0 {
						select {
						case <-ctx.Done():
							req.FailNow("did not receive all candidates")
						case <-awaitReceivedCandidates:
						}
					}
					close(unlockExchange)

					var stats *types.RetrievalStats
					select {
					case <-ctx.Done():
						req.FailNow("did not receive result")
					case result := <-retrievalResult:
						stats, err = result.Stats, result.Err
					}
					if stats != nil {
						receivedStats[cid1] = stats
					}
					if err != nil {
						receivedErrors[cid1] = err
					}

					// reset the clock
					clock.Set(time.Now())
					unlockExchange = make(chan struct{})
					go func() {
						stats, err := retrieval2.RetrieveFromAsyncCandidates(makeAsyncCandidates(t, expectedCandidates[rid2]))
						retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
					}()
					if len(expectedCandidates[rid2]) > 0 {
						select {
						case <-ctx.Done():
							req.FailNow("did not receive all candidates")
						case <-awaitReceivedCandidates:
						}
					}
					close(unlockExchange)

					// collect results & verify
					select {
					case <-ctx.Done():
						req.FailNow("did not receive result")
					case result := <-retrievalResult:
						stats, err = result.Stats, result.Err
					}
					if stats != nil {
						receivedStats[cid2] = stats
					}
					if err != nil {
						receivedErrors[cid2] = err
					}
					receivedCodes := make(map[cid.Cid][]types.EventCode, len(receivedEvents))
					expectedErrors := testCase.expectedErrors
					if expectedErrors == nil {
						expectedErrors = make(map[cid.Cid]string)
					}
					for c, expectedError := range expectedErrors {
						req.Contains(receivedErrors, c)
						req.ErrorContains(receivedErrors[c], expectedError)
					}
					expectedStats := testCase.expectedStats
					if expectedStats == nil {
						expectedStats = make(map[cid.Cid]*types.RetrievalStats)
					}
					req.Equal(expectedStats, receivedStats)
					for key, events := range receivedEvents {
						receivedCodes[key] = make([]types.EventCode, 0, len(events))
						for _, event := range events {
							receivedCodes[key] = append(receivedCodes[key], event.Code())
						}
					}
					if testCase.expectedEvents == nil {
						testCase.expectedEvents = make(map[cid.Cid][]types.EventCode)
					}
					req.Equal(testCase.expectedEvents, receivedCodes)
					req.Equal(expectedCandidates, mir.candidatesAdded)
					expectedCandidatesRemoved := map[types.RetrievalID]struct{}{}
					if len(expectedCandidates[rid1]) > 0 {
						expectedCandidatesRemoved[rid1] = struct{}{}
					}
					if len(expectedCandidates[rid2]) > 0 {
						expectedCandidatesRemoved[rid2] = struct{}{}
					}
					req.Equal(expectedCandidatesRemoved, mir.candidatesRemoved)
					if testCase.expectedCids != nil {
						req.ElementsMatch(testCase.expectedCids, mipc.incremented)
						req.ElementsMatch(testCase.expectedCids, mipc.decremented)
					}
					if testCase.expectedRemoteCids != nil {
						for c, lst := range testCase.expectedRemoteCids {
							req.ElementsMatch(lst, remoteCidLoads[c])
						}
					}
				})
			}
		})
	}
}

type mockExchange struct {
	getLsys func(ctx context.Context) (*linking.LinkSystem, error)
}

// GetBlock returns the block associated with a given key.
func (me *mockExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	lsys, err := me.getLsys(ctx)
	if err != nil {
		return nil, err
	}
	r, err := lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}
	return traceability.Block{
		Block: blk,
	}, nil
}

func (me *mockExchange) GetBlocks(_ context.Context, _ []cid.Cid) (<-chan blocks.Block, error) {
	panic("not implemented") // TODO: Implement
}

// NotifyNewBlocks tells the exchange that new blocks are available and can be served.
func (me *mockExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil
}

func (me *mockExchange) Close() error {
	panic("not implemented") // TODO: Implement
}

func (me *mockExchange) NewSession(_ context.Context) exchange.Fetcher {
	return me
}

type mockIndexerRouting struct {
	incomingRetrievals map[types.RetrievalID]struct{}
	candidatesAdded    map[types.RetrievalID][]types.RetrievalCandidate
	candidatesRemoved  map[types.RetrievalID]struct{}
}

func newMockIndexerRouting() *mockIndexerRouting {
	return &mockIndexerRouting{
		incomingRetrievals: make(map[types.RetrievalID]struct{}),
		candidatesAdded:    make(map[types.RetrievalID][]types.RetrievalCandidate),
		candidatesRemoved:  make(map[types.RetrievalID]struct{}),
	}
}
func (mir *mockIndexerRouting) AddProviders(rid types.RetrievalID, candidates []types.RetrievalCandidate) {
	mir.candidatesAdded[rid] = append(mir.candidatesAdded[rid], candidates...)
}

func (mir *mockIndexerRouting) SignalIncomingRetrieval(rid types.RetrievalID) {
	mir.incomingRetrievals[rid] = struct{}{}
}

func (mir *mockIndexerRouting) RemoveProviders(rid types.RetrievalID) {
	mir.candidatesRemoved[rid] = struct{}{}
}

func makeLsys(blocks []blocks.Block, threadsafe bool) *linking.LinkSystem {
	bag := make(map[string][]byte, len(blocks))
	for _, block := range blocks {
		bag[cidlink.Link{Cid: block.Cid()}.Binary()] = block.RawData()
	}
	lsys := cidlink.DefaultLinkSystem()
	var store testutil.ParentStore = &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{Bag: bag}}
	if threadsafe {
		store = &testutil.ThreadsafeStore{ParentStore: store}
	}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true
	return &lsys
}

func sizeOf(blocks []blocks.Block) uint64 {
	total := uint64(0)
	for _, block := range blocks {
		total += uint64(len(block.RawData()))
	}
	return total
}

type mockInProgressCids struct {
	incremented []cid.Cid
	decremented []cid.Cid
}

func (mipc *mockInProgressCids) Inc(c cid.Cid, _ types.RetrievalID) {
	mipc.incremented = append(mipc.incremented, c)
}

func (mipc *mockInProgressCids) Dec(c cid.Cid, _ types.RetrievalID) {
	mipc.decremented = append(mipc.decremented, c)
}

func repeatCode(code types.EventCode, n int) []types.EventCode {
	eventCodes := make([]types.EventCode, 0, n)
	for i := 0; i < n; i++ {
		eventCodes = append(eventCodes, code)
	}
	return eventCodes
}
