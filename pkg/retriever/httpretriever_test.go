package retriever_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestHTTPRetriever(t *testing.T) {
	ctx := context.Background()

	store := &correctedMemStore{&memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
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
	cid1 := tbc1.TipLink.(cidlink.Link).Cid
	cid2 := tbc2.TipLink.(cidlink.Link).Cid
	cid1Cands := testutil.GenerateRetrievalCandidatesForCID(t, 10, cid1, metadata.IpfsGatewayHttp{})
	cid2Cands := testutil.GenerateRetrievalCandidatesForCID(t, 10, cid2, metadata.IpfsGatewayHttp{})
	remoteBlockDuration := 50 * time.Millisecond
	getTimeout := func(_ peer.ID) time.Duration { return 5 * time.Second }

	testCases := []struct {
		name               string
		localLinkSystems   map[cid.Cid]*linking.LinkSystem
		requestPath        []string
		requestScope       []types.CarScope
		remoteLinkSystems  map[cid.Cid]*linking.LinkSystem
		remoteSelector     map[cid.Cid]ipld.Node // selector to run on the remote that matches this path+scope
		expectedCandidates map[cid.Cid][][]types.RetrievalCandidate
		expectedEvents     map[cid.Cid]map[types.EventCode]int
		expectedStats      map[cid.Cid]*types.RetrievalStats
		expectedErrors     map[cid.Cid]struct{}
		expectedCids       map[cid.Cid][]cid.Cid
	}{
		{
			name:         "successful full remote fetch, single candidates",
			requestPath:  []string{"", ""},
			requestScope: []types.CarScope{types.CarScopeAll, types.CarScopeAll},
			remoteSelector: map[cid.Cid]ipld.Node{
				cid1: selectorparse.CommonSelector_ExploreAllRecursively,
				cid2: selectorparse.CommonSelector_ExploreAllRecursively,
			},
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.AllBlocks()),
				cid2: makeLsys(tbc2.AllBlocks()),
			},
			expectedCandidates: map[cid.Cid][][]types.RetrievalCandidate{
				cid1: {cid1Cands[0:1]},
				cid2: {cid2Cands[0:1]},
			},
			expectedEvents: map[cid.Cid]map[types.EventCode]int{
				cid1: {
					types.StartedCode:   1,
					types.ConnectedCode: 1,
					types.FirstByteCode: 1,
					types.SuccessCode:   1,
				},
				cid2: {
					types.StartedCode:   1,
					types.ConnectedCode: 1,
					types.FirstByteCode: 1,
					types.SuccessCode:   1,
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids,
				cid2: tbc2Cids,
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:           cid1,
					StorageProviderId: cid1Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc1.AllBlocks()),
					Blocks:            100,
					Duration:          remoteBlockDuration * 100,
					AverageSpeed:      uint64(float64(sizeOf(tbc1.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
				cid2: {
					RootCid:           cid2,
					StorageProviderId: cid2Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc2.AllBlocks()),
					Blocks:            100,
					Duration:          remoteBlockDuration * 100,
					AverageSpeed:      uint64(float64(sizeOf(tbc2.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
		},
		{
			name:         "successful full remote fetch, multiple candidates, first successful",
			requestPath:  []string{"", ""},
			requestScope: []types.CarScope{types.CarScopeAll, types.CarScopeAll},
			remoteSelector: map[cid.Cid]ipld.Node{
				cid1: selectorparse.CommonSelector_ExploreAllRecursively,
				cid2: selectorparse.CommonSelector_ExploreAllRecursively,
			},
			remoteLinkSystems: map[cid.Cid]*linking.LinkSystem{
				cid1: makeLsys(tbc1.AllBlocks()),
				cid2: makeLsys(tbc2.AllBlocks()),
			},
			expectedCandidates: map[cid.Cid][][]types.RetrievalCandidate{
				cid1: {cid1Cands},
				cid2: {cid2Cands},
			},
			expectedEvents: map[cid.Cid]map[types.EventCode]int{
				cid1: {
					types.StartedCode:   10,
					types.ConnectedCode: 10,
					types.FirstByteCode: 1,
					types.SuccessCode:   1,
				},
				cid2: {
					types.StartedCode:   10,
					types.ConnectedCode: 10,
					types.FirstByteCode: 1,
					types.SuccessCode:   1,
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids,
				cid2: tbc2Cids,
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:           cid1,
					StorageProviderId: cid1Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc1.AllBlocks()),
					Blocks:            100,
					Duration:          remoteBlockDuration * 100,
					AverageSpeed:      uint64(float64(sizeOf(tbc1.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
				cid2: {
					RootCid:           cid2,
					StorageProviderId: cid2Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc2.AllBlocks()),
					Blocks:            100,
					Duration:          remoteBlockDuration * 100,
					AverageSpeed:      uint64(float64(sizeOf(tbc2.AllBlocks())) / (remoteBlockDuration * 100).Seconds()),
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			// TODO:			t.Parallel()

			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			clock := clock.NewMock()

			localLinkSystems := testCase.localLinkSystems
			if localLinkSystems == nil {
				localLinkSystems = make(map[cid.Cid]*linking.LinkSystem)
			}
			remoteLinkSystems := testCase.remoteLinkSystems
			if remoteLinkSystems == nil {
				remoteLinkSystems = make(map[cid.Cid]*linking.LinkSystem)
			}
			linkSystemForCid := func(c cid.Cid, lsMap map[cid.Cid]*linking.LinkSystem) *linking.LinkSystem {
				if _, ok := lsMap[c]; !ok {
					lsMap[c] = makeLsys(nil)
				}
				return lsMap[c]
			}
			rid1, err := types.NewRetrievalID()
			req.NoError(err)
			req1Context := types.RegisterRetrievalIDToContext(ctx, rid1)
			req1 := types.RetrievalRequest{
				RetrievalID: rid1,
				Cid:         cid1,
				LinkSystem:  *linkSystemForCid(cid1, localLinkSystems),
				Path:        testCase.requestPath[0],
				Scope:       testCase.requestScope[0],
			}
			rid2, err := types.NewRetrievalID()
			req.NoError(err)
			req2Context := types.RegisterRetrievalIDToContext(ctx, rid2)
			req2 := types.RetrievalRequest{
				RetrievalID: rid2,
				Cid:         cid2,
				LinkSystem:  *linkSystemForCid(cid2, localLinkSystems),
				Path:        testCase.requestPath[1],
				Scope:       testCase.requestScope[1],
			}
			awaitReceivedCandidates := make(chan struct{}, 1)

			remoteStats := make([]sentStats, 0)
			var remoteStatsLk sync.Mutex
			var bodyWg sync.WaitGroup
			makeBody := func(root cid.Cid) io.ReadCloser {
				fmt.Println("makeBody")
				bodyWg.Add(1)
				// make traverser
				lsys := linkSystemForCid(root, remoteLinkSystems)
				carR, carW := io.Pipe()
				statsCh := traverseCar(t, ctx, clock, remoteBlockDuration, carW, lsys, root, testCase.remoteSelector[root])
				go func() {
					select {
					case <-ctx.Done():
						return
					case stats, ok := <-statsCh:
						if !ok {
							return
						}
						remoteStatsLk.Lock()
						remoteStats = append(remoteStats, stats)
						remoteStatsLk.Unlock()
					}
					bodyWg.Done()
					fmt.Println("bodyWg.Done()")
				}()
				return carR
			}
			rt := NewCannedBytesRoundTripper(t, ctx, testCase.requestPath, testCase.requestScope, testCase.expectedCandidates, makeBody)
			// mock http client that returns carBytes as the response body regardless
			// of the request
			client := &http.Client{Transport: rt}
			hr := retriever.NewHttpRetrieverWithDeps(getTimeout, client, clock, awaitReceivedCandidates)
			receivedEvents := make(map[cid.Cid][]types.RetrievalEvent)
			retrievalCollector := func(evt types.RetrievalEvent) {
				receivedEvents[evt.PayloadCid()] = append(receivedEvents[evt.PayloadCid()], evt)
			}
			retrieval1 := hr.Retrieve(req1Context, req1, retrievalCollector)
			retrieval2 := hr.Retrieve(req2Context, req2, retrievalCollector)
			receivedStats := make(map[cid.Cid]*types.RetrievalStats, 2)
			receivedErrors := make(map[cid.Cid]struct{}, 2)

			// reset the clock
			clock.Set(time.Now())
			retrievalResult := make(chan types.RetrievalResult, 1)
			go func() {
				stats, err := retrieval1.RetrieveFromAsyncCandidates(makeAsyncCandidates(t, testCase.expectedCandidates[cid1]))
				retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
			}()
			if len(testCase.expectedCandidates[cid1]) > 0 && len(testCase.expectedCandidates[cid1][0]) > 0 {
				select {
				case <-ctx.Done():
					req.FailNow("did not receive all candidates")
				case <-awaitReceivedCandidates:
				}
			}

			{
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
					fmt.Println("retrieval1 error", err)
					receivedErrors[cid1] = struct{}{}
				}
			}

			// reset the clock
			clock.Set(time.Now())
			go func() {
				stats, err := retrieval2.RetrieveFromAsyncCandidates(makeAsyncCandidates(t, testCase.expectedCandidates[cid2]))
				retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
			}()
			if len(testCase.expectedCandidates[cid2]) > 0 && len(testCase.expectedCandidates[cid2][0]) > 0 {
				select {
				case <-ctx.Done():
					req.FailNow("did not receive all candidates")
				case <-awaitReceivedCandidates:
				}
			}

			{
				var stats *types.RetrievalStats
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
					fmt.Println("retrieval2 error", err)
					receivedErrors[cid2] = struct{}{}
				}
			}

			done := make(chan struct{})
			go func() {
				bodyWg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				req.FailNow("Did not end sending all bodies")
			}

			expectedErrors := testCase.expectedErrors
			if expectedErrors == nil {
				expectedErrors = make(map[cid.Cid]struct{})
			}
			req.Equal(expectedErrors, receivedErrors)
			expectedStats := testCase.expectedStats
			if expectedStats == nil {
				expectedStats = make(map[cid.Cid]*types.RetrievalStats)
			}
			// print testCase.expectedCandidates for each CID in the map, in String() form as a command separated list to stdout
			// this is useful for updating the test case
			for key, candidates := range testCase.expectedCandidates {
				fmt.Printf("expectedCandidates[%s] = []types.RetrievalCandidate{\n", key)
				for _, co := range candidates {
					for _, candidate := range co {
						fmt.Printf("\t%s,\n", candidate.MinerPeer.ID.String())
					}
				}
				fmt.Printf("}\n")
				// print StorageProviderId from receivedStats map in the same way
				fmt.Printf("receivedStats[%s] = &types.RetrievalStats{StorageProviderID: %s}\n", key, receivedStats[key].StorageProviderId.String())

			}

			req.Equal(expectedStats, receivedStats)

			receivedCodes := make(map[cid.Cid]map[types.EventCode]int, 0)
			for key, events := range receivedEvents {
				if receivedCodes[key] == nil {
					receivedCodes[key] = make(map[types.EventCode]int, 0)
				}
				for _, event := range events {
					v := receivedCodes[key][event.Code()]
					receivedCodes[key][event.Code()] = v + 1
				}
			}
			expectedRemoteStats := []sentStats{
				{
					root:      cid1,
					byteCount: testCase.expectedStats[cid1].Size,
					blocks:    testCase.expectedCids[cid1],
				},
				{
					root:      cid2,
					byteCount: testCase.expectedStats[cid2].Size,
					blocks:    testCase.expectedCids[cid2],
				},
			}
			req.Equal(expectedRemoteStats, remoteStats)
			if testCase.expectedEvents == nil {
				testCase.expectedEvents = make(map[cid.Cid]map[types.EventCode]int, 0)
			}
			req.Equal(testCase.expectedEvents, receivedCodes)
		})
	}
}

type cannedBytesRoundTripper struct {
	t              *testing.T
	ctx            context.Context
	expectedPath   []string
	expectedScope  []types.CarScope
	candidates     map[cid.Cid][][]types.RetrievalCandidate
	makeBody       func(cid.Cid) io.ReadCloser
	batchCounter   map[cid.Cid][]int
	batchCounterLk *sync.Mutex
	batchWait      *sync.Cond
}

var _ http.RoundTripper = (*cannedBytesRoundTripper)(nil)

func NewCannedBytesRoundTripper(
	t *testing.T,
	ctx context.Context,
	expectedPath []string,
	expectedScope []types.CarScope,
	candidates map[cid.Cid][][]types.RetrievalCandidate,
	makeBody func(cid.Cid) io.ReadCloser,
) *cannedBytesRoundTripper {

	lk := sync.Mutex{}
	return &cannedBytesRoundTripper{
		t,
		ctx,
		expectedPath,
		expectedScope,
		candidates,
		makeBody,
		make(map[cid.Cid][]int),
		&lk,
		&sync.Cond{L: &lk},
	}
}

func (c *cannedBytesRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	us := strings.Split(req.URL.Path, "/")
	require.True(c.t, len(us) > 2)
	require.Equal(c.t, us[1], "ipfs")
	root, err := cid.Parse(us[2])
	require.NoError(c.t, err)
	path := strings.Join(us[3:], "/")
	matchedPath := -1
	for ii, p := range c.expectedPath {
		if p == path {
			matchedPath = ii
			break
		}
	}
	require.True(c.t, matchedPath > -1)
	require.Equal(c.t, req.URL.RawQuery, fmt.Sprintf("car-scope=%s", c.expectedScope[matchedPath]))
	ip := req.URL.Hostname()
	port := req.URL.Port()
	myaddr := fmt.Sprintf("/ip4/%s/tcp/%s/http", ip, port)

	cands, ok := c.candidates[root]
	require.True(c.t, ok)

	c.batchCounterLk.Lock()
	var batchNum int
	var batchCount int
	for {
		if c.ctx.Err() != nil {
			c.batchCounterLk.Unlock()
			return nil, c.ctx.Err()
		}

		// get next expected candidate to serve
		batch, ok := c.batchCounter[root]
		if ok {
			batchNum = batch[0]
			batchCount = batch[1]
		} // else start at 0,0
		if batchNum >= len(cands) {
			require.FailNowf(c.t, "too many requests (exceeded batch count)", "batchNum: %d, len(cands): %d, batchCount: %d", batchNum, len(cands), batchCount)
		}
		if batchCount >= len(cands[batchNum]) {
			require.FailNowf(c.t, "too many requests", "batchNum: %d, len(cands): %d, batchCount: %d, len(cands[batchNum]): %d", batchNum, len(cands), batchCount, len(cands[batchNum]))
		}
		if cands[batchNum][batchCount].MinerPeer.Addrs[0].String() == myaddr {
			break
		} else {
			c.batchWait.Wait()
		}
	}

	// increment values for next candidate to pick up from
	if batchCount+1 >= len(cands[batchNum]) {
		batchNum++
		batchCount = 0
	} else {
		batchCount++
	}
	fmt.Printf("Setting batch counters for %s to %d, %d\n", root, batchNum, batchCount)
	c.batchCounter[root] = []int{batchNum, batchCount}
	c.batchCounterLk.Unlock()

	endCh := make(chan struct{})
	go func() {
		select {
		case <-c.ctx.Done():
		case <-endCh:
		}
		c.batchCounterLk.Lock()
		c.batchWait.Broadcast()
		c.batchCounterLk.Unlock()
	}()

	fmt.Println("setting up response")
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &deferredReader{root: root, makeBody: c.makeBody, endCh: endCh},
	}, nil
}

type sentStats struct {
	root      cid.Cid
	byteCount uint64
	blocks    []cid.Cid
}

// deferredReader is simply a Reader that lazily calls makeBody on the first Read
// so we don't begin CAR generation if the HTTP response body never gets read by
// the client.
type deferredReader struct {
	root     cid.Cid
	makeBody func(cid.Cid) io.ReadCloser
	endCh    chan struct{}

	r    io.ReadCloser
	once sync.Once
}

var _ io.ReadCloser = (*deferredReader)(nil)

func (d *deferredReader) Read(p []byte) (n int, err error) {
	d.once.Do(func() {
		d.r = d.makeBody(d.root)
	})
	n, err = d.r.Read(p)
	if err == io.EOF {
		close(d.endCh)
	}
	return n, err
}

func (d *deferredReader) Close() error {
	if d.r != nil {
		return d.r.Close()
	}
	return nil
}

// given a writer (carW), a linkSystem, a root CID and a selector, traverse the graph
// and write the blocks in CARv1 format to the writer. Return a channel that will
// receive basic stats on what was written _after_ the write is finished.
func traverseCar(t *testing.T,
	ctx context.Context,
	clock *clock.Mock,
	blockDuration time.Duration,
	carW io.WriteCloser,
	lsys *linking.LinkSystem,
	root cid.Cid,
	selNode ipld.Node,
) chan sentStats {
	req := require.New(t)

	sel, err := selector.CompileSelector(selNode)
	req.NoError(err)

	statsCh := make(chan sentStats, 1)
	go func() {
		stats := sentStats{
			root:   root,
			blocks: make([]cid.Cid, 0),
		}

		// instantiating this writes a CARv1 header and waits for more Put()s
		carWriter, err := storage.NewWritable(carW, []cid.Cid{root}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
		req.NoError(err)

		// intercept the StorageReadOpener of the LinkSystem so that for each
		// read that the traverser performs, we take that block and Put() it
		// to the CARv1 writer.
		originalSRO := lsys.StorageReadOpener
		lsys.StorageReadOpener = func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
			stats.blocks = append(stats.blocks, lnk.(cidlink.Link).Cid)
			r, err := originalSRO(lc, lnk)
			if err != nil {
				return nil, err
			}
			byts, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			err = carWriter.Put(ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
			req.NoError(err)
			stats.byteCount += uint64(len(byts)) // only the length of the bytes, not the rest of the CAR infrastructure
			clock.Add(blockDuration)
			return bytes.NewReader(byts), nil
		}

		// load and register the root link so it's pushed to the CAR since
		// the traverser won't load it (we feed the traverser the rood _node_
		// not the link)
		rootNode, err := lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
		req.NoError(err)
		// begin traversal
		traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            ctx,
				LinkSystem:                     *lsys,
				LinkTargetNodePrototypeChooser: basicnode.Chooser,
			},
		}.WalkAdv(rootNode, sel, func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil })
		req.NoError(carW.Close())

		statsCh <- stats
	}()
	return statsCh
}
