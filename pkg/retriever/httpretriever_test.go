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
		expectedCandidates map[cid.Cid][]types.RetrievalCandidate
		expectedEvents     map[cid.Cid][]types.EventCode
		expectedStats      map[cid.Cid]*types.RetrievalStats
		expectedErrors     map[cid.Cid]struct{}
		expectedCids       map[cid.Cid][]cid.Cid
	}{
		{
			name:         "successful full remote fetch x 2",
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
			expectedCandidates: map[cid.Cid][]types.RetrievalCandidate{
				cid1: cid1Cands[0:1],
				cid2: cid2Cands[0:1],
			},
			expectedEvents: map[cid.Cid][]types.EventCode{
				cid1: {types.StartedCode, types.ConnectedCode, types.FirstByteCode, types.SuccessCode},
				cid2: {types.StartedCode, types.ConnectedCode, types.FirstByteCode, types.SuccessCode},
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
				}()
				return carR
			}
			rt := NewCannedBytesRoundTripper(t, testCase.requestPath, testCase.requestScope, testCase.expectedCandidates, makeBody)
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
				stats, err := retrieval1.RetrieveFromAsyncCandidates(makeAsyncCandidates(expectedCandidates[rid1]))
				retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
			}()
			if len(expectedCandidates[rid1]) > 0 {
				select {
				case <-ctx.Done():
					req.FailNow("did not receive all candidates")
				case <-awaitReceivedCandidates:
				}
			}

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
				receivedErrors[cid1] = struct{}{}
			}

			// reset the clock
			clock.Set(time.Now())
			go func() {
				stats, err := retrieval2.RetrieveFromAsyncCandidates(makeAsyncCandidates(expectedCandidates[rid2]))
				retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
			}()
			if len(expectedCandidates[rid2]) > 0 {
				select {
				case <-ctx.Done():
					req.FailNow("did not receive all candidates")
				case <-awaitReceivedCandidates:
				}
			}
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
				receivedErrors[cid2] = struct{}{}
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

			receivedCodes := make(map[cid.Cid][]types.EventCode, len(receivedEvents))
			expectedErrors := testCase.expectedErrors
			if expectedErrors == nil {
				expectedErrors = make(map[cid.Cid]struct{})
			}
			req.Equal(expectedErrors, receivedErrors)
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
				testCase.expectedEvents = make(map[cid.Cid][]types.EventCode)
			}
			req.Equal(testCase.expectedEvents, receivedCodes)
		})
	}
}

/*
func TestHTTPRetrieverX(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := require.New(t)
	clock := clock.NewMock()
	clock.Set(time.Now())
	retrievalID := types.RetrievalID(uuid.New())
	getTimeout := func(_ peer.ID) time.Duration { return 5 * time.Second }
	// setup remote with a DAG ready to serve as a CAR
	remote := newHttpRemote(ctx, t)
	candidate := NewHttpCandidate(peer.ID("foo"), nil, remote.RootCid)

	// local store, track the CIDs in the order we receive them
	receivedBlocks := make([]cid.Cid, 0)
	clientStore := &correctedMemStore{&memstore.Store{}}
	clientLsys := cidlink.DefaultLinkSystem()
	clientLsys.SetReadStorage(clientStore)
	clientLsys.SetWriteStorage(clientStore)
	originalCWO := clientLsys.StorageWriteOpener
	clientLsys.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		w, wc, err := originalCWO(lc)
		if err != nil {
			return nil, nil, err
		}
		return w, func(l datamodel.Link) error {
			receivedBlocks = append(receivedBlocks, l.(cidlink.Link).Cid)
			return wc(l)
		}, nil
	}

	carR, carW := io.Pipe()
	rt := NewCannedBytesRoundTripper(carR)
	// mock http client that returns carBytes as the response body regardless
	// of the request
	client := &http.Client{Transport: rt}

	retriever := retriever.NewHttpRetrieverWithClock(getTimeout, client, clock)

	request := types.RetrievalRequest{
		Cid:         remote.RootCid,
		RetrievalID: retrievalID,
		LinkSystem:  clientLsys,
		Scope:       types.CarScopeAll,
	}

	connectedCh := make(chan struct{})
	eventCb := func(events types.RetrievalEvent) {
		switch events.Code() {
		case types.ConnectedCode:
			close(connectedCh)
		}
	}

	var stats *types.RetrievalStats
	var retrievalErr error
	doneCh := make(chan struct{})
	go func() {
		retrieval := retriever.Retrieve(ctx, request, eventCb)
		stats, retrievalErr = retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{candidate}))
		close(doneCh)
	}()

	waitForClosed(t, ctx, connectedCh) // successfully started the HTTP request, got a body reader
	// check the request that was given to the client
	req.NotNil(rt.req)
	req.Equal(fmt.Sprintf("http://127.1.2.3:6789/ipfs/%s?car-scope=all", remote.RootCid.String()), rt.req.URL.String())
	// write the body from remote side
	remote.GenerateCar(carW)
	waitForClosed(t, ctx, doneCh) // successfully finished the retrieval

	req.NoError(retrievalErr)
	req.NotNil(stats)
	req.Equal(peer.ID("foo"), stats.StorageProviderId)
	req.Equal(remote.RootCid, stats.RootCid)
	req.Equal(uint64(100), stats.Blocks)
	req.Equal(remote.SentBytes, stats.Size)
	req.Len(remote.SentBlocks, 100)
	req.Equal(remote.SentBlocks, receivedBlocks)
}
*/

type cannedBytesRoundTripper struct {
	t             *testing.T
	expectedPath  []string
	expectedScope []types.CarScope
	candidates    map[cid.Cid][]types.RetrievalCandidate
	makeBody      func(cid.Cid) io.ReadCloser
}

var _ http.RoundTripper = (*cannedBytesRoundTripper)(nil)

func NewCannedBytesRoundTripper(
	t *testing.T,
	expectedPath []string,
	expectedScope []types.CarScope,
	candidates map[cid.Cid][]types.RetrievalCandidate,
	makeBody func(cid.Cid) io.ReadCloser,
) *cannedBytesRoundTripper {
	return &cannedBytesRoundTripper{
		t,
		expectedPath,
		expectedScope,
		candidates,
		makeBody,
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
	cands, ok := c.candidates[root]
	require.True(c.t, ok)
	ip := req.URL.Hostname()
	port := req.URL.Port()
	var matchingCands int
	// find a candidate matching multiaddr ip & port
	for _, c := range cands {
		for _, maddr := range c.MinerPeer.Addrs {
			if maddr.String() == fmt.Sprintf("/ip4/%s/tcp/%s/http", ip, port) {
				matchingCands++
				break
			}
		}
	}
	require.Equal(c.t, 1, matchingCands)

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       c.makeBody(root),
	}, nil
}

type sentStats struct {
	root      cid.Cid
	byteCount uint64
	blocks    []cid.Cid
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
			carWriter.Put(ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
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
