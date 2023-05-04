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
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
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

type httpRemote struct {
	peer          peer.AddrInfo
	lsys          *linking.LinkSystem
	sel           ipld.Node
	responseDelay time.Duration
}

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
	rid1 := types.RetrievalID(uuid.New())
	rid2 := types.RetrievalID(uuid.New())
	remoteBlockDuration := 50 * time.Millisecond
	allSelector := selectorparse.CommonSelector_ExploreAllRecursively
	getTimeout := func(_ peer.ID) time.Duration { return 5 * time.Second }
	startTime := time.Now().Add(time.Hour)

	testCases := []struct {
		name           string
		requests       map[cid.Cid]types.RetrievalID
		requestPath    map[cid.Cid]string
		requestScope   map[cid.Cid]types.CarScope
		remotes        map[cid.Cid][]httpRemote
		expectedStats  map[cid.Cid]types.RetrievalStats
		expectedErrors map[cid.Cid]struct{}
		expectedCids   map[cid.Cid][]cid.Cid
		expectSequence []testutil.ExpectedActionsAtTime
	}{
		{
			name:     "single full fetch, one peer",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1},
			remotes: map[cid.Cid][]httpRemote{
				cid1: {
					{
						peer:          cid1Cands[0].MinerPeer,
						lsys:          makeLsys(tbc1.AllBlocks()),
						sel:           allSelector,
						responseDelay: time.Millisecond * 40,
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids,
			},
			expectedStats: map[cid.Cid]types.RetrievalStats{
				cid1: {
					RootCid:           cid1,
					StorageProviderId: cid1Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc1.AllBlocks()),
					Blocks:            100,
					Duration:          40*time.Millisecond + remoteBlockDuration*100,
					AverageSpeed:      uint64(float64(sizeOf(tbc1.AllBlocks())) / (40*time.Millisecond + remoteBlockDuration*100).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
					},
				},
				{
					AfterStart:         0,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: time.Millisecond * 40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(time.Millisecond*40), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer)),
					},
				},
				{
					AfterStart: time.Millisecond*40 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(time.Millisecond*40+remoteBlockDuration*100), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0),
					},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[0].MinerPeer.ID,
							Root:      cid1,
							ByteCount: sizeOf(tbc1.AllBlocks()),
							Blocks:    tbc1Cids,
						},
					},
					CompletedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
			},
		},
		{
			name:     "two parallel full fetch, small offset, one peer each",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1, cid2: rid2},
			remotes: map[cid.Cid][]httpRemote{
				cid1: {
					{
						peer:          cid1Cands[0].MinerPeer,
						lsys:          makeLsys(tbc1.AllBlocks()),
						sel:           allSelector,
						responseDelay: time.Millisecond * 40,
					},
				},
				cid2: {
					{
						peer:          cid2Cands[0].MinerPeer,
						lsys:          makeLsys(tbc2.AllBlocks()),
						sel:           allSelector,
						responseDelay: time.Millisecond * 10,
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{
				cid1: tbc1Cids,
				cid2: tbc2Cids,
			},
			expectedStats: map[cid.Cid]types.RetrievalStats{
				cid1: {
					RootCid:           cid1,
					StorageProviderId: cid1Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc1.AllBlocks()),
					Blocks:            100,
					Duration:          40*time.Millisecond + remoteBlockDuration*100,
					AverageSpeed:      uint64(float64(sizeOf(tbc1.AllBlocks())) / (40*time.Millisecond + remoteBlockDuration*100).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
				cid2: {
					RootCid:           cid2,
					StorageProviderId: cid2Cands[0].MinerPeer.ID,
					Size:              sizeOf(tbc2.AllBlocks()),
					Blocks:            100,
					Duration:          10*time.Millisecond + remoteBlockDuration*100,
					AverageSpeed:      uint64(float64(sizeOf(tbc2.AllBlocks())) / (10*time.Millisecond + remoteBlockDuration*100).Seconds()),
					TimeToFirstByte:   10 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Started(startTime, rid2, startTime, types.RetrievalPhase, toCandidate(cid2, cid2Cands[0].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Connected(startTime, rid2, startTime, types.RetrievalPhase, toCandidate(cid2, cid2Cands[0].MinerPeer)),
					},
				},
				{
					AfterStart:         0,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID, cid2Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: time.Millisecond * 10,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(time.Millisecond*10), rid2, startTime, toCandidate(cid2, cid2Cands[0].MinerPeer)),
					},
				},
				{
					AfterStart: time.Millisecond * 40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(time.Millisecond*40), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer)),
					},
				},
				{
					AfterStart: time.Millisecond*10 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(time.Millisecond*10+remoteBlockDuration*100), rid2, startTime, toCandidate(cid2, cid2Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 10*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0),
					},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid2Cands[0].MinerPeer.ID,
							Root:      cid2,
							ByteCount: sizeOf(tbc2.AllBlocks()),
							Blocks:    tbc2Cids,
						},
					},
					CompletedRetrievals: []peer.ID{cid2Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: time.Millisecond*40 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(time.Millisecond*40+remoteBlockDuration*100), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0),
					},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[0].MinerPeer.ID,
							Root:      cid1,
							ByteCount: sizeOf(tbc1.AllBlocks()),
							Blocks:    tbc1Cids,
						},
					},
					CompletedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
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
			clock.Set(startTime)

			awaitReceivedCandidates := make(chan struct{}, 1)
			getRemote := func(cid cid.Cid, maddr string) httpRemote {
				remotes, ok := testCase.remotes[cid]
				req.True(ok)
				for _, remote := range remotes {
					if remote.peer.Addrs[0].String() == maddr {
						return remote
					}
				}
				t.Fatal("remote not found")
				return httpRemote{}
			}

			roundTripper := NewCannedBytesRoundTripper(t, ctx, clock, remoteBlockDuration, testCase.requestPath, testCase.requestScope, getRemote)
			defer roundTripper.Close()
			client := &http.Client{Transport: roundTripper}
			retriever := retriever.NewHttpRetrieverWithDeps(getTimeout, client, clock, awaitReceivedCandidates)

			retrievals := make([]testutil.RunRetrieval, 0)
			expectedStats := make([]types.RetrievalStats, 0)
			for cid, rid := range testCase.requests {
				cid := cid
				rid := rid
				expectedStats = append(expectedStats, testCase.expectedStats[cid])
				retrievals = append(retrievals, func(eventsCb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
					request := types.RetrievalRequest{
						RetrievalID: rid,
						Cid:         cid,
						LinkSystem:  *makeLsys(nil),
						Path:        testCase.requestPath[cid],
						Scope:       testCase.requestScope[cid],
					}
					candidates := toCandidates(cid, testCase.remotes[cid])
					return retriever.Retrieve(context.Background(), request, eventsCb).
						RetrieveFromAsyncCandidates(makeAsyncCandidates(t, candidates))
				})
			}

			results := testutil.RetrievalVerifier{
				ExpectedSequence: testCase.expectSequence,
			}.RunWithVerification(ctx, t, clock, roundTripper, nil, retrievals)

			require.Len(t, results, len(testCase.requests))
			actualStats := make([]types.RetrievalStats, 0)
			for _, result := range results {
				stats, err := result.Stats, result.Err
				require.NoError(t, err)
				require.NotNil(t, stats)
				actualStats = append(actualStats, *stats)
			}
			require.ElementsMatch(t, expectedStats, actualStats)
		})
	}
}

func toCandidates(root cid.Cid, remotes []httpRemote) []types.RetrievalCandidate {
	candidates := make([]types.RetrievalCandidate, len(remotes))
	for i, r := range remotes {
		candidates[i] = toCandidate(root, r.peer)
	}
	return candidates
}

func toCandidate(root cid.Cid, peer peer.AddrInfo) types.RetrievalCandidate {
	return types.NewRetrievalCandidate(peer.ID, peer.Addrs, root, &metadata.IpfsGatewayHttp{})
}

type cannedBytesRoundTripper struct {
	StartsCh chan peer.ID
	StatsCh  chan testutil.RemoteStats
	EndsCh   chan peer.ID

	t                   *testing.T
	ctx                 context.Context
	clock               *clock.Mock
	remoteBlockDuration time.Duration
	expectedPath        map[cid.Cid]string
	expectedScope       map[cid.Cid]types.CarScope
	getRemote           func(cid cid.Cid, maddr string) httpRemote
}

var _ http.RoundTripper = (*cannedBytesRoundTripper)(nil)

func NewCannedBytesRoundTripper(
	t *testing.T,
	ctx context.Context,
	clock *clock.Mock,
	remoteBlockDuration time.Duration,
	expectedPath map[cid.Cid]string,
	expectedScope map[cid.Cid]types.CarScope,
	getRemote func(cid cid.Cid, maddr string) httpRemote,
) *cannedBytesRoundTripper {
	return &cannedBytesRoundTripper{
		make(chan peer.ID, 32),
		make(chan testutil.RemoteStats, 32),
		make(chan peer.ID, 32),
		t,
		ctx,
		clock,
		remoteBlockDuration,
		expectedPath,
		expectedScope,
		getRemote,
	}
}

func (c *cannedBytesRoundTripper) Close() {
	close(c.StartsCh)
	close(c.EndsCh)
}

func (c *cannedBytesRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	us := strings.Split(req.URL.Path, "/")
	require.True(c.t, len(us) > 2)
	require.Equal(c.t, us[1], "ipfs")
	root, err := cid.Parse(us[2])
	require.NoError(c.t, err)
	path := strings.Join(us[3:], "/")
	expectedPath, ok := c.expectedPath[root]
	if !ok {
		require.Equal(c.t, path, "")
	} else {
		require.Equal(c.t, path, expectedPath)
	}
	expectedScope := types.CarScopeAll
	if scope, ok := c.expectedScope[root]; ok {
		expectedScope = scope
	}
	require.Equal(c.t, req.URL.RawQuery, fmt.Sprintf("car-scope=%s", expectedScope))
	ip := req.URL.Hostname()
	port := req.URL.Port()
	maddr := fmt.Sprintf("/ip4/%s/tcp/%s/http", ip, port)
	remote := c.getRemote(root, maddr)
	c.StartsCh <- remote.peer.ID

	makeBody := func(root cid.Cid, maddr string) io.ReadCloser {
		carR, carW := io.Pipe()
		statsCh := traverseCar(c.t, c.ctx, remote.peer.ID, c.clock, c.remoteBlockDuration, carW, remote.lsys, root, remote.sel)
		go func() {
			select {
			case <-c.ctx.Done():
				return
			case stats, ok := <-statsCh:
				if !ok {
					return
				}
				c.StatsCh <- stats
			}
		}()
		return carR
	}

	c.clock.Sleep(remote.responseDelay)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &deferredReader{root: root, maddr: maddr, makeBody: makeBody, end: func() { c.EndsCh <- remote.peer.ID }},
	}, nil
}

func (c *cannedBytesRoundTripper) VerifyConnectionsReceived(ctx context.Context, t *testing.T, expectedConnections []peer.ID) {
	// connection is currently a noop
}

func (c *cannedBytesRoundTripper) VerifyRetrievalsReceived(ctx context.Context, t *testing.T, expectedRetrievals []peer.ID) {
	retrievals := make([]peer.ID, 0, len(expectedRetrievals))
	for i := 0; i < len(expectedRetrievals); i++ {
		select {
		case retrieval := <-c.StartsCh:
			retrievals = append(retrievals, retrieval)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected retrievals", "expected %d, received %d", len(expectedRetrievals), i)
		}
	}
	require.ElementsMatch(t, expectedRetrievals, retrievals)
}

func (c *cannedBytesRoundTripper) VerifyRetrievalsServed(ctx context.Context, t *testing.T, expectedServed []testutil.RemoteStats) {
	remoteStats := make([]testutil.RemoteStats, 0, len(expectedServed))
	for i := 0; i < len(expectedServed); i++ {
		select {
		case stats := <-c.StatsCh:
			remoteStats = append(remoteStats, stats)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected served", "expected %d, received %d", len(expectedServed), i)
		}
	}
	require.ElementsMatch(t, expectedServed, remoteStats)
}

func (c *cannedBytesRoundTripper) VerifyRetrievalsCompleted(ctx context.Context, t *testing.T, expectedRetrievals []peer.ID) {
	retrievals := make([]peer.ID, 0, len(expectedRetrievals))
	for i := 0; i < len(expectedRetrievals); i++ {
		select {
		case retrieval := <-c.EndsCh:
			retrievals = append(retrievals, retrieval)
		case <-ctx.Done():
			require.FailNowf(t, "failed to complete expected retrievals", "expected %d, received %d", len(expectedRetrievals), i)
		}
	}
	require.ElementsMatch(t, expectedRetrievals, retrievals)
}

// deferredReader is simply a Reader that lazily calls makeBody on the first Read
// so we don't begin CAR generation if the HTTP response body never gets read by
// the client.
type deferredReader struct {
	root     cid.Cid
	maddr    string
	makeBody func(cid.Cid, string) io.ReadCloser
	end      func()

	r    io.ReadCloser
	once sync.Once
}

var _ io.ReadCloser = (*deferredReader)(nil)

func (d *deferredReader) Read(p []byte) (n int, err error) {
	d.once.Do(func() {
		d.r = d.makeBody(d.root, d.maddr)
	})
	n, err = d.r.Read(p)
	if err == io.EOF {
		d.end()
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
	id peer.ID,
	clock *clock.Mock,
	blockDuration time.Duration,
	carW io.WriteCloser,
	lsys *linking.LinkSystem,
	root cid.Cid,
	selNode ipld.Node,
) chan testutil.RemoteStats {
	req := require.New(t)

	sel, err := selector.CompileSelector(selNode)
	req.NoError(err)

	statsCh := make(chan testutil.RemoteStats, 1)
	go func() {
		stats := testutil.RemoteStats{
			Peer:   id,
			Root:   root,
			Blocks: make([]cid.Cid, 0),
		}

		// instantiating this writes a CARv1 header and waits for more Put()s
		carWriter, err := storage.NewWritable(carW, []cid.Cid{root}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
		req.NoError(err)

		// intercept the StorageReadOpener of the LinkSystem so that for each
		// read that the traverser performs, we take that block and Put() it
		// to the CARv1 writer.
		originalSRO := lsys.StorageReadOpener
		lsys.StorageReadOpener = func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
			stats.Blocks = append(stats.Blocks, lnk.(cidlink.Link).Cid)
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
			stats.ByteCount += uint64(len(byts)) // only the length of the bytes, not the rest of the CAR infrastructure
			clock.Sleep(blockDuration)
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
