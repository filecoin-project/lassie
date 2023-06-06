package retriever_test

import (
	"context"
	"io"
	"math"
	"net/http"
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
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestHTTPRetriever(t *testing.T) {
	ctx := context.Background()

	store := &testutil.CorrectedMemStore{ParentStore: &memstore.Store{
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
	initialPause := 10 * time.Millisecond
	startTime := time.Now().Add(time.Hour)

	testCases := []struct {
		name           string
		requests       map[cid.Cid]types.RetrievalID
		requestPath    map[cid.Cid]string
		requestScope   map[cid.Cid]types.DagScope
		remotes        map[cid.Cid][]testutil.MockRoundTripRemote
		expectedStats  map[cid.Cid]*types.RetrievalStats
		expectedErrors map[cid.Cid]struct{}
		expectedCids   map[cid.Cid][]cid.Cid // expected in this order
		expectSequence []testutil.ExpectedActionsAtTime
	}{
		{
			name:     "single, one peer, success",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				cid1: {
					{
						Peer:       cid1Cands[0].MinerPeer,
						LinkSystem: *makeLsys(tbc1.AllBlocks(), false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{cid1: tbc1Cids},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
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
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[0].MinerPeer.ID, Duration: 0},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*100), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0, multicodec.TransportIpfsGatewayHttp),
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
			name:     "two parallel, one peer each, success",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1, cid2: rid2},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				cid1: {
					{
						Peer:       cid1Cands[0].MinerPeer,
						LinkSystem: *makeLsys(tbc1.AllBlocks(), false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
				cid2: {
					{
						Peer:       cid2Cands[0].MinerPeer,
						LinkSystem: *makeLsys(tbc2.AllBlocks(), false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*10),
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{cid1: tbc1Cids, cid2: tbc2Cids},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
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
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[0].MinerPeer.ID, Duration: 0},
						{Type: testutil.SessionMetric_Connect, Provider: cid2Cands[0].MinerPeer.ID, Duration: 0},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID, cid2Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*10,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid2, startTime, toCandidate(cid2, cid2Cands[0].MinerPeer), time.Millisecond*10),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid2Cands[0].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*10 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*10+remoteBlockDuration*100), rid2, startTime, toCandidate(cid2, cid2Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 10*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0, multicodec.TransportIpfsGatewayHttp),
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
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: cid2Cands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(tbc2.AllBlocks())) / (10*time.Millisecond + remoteBlockDuration*100).Seconds())},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*100), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc1.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0, multicodec.TransportIpfsGatewayHttp),
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
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: cid1Cands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(tbc1.AllBlocks())) / (40*time.Millisecond + remoteBlockDuration*100).Seconds())},
					},
				},
			},
		},
		{
			name:     "single, multiple errors",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				cid1: {
					{
						Peer:       cid1Cands[0].MinerPeer,
						LinkSystem: *makeLsys(nil, false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*10),
						Malformed:  true,
					},
					{
						Peer:       cid1Cands[1].MinerPeer,
						LinkSystem: *makeLsys(nil, false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*20),
						Malformed:  true,
					},
					{
						Peer:       cid1Cands[2].MinerPeer,
						LinkSystem: *makeLsys(nil, false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*30),
						Malformed:  true,
					},
				},
			},
			expectedErrors: map[cid.Cid]struct{}{
				cid1: {},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer)),
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[2].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[2].MinerPeer)),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[0].MinerPeer.ID, Duration: 0},
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[1].MinerPeer.ID, Duration: 0},
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[2].MinerPeer.ID, Duration: 0},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*10,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*10),
						events.Failed(startTime.Add(initialPause+time.Millisecond*10), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer), "malformed CAR; unexpected EOF"),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
					ReceivedRetrievals:  []peer.ID{cid1Cands[1].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[0].MinerPeer.ID,
							Root:      cid1,
							ByteCount: 0,
							Blocks:    []cid.Cid{},
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: 10 * time.Millisecond},
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*20,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*20), rid1, startTime, toCandidate(cid1, cid1Cands[1].MinerPeer), time.Millisecond*10),
						events.Failed(startTime.Add(initialPause+time.Millisecond*20), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer), "malformed CAR; unexpected EOF"),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[1].MinerPeer.ID},
					ReceivedRetrievals:  []peer.ID{cid1Cands[2].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[1].MinerPeer.ID,
							Root:      cid1,
							ByteCount: 0,
							Blocks:    []cid.Cid{},
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[1].MinerPeer.ID, Duration: 10 * time.Millisecond},
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[1].MinerPeer.ID},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*30,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*30), rid1, startTime, toCandidate(cid1, cid1Cands[2].MinerPeer), time.Millisecond*10),
						events.Failed(startTime.Add(initialPause+time.Millisecond*30), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[2].MinerPeer), "malformed CAR; unexpected EOF"),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[2].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[2].MinerPeer.ID,
							Root:      cid1,
							ByteCount: 0,
							Blocks:    []cid.Cid{},
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[2].MinerPeer.ID, Duration: 10 * time.Millisecond},
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[2].MinerPeer.ID},
					},
				},
			},
		},
		{
			name:     "single, multiple errors, one success",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				cid1: {
					{
						Peer:       cid1Cands[0].MinerPeer,
						LinkSystem: *makeLsys(nil, false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*10),
						Malformed:  true,
					},
					{
						Peer:       cid1Cands[1].MinerPeer,
						LinkSystem: *makeLsys(nil, false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*20),
						Malformed:  true,
					},
					{
						Peer:       cid1Cands[2].MinerPeer,
						LinkSystem: *makeLsys(tbc1.AllBlocks(), false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*30),
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{cid1: tbc1Cids},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				cid1: {
					RootCid:           cid1,
					StorageProviderId: cid1Cands[2].MinerPeer.ID,
					Size:              sizeOf(tbc1.AllBlocks()),
					Blocks:            100,
					Duration:          10*time.Millisecond + remoteBlockDuration*100,
					AverageSpeed:      uint64(float64(sizeOf(tbc1.AllBlocks())) / (10*time.Millisecond + remoteBlockDuration*100).Seconds()),
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
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer)),
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[2].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[2].MinerPeer)),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[0].MinerPeer.ID, Duration: 0},
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[1].MinerPeer.ID, Duration: 0},
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[2].MinerPeer.ID, Duration: 0},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*10,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*10),
						events.Failed(startTime.Add(initialPause+time.Millisecond*10), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer), "malformed CAR; unexpected EOF"),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
					ReceivedRetrievals:  []peer.ID{cid1Cands[1].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[0].MinerPeer.ID,
							Root:      cid1,
							ByteCount: 0,
							Blocks:    []cid.Cid{},
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 10},
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*20,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*20), rid1, startTime, toCandidate(cid1, cid1Cands[1].MinerPeer), time.Millisecond*10),
						events.Failed(startTime.Add(initialPause+time.Millisecond*20), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[1].MinerPeer), "malformed CAR; unexpected EOF"),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[1].MinerPeer.ID},
					ReceivedRetrievals:  []peer.ID{cid1Cands[2].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[1].MinerPeer.ID,
							Root:      cid1,
							ByteCount: 0,
							Blocks:    []cid.Cid{},
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[1].MinerPeer.ID, Duration: time.Millisecond * 10},
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[1].MinerPeer.ID},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*30,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*30), rid1, startTime, toCandidate(cid1, cid1Cands[2].MinerPeer), time.Millisecond*10),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[2].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*30 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*30+remoteBlockDuration*100), rid1, startTime, toCandidate(cid1, cid1Cands[2].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 10*time.Millisecond+remoteBlockDuration*100, big.Zero(), 0, multicodec.TransportIpfsGatewayHttp),
					},
					CompletedRetrievals: []peer.ID{cid1Cands[2].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[2].MinerPeer.ID,
							Root:      cid1,
							ByteCount: sizeOf(tbc1.AllBlocks()),
							Blocks:    tbc1Cids,
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: cid1Cands[2].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(tbc1.AllBlocks())) / (10*time.Millisecond + remoteBlockDuration*100).Seconds())},
					},
				},
			},
		},
		// TODO: this test demonstrates the incompleteness of the http implementation - it's counted
		// as a success and we only signal an "error" because the selector on the server errors but
		// that in no way carries over to the client.
		{
			name:     "single, one peer, partial served",
			requests: map[cid.Cid]types.RetrievalID{cid1: rid1},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				cid1: {
					{
						Peer:       cid1Cands[0].MinerPeer,
						LinkSystem: *makeLsys(tbc1.AllBlocks()[0:50], false),
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{cid1: tbc1Cids[0:50]},
			expectedErrors: map[cid.Cid]struct{}{
				cid1: {},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
						events.Connected(startTime, rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer)),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: cid1Cands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, startTime, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*50,
					ExpectedEvents: []types.RetrievalEvent{
						events.Failed(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*50), rid1, startTime, types.RetrievalPhase, toCandidate(cid1, cid1Cands[0].MinerPeer), "missing block in CAR; ipld: could not find "+tbc1.AllBlocks()[50].Cid().String()),
					},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      cid1Cands[0].MinerPeer.ID,
							Root:      cid1,
							ByteCount: sizeOf(tbc1.AllBlocks()[0:50]),
							Blocks:    tbc1Cids[0:50],
							Err:       struct{}{},
						},
					},
					CompletedRetrievals: []peer.ID{cid1Cands[0].MinerPeer.ID},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: cid1Cands[0].MinerPeer.ID},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			clock := clock.NewMock()
			clock.Set(startTime)

			awaitReceivedCandidates := make(chan struct{}, 1)
			roundTripper := testutil.NewMockRoundTripper(t, ctx, clock, remoteBlockDuration, testCase.requestPath, testCase.requestScope, testCase.remotes)
			client := &http.Client{Transport: roundTripper}

			mockSession := testutil.NewMockSession(ctx)
			mockSession.SetCandidatePreferenceOrder(append(cid1Cands, cid2Cands...))
			mockSession.SetProviderTimeout(5 * time.Second)
			retriever := retriever.NewHttpRetrieverWithDeps(mockSession, client, clock, awaitReceivedCandidates, initialPause)

			blockAccounting := make([]*blockAccounter, 0)
			expectedCids := make([][]cid.Cid, 0)
			retrievals := make([]testutil.RunRetrieval, 0)
			expectedStats := make([]*types.RetrievalStats, 0)
			expectedErrors := make([]struct{}, 0)
			for c, rid := range testCase.requests {
				c := c
				rid := rid
				ec := testCase.expectedCids[c]
				if ec == nil {
					ec = []cid.Cid{}
				}
				expectedCids = append(expectedCids, ec)
				expectedStats = append(expectedStats, testCase.expectedStats[c])
				expectedErrors = append(expectedErrors, testCase.expectedErrors[c])
				lsys := makeLsys(nil, false)
				blockAccounting = append(blockAccounting, NewBlockAccounter(lsys))
				retrievals = append(retrievals, func(eventsCb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
					request := types.RetrievalRequest{
						RetrievalID: rid,
						Cid:         c,
						LinkSystem:  *lsys,
						Path:        testCase.requestPath[c],
						Scope:       testCase.requestScope[c],
					}
					candidates := toCandidates(c, testCase.remotes[c])
					return retriever.Retrieve(context.Background(), request, eventsCb).
						RetrieveFromAsyncCandidates(makeAsyncCandidates(t, candidates))
				})
			}

			results := testutil.RetrievalVerifier{
				ExpectedSequence: testCase.expectSequence,
			}.RunWithVerification(ctx, t, clock, roundTripper, nil, mockSession, retrievals)

			req.Len(results, len(testCase.requests))
			actualStats := make([]*types.RetrievalStats, len(results))
			actualErrors := make([]struct{}, len(results))
			actualCids := make([][]cid.Cid, len(results))
			for i, result := range results {
				actualStats[i] = result.Stats
				if result.Err != nil {
					actualErrors[i] = struct{}{}
				}
				actualCids[i] = blockAccounting[i].cids
			}
			req.ElementsMatch(expectedStats, actualStats)
			req.ElementsMatch(expectedErrors, actualErrors)
			req.Equal(expectedCids, actualCids)
		})
	}
}

func toCandidates(root cid.Cid, remotes []testutil.MockRoundTripRemote) []types.RetrievalCandidate {
	candidates := make([]types.RetrievalCandidate, len(remotes))
	for i, r := range remotes {
		candidates[i] = toCandidate(root, r.Peer)
	}
	return candidates
}

func toCandidate(root cid.Cid, peer peer.AddrInfo) types.RetrievalCandidate {
	return types.NewRetrievalCandidate(peer.ID, peer.Addrs, root, &metadata.IpfsGatewayHttp{})
}

type blockAccounter struct {
	cids []cid.Cid
	bwo  linking.BlockWriteOpener
}

func NewBlockAccounter(lsys *linking.LinkSystem) *blockAccounter {
	ba := &blockAccounter{
		cids: make([]cid.Cid, 0),
		bwo:  lsys.StorageWriteOpener,
	}
	lsys.StorageWriteOpener = ba.StorageWriteOpener
	return ba
}

func (ba *blockAccounter) StorageWriteOpener(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	w, wc, err := ba.bwo(lctx)
	return w, func(l datamodel.Link) error {
		ba.cids = append(ba.cids, l.(cidlink.Link).Cid)
		return wc(l)
	}, err
}
