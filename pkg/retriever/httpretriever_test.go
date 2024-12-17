package retriever_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestHTTPRetriever(t *testing.T) {
	ctx := context.Background()

	store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true
	tbc1 := trustlesstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	tbc2 := trustlesstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
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

	// testing our strange paths that need to propagate through http requests
	funkyPath, funkyBlocks := mkFunky(lsys)
	funkyCands := testutil.GenerateRetrievalCandidatesForCID(t, 1, funkyBlocks[0].Cid(), metadata.IpfsGatewayHttp{})

	// testing our ability to handle duplicates or not
	dupyBlocks, dupyBlocksDeduped := mkDupy(lsys)
	dupyCands := testutil.GenerateRetrievalCandidatesForCID(t, 1, dupyBlocks[0].Cid(), metadata.IpfsGatewayHttp{})

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
		requestScope   map[cid.Cid]trustlessutils.DagScope
		remotes        map[cid.Cid][]testutil.MockRoundTripRemote
		sendDuplicates map[cid.Cid]bool // will default to true
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
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(len(tbc1.Blocks(0, 1)[0].RawData()))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, tbc1.Blocks(1, 100))...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*100), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, multicodec.TransportIpfsGatewayHttp),
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
			}...),
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
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.StartedRetrieval(startTime, rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*10), rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(len(tbc2.Blocks(0, 1)[0].RawData()))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid2Cands[0].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(len(tbc1.Blocks(0, 1)[0].RawData()))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.SortActions(
					append(
						testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*10+remoteBlockDuration, rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, tbc2.Blocks(1, 100)),
						testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, tbc1.Blocks(1, 100))...),
				)...),
				[]testutil.ExpectedActionsAtTime{
					{
						AfterStart: initialPause + time.Millisecond*10 + remoteBlockDuration*100,
						ExpectedEvents: []types.RetrievalEvent{
							events.Success(startTime.Add(initialPause+time.Millisecond*10+remoteBlockDuration*100), rid2, toCandidate(cid2, cid2Cands[0].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 10*time.Millisecond+remoteBlockDuration*100, multicodec.TransportIpfsGatewayHttp),
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
							events.Success(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*100), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), sizeOf(tbc1.AllBlocks()), 100, 40*time.Millisecond+remoteBlockDuration*100, multicodec.TransportIpfsGatewayHttp),
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
				}...),
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
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*10), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, "malformed CAR; unexpected EOF"),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*20), rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*20), rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp, "malformed CAR; unexpected EOF"),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*30), rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*30), rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp, "malformed CAR; unexpected EOF"),
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
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*10), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*10), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, "malformed CAR; unexpected EOF"),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*20), rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*20), rid1, toCandidate(cid1, cid1Cands[1].MinerPeer), multicodec.TransportIpfsGatewayHttp, "malformed CAR; unexpected EOF"),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*30), rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), time.Millisecond*10, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*30), rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(len(tbc1.Blocks(0, 1)[0].RawData()))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[2].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*30+remoteBlockDuration, rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, tbc1.Blocks(1, 100))...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*30 + remoteBlockDuration*100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(startTime.Add(initialPause+time.Millisecond*30+remoteBlockDuration*100), rid1, toCandidate(cid1, cid1Cands[2].MinerPeer), sizeOf(tbc2.AllBlocks()), 100, 10*time.Millisecond+remoteBlockDuration*100, multicodec.TransportIpfsGatewayHttp),
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
			}...),
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
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(len(tbc1.Blocks(0, 1)[0].RawData()))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, tbc1.Blocks(1, 50))...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*50,
					ExpectedEvents: []types.RetrievalEvent{
						events.FailedRetrieval(startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*50), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, "missing block in CAR; ipld: could not find "+tbc1.AllBlocks()[50].Cid().String()),
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
			}...),
		},
		{
			name:        "single, funky path",
			requests:    map[cid.Cid]types.RetrievalID{funkyBlocks[0].Cid(): rid1},
			requestPath: map[cid.Cid]string{funkyBlocks[0].Cid(): funkyPath},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				funkyBlocks[0].Cid(): {
					{
						Peer:       funkyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				funkyBlocks[0].Cid(): {
					RootCid:           funkyBlocks[0].Cid(),
					StorageProviderId: funkyCands[0].MinerPeer.ID,
					Size:              sizeOf(funkyBlocks),
					Blocks:            uint64(len(funkyBlocks)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(funkyBlocks)),
					AverageSpeed:      uint64(float64(sizeOf(funkyBlocks)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(funkyBlocks))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{funkyBlocks[0].Cid(): toCids(funkyBlocks)},
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: funkyCands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{funkyCands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(sizeOf(funkyBlocks[:0]))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: funkyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, funkyBlocks[1:])...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(funkyBlocks)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(funkyBlocks))),
							rid1,
							toCandidate(funkyBlocks[0].Cid(), funkyCands[0].MinerPeer),
							sizeOf(funkyBlocks),
							uint64(len(funkyBlocks)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(funkyBlocks)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{funkyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      funkyCands[0].MinerPeer.ID,
							Root:      funkyBlocks[0].Cid(),
							ByteCount: sizeOf(funkyBlocks),
							Blocks:    toCids(funkyBlocks),
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: funkyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(funkyBlocks)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(funkyBlocks))).Seconds())},
					},
				},
			}...),
		},
		{
			name:           "dag with duplicates, peer sending duplicates",
			requests:       map[cid.Cid]types.RetrievalID{dupyBlocks[0].Cid(): rid1},
			sendDuplicates: map[cid.Cid]bool{dupyBlocks[0].Cid(): true},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				dupyBlocks[0].Cid(): {
					{
						Peer:       dupyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				dupyBlocks[0].Cid(): {
					RootCid:           dupyBlocks[0].Cid(),
					StorageProviderId: dupyCands[0].MinerPeer.ID,
					Size:              sizeOf(dupyBlocks),
					Blocks:            uint64(len(dupyBlocks)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyBlocks)),
					AverageSpeed:      uint64(float64(sizeOf(dupyBlocks)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyBlocks))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{dupyBlocks[0].Cid(): toCids(dupyBlocks)},
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: dupyCands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(sizeOf(dupyBlocks[:0]))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: dupyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, dupyBlocks[1:])...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyBlocks)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyBlocks))),
							rid1,
							toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer),
							sizeOf(dupyBlocks),
							uint64(len(dupyBlocks)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyBlocks)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      dupyCands[0].MinerPeer.ID,
							Root:      dupyBlocks[0].Cid(),
							ByteCount: sizeOf(dupyBlocks),
							Blocks:    toCids(dupyBlocks),
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: dupyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(dupyBlocks)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyBlocks))).Seconds())},
					},
				},
			}...),
		},
		{
			name:           "dag with duplicates, peer not sending duplicates",
			requests:       map[cid.Cid]types.RetrievalID{dupyBlocks[0].Cid(): rid1},
			sendDuplicates: map[cid.Cid]bool{dupyBlocks[0].Cid(): false},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				dupyBlocks[0].Cid(): {
					{
						Peer:       dupyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				dupyBlocks[0].Cid(): {
					RootCid:           dupyBlocks[0].Cid(),
					StorageProviderId: dupyCands[0].MinerPeer.ID,
					Size:              sizeOf(dupyBlocksDeduped),
					Blocks:            uint64(len(dupyBlocksDeduped)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyBlocksDeduped)),
					AverageSpeed:      uint64(float64(sizeOf(dupyBlocksDeduped)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyBlocksDeduped))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{dupyBlocks[0].Cid(): toCids(dupyBlocksDeduped)},
			expectSequence: append(append([]testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: dupyCands[0].MinerPeer.ID},
					},
				},
				{
					AfterStart:         initialPause,
					ReceivedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
						events.BlockReceived(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, uint64(sizeOf(dupyBlocks[:0]))),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: dupyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
			},
				testutil.BlockReceivedActions(startTime, initialPause+time.Millisecond*40+remoteBlockDuration, rid1, toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp, remoteBlockDuration, dupyBlocksDeduped[1:])...), []testutil.ExpectedActionsAtTime{
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyBlocksDeduped)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyBlocksDeduped))),
							rid1,
							toCandidate(dupyBlocks[0].Cid(), dupyCands[0].MinerPeer),
							sizeOf(dupyBlocksDeduped),
							uint64(len(dupyBlocksDeduped)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyBlocksDeduped)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      dupyCands[0].MinerPeer.ID,
							Root:      dupyBlocks[0].Cid(),
							ByteCount: sizeOf(dupyBlocksDeduped),
							Blocks:    toCids(dupyBlocksDeduped),
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: dupyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOf(dupyBlocksDeduped)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyBlocksDeduped))).Seconds())},
					},
				},
			}...),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			clock := clock.NewMock()
			clock.Set(startTime)

			roundTripper := testutil.NewMockRoundTripper(
				t,
				ctx,
				clock,
				remoteBlockDuration,
				testCase.requestPath,
				testCase.requestScope,
				testCase.remotes,
				testCase.sendDuplicates,
			)
			client := &http.Client{Transport: roundTripper}

			mockSession := testutil.NewMockSession(ctx)
			mockSession.SetCandidatePreferenceOrder(append(cid1Cands, cid2Cands...))
			mockSession.SetProviderTimeout(10 * time.Second)
			retriever := retriever.NewHttpRetrieverWithDeps(mockSession, client, clock, nil, initialPause, true)

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
						Request: trustlessutils.Request{
							Root:  c,
							Path:  testCase.requestPath[c],
							Scope: testCase.requestScope[c],
						},
						LinkSystem: *lsys,
					}
					candidates := toCandidates(c, testCase.remotes[c])
					return retriever.Retrieve(context.Background(), request, eventsCb).
						RetrieveFromAsyncCandidates(makeAsyncCandidates(t, candidates))
				})
			}

			results := testutil.RetrievalVerifier{
				ExpectedSequence: testCase.expectSequence,
			}.RunWithVerification(ctx, t, clock, roundTripper, nil, mockSession, nil, 0, retrievals)

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

var pblp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagProtobuf,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}
var rawlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

func mkBlockWithBytes(lsys linking.LinkSystem, bytes []byte) blocks.Block {
	l, err := lsys.Store(linking.LinkContext{}, rawlp, basicnode.NewBytes(bytes))
	if err != nil {
		panic(err)
	}
	b, err := lsys.LoadRaw(linking.LinkContext{}, l)
	if err != nil {
		panic(err)
	}
	return must(blocks.NewBlockWithCid(b, l.(cidlink.Link).Cid))
}

func mkBlockWithLink(lsys linking.LinkSystem, c cid.Cid, name string) blocks.Block {
	n, err := qp.BuildMap(dagpb.Type.PBNode, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Map(2, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "Name", qp.String(name))
				qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: c}))
			}))
		}))
	})
	if err != nil {
		panic(err)
	}
	l, err := lsys.Store(linking.LinkContext{}, pblp, n)
	if err != nil {
		panic(err)
	}
	b, err := lsys.LoadRaw(linking.LinkContext{}, l)
	if err != nil {
		panic(err)
	}
	return must(blocks.NewBlockWithCid(b, l.(cidlink.Link).Cid))
}

func mkFunky(lsys linking.LinkSystem) (string, []blocks.Block) {
	funkyPath := `=funky/ path/#/with?/weird%/c+h+a+r+s`
	funkyBlocks := make([]blocks.Block, 0)
	funkyBlocks = append(funkyBlocks, mkBlockWithBytes(lsys, []byte("funky data")))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), "c+h+a+r+s"))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), "weird%"))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), "with?"))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), "#"))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), " path"))
	funkyBlocks = append(funkyBlocks, mkBlockWithLink(lsys, funkyBlocks[len(funkyBlocks)-1].Cid(), "=funky"))
	slices.Reverse(funkyBlocks)
	return funkyPath, funkyBlocks
}

func mkDupy(lsys linking.LinkSystem) ([]blocks.Block, []blocks.Block) {
	dupy := mkBlockWithBytes(lsys, []byte("duplicate data"))

	n, err := qp.BuildMap(dagpb.Type.PBNode, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(100, func(la datamodel.ListAssembler) {
			for i := 0; i < 100; i++ {
				qp.ListEntry(la, qp.Map(2, func(ma datamodel.MapAssembler) {
					qp.MapEntry(ma, "Name", qp.String(fmt.Sprintf("%03d", i)))
					qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: dupy.Cid()}))
				}))
			}
		}))
	})
	if err != nil {
		panic(err)
	}
	l, err := lsys.Store(linking.LinkContext{}, pblp, n)
	if err != nil {
		panic(err)
	}
	b, err := lsys.LoadRaw(linking.LinkContext{}, l)
	if err != nil {
		panic(err)
	}
	// dupyBlocks contains the duplicates
	dupyBlocks := []blocks.Block{must(blocks.NewBlockWithCid(b, l.(cidlink.Link).Cid))}
	for i := 0; i < 100; i++ {
		dupyBlocks = append(dupyBlocks, dupy)
	}
	// dupyBlocksDeduped contains just the unique links
	dupyBlocksDeduped := []blocks.Block{must(blocks.NewBlockWithCid(b, l.(cidlink.Link).Cid)), dupy}

	return dupyBlocks, dupyBlocksDeduped
}

func toCids(blocks []blocks.Block) []cid.Cid {
	cids := make([]cid.Cid, len(blocks))
	for i, b := range blocks {
		cids[i] = b.Cid()
	}
	return cids
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}
