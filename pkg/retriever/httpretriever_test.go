package retriever_test

import (
	"context"
	"fmt"
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

	// testing our strange paths that need to propagate through http requests
	funkyPath, funkyLinks := mkFunky(lsys)
	funkyCands := testutil.GenerateRetrievalCandidatesForCID(t, 1, funkyLinks[0], metadata.IpfsGatewayHttp{})

	// testing our ability to handle duplicates or not
	dupyLinks, dupyLinksDeduped := mkDupy(lsys)
	dupyCands := testutil.GenerateRetrievalCandidatesForCID(t, 1, dupyLinks[0], metadata.IpfsGatewayHttp{})

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
			expectSequence: []testutil.ExpectedActionsAtTime{
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
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
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
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid2Cands[0].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40,
					ExpectedEvents: []types.RetrievalEvent{
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(cid1, cid1Cands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
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
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[2].MinerPeer.ID, Duration: time.Millisecond * 10},
					},
				},
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
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: cid1Cands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
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
			},
		},
		{
			name:        "single, funky path",
			requests:    map[cid.Cid]types.RetrievalID{funkyLinks[0]: rid1},
			requestPath: map[cid.Cid]string{funkyLinks[0]: funkyPath},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				funkyLinks[0]: {
					{
						Peer:       funkyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				funkyLinks[0]: {
					RootCid:           funkyLinks[0],
					StorageProviderId: funkyCands[0].MinerPeer.ID,
					Size:              sizeOfStored(lsys, funkyLinks),
					Blocks:            uint64(len(funkyLinks)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(funkyLinks)),
					AverageSpeed:      uint64(float64(sizeOfStored(lsys, funkyLinks)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(funkyLinks))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{funkyLinks[0]: funkyLinks},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(funkyLinks[0], funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(funkyLinks[0], funkyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(funkyLinks[0], funkyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: funkyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(funkyLinks)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(funkyLinks))),
							rid1,
							toCandidate(funkyLinks[0], funkyCands[0].MinerPeer),
							sizeOfStored(lsys, funkyLinks),
							uint64(len(funkyLinks)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(funkyLinks)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{funkyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      funkyCands[0].MinerPeer.ID,
							Root:      funkyLinks[0],
							ByteCount: sizeOfStored(lsys, funkyLinks),
							Blocks:    funkyLinks,
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: funkyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOfStored(lsys, funkyLinks)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(funkyLinks))).Seconds())},
					},
				},
			},
		},
		{
			name:           "dag with duplicates, peer sending duplicates",
			requests:       map[cid.Cid]types.RetrievalID{dupyLinks[0]: rid1},
			sendDuplicates: map[cid.Cid]bool{dupyLinks[0]: true},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				dupyLinks[0]: {
					{
						Peer:       dupyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				dupyLinks[0]: {
					RootCid:           dupyLinks[0],
					StorageProviderId: dupyCands[0].MinerPeer.ID,
					Size:              sizeOfStored(lsys, dupyLinks),
					Blocks:            uint64(len(dupyLinks)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyLinks)),
					AverageSpeed:      uint64(float64(sizeOfStored(lsys, dupyLinks)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyLinks))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{dupyLinks[0]: dupyLinks},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: dupyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyLinks)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyLinks))),
							rid1,
							toCandidate(dupyLinks[0], dupyCands[0].MinerPeer),
							sizeOfStored(lsys, dupyLinks),
							uint64(len(dupyLinks)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyLinks)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      dupyCands[0].MinerPeer.ID,
							Root:      dupyLinks[0],
							ByteCount: sizeOfStored(lsys, dupyLinks),
							Blocks:    dupyLinks,
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: dupyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOfStored(lsys, dupyLinks)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyLinks))).Seconds())},
					},
				},
			},
		},
		{
			name:           "dag with duplicates, peer not sending duplicates",
			requests:       map[cid.Cid]types.RetrievalID{dupyLinks[0]: rid1},
			sendDuplicates: map[cid.Cid]bool{dupyLinks[0]: false},
			remotes: map[cid.Cid][]testutil.MockRoundTripRemote{
				dupyLinks[0]: {
					{
						Peer:       dupyCands[0].MinerPeer,
						LinkSystem: lsys,
						Selector:   allSelector,
						RespondAt:  startTime.Add(initialPause + time.Millisecond*40),
					},
				},
			},
			expectedStats: map[cid.Cid]*types.RetrievalStats{
				dupyLinks[0]: {
					RootCid:           dupyLinks[0],
					StorageProviderId: dupyCands[0].MinerPeer.ID,
					Size:              sizeOfStored(lsys, dupyLinksDeduped),
					Blocks:            uint64(len(dupyLinksDeduped)),
					Duration:          40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyLinksDeduped)),
					AverageSpeed:      uint64(float64(sizeOfStored(lsys, dupyLinksDeduped)) / (40*time.Millisecond + remoteBlockDuration*time.Duration(len(dupyLinksDeduped))).Seconds()),
					TimeToFirstByte:   40 * time.Millisecond,
					TotalPayment:      big.Zero(),
					AskPrice:          big.Zero(),
				},
			},
			expectedCids: map[cid.Cid][]cid.Cid{dupyLinks[0]: dupyLinksDeduped},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart: 0,
					ExpectedEvents: []types.RetrievalEvent{
						events.StartedRetrieval(startTime, rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
						events.ConnectedToProvider(startTime, rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), multicodec.TransportIpfsGatewayHttp),
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
						events.FirstByte(startTime.Add(initialPause+time.Millisecond*40), rid1, toCandidate(dupyLinks[0], dupyCands[0].MinerPeer), time.Millisecond*40, multicodec.TransportIpfsGatewayHttp),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: dupyCands[0].MinerPeer.ID, Duration: time.Millisecond * 40},
					},
				},
				{
					AfterStart: initialPause + time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyLinksDeduped)),
					ExpectedEvents: []types.RetrievalEvent{
						events.Success(
							startTime.Add(initialPause+time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyLinksDeduped))),
							rid1,
							toCandidate(dupyLinks[0], dupyCands[0].MinerPeer),
							sizeOfStored(lsys, dupyLinksDeduped),
							uint64(len(dupyLinksDeduped)),
							time.Millisecond*40+remoteBlockDuration*time.Duration(len(dupyLinksDeduped)),
							multicodec.TransportIpfsGatewayHttp,
						),
					},
					CompletedRetrievals: []peer.ID{dupyCands[0].MinerPeer.ID},
					ServedRetrievals: []testutil.RemoteStats{
						{
							Peer:      dupyCands[0].MinerPeer.ID,
							Root:      dupyLinks[0],
							ByteCount: sizeOfStored(lsys, dupyLinksDeduped),
							Blocks:    dupyLinksDeduped,
						},
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Success, Provider: dupyCands[0].MinerPeer.ID, Value: math.Trunc(float64(sizeOfStored(lsys, dupyLinksDeduped)) / (time.Millisecond*40 + remoteBlockDuration*time.Duration(len(dupyLinksDeduped))).Seconds())},
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
			mockSession.SetProviderTimeout(5 * time.Second)
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

func mkBlockWithBytes(lsys linking.LinkSystem, bytes []byte) cid.Cid {
	l, err := lsys.Store(linking.LinkContext{}, rawlp, basicnode.NewBytes(bytes))
	if err != nil {
		panic(err)
	}
	return l.(cidlink.Link).Cid
}

func mkBlockWithLink(lsys linking.LinkSystem, c cid.Cid, name string) cid.Cid {
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
	return l.(cidlink.Link).Cid
}

func mkFunky(lsys linking.LinkSystem) (string, []cid.Cid) {
	funkyPath := `=funky/ path/#/with?/weird%/c+h+a+r+s`
	funkyLinks := make([]cid.Cid, 0)
	funkyLinks = append(funkyLinks, mkBlockWithBytes(lsys, []byte("funky data")))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], "c+h+a+r+s"))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], "weird%"))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], "with?"))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], "#"))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], " path"))
	funkyLinks = append(funkyLinks, mkBlockWithLink(lsys, funkyLinks[len(funkyLinks)-1], "=funky"))
	slices.Reverse(funkyLinks)
	return funkyPath, funkyLinks
}

func mkDupy(lsys linking.LinkSystem) ([]cid.Cid, []cid.Cid) {
	dupy := mkBlockWithBytes(lsys, []byte("duplicate data"))

	n, err := qp.BuildMap(dagpb.Type.PBNode, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(100, func(la datamodel.ListAssembler) {
			for i := 0; i < 100; i++ {
				qp.ListEntry(la, qp.Map(2, func(ma datamodel.MapAssembler) {
					qp.MapEntry(ma, "Name", qp.String(fmt.Sprintf("%03d", i)))
					qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: dupy}))
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

	// dupyLinks contains the duplicates
	dupyLinks := []cid.Cid{l.(cidlink.Link).Cid}
	for i := 0; i < 100; i++ {
		dupyLinks = append(dupyLinks, dupy)
	}
	// dupyLinksDeduped contains just the unique links
	dupyLinksDeduped := []cid.Cid{l.(cidlink.Link).Cid, dupy}

	return dupyLinks, dupyLinksDeduped
}
