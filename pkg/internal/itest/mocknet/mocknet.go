package mocknet

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	bssrv "github.com/ipfs/go-libipfs/bitswap/server"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	lpmock "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

var QueryErrorTriggerCid = cid.MustParse("bafkqaalb")

type MockRetrievalNet struct {
	ctx               context.Context
	t                 *testing.T
	testPeerGenerator testpeer.TestPeerGenerator

	RemoteEvents [][]datatransfer.Event
	FinishedChan []chan struct{}
	MN           lpmock.Mocknet
	Self         host.Host
	Remotes      []testpeer.TestPeer
	Finder       retriever.CandidateFinder
}

func NewMockRetrievalNet(ctx context.Context, t *testing.T) *MockRetrievalNet {
	mrn := &MockRetrievalNet{
		ctx:          ctx,
		t:            t,
		Remotes:      make([]testpeer.TestPeer, 0),
		RemoteEvents: make([][]datatransfer.Event, 0),
		FinishedChan: make([]chan struct{}, 0),
	}
	mrn.Finder = &mockCandidateFinder{mrn}
	mrn.t.Cleanup(func() {
		require.NoError(mrn.t, mrn.TearDown())
	})
	// Setup network
	mrn.MN = lpmock.New()
	mrn.testPeerGenerator = testpeer.NewTestPeerGenerator(mrn.ctx, mrn.t, mrn.MN, []bsnet.NetOpt{}, []bssrv.Option{})
	h, err := mrn.MN.GenPeer()
	mrn.Self = h
	require.NoError(mrn.t, err)
	return mrn
}

func (mrn *MockRetrievalNet) AddBitswapPeers(n int) {
	peers := mrn.testPeerGenerator.BitswapPeers(n)
	for i := 0; i < n; i++ {
		mrn.Remotes = append(mrn.Remotes, peers[i])
		mrn.RemoteEvents = append(mrn.RemoteEvents, make([]datatransfer.Event, 0)) // not used for bitswap
		mrn.FinishedChan = append(mrn.FinishedChan, make(chan struct{}, 1))        // not used for bitswap
	}
}

func (mrn *MockRetrievalNet) AddGraphsyncPeers(n int) {
	peers := mrn.testPeerGenerator.GraphsyncPeers(n)
	for i := 0; i < n; i++ {
		mrn.Remotes = append(mrn.Remotes, peers[i])
		mrn.RemoteEvents = append(mrn.RemoteEvents, make([]datatransfer.Event, 0))
		mrn.FinishedChan = append(mrn.FinishedChan, make(chan struct{}, 1))
	}
}
func SetupRetrieval(t *testing.T, remote testpeer.TestPeer) chan []datatransfer.Event {
	// Register DealProposal voucher type with automatic Pull acceptance
	remoteDealValidator := &mockDealValidator{t: t, acceptPull: true}
	require.NoError(t, remote.DatatransferServer.RegisterVoucherType(retrievaltypes.DealProposalType, remoteDealValidator))

	remoteEvents := make([]datatransfer.Event, 0)
	finishedChan := make(chan []datatransfer.Event, 1)

	// Record remote events
	subscriberRemote := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		remoteEvents = append(remoteEvents, event)
		if event.Code == datatransfer.CleanupComplete {
			finishedChan <- remoteEvents
		}
	}
	remote.DatatransferServer.SubscribeToEvents(subscriberRemote)

	return finishedChan
}

func WaitForFinish(ctx context.Context, t *testing.T, finishChan chan []datatransfer.Event, timeout time.Duration) []datatransfer.Event {
	var events []datatransfer.Event
	require.Eventually(t, func() bool {
		select {
		case events = <-finishChan:
			return true
		case <-ctx.Done():
			require.Fail(t, ctx.Err().Error())
			return false
		default:
			return false
		}
	}, timeout, 100*time.Millisecond)
	return events
}

func (mrn *MockRetrievalNet) TearDown() error {
	var wg sync.WaitGroup
	for _, h := range mrn.Remotes {
		wg.Add(1)
		go func(h testpeer.TestPeer) {
			defer wg.Done()
			if h.DatatransferServer != nil {
				h.DatatransferServer.Stop(context.Background())
			}
			if h.BitswapServer != nil {
				h.BitswapServer.Close()
			}
			if h.BitswapNetwork != nil {
				h.BitswapNetwork.Stop()
			}
		}(h)
	}
	wg.Wait()
	return mrn.MN.Close()
}

type mockCandidateFinder struct {
	mrn *MockRetrievalNet
}

func (mcf *mockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	candidates := make([]types.RetrievalCandidate, 0)
	for _, h := range mcf.mrn.Remotes {
		if _, has := h.Cids[cid]; has {
			var md metadata.Metadata
			if h.BitswapServer != nil {
				md = metadata.Default.New(metadata.Bitswap{})
			} else {
				md = metadata.Default.New(&metadata.GraphsyncFilecoinV1{PieceCID: cid})
			}
			candidates = append(candidates, types.RetrievalCandidate{MinerPeer: *h.AddrInfo(), RootCid: cid, Metadata: md})
		}
	}
	return candidates, nil
}

func (mcf *mockCandidateFinder) FindCandidatesAsync(ctx context.Context, cid cid.Cid, cb func(types.RetrievalCandidate)) error {
	cand, _ := mcf.FindCandidates(ctx, cid)
	for _, c := range cand {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cb(c)
	}
	return nil
}

var _ datatransfer.RequestValidator = (*mockDealValidator)(nil)

type mockDealValidator struct {
	t          *testing.T
	acceptPull bool
}

func (mdv *mockDealValidator) ValidatePush(
	channel datatransfer.ChannelID,
	sender peer.ID,
	voucher datamodel.Node,
	baseCid cid.Cid,
	selector datamodel.Node,
) (datatransfer.ValidationResult, error) {
	return datatransfer.ValidationResult{Accepted: false}, errors.New("not supported")
}

func (mdv *mockDealValidator) ValidatePull(
	channel datatransfer.ChannelID,
	receiver peer.ID,
	voucher datamodel.Node,
	baseCid cid.Cid,
	selector datamodel.Node,
) (datatransfer.ValidationResult, error) {
	if voucher.Kind() != datamodel.Kind_Map {
		mdv.t.Logf("rejecting pull, bad voucher (!map)")
		return datatransfer.ValidationResult{Accepted: false}, nil
	}
	pcn, err := voucher.LookupByString("PayloadCID")
	if err != nil || pcn.Kind() != datamodel.Kind_Link {
		mdv.t.Logf("rejecting pull, bad voucher PayloadCID")
		return datatransfer.ValidationResult{Accepted: false}, nil
	}
	pcl, err := pcn.AsLink()
	if err != nil || !baseCid.Equals(pcl.(cidlink.Link).Cid) {
		mdv.t.Logf("rejecting pull, bad voucher PayloadCID (doesn't match)")
		return datatransfer.ValidationResult{Accepted: false}, nil
	}
	pn, err := voucher.LookupByString("Params")
	if err != nil || pn.Kind() != datamodel.Kind_Map {
		mdv.t.Logf("rejecting pull, bad voucher Params")
		return datatransfer.ValidationResult{Accepted: false}, nil
	}
	sn, err := pn.LookupByString("Selector")
	if err != nil || !ipld.DeepEqual(sn, selector) {
		mdv.t.Logf("rejecting pull, bad voucher Selector")
		return datatransfer.ValidationResult{Accepted: false}, nil
	}
	return datatransfer.ValidationResult{Accepted: mdv.acceptPull}, nil
}

func (mdv *mockDealValidator) ValidateRestart(
	channelID datatransfer.ChannelID,
	channel datatransfer.ChannelState,
) (datatransfer.ValidationResult, error) {
	return datatransfer.ValidationResult{Accepted: false}, errors.New("not supported")
}
