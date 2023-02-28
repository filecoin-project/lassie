package mocknet

import (
	"context"
	"errors"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	bssrv "github.com/ipfs/go-libipfs/bitswap/server"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
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

func SetupQuery(t *testing.T, remote testpeer.TestPeer, expectCid cid.Cid, qr retrievalmarket.QueryResponse) {
	remote.Host.SetStreamHandler(retrievalmarket.QueryProtocolID, func(s network.Stream) {
		// we're doing some manual IPLD work here to exercise other parts of the
		// messaging stack to make sure we're communicating according to spec

		na := basicnode.Prototype.Any.NewBuilder()
		// IPLD normally doesn't allow non-EOF delimited data, but we can cheat
		decoder := dagcbor.DecodeOptions{AllowLinks: true, DontParseBeyondEnd: true}
		require.NoError(t, decoder.Decode(na, s))
		query := na.Build()
		require.Equal(t, datamodel.Kind_Map, query.Kind())
		pcidn, err := query.LookupByString("PayloadCID")
		require.NoError(t, err)
		require.Equal(t, datamodel.Kind_Link, pcidn.Kind())
		pcidl, err := pcidn.AsLink()
		require.NoError(t, err)
		pcid := pcidl.(cidlink.Link).Cid

		if pcid.Equals(QueryErrorTriggerCid) {
			// premature end, should cause error
			require.NoError(t, s.Close())
			return
		}

		// build and send a QueryResponse
		queryResponse, err := qp.BuildMap(basicnode.Prototype.Map.NewBuilder().Prototype(), 0, func(ma datamodel.MapAssembler) {
			if expectCid.Equals(pcid) {
				qp.MapEntry(ma, "Status", qp.Int(int64(qr.Status)))
				qp.MapEntry(ma, "PieceCIDFound", qp.Int(int64(qr.PieceCIDFound)))
				qp.MapEntry(ma, "Size", qp.Int(int64(qr.Size)))
				qp.MapEntry(ma, "PaymentAddress", qp.Bytes(qr.PaymentAddress.Bytes()))
				priceBytes, err := qr.MinPricePerByte.Bytes()
				require.NoError(t, err)
				qp.MapEntry(ma, "MinPricePerByte", qp.Bytes(priceBytes))
				qp.MapEntry(ma, "MaxPaymentInterval", qp.Int(int64(qr.MaxPaymentInterval)))
				qp.MapEntry(ma, "MaxPaymentIntervalIncrease", qp.Int(int64(qr.MaxPaymentIntervalIncrease)))
				qp.MapEntry(ma, "Message", qp.String(qr.Message))
				priceBytes, err = qr.UnsealPrice.Bytes()
				require.NoError(t, err)
				qp.MapEntry(ma, "UnsealPrice", qp.Bytes(priceBytes))
			} else {
				qp.MapEntry(ma, "Status", qp.Int(int64(retrievalmarket.QueryResponseUnavailable)))
				qp.MapEntry(ma, "PieceCIDFound", qp.Int(int64(retrievalmarket.QueryItemUnavailable)))
				qp.MapEntry(ma, "Size", qp.Int(0))
				qp.MapEntry(ma, "PaymentAddress", qp.Bytes([]byte{}))
				qp.MapEntry(ma, "MinPricePerByte", qp.Bytes([]byte{}))
				qp.MapEntry(ma, "MaxPaymentInterval", qp.Int(0))
				qp.MapEntry(ma, "MaxPaymentIntervalIncrease", qp.Int(0))
				qp.MapEntry(ma, "Message", qp.String(""))
				qp.MapEntry(ma, "UnsealPrice", qp.Bytes([]byte{}))
			}
		})
		require.NoError(t, err)
		require.NoError(t, ipld.EncodeStreaming(s, queryResponse, dagcbor.Encode))
		require.NoError(t, s.Close())
	})
}

func SetupRetrieval(t *testing.T, remote testpeer.TestPeer) chan []datatransfer.Event {
	// Register DealProposal voucher type with automatic Pull acceptance
	remoteDealValidator := &mockDealValidator{acceptPull: true}
	require.NoError(t, remote.DatatransferServer.RegisterVoucherType(retrievalmarket.DealProposalType, remoteDealValidator))

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
	return mrn.MN.Close()
}

type mockCandidateFinder struct {
	mrn *MockRetrievalNet
}

func (mcf *mockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	candidates := make([]types.RetrievalCandidate, 0)
	for _, h := range mcf.mrn.Remotes {
		if h.RootCid.Equals(cid) {
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

func (mcf *mockCandidateFinder) FindCandidatesAsync(ctx context.Context, cid cid.Cid) (<-chan types.FindCandidatesResult, error) {
	ch := make(chan types.FindCandidatesResult)
	go func() {
		cand, _ := mcf.FindCandidates(ctx, cid)
		for _, c := range cand {
			ch <- types.FindCandidatesResult{Candidate: c, Err: nil}
		}
		close(ch)
	}()
	return ch, nil
}

var _ datatransfer.RequestValidator = (*mockDealValidator)(nil)

type mockDealValidator struct {
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
	return datatransfer.ValidationResult{Accepted: mdv.acceptPull}, nil
}

func (mdv *mockDealValidator) ValidateRestart(
	channelID datatransfer.ChannelID,
	channel datatransfer.ChannelState,
) (datatransfer.ValidationResult, error) {
	return datatransfer.ValidationResult{Accepted: false}, errors.New("not supported")
}
