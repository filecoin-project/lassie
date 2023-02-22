package mocknet

import (
	"context"
	"errors"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	lpmock "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	dttestutil "github.com/filecoin-project/go-data-transfer/v2/testutil"
)

var QueryErrorTriggerCid = cid.MustParse("bafkqaalb")

type MockRetrievalNet struct {
	RemoteDatastore datastore.Datastore // can be provided to customise data interactions

	RemoteEvents     []datatransfer.Event
	FinishedChan     chan struct{}
	MN               lpmock.Mocknet
	HostLocal        host.Host
	HostRemote       host.Host
	LinkSystemRemote linking.LinkSystem
	Finder           retriever.CandidateFinder
}

func NewMockRetrievalNet() *MockRetrievalNet {
	mrn := &MockRetrievalNet{
		RemoteEvents: make([]datatransfer.Event, 0),
		FinishedChan: make(chan struct{}, 1),
	}
	mrn.Finder = &mockCandidateFinder{mrn}
	return mrn
}

type mockCandidateFinder struct {
	mrn *MockRetrievalNet
}

func (mcf *mockCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	return []types.RetrievalCandidate{
		{
			MinerPeer: peer.AddrInfo{ID: mcf.mrn.HostRemote.ID()},
			RootCid:   cid,
			Metadata:  metadata.Default.New(&metadata.GraphsyncFilecoinV1{PieceCID: cid}),
		},
	}, nil
}

func (mcf *mockCandidateFinder) FindCandidatesAsync(ctx context.Context, cid cid.Cid) (<-chan types.FindCandidatesResult, error) {
	ch := make(chan types.FindCandidatesResult)
	go func() {
		cand, err := mcf.FindCandidates(ctx, cid)
		ch <- types.FindCandidatesResult{Candidate: cand[0], Err: err}
		close(ch)
	}()
	return ch, nil
}

func (mrn *MockRetrievalNet) SetupNet(ctx context.Context, t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, mrn.TearDown())
	})

	// Setup network
	mrn.MN = lpmock.New()
	var err error
	mrn.HostRemote, err = mrn.MN.GenPeer()
	require.NoError(t, err)
	mrn.HostLocal, err = mrn.MN.GenPeer()
	require.NoError(t, err)
	require.NoError(t, mrn.MN.LinkAll())
}

func (mrn *MockRetrievalNet) SetupQuery(ctx context.Context, t *testing.T, expectCid cid.Cid, qr retrievalmarket.QueryResponse) {
	mrn.HostRemote.SetStreamHandler(retrievalmarket.QueryProtocolID, func(s network.Stream) {
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

func (mrn *MockRetrievalNet) SetupRetrieval(ctx context.Context, t *testing.T) {
	// Setup remote datastore and blockstore
	if mrn.RemoteDatastore == nil {
		mrn.RemoteDatastore = datastore.NewMapDatastore()
	}
	dsRemote := dss.MutexWrap(mrn.RemoteDatastore)
	dtDsRemote := namespace.Wrap(dsRemote, datastore.NewKey("datatransfer"))
	bsRemote := bstore.NewBlockstore(namespace.Wrap(dsRemote, datastore.NewKey("blockstore")))
	mrn.LinkSystemRemote = storeutil.LinkSystemForBlockstore(bsRemote)

	// Setup remote data transfer
	gsNetRemote := gsnet.NewFromLibp2pHost(mrn.HostRemote)
	dtNetRemote := dtnet.NewFromLibp2pHost(mrn.HostRemote, dtnet.RetryParameters(0, 0, 0, 0))
	gsRemote := gsimpl.New(ctx, gsNetRemote, mrn.LinkSystemRemote)
	gstpRemote := gstransport.NewTransport(mrn.HostRemote.ID(), gsRemote)
	dtRemote, err := dtimpl.NewDataTransfer(dtDsRemote, dtNetRemote, gstpRemote)
	require.NoError(t, err)

	// Wait for remote data transfer to be ready
	dttestutil.StartAndWaitForReady(ctx, t, dtRemote)

	// Register DealProposal voucher type with automatic Pull acceptance
	remoteDealValidator := &mockDealValidator{acceptPull: true}
	require.NoError(t, dtRemote.RegisterVoucherType(retrievalmarket.DealProposalType, remoteDealValidator))

	// Record remote events
	subscriberRemote := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		mrn.RemoteEvents = append(mrn.RemoteEvents, event)
		if event.Code == datatransfer.CleanupComplete {
			mrn.FinishedChan <- struct{}{}
		}
	}
	dtRemote.SubscribeToEvents(subscriberRemote)
}

func (mrn *MockRetrievalNet) WaitForFinish(ctx context.Context, t *testing.T) {
	require.Eventually(t, ChanCheck(ctx, t, mrn.FinishedChan), 1*time.Second, 100*time.Millisecond)
}

func (mrn *MockRetrievalNet) TearDown() error {
	return mrn.MN.Close()
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

func ChanCheck(ctx context.Context, t *testing.T, ch <-chan struct{}) func() bool {
	return func() bool {
		select {
		case <-ch:
			return true
		case <-ctx.Done():
			require.Fail(t, ctx.Err().Error())
			return false
		default:
			return false
		}
	}
}
