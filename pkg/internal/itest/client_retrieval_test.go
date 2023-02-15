package itest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestRetrieval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := require.New(t)

	// Setup mocknet and remove
	mrn := newMockRetrievalNet()
	req.NoError(mrn.setup(ctx))
	defer func() {
		req.NoError(mrn.teardown())
	}()

	// Populate remote with a DAG, and get its root
	rootCid, srcBytes, err := mrn.generateRemoteUnixFSFile()
	req.NoError(err)

	// Setup local datastore and blockstore
	dsLocal := dss.MutexWrap(datastore.NewMapDatastore())
	dtDsLocal := namespace.Wrap(dsLocal, datastore.NewKey("datatransfer"))
	bsLocal := bstore.NewBlockstore(namespace.Wrap(dsLocal, datastore.NewKey("blockstore")))
	linkSystemLocal := storeutil.LinkSystemForBlockstore(bsLocal)

	// New client
	client, err := client.NewClient(dtDsLocal, mrn.hostLocal, nil)
	req.NoError(err)
	req.NoError(client.AwaitReady())

	// Collect events & stats
	gotEvents := make([]datatransfer.Event, 0)
	lastReceivedBytes := uint64(0)
	lastReceivedBlocks := uint64(0)
	finishedChan := make(chan struct{}, 1)
	subscriberLocal := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		gotEvents = append(gotEvents, event)
		lastReceivedBytes = channelState.Received()
		lastReceivedBlocks = uint64(channelState.ReceivedCidsTotal())
		if event.Code == datatransfer.CleanupComplete {
			finishedChan <- struct{}{}
		}
	}

	// Retrieve
	proposal := &retrievalmarket.DealProposal{
		PayloadCID: rootCid,
		ID:         retrievalmarket.DealID(100),
		Params: retrievalmarket.Params{
			PricePerByte: big.Zero(),
			UnsealPrice:  big.Zero(),
		},
	}
	paymentAddress := address.TestAddress2
	shutdown := make(chan struct{})
	stats, err := client.RetrieveFromPeer(ctx,
		linkSystemLocal,
		mrn.hostRemote.ID(),
		paymentAddress,
		proposal,
		subscriberLocal,
		shutdown,
	)
	req.NoError(err)
	req.NotNil(stats)

	// Ensure we are properly cleaned up
	req.NoError(waitForChan(ctx, finishedChan))
	req.NoError(mrn.waitForFinish(ctx))

	// Check retrieved data by loading it from the blockstore via UnixFS so we
	// reify the original single file data from the DAG
	linkSystemLocal.NodeReifier = unixfsnode.Reify
	node, err := linkSystemLocal.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: rootCid}, dagpb.Type.PBNode)
	req.NoError(err)
	destData, err := node.AsBytes()
	req.NoError(err)
	req.Equal(srcBytes, destData)

	// Check stats
	req.Equal(lastReceivedBytes, stats.Size)
	req.Equal(lastReceivedBlocks, stats.Blocks)
	req.Equal(0, stats.NumPayments)
	req.Equal(rootCid, stats.RootCid)
	req.True(stats.Duration > 0)
	req.True(stats.TimeToFirstByte <= stats.Duration)

	// Check events
	req.Len(eventSliceFilter(gotEvents, datatransfer.Error), 0)
	req.Len(eventSliceFilter(gotEvents, datatransfer.Open), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.Opened), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.TransferInitiated), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.Accept), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.ResumeResponder), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.FinishTransfer), 1)
	req.Len(eventSliceFilter(gotEvents, datatransfer.CleanupComplete), 1)
	// TODO: is this guaranteed?
	req.Len(eventSliceFilter(gotEvents, datatransfer.DataReceived), int(stats.Blocks))
	req.Len(eventSliceFilter(gotEvents, datatransfer.DataReceivedProgress), int(stats.Blocks))

	// Check remote events
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Error), 0)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Open), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Accept), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.TransferInitiated), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.CleanupComplete), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Complete), 1)
	// TODO: is this guaranteed?
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.DataQueued), int(stats.Blocks))
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.DataSent), int(stats.Blocks))
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.DataSentProgress), int(stats.Blocks))
}

func eventSliceFilter(events []datatransfer.Event, code datatransfer.EventCode) []datatransfer.Event {
	filtered := make([]datatransfer.Event, 0)
	for _, event := range events {
		if event.Code == code {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func waitForChan(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-time.After(1 * time.Second):
		return errors.New("timed out waiting for finish")
	case <-ctx.Done():
		return ctx.Err()
	}
}

type mockRetrievalNet struct {
	remoteEvents []datatransfer.Event

	finishedChan     chan struct{}
	mn               mocknet.Mocknet
	hostLocal        host.Host
	hostRemote       host.Host
	linkSystemRemote linking.LinkSystem
}

func newMockRetrievalNet() *mockRetrievalNet {
	mqn := &mockRetrievalNet{
		remoteEvents: make([]datatransfer.Event, 0),
		finishedChan: make(chan struct{}, 1),
	}
	return mqn
}

func (mrn *mockRetrievalNet) setup(ctx context.Context) error {
	// Setup remote datastore and blockstore
	dsRemote := dss.MutexWrap(datastore.NewMapDatastore())
	dtDsRemote := namespace.Wrap(dsRemote, datastore.NewKey("datatransfer"))
	bsRemote := bstore.NewBlockstore(namespace.Wrap(dsRemote, datastore.NewKey("blockstore")))
	mrn.linkSystemRemote = storeutil.LinkSystemForBlockstore(bsRemote)

	// Setup network
	mrn.mn = mocknet.New()
	var err error
	mrn.hostRemote, err = mrn.mn.GenPeer()
	if err != nil {
		return err
	}
	mrn.hostLocal, err = mrn.mn.GenPeer()
	if err != nil {
		return err
	}
	if err := mrn.mn.LinkAll(); err != nil {
		return err
	}

	// Setup remote data transfer
	gsNetRemote := gsnet.NewFromLibp2pHost(mrn.hostRemote)
	dtNetRemote := dtnet.NewFromLibp2pHost(mrn.hostRemote, dtnet.RetryParameters(0, 0, 0, 0))
	gsRemote := gsimpl.New(ctx, gsNetRemote, mrn.linkSystemRemote)
	gstpRemote := gstransport.NewTransport(mrn.hostRemote.ID(), gsRemote)
	dtRemote, err := dtimpl.NewDataTransfer(dtDsRemote, dtNetRemote, gstpRemote)
	if err != nil {
		return err
	}

	// Wait for remote data transfer to be ready
	ready := make(chan error, 1)
	dtRemote.OnReady(func(err error) {
		ready <- err
	})
	if err := dtRemote.Start(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case err := <-ready:
		if err != nil {
			return err
		}
	}

	// Register DealProposal voucher type with automatic Pull acceptance
	remoteDealValidator := &mockDealValidator{acceptPull: true}
	if err := dtRemote.RegisterVoucherType(retrievalmarket.DealProposalType, remoteDealValidator); err != nil {
		return err
	}

	// Record remote events
	subscriberRemote := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		mrn.remoteEvents = append(mrn.remoteEvents, event)
		if event.Code == datatransfer.CleanupComplete {
			mrn.finishedChan <- struct{}{}
		}
	}
	dtRemote.SubscribeToEvents(subscriberRemote)

	return nil
}

func (mrn *mockRetrievalNet) waitForFinish(ctx context.Context) error {
	return waitForChan(ctx, mrn.finishedChan)
}

func (mrn *mockRetrievalNet) teardown() error {
	return mrn.mn.Close()
}

func (mrn *mockRetrievalNet) generateRemoteUnixFSFile() (cid.Cid, []byte, error) {
	// a file of 4MiB random bytes, packaged into unixfs DAGs, stored in the remote blockstore
	delimited := io.LimitReader(rand.Reader, 1<<22)
	buf := new(bytes.Buffer)
	delimited = io.TeeReader(delimited, buf)
	root, _, err := builder.BuildUnixFSFile(delimited, "size-256144", &mrn.linkSystemRemote)
	if err != nil {
		return cid.Undef, nil, err
	}
	srcData := buf.Bytes()
	rootCid := root.(cidlink.Link).Cid
	return rootCid, srcData, nil
}

var _ datatransfer.RequestValidator = &mockDealValidator{}

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
