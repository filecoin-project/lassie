package itest

import (
	"context"
	"errors"
	"io"
	"math/rand"
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
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	dttestutil "github.com/filecoin-project/go-data-transfer/v2/testutil"
)

func TestRetrieval(t *testing.T) {
	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	tests := []struct {
		name     string
		generate func(*testing.T, linking.LinkSystem) (rootCid cid.Cid, srcData []unixfs.DirEntry)
	}{
		{
			name: "UnixFSFileDAG",
			generate: func(t *testing.T, linkSystem linking.LinkSystem) (cid.Cid, []unixfs.DirEntry) {
				rootCid, srcBytes := unixfs.GenerateFile(t, &linkSystem, rndReader, 4<<20)
				return rootCid, []unixfs.DirEntry{{Path: "", Cid: rootCid, Content: srcBytes}}
			},
		},
		{
			name: "UnixFSDirectoryDAG",
			generate: func(t *testing.T, linkSystem linking.LinkSystem) (cid.Cid, []unixfs.DirEntry) {
				return unixfs.GenerateDirectory(t, &linkSystem, rndReader, 16<<20)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Setup mocknet and remove
			mrn := newMockRetrievalNet()
			mrn.setup(ctx, t)

			// Generate source data on the remote
			rootCid, srcData := tt.generate(t, mrn.linkSystemRemote)

			// Perform retrieval
			linkSystemLocal := runRetrieval(t, ctx, mrn, rootCid)

			// Check retrieved data by loading it from the blockstore via UnixFS so we
			// reify the original single file data from the DAG
			linkSystemLocal.NodeReifier = unixfsnode.Reify
			// Convert to []DirEntry slice
			gotDir := unixfs.ToDirEntry(t, linkSystemLocal, rootCid)
			require.NotNil(t, gotDir)

			// Validate data
			unixfs.CompareDirEntries(t, srcData, gotDir)
		})
	}
}

func runRetrieval(t *testing.T, ctx context.Context, mrn *mockRetrievalNet, rootCid cid.Cid) linking.LinkSystem {
	req := require.New(t)

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
	var lastReceivedBytes uint64
	var lastReceivedBlocks uint64
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
	stats, err := client.RetrieveFromPeer(
		ctx,
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
	req.Eventually(chanCheck(ctx, t, finishedChan), 1*time.Second, 100*time.Millisecond)
	mrn.waitForFinish(ctx, t)

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

	// Check remote events
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Error), 0)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Open), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Accept), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.TransferInitiated), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.CleanupComplete), 1)
	req.Len(eventSliceFilter(mrn.remoteEvents, datatransfer.Complete), 1)

	return linkSystemLocal
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

func chanCheck(ctx context.Context, t *testing.T, ch <-chan struct{}) func() bool {
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

func (mrn *mockRetrievalNet) setup(ctx context.Context, t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, mrn.teardown())
	})

	// Setup remote datastore and blockstore
	dsRemote := dss.MutexWrap(datastore.NewMapDatastore())
	dtDsRemote := namespace.Wrap(dsRemote, datastore.NewKey("datatransfer"))
	bsRemote := bstore.NewBlockstore(namespace.Wrap(dsRemote, datastore.NewKey("blockstore")))
	mrn.linkSystemRemote = storeutil.LinkSystemForBlockstore(bsRemote)

	// Setup network
	mrn.mn = mocknet.New()
	var err error
	mrn.hostRemote, err = mrn.mn.GenPeer()
	require.NoError(t, err)
	mrn.hostLocal, err = mrn.mn.GenPeer()
	require.NoError(t, err)
	require.NoError(t, mrn.mn.LinkAll())

	// Setup remote data transfer
	gsNetRemote := gsnet.NewFromLibp2pHost(mrn.hostRemote)
	dtNetRemote := dtnet.NewFromLibp2pHost(mrn.hostRemote, dtnet.RetryParameters(0, 0, 0, 0))
	gsRemote := gsimpl.New(ctx, gsNetRemote, mrn.linkSystemRemote)
	gstpRemote := gstransport.NewTransport(mrn.hostRemote.ID(), gsRemote)
	dtRemote, err := dtimpl.NewDataTransfer(dtDsRemote, dtNetRemote, gstpRemote)
	require.NoError(t, err)

	// Wait for remote data transfer to be ready
	dttestutil.StartAndWaitForReady(ctx, t, dtRemote)

	// Register DealProposal voucher type with automatic Pull acceptance
	remoteDealValidator := &mockDealValidator{acceptPull: true}
	require.NoError(t, dtRemote.RegisterVoucherType(retrievalmarket.DealProposalType, remoteDealValidator))

	// Record remote events
	subscriberRemote := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		mrn.remoteEvents = append(mrn.remoteEvents, event)
		if event.Code == datatransfer.CleanupComplete {
			mrn.finishedChan <- struct{}{}
		}
	}
	dtRemote.SubscribeToEvents(subscriberRemote)
}

func (mrn *mockRetrievalNet) waitForFinish(ctx context.Context, t *testing.T) {
	require.Eventually(t, chanCheck(ctx, t, mrn.finishedChan), 1*time.Second, 100*time.Millisecond)
}

func (mrn *mockRetrievalNet) teardown() error {
	return mrn.mn.Close()
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
