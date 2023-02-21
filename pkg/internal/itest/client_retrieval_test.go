package itest

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/stretchr/testify/require"
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
				return unixfs.GenerateDirectory(t, &linkSystem, rndReader, 16<<20, false)
			},
		},
		{
			name: "UnixFSShardedDirectoryDAG",
			generate: func(t *testing.T, linkSystem linking.LinkSystem) (cid.Cid, []unixfs.DirEntry) {
				return unixfs.GenerateDirectory(t, &linkSystem, rndReader, 16<<20, true)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Setup mocknet
			mrn := mocknet.NewMockRetrievalNet()
			mrn.SetupNet(ctx, t)
			mrn.SetupRetrieval(ctx, t)

			// Generate source data on the remote
			rootCid, srcData := tt.generate(t, mrn.LinkSystemRemote)

			// Perform retrieval
			linkSystemLocal := runRetrieval(t, ctx, mrn, rootCid)

			// Check retrieved data by loading it from the blockstore via UnixFS so we
			// reify the original single file data from the DAG
			linkSystemLocal.NodeReifier = unixfsnode.Reify
			// Convert to []DirEntry slice
			gotDir := unixfs.ToDirEntry(t, linkSystemLocal, rootCid)

			// Validate data
			unixfs.CompareDirEntries(t, srcData, gotDir)
		})
	}
}

func runRetrieval(t *testing.T, ctx context.Context, mrn *mocknet.MockRetrievalNet, rootCid cid.Cid) linking.LinkSystem {
	req := require.New(t)

	// Setup local datastore and blockstore
	dsLocal := dss.MutexWrap(datastore.NewMapDatastore())
	dtDsLocal := namespace.Wrap(dsLocal, datastore.NewKey("datatransfer"))
	bsLocal := bstore.NewBlockstore(namespace.Wrap(dsLocal, datastore.NewKey("blockstore")))
	linkSystemLocal := storeutil.LinkSystemForBlockstore(bsLocal)

	// New client
	client, err := client.NewClient(dtDsLocal, mrn.HostLocal, nil)
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
		mrn.HostRemote.ID(),
		paymentAddress,
		proposal,
		subscriberLocal,
		shutdown,
	)
	req.NoError(err)
	req.NotNil(stats)

	// Ensure we are properly cleaned up
	req.Eventually(mocknet.ChanCheck(ctx, t, finishedChan), 1*time.Second, 100*time.Millisecond)
	mrn.WaitForFinish(ctx, t)

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
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.Error), 0)
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.Open), 1)
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.Accept), 1)
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.TransferInitiated), 1)
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.CleanupComplete), 1)
	req.Len(eventSliceFilter(mrn.RemoteEvents, datatransfer.Complete), 1)

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
