package itest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestHttpRetrieval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	// Setup remote, with data, and prepare it for query and retrieval
	mrn := mocknet.NewMockRetrievalNet()
	mrn.SetupNet(ctx, t)
	mrn.SetupRetrieval(ctx, t)
	rootCid, srcBytes := unixfs.GenerateFile(t, &mrn.LinkSystemRemote, rndReader, 4<<20)
	srcData := []unixfs.DirEntry{{Path: "", Cid: rootCid, Content: srcBytes}}
	qr := testQueryResponse
	qr.MinPricePerByte = abi.NewTokenAmount(0) // make it free so it's not filtered
	mrn.SetupQuery(ctx, t, rootCid, qr)

	// Setup a new lassie
	req := require.New(t)
	lassie, err := lassie.NewLassie(
		ctx,
		lassie.WithProviderTimeout(20*time.Second),
		lassie.WithHost(mrn.HostLocal),
		lassie.WithFinder(mrn.Finder),
	)
	req.NoError(err)

	// Start an HTTP server
	httpServer, err := httpserver.NewHttpServer(ctx, lassie, "127.0.0.1", 0, t.TempDir())
	req.NoError(err)
	go func() {
		err := httpServer.Start()
		req.NoError(err)
	}()
	t.Cleanup(func() {
		req.NoError(httpServer.Close())
	})

	// Make a request for our CID and read the complete CAR bytes
	addr := fmt.Sprintf("http://%s/ipfs/%s", httpServer.Addr(), rootCid.String())
	getReq, err := http.NewRequest("GET", addr, nil)
	req.NoError(err)
	getReq.Header.Add("Accept", "application/vnd.ipld.car")
	client := &http.Client{}
	resp, err := client.Do(getReq)
	req.NoError(err)
	req.Equal(http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	req.NoError(err)
	resp.Body.Close()

	// Open the CAR bytes as read-only storage
	reader, err := storage.OpenReadable(bytes.NewReader(body))
	req.NoError(err)

	// Load our UnixFS data and compare it to the original
	linkSys := cidlink.DefaultLinkSystem()
	linkSys.SetReadStorage(reader)
	linkSys.NodeReifier = unixfsnode.Reify
	linkSys.TrustedStorage = true
	gotDir := unixfs.ToDirEntry(t, linkSys, rootCid)
	unixfs.CompareDirEntries(t, srcData, gotDir)
}

func TestHttpRetrieval_Cancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	// Setup remote, with data, and prepare it for query and retrieval
	mrn := mocknet.NewMockRetrievalNet()
	mrn.RemoteDatastore = &slowDatastore{parent: datastore.NewMapDatastore()}
	mrn.SetupNet(ctx, t)
	mrn.SetupRetrieval(ctx, t)
	rootCid, _ := unixfs.GenerateFile(t, &mrn.LinkSystemRemote, rndReader, 4<<20)
	qr := testQueryResponse
	qr.MinPricePerByte = abi.NewTokenAmount(0) // make it free so it's not filtered
	mrn.SetupQuery(ctx, t, rootCid, qr)

	// Setup a new lassie
	req := require.New(t)
	lassie, err := lassie.NewLassie(
		ctx,
		lassie.WithProviderTimeout(500*time.Millisecond),
		lassie.WithHost(mrn.HostLocal),
		lassie.WithFinder(mrn.Finder),
	)
	req.NoError(err)

	// Start an HTTP server
	httpServer, err := httpserver.NewHttpServer(ctx, lassie, "127.0.0.1", 0, t.TempDir())
	req.NoError(err)
	go func() {
		err := httpServer.Start()
		req.NoError(err)
	}()
	t.Cleanup(func() {
		req.NoError(httpServer.Close())
	})

	cancellableCtx, cancel := context.WithCancel(ctx)

	// Make a request for our CID and read the complete CAR bytes
	addr := fmt.Sprintf("http://%s/ipfs/%s", httpServer.Addr(), rootCid.String())
	getReq, err := http.NewRequestWithContext(cancellableCtx, "GET", addr, nil)
	req.NoError(err)
	getReq.Header.Add("Accept", "application/vnd.ipld.car")
	client := &http.Client{}
	resp, err := client.Do(getReq)
	req.NoError(err)
	req.Equal(http.StatusOK, resp.StatusCode)
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	_, err = io.ReadAll(resp.Body)
	req.ErrorIs(err, context.Canceled)
	resp.Body.Close()

	select {
	// mrn.FinishedChan
	case <-mrn.FinishedChan:
	case <-time.After(5 * time.Second):
		req.Fail("remote should have closed after cancel")
	}
}

var _ datastore.Datastore = (*slowDatastore)(nil)

type slowDatastore struct {
	parent datastore.Datastore
}

// Get(ctx context.Context, key Key) (value []byte, err error)
func (s slowDatastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	if strings.HasPrefix(key.String(), "/blockstore/") {
		time.Sleep(100 * time.Millisecond)
	}
	return s.parent.Get(ctx, key)
}

// Has(ctx context.Context, key Key) (exists bool, err error)
func (s slowDatastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return s.parent.Has(ctx, key)
}

// GetSize(ctx context.Context, key Key) (size int, err error)
func (s slowDatastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	return s.parent.GetSize(ctx, key)
}

// Query(ctx context.Context, q query.Query) (query.Results, error)
func (s slowDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return s.parent.Query(ctx, q)
}

// Put(ctx context.Context, key Key, value []byte) error
func (s slowDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return s.parent.Put(ctx, key, value)
}

// Delete(ctx context.Context, key Key) error
func (s slowDatastore) Delete(ctx context.Context, key datastore.Key) error {
	return s.parent.Delete(ctx, key)
}

// Sync(ctx context.Context, prefix Key) error
func (s slowDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return s.parent.Sync(ctx, prefix)
}

// Close
func (s slowDatastore) Close() error {
	return s.parent.Close()
}
