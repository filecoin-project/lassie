package testpeer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	delayed "github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipfs/go-libipfs/bitswap/server"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	p2ptestutil "github.com/libp2p/go-libp2p-testing/netutil"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

var logger = log.Logger("lassie/mocknet")

// NewTestPeerGenerator generates a new TestPeerGenerator for the given
// mocknet
func NewTestPeerGenerator(ctx context.Context, t *testing.T, mn mocknet.Mocknet, netOptions []bsnet.NetOpt, bsOptions []server.Option) TestPeerGenerator {
	ctx, cancel := context.WithCancel(ctx)
	return TestPeerGenerator{
		seq:        0,
		t:          t,
		ctx:        ctx, // TODO take ctx as param to Next, Instances
		mn:         mn,
		cancel:     cancel,
		bsOptions:  bsOptions,
		netOptions: netOptions,
	}
}

// TestPeerGenerator generates new test peers bitswap+dependencies
// TODO: add graphsync/markets stack, make protocols choosable
type TestPeerGenerator struct {
	seq        int
	t          *testing.T
	mn         mocknet.Mocknet
	ctx        context.Context
	cancel     context.CancelFunc
	bsOptions  []server.Option
	netOptions []bsnet.NetOpt
}

// Close closes the clobal context, shutting down all test peers
func (g *TestPeerGenerator) Close() error {
	g.cancel()
	return nil // for Closer interface
}

// NextBitswap generates a new test peer with bitswap + dependencies
func (g *TestPeerGenerator) NextBitswap() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestBitswapPeer(g.ctx, g.mn, p, g.netOptions, g.bsOptions)
	require.NoError(g.t, err)
	return tp
}

// NextGraphsync generates a new test peer with graphsync + dependencies
func (g *TestPeerGenerator) NextGraphsync() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestGraphsyncPeer(g.ctx, g.mn, p)
	require.NoError(g.t, err)
	return tp
}

// NextHttp generates a new test peer with http + dependencies
func (g *TestPeerGenerator) NextHttp() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestHttpPeer(g.ctx, g.mn, p, g.t)
	require.NoError(g.t, err)
	return tp
}

// BitswapPeers creates N test peers with bitswap + dependencies
func (g *TestPeerGenerator) BitswapPeers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextBitswap()
		instances = append(instances, inst)
	}
	return instances
}

// GraphsyncPeers creates N test peers with graphsync + dependencies
func (g *TestPeerGenerator) GraphsyncPeers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextGraphsync()
		instances = append(instances, inst)
	}
	return instances
}

// HttpPeers creates N test peers with http  + dependencies
func (g *TestPeerGenerator) HttpPeers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextHttp()
		instances = append(instances, inst)
	}
	return instances
}

// ConnectPeers connects the given peers to each other
func ConnectPeers(instances []TestPeer) {
	for i, inst := range instances {
		for j := i + 1; j < len(instances); j++ {
			oinst := instances[j]
			err := inst.Host.Connect(context.Background(), peer.AddrInfo{ID: oinst.ID})
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

// TestPeer is a test instance of bitswap + dependencies for integration testing
type TestPeer struct {
	ID                 peer.ID
	BitswapServer      *server.Server
	BitswapNetwork     bsnet.BitSwapNetwork
	DatatransferServer datatransfer.Manager
	HttpServer         *TestPeerHttpServer
	blockstore         blockstore.Blockstore
	Host               host.Host
	blockstoreDelay    delay.D
	LinkSystem         *linking.LinkSystem
	Cids               map[cid.Cid]struct{}
	Protocol           multicodec.Code
}

// Blockstore returns the block store for this test instance
func (i *TestPeer) Blockstore() blockstore.Blockstore {
	return i.blockstore
}

// SetBlockstoreLatency customizes the artificial delay on receiving blocks
// from a blockstore test instance.
func (i *TestPeer) SetBlockstoreLatency(t time.Duration) time.Duration {
	return i.blockstoreDelay.Set(t)
}

func (i TestPeer) AddrInfo() *peer.AddrInfo {
	return &peer.AddrInfo{
		ID:    i.ID,
		Addrs: i.Host.Addrs(),
	}
}

// NewTestBitswapPeer creates a test peer instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// instances. To safeguard, use the InstanceGenerator to generate instances. It's
// just a much better idea.
func NewTestBitswapPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity, netOptions []bsnet.NetOpt, bsOptions []server.Option) (TestPeer, error) {
	peer, _, err := newTestPeer(ctx, mn, p)
	if err != nil {
		return TestPeer{}, err
	}
	bsNet := bsnet.NewFromIpfsHost(peer.Host, routinghelpers.Null{}, netOptions...)
	bs := server.New(ctx, bsNet, peer.blockstore, bsOptions...)
	bsNet.Start(bs)
	go func() {
		<-ctx.Done()
		bsNet.Stop()
	}()
	peer.BitswapServer = bs
	peer.BitswapNetwork = bsNet
	peer.Protocol = multicodec.TransportBitswap
	return peer, nil
}

func NewTestGraphsyncPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity) (TestPeer, error) {
	peer, dstore, err := newTestPeer(ctx, mn, p)
	if err != nil {
		return TestPeer{}, err
	}

	// Setup remote data transfer
	gsNetRemote := gsnet.NewFromLibp2pHost(peer.Host)
	dtNetRemote := dtnet.NewFromLibp2pHost(peer.Host, dtnet.RetryParameters(0, 0, 0, 0))
	gsRemote := gsimpl.New(ctx, gsNetRemote, *peer.LinkSystem)
	gstpRemote := gstransport.NewTransport(peer.Host.ID(), gsRemote)
	dtRemote, err := dtimpl.NewDataTransfer(dstore, dtNetRemote, gstpRemote)
	if err != nil {
		return TestPeer{}, err
	}

	// Wait for remote data transfer to be ready
	if err := StartAndWaitForReady(ctx, dtRemote); err != nil {
		return TestPeer{}, err
	}

	peer.DatatransferServer = dtRemote
	peer.Protocol = multicodec.TransportGraphsyncFilecoinv1
	return peer, nil
}

func NewTestHttpPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity, t *testing.T) (TestPeer, error) {
	peer, _, err := newTestPeer(ctx, mn, p)
	if err != nil {
		return TestPeer{}, err
	}

	// Create http multiaddr from random peer addr and add it to the peer's addreses
	httpAddr := p.Address().Encapsulate(ma.StringCast("/http"))
	peer.Host.Peerstore().AddAddr(p.ID(), httpAddr, 10*time.Minute) // TODO: Look into ttl duration?

	go func() {
		// Parse multiaddr IP and port, serve http server from address
		addrParts := strings.Split(p.Address().String(), "/")
		peerHttpServer, err := NewTestPeerHttpServer(ctx, addrParts[2], addrParts[4])
		if err != nil {
			logger.Errorw("failed to make test peer http server", "err", err)
			ctx.Done()
		}
		peer.HttpServer = peerHttpServer

		// Handle custom /ipfs/ endpoint
		peerHttpServer.Mux.HandleFunc("/ipfs/", MockIpfsHandler(ctx, *peer.LinkSystem))

		// Start the server
		peerHttpServer.Start()
		if err != http.ErrServerClosed {
			logger.Errorw("failed to start peer http server", "err", err)
			ctx.Done()
		}
	}()

	peer.Protocol = multicodec.TransportIpfsGatewayHttp
	return peer, nil
}

func newTestPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity) (TestPeer, ds.Batching, error) {
	bsdelay := delay.Fixed(0)

	client, err := mn.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}

	dstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	dstoreDelayed := delayed.New(dstore, bsdelay)

	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(dstoreDelayed),
		blockstore.DefaultCacheOpts())
	if err != nil {
		return TestPeer{}, nil, err
	}
	lsys := storeutil.LinkSystemForBlockstore(bstore)
	tp := TestPeer{
		Host:            client,
		ID:              p.ID(),
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
		LinkSystem:      &lsys,
		Cids:            make(map[cid.Cid]struct{}),
	}

	wo := tp.LinkSystem.StorageWriteOpener
	// track CIDs put into this store so we can serve via the CandidateFinder
	tp.LinkSystem.StorageWriteOpener = func(lnkCtx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		w, c, err := wo(lnkCtx)
		if err != nil {
			return nil, nil, err
		}
		return w, func(lnk ipld.Link) error {
			cid, ok := lnk.(cidlink.Link)
			if !ok {
				return fmt.Errorf("expected cidlink.Link, got %T", lnk)
			}
			tp.Cids[cid.Cid] = struct{}{}
			return c(lnk)
		}, nil
	}

	return tp, dstore, nil
}

func StartAndWaitForReady(ctx context.Context, manager datatransfer.Manager) error {
	ready := make(chan error, 1)
	manager.OnReady(func(err error) {
		ready <- err
	})
	if err := manager.Start(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ready:
		return err
	}
}

func MockIpfsHandler(ctx context.Context, lsys linking.LinkSystem) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to parse CID path parameter: %s", cidStr), http.StatusBadRequest)
			return
		}

		// Grab unixfs path if it exists
		unixfsPath := ""
		if len(urlPath) > 2 {
			unixfsPath = "/" + strings.Join(urlPath[2:], "/")
		}

		// We're always providing the car-scope parameter, so add a failure case if we stop
		// providing it in the future
		if !req.URL.Query().Has("car-scope") {
			http.Error(res, "Missing car-scope parameter", http.StatusBadRequest)
			return
		}

		// Parse car scope and use it to get selector
		var carScope types.CarScope
		switch req.URL.Query().Get("car-scope") {
		case "all":
			carScope = types.CarScopeAll
		case "file":
			carScope = types.CarScopeFile
		case "block":
			carScope = types.CarScopeBlock
		default:
			http.Error(res, fmt.Sprintf("Invalid car-scope parameter: %s", req.URL.Query().Get("car-scope")), http.StatusBadRequest)
			return
		}

		selNode := unixfsnode.UnixFSPathSelectorBuilder(unixfsPath, carScope.TerminalSelectorSpec(), false)
		sel, err := selector.CompileSelector(selNode)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to compile selector from car-scope: %v", err), http.StatusInternalServerError)
			return
		}

		// Write to response writer
		carWriter, err := storage.NewWritable(res, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to create car writer: %v", err), http.StatusInternalServerError)
			return
		}

		// Extend the StorageReadOpener func to write to the carWriter
		originalSRO := lsys.StorageReadOpener
		lsys.StorageReadOpener = func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
			r, err := originalSRO(lc, lnk)
			if err != nil {
				return nil, err
			}
			byts, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			err = carWriter.Put(ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
			if err != nil {
				return nil, err
			}

			return bytes.NewReader(byts), nil
		}

		protoChooser := dagpb.AddSupportToChooser(basicnode.Chooser)
		lnk := cidlink.Link{Cid: rootCid}
		lnkCtx := linking.LinkContext{}
		proto, err := protoChooser(lnk, lnkCtx)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to choose prototype node: %s", cidStr), http.StatusBadRequest)
			return
		}

		rootNode, err := lsys.Load(lnkCtx, lnk, proto)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to load root cid into link system: %v", err), http.StatusInternalServerError)
			return
		}

		cfg := &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
		}
		progress := traversal.Progress{Cfg: cfg}

		err = progress.WalkAdv(rootNode, sel, visitNoop)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to traverse from root node: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

func visitNoop(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil }
