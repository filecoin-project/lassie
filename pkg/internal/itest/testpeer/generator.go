package testpeer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/filecoin-project/lassie/pkg/internal/itest/linksystemutil"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	delayed "github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
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
func (g *TestPeerGenerator) NextBitswap(opts ...PeerOption) TestPeer {
	g.seq++
	p, err := RandTestPeerIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestBitswapPeer(g.ctx, g.mn, p, g.netOptions, g.bsOptions, opts...)
	require.NoError(g.t, err)
	return tp
}

// NextGraphsync generates a new test peer with graphsync + dependencies
func (g *TestPeerGenerator) NextGraphsync(opts ...PeerOption) TestPeer {
	g.seq++
	p, err := RandTestPeerIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestGraphsyncPeer(g.ctx, g.mn, p, opts...)
	require.NoError(g.t, err)
	return tp
}

// NextHttp generates a new test peer with http + dependencies
func (g *TestPeerGenerator) NextHttp(opts ...PeerOption) TestPeer {
	g.seq++
	p, err := RandTestPeerIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestHttpPeer(g.ctx, g.mn, p, g.t, opts...)
	require.NoError(g.t, err)
	return tp
}

// BitswapPeers creates N test peers with bitswap + dependencies
func (g *TestPeerGenerator) BitswapPeers(n int, opts ...PeerOption) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextBitswap(opts...)
		instances = append(instances, inst)
	}
	return instances
}

// GraphsyncPeers creates N test peers with graphsync + dependencies
func (g *TestPeerGenerator) GraphsyncPeers(n int, opts ...PeerOption) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextGraphsync(opts...)
		instances = append(instances, inst)
	}
	return instances
}

// HttpPeers creates N test peers with http  + dependencies
func (g *TestPeerGenerator) HttpPeers(n int, opts ...PeerOption) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextHttp(opts...)
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
func NewTestBitswapPeer(
	ctx context.Context,
	mn mocknet.Mocknet,
	p tnet.Identity,
	netOptions []bsnet.NetOpt,
	bsOptions []server.Option,
	opts ...PeerOption,
) (TestPeer, error) {
	peer, _, err := newTestPeer(ctx, mn, p, opts...)
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

func NewTestGraphsyncPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity, opts ...PeerOption) (TestPeer, error) {
	peer, dstore, err := newTestPeer(ctx, mn, p, opts...)
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

func NewTestHttpPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity, t *testing.T, opts ...PeerOption) (TestPeer, error) {
	peer, _, err := newTestPeer(ctx, mn, p, opts...)
	if err != nil {
		return TestPeer{}, err
	}

	// Create http multiaddr from random peer addr and add it to the peer's addresses
	httpAddr := p.Address().Encapsulate(ma.StringCast("/http"))
	peer.Host.Peerstore().AddAddr(p.ID(), httpAddr, 10*time.Minute) // TODO: Look into ttl duration?

	// Parse multiaddr IP and port, serve http server from address
	port := strings.Split(p.Address().String(), "/")[4]
	peerHttpServer, err := NewTestPeerHttpServer(ctx, "127.0.0.1", port)
	if err != nil {
		logger.Errorw("failed to make test peer http server", "err", err)
		return TestPeer{}, err
	}
	peer.HttpServer = peerHttpServer
	// Handle custom /ipfs/ endpoint
	peerHttpServer.Mux.HandleFunc("/ipfs/", MockIpfsHandler(ctx, *peer.LinkSystem))
	peer.Protocol = multicodec.TransportIpfsGatewayHttp

	// Start the server
	go func() {
		peerHttpServer.Start()
	}()

	// Close the server when the context is done
	go func() {
		<-ctx.Done()
		if err := peerHttpServer.Close(); err != nil {
			logger.Errorw("failed to close peer http server", "err", err)
		}
	}()

	return peer, nil
}

func newTestPeer(
	ctx context.Context,
	mn mocknet.Mocknet,
	p tnet.Identity,
	opts ...PeerOption,
) (TestPeer, ds.Batching, error) {
	cfg := peerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	bsdelay := delay.Fixed(0)

	client, err := mn.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}

	baseStore := ds.NewMapDatastore()
	dstore := ds_sync.MutexWrap(baseStore)
	dstoreDelayed := delayed.New(dstore, bsdelay)

	if cfg.bstore == nil {
		bstore, err := blockstore.CachedBlockstore(ctx,
			blockstore.NewBlockstore(dstoreDelayed),
			blockstore.DefaultCacheOpts())
		if err != nil {
			return TestPeer{}, nil, err
		}
		cfg.bstore = blockstore.NewIdStore(bstore)
	}
	lsys := storeutil.LinkSystemForBlockstore(cfg.bstore)
	tp := TestPeer{
		Host:            client,
		ID:              p.ID(),
		blockstore:      cfg.bstore,
		blockstoreDelay: bsdelay,
		LinkSystem:      &lsys,
		Cids:            make(map[cid.Cid]struct{}),
	}

	wo := tp.LinkSystem.StorageWriteOpener
	// track CIDs put into this store so we can serve via the CandidateSource
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

// RandTestPeerIdentity is a wrapper around
// github.com/libp2p/go-libp2p-testing/netutil/RandTestBogusIdentity that
// ensures the returned identity has an available port. The identity generated
// by netutil/RandTestBogusIdentity is not guaranteed to have an available port,
// so we use a net.Listen to check if the port is available and try again if
// it's not.
func RandTestPeerIdentity() (tnet.Identity, error) {
	for i := 0; i < 10; i++ {
		id, err := p2ptestutil.RandTestBogusIdentity()
		if err != nil {
			return nil, err
		}
		addr := id.Address()
		port := strings.Split(addr.String(), "/")[4]
		// check if 127.0.0.1:port is available or not
		ln, err := net.Listen("tcp4", "127.0.0.1:"+port)
		if err == nil {
			ln.Close()
			return id, nil
		} // else assume it's in use and try again
	}
	return nil, errors.New("failed to find an available port")
}

type peerConfig struct {
	bstore blockstore.Blockstore
}

type PeerOption func(*peerConfig)

func WithLinkSystem(lsys linking.LinkSystem) PeerOption {
	return func(pc *peerConfig) {
		pc.bstore = linksystemutil.NewLinkSystemBlockstore(lsys)
	}
}
