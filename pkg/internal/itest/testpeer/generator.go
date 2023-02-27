package testpeer

import (
	"context"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
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
	"github.com/ipld/go-ipld-prime/linking"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	p2ptestutil "github.com/libp2p/go-libp2p-testing/netutil"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

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

// Next generates a new test peer with bitswap + dependencies
func (g *TestPeerGenerator) NextBitswap() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestBitswapPeer(g.ctx, g.mn, p, g.netOptions, g.bsOptions)
	require.NoError(g.t, err)
	return tp
}

// Next generates a new test peer with graphsync + dependencies
func (g *TestPeerGenerator) NextGraphsync() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestGraphsyncPeer(g.ctx, g.mn, p)
	require.NoError(g.t, err)
	return tp
}

// Peers creates N test peers with bitswap + dependencies
func (g *TestPeerGenerator) BitswapPeers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextBitswap()
		instances = append(instances, inst)
	}
	return instances
}

// Peers creates N test peers with bitswap + dependencies
func (g *TestPeerGenerator) GraphsyncPeers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.NextGraphsync()
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
	DatatransferServer datatransfer.Manager
	blockstore         blockstore.Blockstore
	Host               host.Host
	blockstoreDelay    delay.D
	LinkSystem         linking.LinkSystem
	RootCid            cid.Cid
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

// NewTestPeer creates a test peer instance.
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
	peer.BitswapServer = bs
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
	gsRemote := gsimpl.New(ctx, gsNetRemote, peer.LinkSystem)
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
	return TestPeer{
		Host:            client,
		ID:              p.ID(),
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
		LinkSystem:      storeutil.LinkSystemForBlockstore(bstore),
	}, dstore, nil
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
