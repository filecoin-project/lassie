package testpeer

import (
	"context"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	delayed "github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipfs/go-libipfs/bitswap/server"
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
func (g *TestPeerGenerator) Next() TestPeer {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	require.NoError(g.t, err)
	tp, err := NewTestPeer(g.ctx, g.mn, p, g.netOptions, g.bsOptions)
	require.NoError(g.t, err)
	return tp
}

// Peers creates N test peers with bitswap + dependencies
func (g *TestPeerGenerator) Peers(n int) []TestPeer {
	var instances []TestPeer
	for j := 0; j < n; j++ {
		inst := g.Next()
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
	ID              peer.ID
	BitswapServer   *server.Server
	blockstore      blockstore.Blockstore
	Host            host.Host
	blockstoreDelay delay.D
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

// NewTestPeer creates a test peer instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// instances. To safeguard, use the InstanceGenerator to generate instances. It's
// just a much better idea.
func NewTestPeer(ctx context.Context, mn mocknet.Mocknet, p tnet.Identity, netOptions []bsnet.NetOpt, bsOptions []server.Option) (TestPeer, error) {
	bsdelay := delay.Fixed(0)

	client, err := mn.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	bsNet := bsnet.NewFromIpfsHost(client, routinghelpers.Null{}, netOptions...)

	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))

	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds_sync.MutexWrap(dstore)),
		blockstore.DefaultCacheOpts())
	if err != nil {
		return TestPeer{}, err
	}

	bs := server.New(ctx, bsNet, bstore, bsOptions...)
	bsNet.Start(bs)
	return TestPeer{
		Host:            client,
		ID:              p.ID(),
		BitswapServer:   bs,
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
	}, nil
}
