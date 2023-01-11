package internal

import (
	"context"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

func InitHost(ctx context.Context, listenAddrs ...multiaddr.Multiaddr) (host.Host, error) {
	return libp2p.New(libp2p.ListenAddrs(listenAddrs...), libp2p.Identity(nil), libp2p.ResourceManager(network.NullResourceManager))
}
