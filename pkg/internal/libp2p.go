package internal

import (
	"context"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

func InitHost(ctx context.Context, opts []libp2p.Option, listenAddrs ...multiaddr.Multiaddr) (host.Host, error) {
	opts = append([]libp2p.Option{libp2p.Identity(nil), libp2p.ResourceManager(&network.NullResourceManager{})}, opts...)
	if len(listenAddrs) > 0 {
		opts = append([]libp2p.Option{libp2p.ListenAddrs(listenAddrs...)}, opts...)
	}
	return libp2p.New(opts...)
}
