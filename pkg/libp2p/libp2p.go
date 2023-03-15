package libp2p

import (
	"context"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/multiformats/go-multiaddr"
)

const yamuxID = "/yamux/1.0.0"
const mplexID = "/mplex/6.7.0"

func InitHost(ctx context.Context, opts []libp2p.Option, listenAddrs ...multiaddr.Multiaddr) (host.Host, error) {
	opts = append([]libp2p.Option{libp2p.Identity(nil), libp2p.ResourceManager(&network.NullResourceManager{})}, opts...)
	if len(listenAddrs) > 0 {
		opts = append([]libp2p.Option{libp2p.ListenAddrs(listenAddrs...)}, opts...)
	}
	opts = append(opts, libp2p.Muxer(yamuxID, yamuxTransport()))
	opts = append(opts, libp2p.Muxer(mplexID, mplex.DefaultTransport))
	return libp2p.New(opts...)
}

func yamuxTransport() network.Multiplexer {
	tpt := *yamux.DefaultTransport
	tpt.AcceptBacklog = 512
	return &tpt
}
