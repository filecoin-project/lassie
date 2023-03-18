package host

import (
	"context"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	"github.com/multiformats/go-multiaddr"
)

const yamuxID = "/yamux/1.0.0"
const mplexID = "/mplex/6.7.0"

func InitHost(ctx context.Context, opts []libp2p.Option, listenAddrs ...multiaddr.Multiaddr) (Host, error) {
	opts = append([]libp2p.Option{libp2p.Identity(nil), libp2p.ResourceManager(&network.NullResourceManager{})}, opts...)
	if len(listenAddrs) > 0 {
		opts = append([]libp2p.Option{libp2p.ListenAddrs(listenAddrs...)}, opts...)
	}
	// add transports
	opts = append([]libp2p.Option{libp2p.Transport(tcp.NewTCPTransport, tcp.WithMetrics()), libp2p.Transport(websocket.New), libp2p.Transport(quic.NewTransport), libp2p.Transport(webtransport.New)}, opts...)
	// add security
	opts = append([]libp2p.Option{libp2p.Security(tls.ID, tls.New), libp2p.Security(noise.ID, noise.New)}, opts...)

	// add muxers
	opts = append([]libp2p.Option{libp2p.Muxer(yamuxID, yamuxTransport()), libp2p.Muxer(mplexID, mplex.DefaultTransport)}, opts...)
	return libp2p.New(opts...)
}

func yamuxTransport() network.Multiplexer {
	tpt := *yamux.DefaultTransport
	tpt.AcceptBacklog = 512
	return &tpt
}

// Host is a type alias for libp2p host
type Host = host.Host
