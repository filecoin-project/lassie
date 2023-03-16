package host

import (
	"context"
	"fmt"

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

var defaultAddrs = []string{"/ip4/0.0.0.0/tcp/4001",
	"/ip6/::/tcp/4001",
	"/ip4/0.0.0.0/udp/4001/quic",
	"/ip4/0.0.0.0/udp/4001/quic-v1",
	"/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
	"/ip6/::/udp/4001/quic",
	"/ip6/::/udp/4001/quic-v1",
	"/ip6/::/udp/4001/quic-v1/webtransport",
}

func InitHost(ctx context.Context, opts []libp2p.Option, listenAddrs ...multiaddr.Multiaddr) (Host, error) {
	opts = append([]libp2p.Option{libp2p.Identity(nil), libp2p.ResourceManager(&network.NullResourceManager{})}, opts...)
	if len(listenAddrs) > 0 {
		opts = append([]libp2p.Option{libp2p.ListenAddrs(listenAddrs...)}, opts...)
	} else {
		addrs, err := listenAddresses(defaultAddrs)
		if err != nil {
			return nil, err
		}
		opts = append([]libp2p.Option{libp2p.ListenAddrs(addrs...)}, opts...)
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

func listenAddresses(addresses []string) ([]multiaddr.Multiaddr, error) {
	listen := make([]multiaddr.Multiaddr, len(addresses))
	for i, addr := range addresses {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("failure to parse config.Addresses.Swarm: %s", addresses)
		}
		listen[i] = maddr
	}

	return listen, nil
}

// Host is a type alias for libp2p host
type Host = host.Host
