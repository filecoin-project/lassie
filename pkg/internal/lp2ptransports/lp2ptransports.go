package lp2ptransports

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var logger = log.Logger("lassie/lp2p/tspt/client")

// TransportsProtocolID is the protocol for querying which retrieval transports
// the Storage Provider supports (http, libp2p, etc)
const TransportsProtocolID = protocol.ID("/fil/retrieval/transports/1.0.0")

const streamReadDeadline = 5 * time.Second

// TransportsClient sends retrieval queries over libp2p
type TransportsClient struct {
	h host.Host
}

// NewTransportsClient creates a new query over libp2p
func NewTransportsClient(h host.Host) *TransportsClient {
	c := &TransportsClient{
		h: h,
	}
	return c
}

// SendQuery sends a retrieval query over a libp2p stream to the peer
func (c *TransportsClient) SendQuery(ctx context.Context, id peer.ID) (*QueryResponse, error) {
	logger.Debugw("query", "peer", id)

	// Create a libp2p stream to the provider
	s, err := c.h.NewStream(ctx, id, TransportsProtocolID)
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(streamReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	queryResponsei, err := BindnodeRegistry.TypeFromReader(s, (*QueryResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, fmt.Errorf("reading query response: %w", err)
	}
	queryResponse := queryResponsei.(*QueryResponse)

	logger.Debugw("response", "peer", id)

	return queryResponse, nil
}
