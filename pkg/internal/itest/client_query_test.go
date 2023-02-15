package itest

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

var expectedQueryResponse = retrievalmarket.QueryResponse{
	Status:                     retrievalmarket.QueryResponseAvailable,
	PieceCIDFound:              retrievalmarket.QueryItemAvailable,
	Size:                       1234,
	PaymentAddress:             address.TestAddress,
	MinPricePerByte:            abi.NewTokenAmount(5678),
	MaxPaymentInterval:         4321,
	MaxPaymentIntervalIncrease: 0,
	Message:                    "yep!",
	UnsealPrice:                abi.NewTokenAmount(0),
}
var testCid1 = cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfy")
var testCid2 = cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfa")

func TestSuccessfulQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mqn := newMockQueryNet(testCid1, expectedQueryResponse)
	require.NoError(t, mqn.setup())
	defer func() {
		require.NoError(t, mqn.teardown())
	}()

	go func() {
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case err := <-mqn.errChan:
				cancel()
				require.Fail(t, err.Error())
				return
			}
		}
	}()

	ds1 := dss.MutexWrap(datastore.NewMapDatastore())
	client, err := client.NewClient(ds1, mqn.client, nil)
	require.NoError(t, err)

	var connected bool
	qr, err := client.RetrievalQueryToPeer(ctx, mqn.serverPeer(), testCid1, func() {
		connected = true
	})
	require.NoError(t, err)
	require.NotNil(t, qr)
	require.True(t, connected)
	require.Equal(t, &expectedQueryResponse, qr)
}

func TestBadQuery(t *testing.T) {
	// in this test we're simulating an error on the query server side by making
	// the mock query server expect a different CID, in which case it'll return
	// an error back on its channel but mess up the query response
	// we're also testing proper handling of context cancellation here too

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mqn := newMockQueryNet(testCid1, expectedQueryResponse)
	require.NoError(t, mqn.setup())
	defer func() {
		require.NoError(t, mqn.teardown())
	}()

	go func() {
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case err := <-mqn.errChan:
				cancel()
				require.Errorf(t, err, fmt.Sprintf("expected PayloadCID to be %s, got %s", testCid1, testCid2))
				return
			}
		}
	}()

	ds1 := dss.MutexWrap(datastore.NewMapDatastore())
	client, err := client.NewClient(ds1, mqn.client, nil)
	require.NoError(t, err)

	var connected bool
	qr, err := client.RetrievalQueryToPeer(ctx, mqn.serverPeer(), testCid2, func() {
		connected = true
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, qr)
	require.True(t, connected)
	require.Nil(t, qr)
}

type mockQueryNet struct {
	cid     cid.Cid
	qr      retrievalmarket.QueryResponse
	errChan chan error
	mn      mocknet.Mocknet
	server  host.Host
	client  host.Host
}

func newMockQueryNet(cid cid.Cid, qr retrievalmarket.QueryResponse) *mockQueryNet {
	mqn := &mockQueryNet{
		cid:     cid,
		qr:      qr,
		errChan: make(chan error),
	}
	return mqn
}

func (mqn *mockQueryNet) serverPeer() peer.AddrInfo {
	return peer.AddrInfo{ID: mqn.server.ID(), Addrs: mqn.server.Addrs()}
}

// Setup
func (mqn *mockQueryNet) setup() error {
	// setup network
	var err error

	mqn.mn = mocknet.New()
	if mqn.server, err = mqn.mn.GenPeer(); err != nil {
		return err
	}
	if mqn.client, err = mqn.mn.GenPeer(); err != nil {
		return err
	}
	if err = mqn.mn.LinkAll(); err != nil {
		return err
	}

	mqn.server.SetStreamHandler(retrievalmarket.QueryProtocolID, func(s network.Stream) {
		// we're doing some manual IPLD work here to exercise other parts of the
		// messaging stack to make sure we're communicating according to spec

		na := basicnode.Prototype.Any.NewBuilder()
		// IPLD normally doesn't allow non-EOF delimited data, but we can cheat
		decoder := dagcbor.DecodeOptions{AllowLinks: true, DontParseBeyondEnd: true}
		if err := decoder.Decode(na, s); err != nil {
			mqn.errChan <- err
			return
		}
		query := na.Build()
		if query.Kind() != datamodel.Kind_Map {
			mqn.errChan <- fmt.Errorf("expected query to be a map, got a %s", query.Kind().String())
			return
		}
		pcidn, err := query.LookupByString("PayloadCID")
		if err != nil {
			mqn.errChan <- fmt.Errorf("query didn't have a valid PayloadCID: %w", err)
			return
		}
		if pcidn.Kind() != datamodel.Kind_Link {
			mqn.errChan <- fmt.Errorf("expected PayloadCID to be a link, got a %s", pcidn.Kind().String())
			return
		}
		pcidl, err := pcidn.AsLink()
		if err != nil {
			mqn.errChan <- fmt.Errorf("query didn't have a valid PayloadCID (not a link): %w", err)
			return
		}
		pcid := pcidl.(cidlink.Link).Cid
		if !pcid.Equals(mqn.cid) {
			mqn.errChan <- fmt.Errorf("expected PayloadCID to be %s, got %s", mqn.cid.String(), pcid.String())
			return
		}

		// build and send a QueryResponse
		queryResponse, err := qp.BuildMap(basicnode.Prototype.Map.NewBuilder().Prototype(), 0, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "Status", qp.Int(int64(mqn.qr.Status)))
			qp.MapEntry(ma, "PieceCIDFound", qp.Int(int64(mqn.qr.PieceCIDFound)))
			qp.MapEntry(ma, "Size", qp.Int(int64(mqn.qr.Size)))
			qp.MapEntry(ma, "PaymentAddress", qp.Bytes(mqn.qr.PaymentAddress.Bytes()))
			priceBytes, err := mqn.qr.MinPricePerByte.Bytes()
			if err != nil {
				mqn.errChan <- err
				return
			}
			qp.MapEntry(ma, "MinPricePerByte", qp.Bytes(priceBytes))
			qp.MapEntry(ma, "MaxPaymentInterval", qp.Int(int64(mqn.qr.MaxPaymentInterval)))
			qp.MapEntry(ma, "MaxPaymentIntervalIncrease", qp.Int(int64(mqn.qr.MaxPaymentIntervalIncrease)))
			qp.MapEntry(ma, "Message", qp.String(mqn.qr.Message))
			priceBytes, err = mqn.qr.UnsealPrice.Bytes()
			if err != nil {
				mqn.errChan <- err
				return
			}
			qp.MapEntry(ma, "UnsealPrice", qp.Bytes(priceBytes))
		})
		if err != nil {
			mqn.errChan <- err
			return
		}
		if err := ipld.EncodeStreaming(s, queryResponse, dagcbor.Encode); err != nil {
			mqn.errChan <- err
			return
		}
		if err := s.Close(); err != nil {
			mqn.errChan <- err
			return
		}
	})

	return nil
}

func (mqn *mockQueryNet) teardown() error {
	return mqn.mn.Close()
}
