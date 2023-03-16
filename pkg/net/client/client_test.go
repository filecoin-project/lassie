package client

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/connmgr"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	ctx := context.Background()

	client := &RetrievalClient{
		host:         newFakeHost(),
		dataTransfer: &fakeDataTransfer{},
	}
	p := peer.ID("testpeer")
	linkSys := cidlink.DefaultLinkSystem()
	gotLoadFor := cid.Undef
	linkSys.StorageReadOpener = func(linkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		gotLoadFor = lnk.(cidlink.Link).Cid
		return nil, nil
	}
	selector := selectorparse.CommonSelector_MatchPoint
	minerWallet := address.Address{}
	proposal := &retrievaltypes.DealProposal{
		PayloadCID: cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfa"),
		Params: retrievaltypes.Params{
			Selector:     retrievaltypes.CborGenCompatibleNode{Node: selector},
			PricePerByte: big.Zero(),
			UnsealPrice:  big.Zero(),
		},
	}
	eventsCb := func(event datatransfer.Event, channelState datatransfer.ChannelState) {}
	gracefulShutdownRequested := make(chan struct{})

	stats, err := client.RetrieveFromPeer(
		ctx,
		linkSys,
		p,
		minerWallet,
		proposal,
		selector,
		eventsCb,
		gracefulShutdownRequested,
	)

	require.NoError(t, err)
	require.NotNil(t, stats)

	require.Len(t, client.dataTransfer.(*fakeDataTransfer).openedPulls, 1)
	require.Same(t, selector, client.dataTransfer.(*fakeDataTransfer).openedPulls[0].selector)
	require.Equal(t, proposal.PayloadCID, client.dataTransfer.(*fakeDataTransfer).openedPulls[0].baseCid)
	require.Equal(t, p, client.dataTransfer.(*fakeDataTransfer).openedPulls[0].to)
	voucher := client.dataTransfer.(*fakeDataTransfer).openedPulls[0].voucher
	require.Equal(t, retrievaltypes.DealProposalType, voucher.Type)
	gotVoucher, err := retrievaltypes.BindnodeRegistry.TypeFromNode(voucher.Voucher, (*retrievaltypes.DealProposal)(nil))
	require.NoError(t, err)
	require.Equal(t, proposal, gotVoucher)

	require.Equal(t, proposal.PayloadCID, gotLoadFor)
}

func TestClient_BadSelector(t *testing.T) {
	ctx := context.Background()

	client := &RetrievalClient{
		host:         newFakeHost(),
		dataTransfer: &fakeDataTransfer{},
	}
	p := peer.ID("testpeer")
	linkSys := cidlink.DefaultLinkSystem()
	selector := basicnode.NewFloat(100.2)
	minerWallet := address.Address{}
	proposal := &retrievaltypes.DealProposal{
		PayloadCID: cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfa"),
		Params: retrievaltypes.Params{
			Selector:     retrievaltypes.CborGenCompatibleNode{Node: selector},
			PricePerByte: big.Zero(),
			UnsealPrice:  big.Zero(),
		},
	}
	eventsCb := func(event datatransfer.Event, channelState datatransfer.ChannelState) {}
	gracefulShutdownRequested := make(chan struct{})

	stats, err := client.RetrieveFromPeer(
		ctx,
		linkSys,
		p,
		minerWallet,
		proposal,
		selector,
		eventsCb,
		gracefulShutdownRequested,
	)

	require.ErrorContains(t, err, "invalid selector")
	require.Nil(t, stats)
}

type fakeHost struct {
	cm connmgr.ConnManager
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		cm: &fakeConnManager{},
	}
}

func (f fakeHost) ID() peer.ID                                                                      { return peer.ID("testpeer") }
func (f fakeHost) Peerstore() peerstore.Peerstore                                                   { return nil }
func (f fakeHost) Addrs() []ma.Multiaddr                                                            { return nil }
func (f fakeHost) Network() network.Network                                                         { return nil }
func (f fakeHost) Mux() protocol.Switch                                                             { return nil }
func (f fakeHost) Connect(ctx context.Context, pi peer.AddrInfo) error                              { return nil }
func (f fakeHost) SetStreamHandler(pid protocol.ID, handler network.StreamHandler)                  {}
func (f fakeHost) SetStreamHandlerMatch(protocol.ID, func(protocol.ID) bool, network.StreamHandler) {}
func (f fakeHost) RemoveStreamHandler(pid protocol.ID)                                              {}
func (f fakeHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	return fakeStream{}, nil
}
func (f fakeHost) Close() error                     { return nil }
func (f fakeHost) ConnManager() connmgr.ConnManager { return f.cm }
func (f fakeHost) EventBus() event.Bus              { return nil }

type fakeConnManager struct {
	protected bool
}

func (f fakeConnManager) TagPeer(peer.ID, string, int)                          {}
func (f fakeConnManager) UntagPeer(p peer.ID, tag string)                       {}
func (f fakeConnManager) UpsertTag(p peer.ID, tag string, upsert func(int) int) {}
func (f fakeConnManager) GetTagInfo(p peer.ID) *connmgr.TagInfo                 { return nil }
func (f fakeConnManager) TrimOpenConns(ctx context.Context)                     {}
func (f fakeConnManager) Notifee() network.Notifiee                             { return nil }
func (f *fakeConnManager) Protect(id peer.ID, tag string)                       { f.protected = true }
func (f *fakeConnManager) Unprotect(id peer.ID, tag string) (protected bool) {
	f.protected = false
	return f.protected
}
func (f *fakeConnManager) IsProtected(id peer.ID, tag string) (protected bool) { return f.protected }
func (f fakeConnManager) Close() error                                         { return nil }

type fakeStream struct{}

func (f fakeStream) Read(p []byte) (n int, err error)  { return 0, nil }
func (f fakeStream) Write(p []byte) (n int, err error) { return 0, nil }
func (f fakeStream) CloseWrite() error                 { return nil }
func (f fakeStream) CloseRead() error                  { return nil }
func (f fakeStream) Close() error                      { return nil }
func (f fakeStream) Reset() error                      { return nil }
func (f fakeStream) SetDeadline(time.Time) error       { return nil }
func (f fakeStream) SetReadDeadline(time.Time) error   { return nil }
func (f fakeStream) SetWriteDeadline(time.Time) error  { return nil }
func (f fakeStream) ID() string                        { return "" }
func (f fakeStream) Protocol() protocol.ID             { return "" }
func (f fakeStream) SetProtocol(id protocol.ID) error  { return nil }
func (f fakeStream) Stat() network.Stats               { return network.Stats{} }
func (f fakeStream) Conn() network.Conn                { return fakeConn{} }
func (f fakeStream) Scope() network.StreamScope        { return nil }

type fakeConn struct{}

func (f fakeConn) Close() error                                      { return nil }
func (f fakeConn) LocalPeer() peer.ID                                { return peer.ID("") }
func (f fakeConn) LocalPrivateKey() ic.PrivKey                       { return nil }
func (f fakeConn) RemotePeer() peer.ID                               { return peer.ID("") }
func (f fakeConn) RemotePublicKey() ic.PubKey                        { return nil }
func (f fakeConn) ConnState() network.ConnectionState                { return network.ConnectionState{} }
func (f fakeConn) LocalMultiaddr() ma.Multiaddr                      { return nil }
func (f fakeConn) RemoteMultiaddr() ma.Multiaddr                     { return nil }
func (f fakeConn) Stat() network.ConnStats                           { return network.ConnStats{} }
func (f fakeConn) Scope() network.ConnScope                          { return nil }
func (f fakeConn) ID() string                                        { return "" }
func (f fakeConn) NewStream(context.Context) (network.Stream, error) { return nil, nil }
func (f fakeConn) GetStreams() []network.Stream                      { return nil }

type openedDt struct {
	to       peer.ID
	voucher  datatransfer.TypedVoucher
	baseCid  cid.Cid
	selector datamodel.Node
}
type fakeDataTransfer struct {
	openedPulls []openedDt
}

func (f fakeDataTransfer) Start(ctx context.Context) error { return nil }
func (f fakeDataTransfer) OnReady(datatransfer.ReadyFunc)  {}
func (f fakeDataTransfer) Stop(ctx context.Context) error  { return nil }
func (f fakeDataTransfer) RegisterVoucherType(voucherType datatransfer.TypeIdentifier, validator datatransfer.RequestValidator) error {
	return nil
}
func (f fakeDataTransfer) RegisterTransportConfigurer(voucherType datatransfer.TypeIdentifier, configurer datatransfer.TransportConfigurer) error {
	return nil
}
func (f fakeDataTransfer) OpenPushDataChannel(ctx context.Context, to peer.ID, voucher datatransfer.TypedVoucher, baseCid cid.Cid, selector datamodel.Node, options ...datatransfer.TransferOption) (datatransfer.ChannelID, error) {
	panic("unexpected")
}
func (f *fakeDataTransfer) OpenPullDataChannel(ctx context.Context, to peer.ID, voucher datatransfer.TypedVoucher, baseCid cid.Cid, selector datamodel.Node, options ...datatransfer.TransferOption) (datatransfer.ChannelID, error) {
	if f.openedPulls == nil {
		f.openedPulls = make([]openedDt, 0)
	}
	f.openedPulls = append(f.openedPulls, openedDt{to, voucher, baseCid, selector})
	eventsCb := datatransfer.FromOptions(options).EventsCb()
	if eventsCb == nil {
		panic("no events callback")
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		eventsCb(datatransfer.Event{
			Code: datatransfer.CleanupComplete,
		}, nil)
	}()
	return datatransfer.ChannelID{}, nil
}
func (f fakeDataTransfer) SendVoucher(ctx context.Context, chid datatransfer.ChannelID, voucher datatransfer.TypedVoucher) error {
	return nil
}
func (f fakeDataTransfer) SendVoucherResult(ctx context.Context, chid datatransfer.ChannelID, voucherResult datatransfer.TypedVoucher) error {
	return nil
}
func (f fakeDataTransfer) UpdateValidationStatus(ctx context.Context, chid datatransfer.ChannelID, validationResult datatransfer.ValidationResult) error {
	return nil
}
func (f fakeDataTransfer) CloseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	return nil
}
func (f fakeDataTransfer) PauseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	return nil
}
func (f fakeDataTransfer) ResumeDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	return nil
}
func (f fakeDataTransfer) TransferChannelStatus(ctx context.Context, x datatransfer.ChannelID) datatransfer.Status {
	return datatransfer.Requested
}
func (f fakeDataTransfer) ChannelState(ctx context.Context, chid datatransfer.ChannelID) (datatransfer.ChannelState, error) {
	return fakeChannelState{}, nil
}
func (f fakeDataTransfer) SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	return nil
}
func (f fakeDataTransfer) InProgressChannels(ctx context.Context) (map[datatransfer.ChannelID]datatransfer.ChannelState, error) {
	return nil, nil
}
func (f fakeDataTransfer) RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	return nil
}

type fakeChannelState struct{}

func (f fakeChannelState) TransferID() datatransfer.TransferID         { return datatransfer.TransferID(100) }
func (f fakeChannelState) BaseCID() cid.Cid                            { return cid.Undef }
func (f fakeChannelState) Selector() datamodel.Node                    { return nil }
func (f fakeChannelState) Voucher() datatransfer.TypedVoucher          { return datatransfer.TypedVoucher{} }
func (f fakeChannelState) Sender() peer.ID                             { return peer.ID("") }
func (f fakeChannelState) Recipient() peer.ID                          { return peer.ID("") }
func (f fakeChannelState) TotalSize() uint64                           { return 200 }
func (f fakeChannelState) IsPull() bool                                { return false }
func (f fakeChannelState) ChannelID() datatransfer.ChannelID           { return datatransfer.ChannelID{} }
func (f fakeChannelState) OtherPeer() peer.ID                          { return peer.ID("") }
func (f fakeChannelState) SelfPeer() peer.ID                           { return peer.ID("") }
func (f fakeChannelState) Status() datatransfer.Status                 { return datatransfer.Completed }
func (f fakeChannelState) Sent() uint64                                { return 300 }
func (f fakeChannelState) Received() uint64                            { return 400 }
func (f fakeChannelState) Message() string                             { return "500" }
func (f fakeChannelState) Vouchers() []datatransfer.TypedVoucher       { return nil }
func (f fakeChannelState) VoucherResults() []datatransfer.TypedVoucher { return nil }
func (f fakeChannelState) LastVoucher() datatransfer.TypedVoucher      { return datatransfer.TypedVoucher{} }
func (f fakeChannelState) LastVoucherResult() datatransfer.TypedVoucher {
	return datatransfer.TypedVoucher{}
}
func (f fakeChannelState) ReceivedCidsTotal() int64            { return 600 }
func (f fakeChannelState) QueuedCidsTotal() int64              { return 700 }
func (f fakeChannelState) SentCidsTotal() int64                { return 800 }
func (f fakeChannelState) Queued() uint64                      { return 900 }
func (f fakeChannelState) DataLimit() uint64                   { return 1000 }
func (f fakeChannelState) RequiresFinalization() bool          { return false }
func (f fakeChannelState) InitiatorPaused() bool               { return false }
func (f fakeChannelState) ResponderPaused() bool               { return false }
func (f fakeChannelState) BothPaused() bool                    { return false }
func (f fakeChannelState) SelfPaused() bool                    { return false }
func (f fakeChannelState) Stages() *datatransfer.ChannelStages { return nil }
