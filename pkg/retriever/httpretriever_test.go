package retriever_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
)

func TestHTTPRetriever(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := require.New(t)
	clock := clock.NewMock()
	clock.Set(time.Now())
	retrievalID := types.RetrievalID(uuid.New())
	getTimeout := func(_ peer.ID) time.Duration { return 5 * time.Second }
	// setup remote with a DAG ready to serve as a CAR
	remote := newHttpRemote(ctx, t)
	candidate := NewHttpCandidate(peer.ID("foo"), nil, remote.RootCid)

	// local store, track the CIDs in the order we receive them
	receivedBlocks := make([]cid.Cid, 0)
	clientStore := &correctedMemStore{&memstore.Store{}}
	clientLsys := cidlink.DefaultLinkSystem()
	clientLsys.SetReadStorage(clientStore)
	clientLsys.SetWriteStorage(clientStore)
	originalCWO := clientLsys.StorageWriteOpener
	clientLsys.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		w, wc, err := originalCWO(lc)
		if err != nil {
			return nil, nil, err
		}
		return w, func(l datamodel.Link) error {
			receivedBlocks = append(receivedBlocks, l.(cidlink.Link).Cid)
			return wc(l)
		}, nil
	}

	carR, carW := io.Pipe()
	rt := NewCannedBytesRoundTripper(carR)
	// mock http client that returns carBytes as the response body regardless
	// of the request
	client := &http.Client{Transport: rt}

	retriever := retriever.NewHttpRetrieverWithClock(getTimeout, client, clock)

	request := types.RetrievalRequest{
		Cid:         remote.RootCid,
		RetrievalID: retrievalID,
		LinkSystem:  clientLsys,
		Scope:       types.CarScopeAll,
	}

	connectedCh := make(chan struct{})
	eventCb := func(events types.RetrievalEvent) {
		switch events.Code() {
		case types.ConnectedCode:
			close(connectedCh)
		}
	}

	var stats *types.RetrievalStats
	var retrievalErr error
	doneCh := make(chan struct{})
	go func() {
		retrieval := retriever.Retrieve(ctx, request, eventCb)
		stats, retrievalErr = retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{candidate}))
		close(doneCh)
	}()

	waitForClosed(t, ctx, connectedCh) // successfully started the HTTP request, got a body reader
	// check the request that was given to the client
	req.NotNil(rt.req)
	req.Equal(fmt.Sprintf("http://127.1.2.3:6789/ipfs/%s?car-scope=all", remote.RootCid.String()), rt.req.URL.String())
	// write the body from remote side
	remote.GenerateCar(carW)
	waitForClosed(t, ctx, doneCh) // successfully finished the retrieval

	req.NoError(retrievalErr)
	req.NotNil(stats)
	req.Equal(peer.ID("foo"), stats.StorageProviderId)
	req.Equal(remote.RootCid, stats.RootCid)
	req.Equal(uint64(100), stats.Blocks)
	req.Equal(remote.SentBytes, stats.Size)
	req.Len(remote.SentBlocks, 100)
	req.Equal(remote.SentBlocks, receivedBlocks)
}

type cannedBytesRoundTripper struct {
	r   io.ReadCloser
	req *http.Request
}

var _ http.RoundTripper = (*cannedBytesRoundTripper)(nil)

func NewCannedBytesRoundTripper(r io.ReadCloser) *cannedBytesRoundTripper {
	return &cannedBytesRoundTripper{r: r}
}

func (c *cannedBytesRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	c.req = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       c.r,
	}, nil
}

func waitForClosed(t *testing.T, ctx context.Context, ch chan struct{}) {
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
}

func NewHttpCandidate(pid peer.ID, addr net.Addr, rootCid cid.Cid) types.RetrievalCandidate {
	rc := types.NewRetrievalCandidate(peer.ID("foo"), rootCid, &metadata.IpfsGatewayHttp{})
	if addr == nil {
		addr = &net.TCPAddr{IP: net.ParseIP("127.1.2.3"), Port: 6789}
	}

	tcpAddr := addr.(*net.TCPAddr)
	n := net.ParseIP(tcpAddr.IP.String())
	if n == nil {
		panic("expected IP address")
	}
	maddr, err := manet.FromIP(n)
	if err != nil {
		panic(err)
	}
	port, err := multiaddr.NewComponent(multiaddr.ProtocolWithCode(multiaddr.P_TCP).Name, strconv.Itoa(tcpAddr.Port))
	if err != nil {
		panic(err)
	}
	maddr = multiaddr.Join(maddr, port)
	scheme, err := multiaddr.NewComponent("http", "")
	if err != nil {
		panic(err)
	}
	maddr = multiaddr.Join(maddr, scheme)
	rc.MinerPeer.Addrs = []multiaddr.Multiaddr{maddr}

	return rc
}

type httpRemote struct {
	RootCid    cid.Cid
	SentBlocks []cid.Cid
	SentBytes  uint64

	ctx  context.Context
	t    *testing.T
	lsys linking.LinkSystem
}

func newHttpRemote(ctx context.Context, t *testing.T) httpRemote {
	serverStore := &correctedMemStore{&memstore.Store{}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(serverStore)
	lsys.SetWriteStorage(serverStore)

	tbc := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	return httpRemote{
		RootCid:    tbc.TipLink.(cidlink.Link).Cid,
		SentBlocks: make([]cid.Cid, 0),

		ctx:  ctx,
		t:    t,
		lsys: lsys,
	}
}

func (hr *httpRemote) GenerateCar(w io.WriteCloser) {
	req := require.New(hr.t)

	carWriter, err := storage.NewWritable(w, []cid.Cid{hr.RootCid}, car.WriteAsCarV1(true))
	req.NoError(err)
	sel, err := selector.CompileSelector(selectorparse.CommonSelector_ExploreAllRecursively)
	req.NoError(err)

	originalSRO := hr.lsys.StorageReadOpener
	hr.lsys.StorageReadOpener = func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		hr.SentBlocks = append(hr.SentBlocks, lnk.(cidlink.Link).Cid)
		r, err := originalSRO(lc, lnk)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		carWriter.Put(hr.ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
		hr.SentBytes += uint64(len(byts)) // only the length of the bytes, not the rest of the CAR infrastructure
		return bytes.NewReader(byts), nil
	}

	// load and register the root link so it's pushed to the CAR
	rootNode, err := hr.lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: hr.RootCid}, basicnode.Prototype.Any)
	req.NoError(err)
	traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            hr.ctx,
			LinkSystem:                     hr.lsys,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
			LinkVisitOnlyOnce:              true,
		},
	}.WalkAdv(rootNode, sel, func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil })
	w.Close()
}
