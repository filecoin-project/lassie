package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type MockRoundTripRemote struct {
	Peer       peer.AddrInfo
	LinkSystem linking.LinkSystem
	Selector   ipld.Node
	RespondAt  time.Time
	Malformed  bool
}

type MockRoundTripper struct {
	t                   *testing.T
	ctx                 context.Context
	clock               *clock.Mock
	remoteBlockDuration time.Duration
	expectedPath        map[cid.Cid]string
	expectedScope       map[cid.Cid]trustlessutils.DagScope
	remotes             map[cid.Cid][]MockRoundTripRemote
	startsCh            chan peer.ID
	statsCh             chan RemoteStats
	endsCh              chan peer.ID
}

var _ http.RoundTripper = (*MockRoundTripper)(nil)
var _ VerifierClient = (*MockRoundTripper)(nil)

func NewMockRoundTripper(
	t *testing.T,
	ctx context.Context,
	clock *clock.Mock,
	remoteBlockDuration time.Duration,
	expectedPath map[cid.Cid]string,
	expectedScope map[cid.Cid]trustlessutils.DagScope,
	remotes map[cid.Cid][]MockRoundTripRemote,
) *MockRoundTripper {
	return &MockRoundTripper{
		t,
		ctx,
		clock,
		remoteBlockDuration,
		expectedPath,
		expectedScope,
		remotes,
		make(chan peer.ID, 32),
		make(chan RemoteStats, 32),
		make(chan peer.ID, 32),
	}
}

func (mrt *MockRoundTripper) getRemote(cid cid.Cid, maddr string) MockRoundTripRemote {
	remotes, ok := mrt.remotes[cid]
	require.True(mrt.t, ok)
	for _, remote := range remotes {
		if remote.Peer.Addrs[0].String() == maddr {
			return remote
		}
	}
	mrt.t.Fatal("remote not found")
	return MockRoundTripRemote{}
}

func (mrt *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	us := strings.Split(req.URL.Path, "/")
	require.True(mrt.t, len(us) > 2)
	require.Equal(mrt.t, us[1], "ipfs")
	root, err := cid.Parse(us[2])
	require.NoError(mrt.t, err)
	path := strings.TrimPrefix(strings.TrimPrefix(req.URL.Path, "/ipfs/"+root.String()), "/")
	expectedPath, ok := mrt.expectedPath[root]
	if !ok {
		require.Equal(mrt.t, path, "")
	} else {
		require.Equal(mrt.t, path, expectedPath)
	}
	expectedScope := trustlessutils.DagScopeAll
	if scope, ok := mrt.expectedScope[root]; ok {
		expectedScope = scope
	}
	require.Equal(mrt.t, req.URL.RawQuery, fmt.Sprintf("dag-scope=%s", expectedScope))
	require.Equal(mrt.t, []string{"application/vnd.ipld.car; version=1; order=dfs; dups=y"}, req.Header["Accept"])
	reqId := req.Header["X-Request-Id"]
	require.Len(mrt.t, reqId, 1)
	_, err = uuid.Parse(reqId[0])
	require.NoError(mrt.t, err)
	ua := req.Header["User-Agent"]
	require.Len(mrt.t, ua, 1)
	require.Regexp(mrt.t, `^lassie\/`, ua[0])

	ip := req.URL.Hostname()
	port := req.URL.Port()
	maddr := fmt.Sprintf("/ip4/%s/tcp/%s/http", ip, port)
	remote := mrt.getRemote(root, maddr)
	mrt.startsCh <- remote.Peer.ID

	sleepFor := mrt.clock.Until(remote.RespondAt)
	if sleepFor > 0 {
		select {
		case <-mrt.ctx.Done():
			return nil, mrt.ctx.Err()
		case <-mrt.clock.After(sleepFor):
		}
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       newDeferredBody(mrt, remote, root),
	}, nil
}

func (mrt *MockRoundTripper) VerifyConnectionsReceived(ctx context.Context, t *testing.T, afterStart time.Duration, expectedConnections []peer.ID) {
	if len(expectedConnections) > 0 {
		require.FailNowf(t, "unexpected ConnectionsReceived", "@ %s", afterStart)
	}
}

func (mrt *MockRoundTripper) VerifyRetrievalsReceived(ctx context.Context, t *testing.T, afterStart time.Duration, expectedRetrievals []peer.ID) {
	retrievals := make([]string, 0, len(expectedRetrievals))
	er := make([]string, 0, len(expectedRetrievals))
	for i := 0; i < len(expectedRetrievals); i++ {
		er = append(er, expectedRetrievals[i].String())
		select {
		case retrieval := <-mrt.startsCh:
			retrievals = append(retrievals, retrieval.String())
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected retrievals", "expected %d, received %d @ %s", len(expectedRetrievals), i, afterStart)
		}
	}
	require.ElementsMatch(t, er, retrievals)
}

func (mrt *MockRoundTripper) VerifyRetrievalsServed(ctx context.Context, t *testing.T, afterStart time.Duration, expectedServed []RemoteStats) {
	remoteStats := make([]RemoteStats, 0, len(expectedServed))
	for i := 0; i < len(expectedServed); i++ {
		select {
		case stats := <-mrt.statsCh:
			remoteStats = append(remoteStats, stats)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected served", "expected %d, received %d @ %s", len(expectedServed), i, afterStart)
		}
	}
	require.ElementsMatch(t, expectedServed, remoteStats)
}

func (mrt *MockRoundTripper) VerifyRetrievalsCompleted(ctx context.Context, t *testing.T, afterStart time.Duration, expectedRetrievals []peer.ID) {
	retrievals := make([]peer.ID, 0, len(expectedRetrievals))
	for i := 0; i < len(expectedRetrievals); i++ {
		select {
		case retrieval := <-mrt.endsCh:
			retrievals = append(retrievals, retrieval)
		case <-ctx.Done():
			require.FailNowf(t, "failed to complete expected retrievals", "expected %d, received %d @ %s", len(expectedRetrievals), i, afterStart)
		}
	}
	require.ElementsMatch(t, expectedRetrievals, retrievals)
}

// deferredBody is simply a Reader that lazily starts a CAR writer on the first
// Read call.
type deferredBody struct {
	mrt    *MockRoundTripper
	remote MockRoundTripRemote
	root   cid.Cid

	r    io.ReadCloser
	once sync.Once
}

func newDeferredBody(mrt *MockRoundTripper, remote MockRoundTripRemote, root cid.Cid) *deferredBody {
	return &deferredBody{
		mrt:    mrt,
		remote: remote,
		root:   root,
	}
}

var _ io.ReadCloser = (*deferredBody)(nil)

func (d *deferredBody) makeBody() io.ReadCloser {
	carR, carW := io.Pipe()
	req := require.New(d.mrt.t)

	sel, err := selector.CompileSelector(d.remote.Selector)
	req.NoError(err)

	go func() {
		stats := RemoteStats{
			Peer:   d.remote.Peer.ID,
			Root:   d.root,
			Blocks: make([]cid.Cid, 0),
		}

		defer func() {
			d.mrt.statsCh <- stats
			req.NoError(carW.Close())
		}()

		if d.remote.Malformed {
			carW.Write([]byte("nope, this is not what you're looking for"))
			return
		}

		// instantiating this writes a CARv1 header and waits for more Put()s
		carWriter, err := storage.NewWritable(carW, []cid.Cid{d.root}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
		req.NoError(err)

		// intercept the StorageReadOpener of the LinkSystem so that for each
		// read that the traverser performs, we take that block and Put() it
		// to the CARv1 writer.
		lsys := d.remote.LinkSystem
		originalSRO := lsys.StorageReadOpener
		lsys.StorageReadOpener = func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
			r, err := originalSRO(lc, lnk)
			if err != nil {
				return nil, err
			}
			byts, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			err = carWriter.Put(d.mrt.ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
			req.NoError(err)
			stats.Blocks = append(stats.Blocks, lnk.(cidlink.Link).Cid)
			stats.ByteCount += uint64(len(byts)) // only the length of the bytes, not the rest of the CAR infrastructure

			// ensure there is blockDuration between each block send
			sendAt := d.remote.RespondAt.Add(d.mrt.remoteBlockDuration * time.Duration(len(stats.Blocks)))
			if d.mrt.clock.Until(sendAt) > 0 {
				select {
				case <-d.mrt.ctx.Done():
					return nil, d.mrt.ctx.Err()
				case <-d.mrt.clock.After(d.mrt.clock.Until(sendAt)):
					time.Sleep(1 * time.Millisecond) // let em goroutines breathe
				}
			}
			return bytes.NewReader(byts), nil
		}

		// load and register the root link so it's pushed to the CAR since
		// the traverser won't load it (we feed the traverser the rood _node_
		// not the link)
		var proto datamodel.NodePrototype = basicnode.Prototype.Any
		if d.root.Prefix().Codec == cid.DagProtobuf {
			proto = dagpb.Type.PBNode
		}
		rootNode, err := lsys.Load(linking.LinkContext{Ctx: d.mrt.ctx}, cidlink.Link{Cid: d.root}, proto)
		if err != nil {
			stats.Err = struct{}{}
		} else {
			// begin traversal
			err := traversal.Progress{
				Cfg: &traversal.Config{
					Ctx:                            d.mrt.ctx,
					LinkSystem:                     lsys,
					LinkTargetNodePrototypeChooser: dagpb.AddSupportToChooser(basicnode.Chooser),
				},
			}.WalkAdv(rootNode, sel, func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil })
			if err != nil {
				stats.Err = struct{}{}
			}
		}
	}()

	return carR
}

func (d *deferredBody) Read(p []byte) (n int, err error) {
	d.once.Do(func() {
		d.r = d.makeBody()
	})
	n, err = d.r.Read(p)
	if err == io.EOF {
		d.mrt.endsCh <- d.remote.Peer.ID
	}
	return n, err
}

func (d *deferredBody) Close() error {
	if d.r != nil {
		return d.r.Close()
	}
	return nil
}
