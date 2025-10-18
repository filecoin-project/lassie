package httpserver

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/lassie"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

// This test won't reliably fail but will be flaky if the handler performs any
// writes after the request context has been cancelled.
// > while (go test -run TestHttpClientClose -count 50) do :; done

func TestHttpClientClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	mrn := mocknet.NewMockRetrievalNet(ctx, t)
	mrn.AddBitswapPeers(1)
	require.NoError(t, mrn.MN.LinkAll())

	srcData := unixfs.GenerateFile(t, mrn.Remotes[0].LinkSystem, rndReader, 20<<20)

	// Setup a new lassie
	req := require.New(t)
	lassie, err := lassie.NewLassie(
		ctx,
		lassie.WithProviderTimeout(20*time.Second),
		lassie.WithHost(mrn.Self),
		lassie.WithCandidateSource(mrn.Source),
		lassie.WithProtocols([]multicodec.Code{multicodec.TransportBitswap}),
	)
	req.NoError(err)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	handler := IpfsHandler(lassie, HttpServerConfig{TempDir: t.TempDir()})
	response := &responseWriter{t: t, header: http.Header{}, ctx: reqCtx, cancelFn: reqCancel, fatalCh: make(chan struct{})}
	addr := fmt.Sprintf("http://%s/ipfs/%s%s", "127.0.0.1", srcData.Root.String(), "")
	request, err := http.NewRequestWithContext(reqCtx, "GET", addr, nil)
	req.NoError(err)
	request.Header.Add("Accept", "application/vnd.ipld.car")
	handler(response, request)
	close(response.fatalCh)
}

var _ http.ResponseWriter = (*responseWriter)(nil)

type responseWriter struct {
	t          *testing.T
	wroteCount int
	header     http.Header
	ctx        context.Context
	cancelFn   context.CancelFunc
	fatalCh    chan struct{}
}

func (rw *responseWriter) Header() http.Header {
	return rw.header
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	// check if rw.endedCh is closed and t.Fatal if it is
	select {
	case <-rw.fatalCh:
		rw.t.Fatal("unexpected Write() call")
		return 0, io.ErrClosedPipe
	default:
	}

	rw.wroteCount += len(b)
	if rw.wroteCount > 1<<20 {
		rw.cancelFn()
	}
	return len(b), nil
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	rw.t.Fatalf("unexpected WriteHeader() call, code: %d", statusCode)
}
