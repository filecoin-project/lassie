package retriever

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestHTTPRetriever(t *testing.T) {
	t.Skip()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := require.New(t)
	clock := clock.NewMock()

	retrievalID := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

	// Create a new instance of the httptest.Server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello, World!"))
	}))
	defer server.Close()

	getTimeout := func(_ peer.ID) time.Duration { return 1 * time.Second }

	retriever := NewHttpRetriever(getTimeout, server.Client())
	retriever.(*parallelPeerRetriever).Clock = clock

	request := types.RetrievalRequest{
		Cid:         cid1,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
	}

	eventCb := func(events types.RetrievalEvent) {
		// TODO: check these
	}

	retrieval := retriever.Retrieve(ctx, request, eventCb)
	stats, err := retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
		types.NewRetrievalCandidate(peer.ID("foo"), cid.Undef, &metadata.IpfsGatewayHttp{}),
	}))
	req.NoError(err)
	req.NotNil(stats)
	req.Equal(peer.ID("foo"), stats.StorageProviderId)
	req.Equal(cid1, stats.RootCid)
}
