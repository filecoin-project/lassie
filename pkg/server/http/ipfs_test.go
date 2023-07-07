package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/mockfetcher"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIpfsHandler(t *testing.T) {
	tests := []struct {
		name             string
		fetchFunc        func(ctx context.Context, request types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error)
		httpServerConfig *HttpServerConfig
		method           string
		path             string
		headers          map[string]string
		wantStatus       int
		wantHeaders      map[string]string
		wantBody         string
	}{
		{
			name:        "405 on non-GET requests",
			method:      "HEAD",
			path:        "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			wantStatus:  http.StatusMethodNotAllowed,
			wantHeaders: map[string]string{"Allow": "GET"},
			wantBody:    "method not allowed\n",
		},
		{
			name:       "404 on missing CID path parameter",
			method:     "GET",
			path:       "/ipfs/",
			wantStatus: http.StatusNotFound,
			wantBody:   "not found\n",
		},
		{
			name:       "400 on invalid Accept header - mime type",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers:    map[string]string{"Accept": "application/json"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "no acceptable content type\n",
		},
		{
			name:       "400 on invalid Accept header - bad dups",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car;dups=invalid"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "no acceptable content type\n",
		},
		{
			name:       "400 on invalid Accept header - bad version",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car;version=2"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "no acceptable content type\n",
		},
		{
			name:       "400 on invalid Accept header - bad order",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car;order=invalid"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "no acceptable content type\n",
		},
		{
			name:       "400 on invalid format query param",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?format=invalid",
			wantStatus: http.StatusBadRequest,
			wantBody:   "requested non-supported format invalid\n",
		},
		{
			name:       "400 on missing Accept header and format query param",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			wantStatus: http.StatusBadRequest,
			wantBody:   "neither a valid accept header or format parameter were provided\n",
		},
		{
			name:       "400 on missing extension in filename query param",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?filename=birb",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "filename missing extension\n",
		},
		{
			name:       "400 on non-supported extension in filename query param",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?filename=birb.tar",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "filename uses non-supported extension .tar\n",
		},
		{
			name:       "500 when we fail to parse the CID path param",
			method:     "GET",
			path:       "/ipfs/bafyfoo",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusInternalServerError,
			wantBody:   "failed to parse CID path parameter\n",
		},
		{
			name:       "400 on invalid dag-scope query parameter",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?dag-scope=invalid",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "invalid dag-scope parameter\n",
		},
		{
			name:       "400 on unrecognized protocols query parameter value",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?protocols=invalid",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "unrecognized protocol: invalid\n",
		},
		{
			name:       "400 on invalid providers query parameter value",
			method:     "GET",
			path:       "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?providers=invalid",
			headers:    map[string]string{"Accept": "application/vnd.ipld.car"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "invalid providers parameter\n",
		},
		{
			name:    "404 when no candidates can be found",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				return nil, retriever.ErrNoCandidates
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "no candidates found\n",
		},
		{
			name:    "504 for any retrieval error other than ErrNoCandidates",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				return nil, errors.New("fetch error")
			},
			wantStatus: http.StatusGatewayTimeout,
			wantBody:   "failed to fetch CID: fetch error\n",
		},
		{
			name:    "retrieval request has valid cid, path, scope, and max blocks",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4/birb.mp4?dag-scope=entity",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				require.Equal(t, "bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4", r.Cid.String())
				require.Equal(t, "birb.mp4", r.Path)
				require.Equal(t, types.DagScopeEntity, r.Scope)
				require.Equal(t, uint64(0), r.MaxBlocks)
				return &types.RetrievalStats{}, nil
			},
		},
		{
			name:    "retrieval request MaxBlocks is set to provided blockLimit query parameter",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?blockLimit=1000",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				require.Equal(t, uint64(1000), r.MaxBlocks)
				return &types.RetrievalStats{}, nil
			},
		},
		{
			name:    "retrieval request MaxBlocks is set to provided MaxBlocksPerRequest http server config value",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			httpServerConfig: &HttpServerConfig{
				MaxBlocksPerRequest: 1000,
			},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				require.Equal(t, uint64(1000), r.MaxBlocks)
				return &types.RetrievalStats{}, nil
			},
		},
		{
			name:    "retrieval request MaxBlocks is set to lowest non-zero value - blockLimit query parameter takes precedence",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?blockLimit=1000",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			httpServerConfig: &HttpServerConfig{
				MaxBlocksPerRequest: 2000,
			},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				require.Equal(t, uint64(1000), r.MaxBlocks)
				return &types.RetrievalStats{}, nil
			},
		},
		{
			name:    "retrieval request MaxBlocks is set to lowest non-zero value - MaxBlocksPerRequest http server config value takes precedence",
			method:  "GET",
			path:    "/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?blockLimit=2000",
			headers: map[string]string{"Accept": "application/vnd.ipld.car"},
			httpServerConfig: &HttpServerConfig{
				MaxBlocksPerRequest: 1000,
			},
			fetchFunc: func(ctx context.Context, r types.RetrievalRequest, cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				require.Equal(t, uint64(1000), r.MaxBlocks)
				return &types.RetrievalStats{}, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetcher := mockfetcher.NewMockFetcher()
			if tt.fetchFunc != nil {
				fetcher.FetchFunc = tt.fetchFunc
			}
			var cfg *HttpServerConfig = &HttpServerConfig{}
			if tt.httpServerConfig != nil {
				cfg = tt.httpServerConfig
			}
			handler := ipfsHandler(fetcher, *cfg)

			req, err := http.NewRequest(tt.method, tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rr := httptest.NewRecorder()
			http.HandlerFunc(handler).ServeHTTP(rr, req)

			if status := rr.Code; tt.wantStatus != 0 && status != tt.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tt.wantStatus)
			}

			for k, v := range tt.wantHeaders {
				if rr.Header().Get(k) != v {
					t.Errorf("handler returned wrong header: got %v want %v",
						rr.Header().Get(k), v)
				}
			}

			if tt.wantBody != "" && rr.Body.String() != tt.wantBody {
				t.Errorf("handler returned unexpected body: got %v want %v",
					rr.Body.String(), tt.wantBody)
			}
		})
	}
}
