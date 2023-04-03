package mockindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

type MockIndexer struct {
	returnedValues map[string][]model.ProviderResult
	cancel         context.CancelFunc
	ctx            context.Context
	listener       net.Listener
	server         *http.Server
	clock          clock.Clock
}

func NewMockIndexer(ctx context.Context, address string, port uint64, cidProviders map[cid.Cid][]model.ProviderResult, clock clock.Clock) (*MockIndexer, error) {
	addr := fmt.Sprintf("%s:%d", address, port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// create server
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     mux,
	}

	returnedValues := make(map[string][]model.ProviderResult, len(cidProviders))
	for c, providers := range cidProviders {
		returnedValues[string(c.Hash())] = providers
	}
	httpServer := &MockIndexer{
		cancel:         cancel,
		ctx:            ctx,
		listener:       listener,
		server:         server,
		returnedValues: returnedValues,
		clock:          clock,
	}

	// Routes
	mux.HandleFunc("/multihash/", httpServer.handleMultihash)

	return httpServer, nil
}

// Addr returns the listening address of the server
func (s *MockIndexer) Addr() string {
	return s.listener.Addr().String()
}

// Start starts the http server, returning an error if the server failed to start
func (s *MockIndexer) Start() error {
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Close shutsdown the server and cancels the server context
func (s *MockIndexer) Close() error {
	s.cancel()
	return s.server.Shutdown(context.Background())
}

func (s *MockIndexer) handleMultihash(res http.ResponseWriter, req *http.Request) {
	urlPath := strings.Split(req.URL.Path, "/")[1:]

	// check if CID path param is missing
	if len(urlPath) < 2 {
		// not a valid path to hit
		res.WriteHeader(http.StatusNotFound)
		return
	}

	// validate CID path parameter
	mhStr := urlPath[1]
	mh, err := multihash.FromB58String(mhStr)
	if err != nil {
		http.Error(res, "Failed to parse multihash parameter", http.StatusInternalServerError)
		return
	}

	returnResults := s.returnedValues[string(mh)]
	if len(returnResults) == 0 {
		http.NotFound(res, req)
		return
	}

	// check if Accept header includes applica
	isNDJson := false
	hasAccept := req.Header.Get("Accept") != ""
	acceptTypes := strings.Split(req.Header.Get("Accept"), ",")
	if hasAccept {
		for _, acceptType := range acceptTypes {
			if acceptType == "application/x-ndjson" {
				isNDJson = true
				break
			}
		}
	}

	if !isNDJson {

		err := json.NewEncoder(res).Encode(model.FindResponse{
			MultihashResults: []model.MultihashResult{
				{
					Multihash:       mh,
					ProviderResults: returnResults,
				},
			},
		})
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	encoder := json.NewEncoder(res)
	for _, result := range returnResults {
		encoder.Encode(result)
		res.Write([]byte("\n"))
		s.clock.Sleep(1 * time.Second)
	}
}
