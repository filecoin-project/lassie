package mockindexer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type MockIndexer struct {
	returnedValues map[string][]model.ProviderResult
	cancel         context.CancelFunc
	ctx            context.Context
	listener       net.Listener
	server         *http.Server
	clock          clock.Clock
	connectCh      chan<- string
}

func NewMockIndexer(
	ctx context.Context,
	address string,
	port uint64,
	cidProviders map[cid.Cid][]model.ProviderResult,
	clock clock.Clock,
	connectCh chan<- string,
) (*MockIndexer, error) {

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
		connectCh:      connectCh,
	}

	// Routes
	mux.HandleFunc("/multihash/", httpServer.handleMultihash)
	mux.HandleFunc("/routing/v1/providers/", httpServer.handleDelegatedRouting)

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
	if s.connectCh != nil {
		s.connectCh <- req.URL.String()
	}

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
	for _, acceptType := range strings.Split(req.Header.Get("Accept"), ",") {
		if acceptType == "application/x-ndjson" {
			isNDJson = true
			break
		}
	}

	encoder := json.NewEncoder(res)

	if !isNDJson {
		findResp := model.FindResponse{
			MultihashResults: []model.MultihashResult{
				{
					Multihash:       mh,
					ProviderResults: returnResults,
				},
			},
		}
		if err := encoder.Encode(findResp); err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	for _, result := range returnResults {
		if err := encoder.Encode(result); err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := res.Write([]byte("\n")); err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
		s.clock.Sleep(1 * time.Second)
	}
}

func (s *MockIndexer) handleDelegatedRouting(res http.ResponseWriter, req *http.Request) {
	if s.connectCh != nil {
		s.connectCh <- req.URL.String()
	}

	// Parse path: /routing/v1/providers/{cid}
	urlPath := strings.Split(req.URL.Path, "/")

	// Expected: ["", "routing", "v1", "providers", "{cid}"]
	if len(urlPath) < 5 {
		res.WriteHeader(http.StatusNotFound)
		return
	}

	// Extract CID from path
	cidStr := urlPath[4]
	c, err := cid.Decode(cidStr)
	if err != nil {
		http.Error(res, "Failed to parse CID parameter", http.StatusBadRequest)
		return
	}

	// Look up providers by multihash
	returnResults := s.returnedValues[string(c.Hash())]
	if len(returnResults) == 0 {
		http.NotFound(res, req)
		return
	}

	// Convert to delegated routing format
	var providers []indexerlookup.DelegatedProvider
	for _, result := range returnResults {
		// Decode metadata to get protocols
		md := metadata.Default.New()
		if err := md.UnmarshalBinary(result.Metadata); err != nil {
			continue // Skip providers with bad metadata
		}

		// Build provider record
		provider := indexerlookup.DelegatedProvider{
			Schema:    "peer",
			ID:        result.Provider.ID.String(),
			Addrs:     make([]string, 0, len(result.Provider.Addrs)),
			Protocols: make([]string, 0),
			Metadata:  make(map[string]interface{}),
		}

		// Convert multiaddrs to strings
		for _, addr := range result.Provider.Addrs {
			provider.Addrs = append(provider.Addrs, addr.String())
		}

		// Convert protocols
		for _, protoCode := range md.Protocols() {
			var protoName string
			switch protoCode {
			case multicodec.TransportGraphsyncFilecoinv1:
				protoName = "transport-graphsync-filecoinv1"
				// Get the protocol-specific metadata
				if proto := md.Get(protoCode); proto != nil {
					if gs, ok := proto.(*metadata.GraphsyncFilecoinV1); ok {
						// Encode as base64 for testing compatibility
						if metadataBytes, err := proto.MarshalBinary(); err == nil {
							// Remove the varint protocol ID prefix for the base64 encoding
							// The metadata field should just contain the CBOR payload
							provider.Metadata[protoName] = base64.StdEncoding.EncodeToString(metadataBytes)
						} else {
							// Fallback: provide as JSON object
							provider.Metadata[protoName] = map[string]interface{}{
								"PieceCID":      gs.PieceCID.String(),
								"VerifiedDeal":  gs.VerifiedDeal,
								"FastRetrieval": gs.FastRetrieval,
							}
						}
					}
				}
			case multicodec.TransportIpfsGatewayHttp:
				protoName = "transport-ipfs-gateway-http"
				provider.Metadata[protoName] = "" // No additional metadata
			default:
				continue // Skip unknown protocols
			}
			provider.Protocols = append(provider.Protocols, protoName)
		}

		providers = append(providers, provider)
	}

	// Build response
	response := indexerlookup.DelegatedRoutingResponse{
		Providers: providers,
	}

	// Send JSON response
	res.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(res)
	if err := encoder.Encode(response); err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
	}
}
