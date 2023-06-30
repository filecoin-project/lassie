package testpeer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	servertiming "github.com/mitchellh/go-server-timing"
)

type TestPeerHttpServer struct {
	cancel   context.CancelFunc
	ctx      context.Context
	listener net.Listener
	server   *http.Server
	Mux      *http.ServeMux
}

// NewTestPeerHttpServer creates a new HttpServer
func NewTestPeerHttpServer(ctx context.Context, host string, port string) (*TestPeerHttpServer, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// create server
	mux := http.NewServeMux()
	handler := servertiming.Middleware(mux, nil)
	server := &http.Server{
		Addr:        addr,
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     handler,
	}

	httpServer := &TestPeerHttpServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
		Mux:      mux,
	}

	return httpServer, nil
}

// Start starts the http server, returning an error if the server failed to start
func (s *TestPeerHttpServer) Start() error {
	logger.Infow("starting test peer http server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		logger.Errorw("failed to start test peer http server", "err", err)
		return err
	}

	return nil
}

// Close shutsdown the server and cancels the server context
func (s *TestPeerHttpServer) Close() error {
	logger.Info("closing test peer http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}

func MockIpfsHandler(ctx context.Context, lsys linking.LinkSystem) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to parse CID path parameter: %s", cidStr), http.StatusBadRequest)
			return
		}

		// Grab unixfs path if it exists
		unixfsPath := ""
		if len(urlPath) > 2 {
			unixfsPath = "/" + strings.Join(urlPath[2:], "/")
		}

		acceptTypes := strings.Split(req.Header.Get("Accept"), ",")
		includeDupes := false
		for _, acceptType := range acceptTypes {
			typeParts := strings.Split(acceptType, ";")
			if typeParts[0] == "application/vnd.ipld.car" {
				for _, nextPart := range typeParts[1:] {
					pair := strings.Split(nextPart, "=")
					if len(pair) == 2 {
						attr := strings.TrimSpace(pair[0])
						value := strings.TrimSpace(pair[1])
						if attr == "dups" && value == "y" {
							includeDupes = true
						}
					}
				}
			}
		}

		// We're always providing the dag-scope parameter, so add a failure case if we stop
		// providing it in the future
		if !req.URL.Query().Has("dag-scope") {
			http.Error(res, "Missing dag-scope parameter", http.StatusBadRequest)
			return
		}

		// Parse car scope and use it to get selector
		var dagScope types.DagScope
		switch req.URL.Query().Get("dag-scope") {
		case "all":
			dagScope = types.DagScopeAll
		case "entity":
			dagScope = types.DagScopeEntity
		case "block":
			dagScope = types.DagScopeBlock
		default:
			http.Error(res, fmt.Sprintf("Invalid dag-scope parameter: %s", req.URL.Query().Get("dag-scope")), http.StatusBadRequest)
			return
		}
		var byteRange *types.ByteRange
		if req.URL.Query().Get("entity-bytes") != "" {
			br, err := types.ParseByteRange(req.URL.Query().Get("entity-bytes"))
			if err != nil {
				http.Error(res, fmt.Sprintf("Invalid entity-bytes parameter: %s", req.URL.Query().Get("entity-bytes")), http.StatusBadRequest)
				return
			}
			byteRange = &br
		}

		sel, err := selector.CompileSelector(types.PathScopeSelector(unixfsPath, dagScope, byteRange))
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to compile selector from dag-scope: %v", err), http.StatusInternalServerError)
			return
		}

		// Write to response writer
		carWriter, err := storage.NewWritable(res, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(includeDupes))
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to create car writer: %v", err), http.StatusInternalServerError)
			return
		}

		// Extend the StorageReadOpener func to write to the carWriter
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
			err = carWriter.Put(ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
			if err != nil {
				return nil, err
			}

			return bytes.NewReader(byts), nil
		}

		protoChooser := dagpb.AddSupportToChooser(basicnode.Chooser)
		lnk := cidlink.Link{Cid: rootCid}
		lnkCtx := linking.LinkContext{}
		proto, err := protoChooser(lnk, lnkCtx)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to choose prototype node: %s", cidStr), http.StatusBadRequest)
			return
		}

		rootNode, err := lsys.Load(lnkCtx, lnk, proto)
		if err != nil {
			http.Error(res, fmt.Sprintf("Failed to load root cid into link system: %v", err), http.StatusInternalServerError)
			return
		}

		cfg := &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
		}
		progress := traversal.Progress{Cfg: cfg}

		_ = progress.WalkMatching(rootNode, sel, unixfsnode.BytesConsumingMatcher)
		// if we loaded the first block, we can't write headers any more so don't bother
	}
}
