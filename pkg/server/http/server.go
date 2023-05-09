package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/filecoin-project/lassie/pkg/lassie"
	logging "github.com/ipfs/go-log/v2"
	servertiming "github.com/mitchellh/go-server-timing"
)

var log = logging.Logger("lassie/httpserver")

// HttpServer is a Lassie server for fetching data from the network via HTTP
type HttpServer struct {
	cancel   context.CancelFunc
	ctx      context.Context
	listener net.Listener
	server   *http.Server
}

type HttpServerConfig struct {
	Address             string
	Port                uint
	TempDir             string
	MaxBlocksPerRequest uint64
}

// NewHttpServer creates a new HttpServer
func NewHttpServer(ctx context.Context, lassie *lassie.Lassie, cfg HttpServerConfig) (*HttpServer, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// create server
	mux := http.NewServeMux()
	handler := servertiming.Middleware(mux, nil)
	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     handler,
	}

	httpServer := &HttpServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
	}

	// Routes
	mux.HandleFunc("/ipfs/", ipfsHandler(lassie, cfg))

	return httpServer, nil
}

// Addr returns the listening address of the server
func (s HttpServer) Addr() string {
	return s.listener.Addr().String()
}

// Start starts the http server, returning an error if the server failed to start
func (s *HttpServer) Start() error {
	log.Infow("starting http server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		log.Errorw("failed to start http server", "err", err)
		return err
	}

	return nil
}

// Close shutsdown the server and cancels the server context
func (s *HttpServer) Close() error {
	log.Info("closing http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}
