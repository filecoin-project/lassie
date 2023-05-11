package testpeer

import (
	"context"
	"fmt"
	"net"
	"net/http"

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
