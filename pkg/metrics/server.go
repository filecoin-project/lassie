package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"

	// expose default metrics
	_ "net/http/pprof"
)

// MetricsServer simply exposes prometheus and pprof metrics via HTTP
type MetricsServer struct {
	cancel   context.CancelFunc
	ctx      context.Context
	listener net.Listener
	server   *http.Server
}

// NewHttpServer creates a new HttpServer
func NewHttpServer(ctx context.Context, address string, port uint) (*MetricsServer, error) {
	addr := fmt.Sprintf("%s:%d", address, port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// create server
	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     http.DefaultServeMux,
	}

	metricsServer := &MetricsServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
	}

	// Routes
	http.Handle("/metrics", NewExporter())

	return metricsServer, nil
}

// Addr returns the listening address of the server
func (s MetricsServer) Addr() string {
	return s.listener.Addr().String()
}

// Start starts the metrics http server, returning an error if the server failed to start
func (s *MetricsServer) Start() error {
	logger.Infow("starting metrics server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		logger.Errorw("failed to start metrics server", "err", err)
		return err
	}

	return nil
}

// Close shutsdown the server and cancels the server context
func (s *MetricsServer) Close() error {
	logger.Info("closing http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}
