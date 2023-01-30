package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("lassie/httpserver")

type HttpServer struct {
	cancel   context.CancelFunc
	ctx      context.Context
	listener net.Listener
	server   *http.Server
}

func NewHttpServer(ctx context.Context, address string, port uint) (*HttpServer, error) {
	addr := fmt.Sprintf("%s:%d", address, port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()

	ctx, cancel := context.WithCancel(ctx)
	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     mux,
	}

	httpServer := &HttpServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
	}

	// Routes
	mux.HandleFunc("/ping", pingHandler)
	mux.HandleFunc("/ipfs/", ipfsHandler)

	return httpServer, nil
}

func (s HttpServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *HttpServer) Start() error {
	log.Infow("starting http server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		log.Errorw("failed to start http server", "err", err)
		return err
	}

	return nil
}

func (s *HttpServer) Close() error {
	log.Info("closing http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}
