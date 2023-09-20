package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/ipfs/go-log/v2"
	servertiming "github.com/mitchellh/go-server-timing"
)

var logger = log.Logger("lassie/httpserver")

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
	AccessToken         string
}

type contextKey struct {
	key string
}

var connContextKey = &contextKey{"http-conn"}

func saveConnInCTX(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, connContextKey, c)
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

	if cfg.AccessToken != "" {
		handler = authorizationMiddleware(handler, cfg.AccessToken)
	}

	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     handler,
		ConnContext: saveConnInCTX,
	}

	httpServer := &HttpServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
	}

	// Routes
	mux.HandleFunc("/ipfs/", IpfsHandler(lassie, cfg))

	// Handle pprof endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return httpServer, nil
}

// Addr returns the listening address of the server
func (s HttpServer) Addr() string {
	return s.listener.Addr().String()
}

// Start starts the http server, returning an error if the server failed to start
func (s *HttpServer) Start() error {
	logger.Infow("starting http server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		logger.Errorw("failed to start http server", "err", err)
		return err
	}
	return nil
}

// Close shuts down the server and cancels the server context
func (s *HttpServer) Close() error {
	logger.Info("closing http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}

func authorizationMiddleware(next http.Handler, accessToken string) http.Handler {
	requiredHeaderValue := fmt.Sprintf("Bearer %s", accessToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == requiredHeaderValue {
			next.ServeHTTP(w, r)
			return
		}

		// Unauthorized
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, "Unauthorized")
	})
}
