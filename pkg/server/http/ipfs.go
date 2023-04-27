package httpserver

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	lassie "github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	servertiming "github.com/mitchellh/go-server-timing"
	"github.com/multiformats/go-multicodec"
)

func ipfsHandler(lassie *lassie.Lassie, cfg HttpServerConfig) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		statusLogger := newStatusLogger(req.Method, req.URL.Path)

		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// filter out everything but GET requests
		switch req.Method {
		case http.MethodGet:
			break
		default:
			statusLogger.logStatus(http.StatusMethodNotAllowed, "Method not allowed")
			res.Header().Add("Allow", http.MethodGet)
			res.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// check if CID path param is missing
		if len(urlPath) < 2 {
			// not a valid path to hit
			statusLogger.logStatus(http.StatusNotFound, "Not found")
			res.WriteHeader(http.StatusNotFound)
			return
		}

		// check if Accept header includes application/vnd.ipld.car
		hasAccept := req.Header.Get("Accept") != ""
		acceptTypes := strings.Split(req.Header.Get("Accept"), ",")
		validAccept := false
		for _, acceptType := range acceptTypes {
			typeParts := strings.Split(acceptType, ";")
			if typeParts[0] == "*/*" || typeParts[0] == "application/*" || typeParts[0] == "application/vnd.ipld.car" {
				validAccept = true
				break
			}
		}
		if hasAccept && !validAccept {
			statusLogger.logStatus(http.StatusBadRequest, "No acceptable content type")
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if format is car
		hasFormat := req.URL.Query().Has("format")
		if hasFormat && req.URL.Query().Get("format") != "car" {
			statusLogger.logStatus(http.StatusBadRequest, fmt.Sprintf("Requested non-supported format %s", req.URL.Query().Get("format")))
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// if neither are provided return
		// one of them has to be given with a CAR type since we only return CAR data
		if !validAccept && !hasFormat {
			statusLogger.logStatus(http.StatusBadRequest, "Neither a valid accept header or format parameter were provided")
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if provided filename query parameter has .car extension
		if req.URL.Query().Has("filename") {
			filename := req.URL.Query().Get("filename")
			ext := filepath.Ext(filename)
			if ext == "" {
				statusLogger.logStatus(http.StatusBadRequest, "Filename missing extension")
				res.WriteHeader(http.StatusBadRequest)
				return
			}
			if ext != ".car" {
				statusLogger.logStatus(http.StatusBadRequest, fmt.Sprintf("Filename uses non-supported extension %s", ext))
				res.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			statusLogger.logStatus(http.StatusInternalServerError, "Failed to parse CID path parameter")
			http.Error(res, "Failed to parse CID path parameter", http.StatusInternalServerError)
			return
		}

		// Grab unixfs path if it exists
		unixfsPath := ""
		if len(urlPath) > 2 {
			unixfsPath = "/" + strings.Join(urlPath[2:], "/")
		}

		carScope := types.CarScopeAll
		if req.URL.Query().Has("car-scope") {
			switch req.URL.Query().Get("car-scope") {
			case "all":
			case "file":
				carScope = types.CarScopeFile
			case "block":
				carScope = types.CarScopeBlock
			default:
				statusLogger.logStatus(http.StatusBadRequest, "Invalid car-scope parameter")
				res.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		var protocols []multicodec.Code
		if req.URL.Query().Has("protocols") {
			var err error
			protocols, err = types.ParseProtocolsString(req.URL.Query().Get("protocols"))
			if err != nil {
				statusLogger.logStatus(http.StatusBadRequest, "Invalid protocols parameter")
				res.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		var fixedPeers []peer.AddrInfo
		if req.URL.Query().Has("providers") {
			var err error
			fixedPeers, err = types.ParseProviderStrings(req.URL.Query().Get("providers"))
			if err != nil {
				statusLogger.logStatus(http.StatusBadRequest, "Invalid providers parameter")
				res.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// for setting Content-Disposition header based on filename url parameter
		var filename string
		if req.URL.Query().Has("filename") {
			filename = req.URL.Query().Get("filename")
		} else {
			filename = fmt.Sprintf("%s.car", rootCid.String())
		}

		retrievalId, err := types.NewRetrievalID()
		if err != nil {
			msg := fmt.Sprintf("Failed to generate retrieval ID: %s", err.Error())
			statusLogger.logStatus(http.StatusInternalServerError, msg)
			http.Error(res, msg, http.StatusInternalServerError)
			return
		}

		// TODO: we should propogate this value throughout logs so
		// that we can correlate specific requests to related logs.
		// For now just using to log the corrolation and return the
		// X-Trace-Id header.
		requestId := req.Header.Get("X-Request-Id")
		if requestId == "" {
			requestId = retrievalId.String()
		} else {
			logger.Debugw("Corrolating provided request ID with retrieval ID", "request_id", requestId, "retrieval_id", retrievalId)
		}

		// bytesWritten will be closed once we've started writing CAR content to
		// the response writer. Once closed, no other content should be written.
		bytesWritten := make(chan struct{}, 1)

		carWriter := storage.NewDeferredCarWriterForStream(rootCid, res)
		carStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), cfg.TempDir)
		defer func() {
			if err := carStore.Close(); err != nil {
				logger.Errorf("error closing temp store: %s", err)
			}
		}()
		var store types.ReadableWritableStorage = carStore

		carWriter.OnPut(func(int) {
			// called once we start writing blocks into the CAR (on the first Put())
			res.Header().Set("Content-Disposition", "attachment; filename="+filename)
			res.Header().Set("Accept-Ranges", "none")
			res.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
			res.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
			res.Header().Set("Etag", fmt.Sprintf("%s.car", rootCid.String()))
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", req.URL.Path)
			// TODO: set X-Ipfs-Roots header when we support root+path
			// see https://github.com/ipfs/kubo/pull/8720
			res.Header().Set("X-Trace-Id", requestId)
			statusLogger.logStatus(200, "OK")
			close(bytesWritten)
		}, true)

		request, err := types.NewRequestForPath(store, rootCid, unixfsPath, carScope)
		request.Protocols = protocols
		request.FixedPeers = fixedPeers
		if err != nil {
			msg := fmt.Sprintf("Failed to create request: %s", err.Error())
			statusLogger.logStatus(http.StatusInternalServerError, msg)
			http.Error(res, msg, http.StatusInternalServerError)
			return
		}
		request.RetrievalID = retrievalId
		// setup preload storage for bitswap, the temporary CAR store can set up a
		// separate preload space in its storage
		request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
		preloadStore := carStore.PreloadStore()
		request.PreloadLinkSystem.SetReadStorage(preloadStore)
		request.PreloadLinkSystem.SetWriteStorage(preloadStore)
		request.PreloadLinkSystem.TrustedStorage = true

		// extract block limit from query param as needed
		var blockLimit uint64
		if req.URL.Query().Has("blockLimit") {
			if parsedBlockLimit, err := strconv.ParseUint(req.URL.Query().Get("blockLimit"), 10, 64); err == nil {
				blockLimit = parsedBlockLimit
			}
		}
		if cfg.MaxBlocksPerRequest > 0 || blockLimit > 0 {
			// use the lowest non-zero value for block limit
			if blockLimit == 0 || (cfg.MaxBlocksPerRequest > 0 && blockLimit > cfg.MaxBlocksPerRequest) {
				blockLimit = cfg.MaxBlocksPerRequest
			}
			request.MaxBlocks = blockLimit
		}

		logger.Debugw("fetching CID", "retrievalId", retrievalId, "CID", rootCid.String(), "path", unixfsPath, "carScope", carScope)
		stats, err := lassie.Fetch(req.Context(), request, func(re types.RetrievalEvent) {
			header := servertiming.FromContext(req.Context())
			if header == nil {
				return
			}

			if header.Metrics != nil {
				for _, m := range header.Metrics {
					if m.Name == string(re.Phase()) {
						if m.Extra == nil {
							m.Extra = map[string]string{}
						}
						m.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(re.PhaseStartTime()))
						return
					}
				}
			}

			metric := header.NewMetric(string(re.Phase()))
			metric.Duration = re.Time().Sub(re.PhaseStartTime())
		})
		if err != nil {
			select {
			case <-bytesWritten:
				return
			default:
			}
			if errors.Is(err, retriever.ErrNoCandidates) {
				msg := "No candidates found"
				statusLogger.logStatus(http.StatusNotFound, msg)
				http.Error(res, msg, http.StatusNotFound)
			} else {
				msg := fmt.Sprintf("Failed to fetch CID: %s", err.Error())
				statusLogger.logStatus(http.StatusGatewayTimeout, msg)
				http.Error(res, msg, http.StatusGatewayTimeout)
			}

			return
		}
		logger.Debugw("successfully fetched CID",
			"retrievalId", retrievalId,
			"CID", rootCid,
			"duration", stats.Duration,
			"bytes", stats.Size,
		)
	}
}

// statusLogger is a logger for logging response statuses for a given request
type statusLogger struct {
	method string
	path   string
}

func newStatusLogger(method string, path string) *statusLogger {
	return &statusLogger{method, path}
}

// logStatus logs the method, path, status code and message
func (l statusLogger) logStatus(statusCode int, message string) {
	logger.Infof("%s\t%s\t%d: %s\n", l.method, l.path, statusCode, message)
}
