package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

const (
	MimeTypeCar                = "application/vnd.ipld.car"            // The only accepted MIME type
	MimeTypeCarVersion         = "1"                                   // We only accept version 1 of the MIME type
	FormatParameterCar         = "car"                                 // The only valid format parameter value
	FilenameExtCar             = ".car"                                // The only valid filename extension
	DefaultIncludeDupes        = true                                  // The default value for an unspecified "dups" parameter. See https://github.com/ipfs/specs/pull/412.
	ResponseAcceptRangesHeader = "none"                                // We currently don't accept range requests
	ResponseCacheControlHeader = "public, max-age=29030400, immutable" // Magic cache control values
)

var (
	ResponseChunkDelimeter    = []byte("0\r\n") // An http/1.1 chunk delimeter, used for specifying an early end to the response
	ResponseContentTypeHeader = fmt.Sprintf("%s; version=%s", MimeTypeCar, MimeTypeCarVersion)
)

func ipfsHandler(fetcher types.Fetcher, cfg HttpServerConfig) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		statusLogger := newStatusLogger(req.Method, req.URL.Path)
		path := datamodel.ParsePath(req.URL.Path)
		_, path = path.Shift() // remove /ipfs

		// filter out everything but GET requests
		switch req.Method {
		case http.MethodGet:
			break
		default:
			res.Header().Add("Allow", http.MethodGet)
			errorResponse(res, statusLogger, http.StatusMethodNotAllowed, errors.New("method not allowed"))
			return
		}

		// check if CID path param is missing
		if path.Len() == 0 {
			// not a valid path to hit
			errorResponse(res, statusLogger, http.StatusNotFound, errors.New("not found"))
			return
		}

		includeDupes, err := CheckFormat(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		fileName, err := ParseFilename(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		// validate CID path parameter
		var cidSeg datamodel.PathSegment
		cidSeg, path = path.Shift()
		rootCid, err := cid.Parse(cidSeg.String())
		if err != nil {
			errorResponse(res, statusLogger, http.StatusInternalServerError, errors.New("failed to parse CID path parameter"))
			return
		}

		dagScope, err := ParseScope(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		protocols, err := parseProtocols(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		fixedPeers, err := parseProviders(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		// for setting Content-Disposition header based on filename url parameter
		if fileName == "" {
			fileName = fmt.Sprintf("%s%s", rootCid.String(), FilenameExtCar)
		}

		retrievalId, err := types.NewRetrievalID()
		if err != nil {
			errorResponse(res, statusLogger, http.StatusInternalServerError, fmt.Errorf("failed to generate retrieval ID: %w", err))
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

		tempStore := storage.NewDeferredStorageCar(cfg.TempDir, rootCid)
		var carWriter storage.DeferredWriter
		if includeDupes {
			carWriter = storage.NewDuplicateAdderCarForStream(req.Context(), rootCid, path.String(), dagScope, tempStore, res)
		} else {
			carWriter = storage.NewDeferredCarWriterForStream(rootCid, res)
		}
		carStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
		defer func() {
			if err := carStore.Close(); err != nil {
				logger.Errorf("error closing temp store: %s", err)
			}
		}()
		var store types.ReadableWritableStorage = carStore

		request, err := types.NewRequestForPath(store, rootCid, path.String(), dagScope)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusInternalServerError, fmt.Errorf("failed to create request: %w", err))
			return
		}
		request.Protocols = protocols
		request.FixedPeers = fixedPeers
		request.RetrievalID = retrievalId
		request.Duplicates = includeDupes // needed for etag

		carWriter.OnPut(func(int) {
			// called once we start writing blocks into the CAR (on the first Put())
			res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
			res.Header().Set("Accept-Ranges", ResponseAcceptRangesHeader)
			res.Header().Set("Cache-Control", ResponseCacheControlHeader)
			res.Header().Set("Content-Type", ResponseContentTypeHeader)
			res.Header().Set("Etag", request.Etag())
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", "/"+datamodel.ParsePath(req.URL.Path).String())
			// TODO: set X-Ipfs-Roots header when we support root+path
			// see https://github.com/ipfs/kubo/pull/8720
			res.Header().Set("X-Trace-Id", requestId)
			statusLogger.logStatus(200, "OK")
			close(bytesWritten)
		}, true)

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

		// servertiming metrics
		logger.Debugw("fetching CID", "retrievalId", retrievalId, "CID", rootCid.String(), "path", path.String(), "dagScope", dagScope)
		stats, err := fetcher.Fetch(req.Context(), request, servertimingsSubscriber(req))

		// force all blocks to flush
		if cerr := carWriter.Close(); cerr != nil && !errors.Is(cerr, context.Canceled) {
			logger.Infof("error closing car writer: %s", cerr)
		}

		if err != nil {
			select {
			case <-bytesWritten:
				logger.Debugw("unclean close", "cid", request.Cid, "retrievalID", request.RetrievalID)
				if err := closeWithUnterminatedChunk(res); err != nil {
					logger.Infow("unable to send early termination", "err", err)
				}
				return
			default:
			}
			if errors.Is(err, retriever.ErrNoCandidates) {
				errorResponse(res, statusLogger, http.StatusNotFound, errors.New("no candidates found"))
			} else {
				errorResponse(res, statusLogger, http.StatusGatewayTimeout, fmt.Errorf("failed to fetch CID: %w", err))
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

func parseProtocols(req *http.Request) ([]multicodec.Code, error) {
	if req.URL.Query().Has("protocols") {
		return types.ParseProtocolsString(req.URL.Query().Get("protocols"))
	}
	return nil, nil
}

func parseProviders(req *http.Request) ([]peer.AddrInfo, error) {
	if req.URL.Query().Has("providers") {
		fixedPeers, err := types.ParseProviderStrings(req.URL.Query().Get("providers"))
		if err != nil {
			return nil, errors.New("invalid providers parameter")
		}
		return fixedPeers, nil
	}
	return nil, nil
}

// errorResponse logs and replies to the request with the status code and error
func errorResponse(res http.ResponseWriter, statusLogger *statusLogger, code int, err error) {
	statusLogger.logStatus(code, err.Error())
	http.Error(res, err.Error(), code)
}

// closeWithUnterminatedChunk attempts to take control of the the http conn and terminate the stream early
func closeWithUnterminatedChunk(res http.ResponseWriter) error {
	hijacker, ok := res.(http.Hijacker)
	if !ok {
		return errors.New("unable to access hijack interface")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return fmt.Errorf("unable to access conn through hijack interface: %w", err)
	}
	if _, err := buf.Write(ResponseChunkDelimeter); err != nil {
		return fmt.Errorf("writing response chunk delimiter: %w", err)
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("flushing buff: %w", err)
	}
	// attempt to close just the write side
	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing write conn: %w", err)
	}
	return nil
}
