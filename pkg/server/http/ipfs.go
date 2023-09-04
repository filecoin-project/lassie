package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/filecoin-project/lassie/pkg/build"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2/storage/deferred"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

func IpfsHandler(fetcher types.Fetcher, cfg HttpServerConfig) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		statusLogger := newStatusLogger(req.Method, req.URL.Path)

		if !checkGet(req, res, statusLogger) {
			return
		}

		ok, request := decodeRetrievalRequest(cfg, res, req, statusLogger)
		if !ok {
			return
		}

		ok, fileName := decodeFilename(res, req, statusLogger, request.Root)
		if !ok {
			return
		}

		// TODO: this needs to be propagated through the request, perhaps on
		// RetrievalRequest or we decode it as a UUID and override RetrievalID?
		requestId := req.Header.Get("X-Request-Id")
		if requestId == "" {
			requestId = request.RetrievalID.String()
		} else {
			logger.Debugw("custom X-Request-Id fore retrieval", "request_id", requestId, "retrieval_id", request.RetrievalID)
		}

		tempStore := storage.NewDeferredStorageCar(cfg.TempDir, request.Root)
		var carWriter storage.DeferredWriter
		if request.Duplicates {
			carWriter = storage.NewDuplicateAdderCarForStream(req.Context(), res, request.Root, request.Path, request.Scope, request.Bytes, tempStore)
		} else {
			carWriter = deferred.NewDeferredCarWriterForStream(res, []cid.Cid{request.Root})
		}
		carStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
		defer func() {
			if err := carStore.Close(); err != nil {
				logger.Errorf("error closing temp store: %s", err)
			}
		}()

		request.LinkSystem.SetWriteStorage(carStore)
		request.LinkSystem.SetReadStorage(carStore)

		// setup preload storage for bitswap, the temporary CAR store can set up a
		// separate preload space in its storage
		request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
		preloadStore := carStore.PreloadStore()
		request.PreloadLinkSystem.SetReadStorage(preloadStore)
		request.PreloadLinkSystem.SetWriteStorage(preloadStore)
		request.PreloadLinkSystem.TrustedStorage = true

		// bytesWritten will be closed once we've started writing CAR content to
		// the response writer. Once closed, no other content should be written.
		bytesWritten := make(chan struct{}, 1)

		carWriter.OnPut(func(int) {
			// called once we start writing blocks into the CAR (on the first Put())
			res.Header().Set("Server", build.UserAgent) // "lassie/vx.y.z-<git commit hash>"
			res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
			res.Header().Set("Accept-Ranges", "none")
			res.Header().Set("Cache-Control", trustlesshttp.ResponseCacheControlHeader)
			res.Header().Set("Content-Type", trustlesshttp.DefaultContentType().WithDuplicates(request.Duplicates).String())
			res.Header().Set("Etag", request.Etag())
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", trustlessutils.PathEscape(req.URL.Path))
			res.Header().Set("X-Trace-Id", requestId)
			statusLogger.logStatus(200, "OK")
			close(bytesWritten)
		}, true)

		logger.Debugw("fetching",
			"retrieval_id", request.RetrievalID,
			"root", request.Root.String(),
			"path", request.Path,
			"dag-scope", request.Scope,
			"entity-bytes", request.Bytes,
			"dups", request.Duplicates,
			"maxBlocks", request.MaxBlocks,
		)

		stats, err := fetcher.Fetch(req.Context(), request, servertimingsSubscriber(req))

		// force all blocks to flush
		if cerr := carWriter.Close(); cerr != nil && !errors.Is(cerr, context.Canceled) {
			logger.Infof("error closing car writer: %s", cerr)
		}

		if err != nil {
			select {
			case <-bytesWritten:
				logger.Debugw("unclean close", "cid", request.Root, "retrievalID", request.RetrievalID)
				if err := closeWithUnterminatedChunk(res); err != nil {
					logger.Infow("unable to send early termination", "err", err)
				}
				return
			default:
			}
			if errors.Is(err, retriever.ErrNoCandidates) {
				errorResponse(res, statusLogger, http.StatusBadGateway, errors.New("no candidates found"))
			} else {
				errorResponse(res, statusLogger, http.StatusGatewayTimeout, fmt.Errorf("failed to fetch CID: %w", err))
			}
			return
		}

		logger.Debugw("successfully fetched",
			"retrieval_id", request.RetrievalID,
			"root", request.Root.String(),
			"path", request.Path,
			"dag-scope", request.Scope,
			"entity-bytes", request.Bytes,
			"dups", request.Duplicates,
			"maxBlocks", request.MaxBlocks,
			"duration", stats.Duration,
			"bytes", stats.Size,
		)
	}
}

func checkGet(req *http.Request, res http.ResponseWriter, statusLogger *statusLogger) bool {
	// filter out everything but GET requests
	if req.Method == http.MethodGet {
		return true
	}
	res.Header().Add("Allow", http.MethodGet)
	errorResponse(res, statusLogger, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	return false
}

func decodeRequest(res http.ResponseWriter, req *http.Request, statusLogger *statusLogger) (bool, trustlessutils.Request) {
	rootCid, path, err := trustlesshttp.ParseUrlPath(req.URL.Path)
	if err != nil {
		if errors.Is(err, trustlesshttp.ErrPathNotFound) {
			errorResponse(res, statusLogger, http.StatusNotFound, err)
		} else if errors.Is(err, trustlesshttp.ErrBadCid) {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
		} else {
			errorResponse(res, statusLogger, http.StatusInternalServerError, err)
		}
		return false, trustlessutils.Request{}
	}

	accept, err := trustlesshttp.CheckFormat(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}

	dagScope, err := trustlesshttp.ParseScope(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}

	byteRange, err := trustlesshttp.ParseByteRange(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}

	return true, trustlessutils.Request{
		Root:       rootCid,
		Path:       path.String(),
		Scope:      dagScope,
		Bytes:      byteRange,
		Duplicates: accept.Duplicates,
	}
}

func decodeRetrievalRequest(cfg HttpServerConfig, res http.ResponseWriter, req *http.Request, statusLogger *statusLogger) (bool, types.RetrievalRequest) {
	ok, request := decodeRequest(res, req, statusLogger)
	if !ok {
		return false, types.RetrievalRequest{}
	}

	protocols, err := parseProtocols(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, types.RetrievalRequest{}
	}

	fixedPeers, err := parseProviders(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, types.RetrievalRequest{}
	}

	// extract block limit from query param as needed
	var maxBlocks uint64
	if req.URL.Query().Has("blockLimit") {
		if parsedBlockLimit, err := strconv.ParseUint(req.URL.Query().Get("blockLimit"), 10, 64); err == nil {
			maxBlocks = parsedBlockLimit
		}
	}
	// use the lowest non-zero value for block limit
	if maxBlocks == 0 || (cfg.MaxBlocksPerRequest > 0 && maxBlocks > cfg.MaxBlocksPerRequest) {
		maxBlocks = cfg.MaxBlocksPerRequest
	}

	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		errorResponse(res, statusLogger, http.StatusInternalServerError, fmt.Errorf("failed to generate retrieval ID: %w", err))
		return false, types.RetrievalRequest{}
	}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	return true, types.RetrievalRequest{
		Request:     request,
		RetrievalID: retrievalId,
		LinkSystem:  linkSystem,
		Protocols:   protocols,
		FixedPeers:  fixedPeers,
		MaxBlocks:   maxBlocks,
	}
}

func decodeFilename(res http.ResponseWriter, req *http.Request, statusLogger *statusLogger, root cid.Cid) (bool, string) {
	fileName, err := trustlesshttp.ParseFilename(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, ""
	}
	// for setting Content-Disposition header based on filename url parameter
	if fileName == "" {
		fileName = fmt.Sprintf("%s%s", root, trustlesshttp.FilenameExtCar)
	}
	return true, fileName
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
	if _, err := buf.Write(trustlesshttp.ResponseChunkDelimeter); err != nil {
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
