package httpserver

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/streamingstore"
	lassie "github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func ipfsHandler(lassie *lassie.Lassie) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		logger := newRequestLogger(req.Method, req.URL.Path)
		logger.logPath()

		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// filter out everything but GET requests
		switch req.Method {
		case http.MethodGet:
			break
		default:
			logger.logStatus(http.StatusMethodNotAllowed, "Method not allowed")
			res.Header().Add("Allow", http.MethodGet)
			res.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// check if CID path param is missing
		if len(urlPath) < 2 {
			// not a valid path to hit
			logger.logStatus(http.StatusNotFound, "Not found")
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
			logger.logStatus(http.StatusBadRequest, "No acceptable content type")
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if format is car
		hasFormat := req.URL.Query().Has("format")
		if hasFormat && req.URL.Query().Get("format") != "car" {
			logger.logStatus(http.StatusBadRequest, fmt.Sprintf("Requested non-supported format %s", req.URL.Query().Get("format")))
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// if neither are provided return
		// one of them has to be given with a CAR type since we only return CAR data
		if !validAccept && !hasFormat {
			logger.logStatus(http.StatusBadRequest, "Neither a valid accept header or format parameter were provided")
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if provided filename query parameter has .car extension
		if req.URL.Query().Has("filename") {
			filename := req.URL.Query().Get("filename")
			ext := filepath.Ext(filename)
			if ext == "" {
				logger.logStatus(http.StatusBadRequest, "Filename missing extension")
				res.WriteHeader(http.StatusBadRequest)
				return
			}
			if ext != ".car" {
				logger.logStatus(http.StatusBadRequest, fmt.Sprintf("Filename uses non-supported extension %s", ext))
				res.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			logger.logStatus(http.StatusInternalServerError, "Failed to parse CID path parameter")
			http.Error(res, "Failed to parse CID path parameter", http.StatusInternalServerError)
			return
		}

		// Grab unixfs path if it exists
		// var unixfsPath []string
		// if len(urlPath) > 2 {
		// 	unixfsPath = urlPath[2:]
		// }
		// TODO: Do something with unixfs path

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
			logger.logStatus(http.StatusInternalServerError, msg)
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
			log.Debugw("Corrolating provided request ID with retrieval ID", "request_id", requestId, "retrieval_id", retrievalId)
		}

		bytesWritten := make(chan struct{}, 1)
		// called once we start writing blocks into the CAR (on the first Put())
		getWriter := func() (io.Writer, error) {
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

			logger.logStatus(200, "OK")
			bytesWritten <- struct{}{}
			return res, nil
		}

		// called when the store errors from any of its operations; only log the
		// first error and assume that the error will propagate through to
		// lassie.Retrieve
		var errored bool
		errorCb := func(error) {
			if !errored {
				errored = true
				msg := fmt.Sprintf("Failed to write to CAR: %s", err.Error())
				logger.logStatus(http.StatusInternalServerError, msg)
				http.Error(res, msg, http.StatusInternalServerError)
			}
		}

		store := streamingstore.NewStreamingStore(req.Context(), []cid.Cid{rootCid}, getWriter, errorCb)
		defer func() {
			if err := store.Close(); err != nil {
				log.Errorw("failed to close streaming store after retrieval", "retrievalId", retrievalId, "err", err)
			}
		}()
		linkSystem := cidlink.DefaultLinkSystem()
		linkSystem.SetReadStorage(store)
		linkSystem.SetWriteStorage(store)
		linkSystem.TrustedStorage = true

		log.Debugw("fetching CID", "retrievalId", retrievalId, "CID", rootCid.String())
		request := types.RetrievalRequest{RetrievalID: retrievalId, Cid: rootCid, LinkSystem: linkSystem}
		stats, err := lassie.Retrieve(req.Context(), request)
		if err != nil {
			select {
			case <-bytesWritten:
				return
			default:
			}
			if errors.Is(err, retriever.ErrNoCandidates) {
				msg := "No candidates found"
				logger.logStatus(http.StatusNotFound, msg)
				http.Error(res, msg, http.StatusNotFound)
			} else {
				msg := fmt.Sprintf("Failed to fetch CID: %s", err.Error())
				logger.logStatus(http.StatusGatewayTimeout, msg)
				http.Error(res, msg, http.StatusGatewayTimeout)
			}

			return
		}
		log.Debugw("successfully fetched CID",
			"retrievalId", retrievalId,
			"CID", rootCid,
			"duration", stats.Duration,
			"bytes", stats.Size,
		)
	}
}

// A logger for the requests and responses, separate from the application logging
type requestLogger struct {
	method string
	path   string
}

func newRequestLogger(method string, path string) *requestLogger {
	return &requestLogger{method, path}
}

// Logs the method and path
func (l requestLogger) logPath() {
	now := time.Now().UTC().Local().Format(time.RFC3339)
	fmt.Printf("%s\t%s\t%s\n", now, l.method, l.path)
}

// Logs the method, path, and any status code and message depending on the status code
func (l requestLogger) logStatus(statusCode int, message string) {
	now := time.Now().UTC().Local().Format(time.RFC3339)
	if statusCode == http.StatusOK {
		fmt.Printf("%s\t%s\t%s\t%s\n", now, l.method, l.path, message)
	} else {
		fmt.Printf("%s\t%s\t%s\tError (%d): %s\n", now, l.method, l.path, statusCode, message)
	}
}
