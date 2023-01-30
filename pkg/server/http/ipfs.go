package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	lassie "github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/ipfs/go-cid"
)

func ipfsHandler(res http.ResponseWriter, req *http.Request) {
	logger := NewRequestLogger(req.Method, req.URL.Path)
	logger.LogPath()

	urlPath := strings.Split(req.URL.Path, "/")[1:]

	// filter out everthing but GET requests
	switch req.Method {
	case http.MethodGet:
		break
	default:
		logger.LogStatus(http.StatusMethodNotAllowed, "Method not allowed")
		res.Header().Add("Allow", http.MethodGet)
		res.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// check if cid path param is missing
	if len(urlPath) < 2 {
		// not a valid path to hit
		logger.LogStatus(http.StatusNotFound, "Not found")
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
		logger.LogStatus(http.StatusBadRequest, "No acceptable content type")
		res.WriteHeader(http.StatusBadRequest)
		return
	}

	// check if format is car
	hasFormat := req.URL.Query().Has("format")
	if hasFormat && req.URL.Query().Get("format") != "car" {
		logger.LogStatus(http.StatusBadRequest, fmt.Sprintf("Requested non-supported format %s", req.URL.Query().Get("format")))
		res.WriteHeader(http.StatusBadRequest)
		return
	}

	// if neither are provided return
	// one of them has to be given with a car type since we only return car files
	if !validAccept && !hasFormat {
		logger.LogStatus(http.StatusBadRequest, "Neither a valid accept header or format parameter were provided")
		res.WriteHeader(http.StatusBadRequest)
		return
	}

	// check if provided filename query parameter has .car extension
	if req.URL.Query().Has("filename") {
		filename := req.URL.Query().Get("filename")
		ext := filepath.Ext(filename)
		if ext == "" {
			logger.LogStatus(http.StatusBadRequest, "Filename missing extension")
			res.WriteHeader(http.StatusBadRequest)
			return
		}
		if ext != ".car" {
			logger.LogStatus(http.StatusBadRequest, fmt.Sprintf("Filename uses non-supported extension %s", ext))
			res.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// validate cid path parameter
	cidStr := urlPath[1]
	cid, err := cid.Parse(cidStr)
	if err != nil {
		logger.LogStatus(http.StatusInternalServerError, "Failed to parse cid path parameter")
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Grab unixfs path if it exists
	// var unixfsPath []string
	// if len(urlPath) > 2 {
	// 	unixfsPath = urlPath[2:]
	// }
	// TODO: Do something with unixfs path

	log.Debug("creating temp car file")
	carfile, err := os.CreateTemp("", cid.String())
	if err != nil {
		logger.LogStatus(http.StatusInternalServerError, fmt.Sprintf("Failed to create temp car file before retrieval: %v", err))
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	var lassie = lassie.NewLassie(20 * time.Second)
	log.Debugw("fetching cid into car file", "cid", cid.String(), "file", carfile.Name())
	_, id, err := lassie.Fetch(context.Background(), cid, carfile)
	if err != nil {
		logger.LogStatus(http.StatusInternalServerError, fmt.Sprintf("Failed to fetch cid: %v", err))
		res.WriteHeader(http.StatusInternalServerError)
		carfile.Close()
		return
	}

	log.Debugw("closing car file", "file", carfile.Name())
	err = carfile.Close()
	if err != nil {
		logger.LogStatus(http.StatusInternalServerError, fmt.Sprintf("Failed to close temp car file after retrieval: %v", err))
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	// set Content-Disposition header based on filename url parameter
	var filename string
	if req.URL.Query().Has("filename") {
		filename = req.URL.Query().Get("filename")
	} else {
		filename = fmt.Sprintf("%s.car", cid.String())
	}
	res.Header().Set("Content-Disposition", "attachment; filename="+filename)

	res.Header().Set("Accept-Ranges", "none")
	res.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
	res.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
	res.Header().Set("Etag", fmt.Sprintf("%s.car", cid.String()))
	res.Header().Set("C-Content-Type-Options", "nosniff")
	res.Header().Set("X-Ipfs-Path", req.URL.Path)
	// TODO: set X-Ipfs-Roots header
	res.Header().Set("X-Trace-Id", id.String())

	logger.LogStatus(200, "OK")
	http.ServeFile(res, req, carfile.Name())

	log.Debugw("removing temp car file", "file", carfile.Name())
	err = os.Remove(carfile.Name())
	if err != nil {
		log.Errorw("failed to remove temp car file after retrieval", "file", carfile.Name(), "err", err)
	}
}

// A logger for the requests and responses, separate from the application logging
type requestLogger struct {
	method string
	path   string
}

func NewRequestLogger(method string, path string) *requestLogger {
	return &requestLogger{method, path}
}

// Logs the method and path
func (l requestLogger) LogPath() {
	now := time.Now().UTC().Local().Format(time.RFC3339)
	fmt.Printf("%s\t%s\t%s\n", now, l.method, l.path)
}

// Logs the method, path, and any status code and message depending on the status code
func (l requestLogger) LogStatus(statusCode int, message string) {
	now := time.Now().UTC().Local().Format(time.RFC3339)
	if statusCode == http.StatusOK {
		fmt.Printf("%s\t%s\t%s\t%s\n", now, l.method, l.path, message)
	} else {
		fmt.Printf("%s\t%s\t%s\tError (%d): %s\n", now, l.method, l.path, statusCode, message)
	}
}
