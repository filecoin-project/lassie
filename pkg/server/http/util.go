package httpserver

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/lassie/pkg/types"
)

// ParseScope returns the dag-scope query parameter or an error if the dag-scope
// parameter is not one of the supported values.
func ParseScope(req *http.Request) (types.DagScope, error) {
	if req.URL.Query().Has("dag-scope") {
		if ds, err := types.ParseDagScope(req.URL.Query().Get("dag-scope")); err != nil {
			return ds, errors.New("invalid dag-scope parameter")
		} else {
			return ds, nil
		}
	}
	// check for legacy param name -- to do -- delete once we confirm this isn't used any more
	if req.URL.Query().Has("car-scope") {
		switch req.URL.Query().Get("car-scope") {
		case "all":
			return types.DagScopeAll, nil
		case "file":
			return types.DagScopeEntity, nil
		case "block":
			return types.DagScopeBlock, nil
		default:
			return types.DagScopeAll, errors.New("invalid car-scope parameter")
		}
	}
	return types.DagScopeAll, nil
}

// ParseByteRange returns the entity-bytes query parameter if one is set in the
// query string or nil if one is not set. An error is returned if an
// entity-bytes query string is not a valid byte range.
func ParseByteRange(req *http.Request) (*types.ByteRange, error) {
	if req.URL.Query().Has("entity-bytes") {
		br, err := types.ParseByteRange(req.URL.Query().Get("entity-bytes"))
		if err != nil {
			return nil, errors.New("invalid entity-bytes parameter")
		}
		return &br, nil
	}
	return nil, nil
}

// ParseFilename returns the filename query parameter or an error if the filename
// extension is not ".car". Lassie only supports returning CAR data.
// See https://specs.ipfs.tech/http-gateways/path-gateway/#filename-request-query-parameter
func ParseFilename(req *http.Request) (string, error) {
	// check if provided filename query parameter has .car extension
	if req.URL.Query().Has("filename") {
		filename := req.URL.Query().Get("filename")
		ext := filepath.Ext(filename)
		if ext == "" {
			return "", errors.New("filename missing extension")
		}
		if ext != FilenameExtCar {
			return "", fmt.Errorf("filename uses non-supported extension %s", ext)
		}
		return filename, nil
	}
	return "", nil
}

// CheckFormat validates that the data being requested is of the type CAR.
// We do this validation because the http gateway path spec allows for additional
// response formats that Lassie does not currently support, so we throw an error in
// the cases where the request is requesting one of Lassie's unsupported response
// formats. Lassie only supports returning CAR data.
//
// The spec outlines that the requesting format can be provided
// via the Accept header or the format query parameter.
//
// Lassie only allows the application/vnd.ipld.car Accept header
// https://specs.ipfs.tech/http-gateways/path-gateway/#accept-request-header
//
// Lassie only allows the "car" format query parameter
// https://specs.ipfs.tech/http-gateways/path-gateway/#format-request-query-parameter
func CheckFormat(req *http.Request) (bool, error) {
	hasAccept := req.Header.Get("Accept") != ""
	// check if Accept header includes application/vnd.ipld.car
	validAccept, includeDupes := ParseAccept(req.Header.Get("Accept"))
	if hasAccept && !validAccept {
		return false, fmt.Errorf("no acceptable content type")
	}

	// check if format is "car"
	hasFormat := req.URL.Query().Has("format")
	if hasFormat && req.URL.Query().Get("format") != FormatParameterCar {
		return false, fmt.Errorf("requested non-supported format %s", req.URL.Query().Get("format"))
	}

	// if neither are provided return
	// one of them has to be given with a CAR type since we only return CAR data
	if !validAccept && !hasFormat {
		return false, fmt.Errorf("neither a valid accept header or format parameter were provided")
	}

	return includeDupes, nil
}

// ParseAccept validates that the request Accept header is of the type CAR and
// returns whether or not duplicate blocks are allowed in the response via
// IPIP-412: https://github.com/ipfs/specs/pull/412.
func ParseAccept(acceptHeader string) (validAccept bool, includeDupes bool) {
	acceptTypes := strings.Split(acceptHeader, ",")
	validAccept = false
	includeDupes = DefaultIncludeDupes
	for _, acceptType := range acceptTypes {
		typeParts := strings.Split(acceptType, ";")
		if typeParts[0] == "*/*" || typeParts[0] == "application/*" || typeParts[0] == MimeTypeCar {
			validAccept = true
			if typeParts[0] == MimeTypeCar {
				// parse additional car attributes outlined in IPIP-412: https://github.com/ipfs/specs/pull/412
				for _, nextPart := range typeParts[1:] {
					pair := strings.Split(nextPart, "=")
					if len(pair) == 2 {
						attr := strings.TrimSpace(pair[0])
						value := strings.TrimSpace(pair[1])
						switch attr {
						case "dups":
							switch value {
							case "y":
								includeDupes = true
							case "n":
								includeDupes = false
							default:
								// don't accept unexpected values
								validAccept = false
							}
						case "version":
							switch value {
							case MimeTypeCarVersion:
							default:
								validAccept = false
							}
						case "order":
							switch value {
							case "dfs":
							case "unk":
							default:
								// we only do dfs, which also satisfies unk, future extensions are not yet supported
								validAccept = false
							}
						default:
							// ignore others
						}
					}
				}
			}
			// only break if further validation didn't fail
			if validAccept {
				break
			}
		}
	}
	return
}
