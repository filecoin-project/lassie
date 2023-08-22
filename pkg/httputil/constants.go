package httputil

import "fmt"

const (
	MimeTypeCar                = "application/vnd.ipld.car"            // The only accepted MIME type
	MimeTypeCarVersion         = "1"                                   // We only accept version 1 of the MIME type
	ResponseAcceptRangesHeader = "none"                                // We currently don't accept range requests
	ResponseCacheControlHeader = "public, max-age=29030400, immutable" // Magic cache control values
	FilenameExtCar             = ".car"                                // The only valid filename extension
	FormatParameterCar         = "car"                                 // The only valid format parameter value
	DefaultIncludeDupes        = true                                  // The default value for an unspecified "dups" parameter. See https://github.com/ipfs/specs/pull/412.
)

var (
	ResponseChunkDelimeter    = []byte("0\r\n") // An http/1.1 chunk delimeter, used for specifying an early end to the response
	ResponseContentTypeHeader = fmt.Sprintf("%s; version=%s; order=dfs; dups=y", MimeTypeCar, MimeTypeCarVersion)
	RequestAcceptHeader       = fmt.Sprintf("%s; version=%s; order=dfs; dups=y; meta=eof", MimeTypeCar, MimeTypeCarVersion)
)
