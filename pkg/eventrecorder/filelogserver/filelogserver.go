// A simple HTTP server that receives EventRecorder events and writes them,
// as line-delimited JSON to an output file.
//
//   - Use `--port` to set the port (default: 8080)
//
//   - Use `--output` to set the output file path (default: output.json)
//
// When encoded as JSON, the original event JSON will be wrapped in a parent
// with the "retrievalId" UUID and "peerId" peerID that come from the URL.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/ipfs/go-log/v2"
)

var logger = log.Logger("lassie/filelogserver")

func main() {
	port := flag.Int("port", 8080, "set port to listen to")
	output := flag.String("output", "output.json", "set a file to write to")
	flag.Parse()

	outfile, err := os.OpenFile(*output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	logger.Infof("Writing to file %s", *output)

	http.HandleFunc("/retrieval-events", func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			http.Error(w, "Expected request body", http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorf("Could not decode body: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(body, &m)
		if err != nil {
			logger.Errorf("Could not decode body: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reencode, err := json.Marshal(m)
		if err != nil {
			logger.Errorf("Error building log entry: %s", err.Error())
			return
		}
		logger.Debugf("Got: %s", string(reencode))
		outfile.WriteString(string(reencode))
		outfile.Write([]byte{'\n'})
	})

	logger.Infof("Listening on port %d", *port)
	logger.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
