// A simple HTTP server that receives EventRecorder events and writes them,
// as line-delimited JSON to an output file.
//
// 	* Use `--port` to set the port (default: 8080)
//
//  * Use `--output` to set the output file path (default: output.json)
//
// When encoded as JSON, the original event JSON will be wrapped in a parent
// with the "retrievalId" UUID and "peerId" peerID that come from the URL.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("filelogserver")

func main() {
	port := flag.Int("port", 8080, "set port to listen to")
	output := flag.String("output", "output.json", "set a file to write to")
	flag.Parse()

	outfile, err := os.OpenFile(*output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	log.Infof("Writing to file %s", *output)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			http.Error(w, "Expected request body", http.StatusBadRequest)
			return
		}

		// validate path
		urlSegments := strings.Split(r.URL.Path, "/")
		errAction := func() {
			log.Errorf("Malformed URL, expected /(retrieval|query)-event/~uuid~[/providers/~peerid~], got: %s", r.URL.Path)
			http.Error(w, "Malformed URL, expected /(retrieval|query)-event/~uuid~[/providers/~peerid~]", http.StatusBadRequest)
		}
		switch len(urlSegments) {
		case 3:
			if urlSegments[1] != "query-event" {
				errAction()
				return
			}
		case 5:
			if !(urlSegments[1] == "retrieval-event" || urlSegments[1] == "query-event") || urlSegments[3] != "providers" {
				errAction()
				return
			}
		default:
			errAction()
			return
		}

		retrievalId, err := uuid.Parse(urlSegments[2])
		if err != nil {
			log.Errorf("Malformed URL, expected /retrieval-event/~uuid~/providers/~peerid~, got bad uuid: %s", err.Error())
			http.Error(w, "Malformed URL, expected /retrieval-event/~uuid~/providers/~peerid~", http.StatusBadRequest)
			return
		}
		var peerId peer.ID

		if len(urlSegments) > 3 {
			peerId, err = peer.Decode(urlSegments[4])
			if err != nil {
				log.Errorf("Malformed URL, expected /(retrieval|query)-event/~uuid~/providers/~peerid~, got bad peerID: %s", err.Error())
				http.Error(w, "Malformed URL, expected /(retrieval|query)-event/~uuid~/providers/~peerid~", http.StatusBadRequest)
				return
			}
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorf("Could not decode body: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		node, err := ipld.Decode(body, dagjson.Decode)
		if err != nil {
			log.Errorf("Could not decode body: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		logNode, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "retrievalId", qp.String(retrievalId.String()))
			qp.MapEntry(ma, "mode", qp.String(strings.Split(urlSegments[1], "-")[0]))
			if peerId != "" {
				qp.MapEntry(ma, "peerId", qp.String(peerId.String()))
			}
			qp.MapEntry(ma, "event", qp.Node(node))
		})
		if err != nil {
			log.Errorf("Error building log entry: %s", err.Error())
			return
		}
		logEntry, err := ipld.Encode(logNode, dagjson.Encode)
		if err != nil {
			log.Errorf("Error encoding log entry: %s", err.Error())
			return
		}
		log.Debugf("Got: %s", string(logEntry))
		outfile.WriteString(string(logEntry))
		outfile.Write([]byte{'\n'})
	})

	log.Infof("Listening on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
