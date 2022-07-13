package eventrecorder_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/application-research/autoretrieve/filecoin/eventrecorder"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/big"
	qt "github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
)

var testCid1 cid.Cid = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")
var testCid2 cid.Cid = mustCid("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")

func TestEventRecorder_Success(t *testing.T) {
	runTest := func(rootCid cid.Cid) func(t *testing.T) {
		return func(t *testing.T) {
			var req datamodel.Node
			var path string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
				qt.Assert(t, err, qt.IsNil)
				path = r.URL.Path
			}))
			defer ts.Close()

			er := eventrecorder.NewEventRecorder(ts.URL)
			id, err := uuid.NewRandom()
			qt.Assert(t, err, qt.IsNil)
			startTime := time.Now().Add(time.Duration(500))
			minerId := peer.NewPeerRecord().PeerID
			stats := &filclient.RetrievalStats{
				Peer:         minerId,
				Duration:     time.Duration(1010),
				Size:         2020,
				AskPrice:     big.NewInt(3030),
				TotalPayment: big.NewInt(4040),
				NumPayments:  2,
			}
			qt.Assert(t, er.RecordSuccess(id, testCid1, rootCid, startTime, stats), qt.IsNil)

			// expect something like this:
			// {
			//   "cid":{"/":"bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm"},
			//   "startTime":"2022-07-12T19:57:53.112375079+10:00",
			//   "size":2020,
			//   "askPrice":"3030",
			//   "totalPayment":"4040",
			//   "numPayments":2
			// }

			qt.Assert(t, req, qt.IsNotNil)

			pathSegments := strings.Split(path, "/")
			qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", minerId.String()})

			expectedMessage, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "askPrice", qp.String("3030"))
				qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
				qp.MapEntry(ma, "durationMs", qp.Int(0))
				qp.MapEntry(ma, "numPayments", qp.Int(2))
				if !testCid1.Equals(rootCid) {
					qp.MapEntry(ma, "rootCid", qp.Link(cidlink.Link{Cid: rootCid}))
				}
				qp.MapEntry(ma, "size", qp.Int(2020))
				qp.MapEntry(ma, "startTime", qp.String(startTime.Format(time.RFC3339Nano)))
				qp.MapEntry(ma, "totalPayment", qp.String("4040"))
			})
			qt.Assert(t, err, qt.IsNil)

			equal := ipld.DeepEqual(req, expectedMessage)
			if !equal {
				t.Logf("\nwant: %s\n got: %s", mustDagJson(expectedMessage), mustDagJson(req))
			}
			qt.Assert(t, equal, qt.IsTrue)
		}
	}

	t.Run("same-rootcid", runTest(testCid1))
	t.Run("different-rootcid", runTest(testCid2))
}

func TestEventRecorder_Failure(t *testing.T) {
	runTest := func(rootCid cid.Cid) func(t *testing.T) {
		return func(t *testing.T) {
			var req datamodel.Node
			var path string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
				qt.Assert(t, err, qt.IsNil)
				path = r.URL.Path
			}))
			defer ts.Close()

			er := eventrecorder.NewEventRecorder(ts.URL)
			id, err := uuid.NewRandom()
			qt.Assert(t, err, qt.IsNil)
			minerId := peer.NewPeerRecord().PeerID
			startTime := time.Now().Add(time.Duration(500))
			err = errors.New("some error message here")

			qt.Assert(t, er.RecordFailure(id, minerId, testCid1, rootCid, startTime, err), qt.IsNil)

			// expect something like this:
			// {
			//   "cid":{"/":"bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm"},
			//   "startTime":"2022-07-12T19:57:53.112375079+10:00",
			//   "error":"some error message here",
			// }

			qt.Assert(t, req, qt.IsNotNil)

			pathSegments := strings.Split(path, "/")
			qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", minerId.String()})

			expectedMessage, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
				qp.MapEntry(ma, "error", qp.String("some error message here"))
				if !testCid1.Equals(rootCid) {
					qp.MapEntry(ma, "rootCid", qp.Link(cidlink.Link{Cid: rootCid}))
				}
				qp.MapEntry(ma, "startTime", qp.String(startTime.Format(time.RFC3339Nano)))
			})
			qt.Assert(t, err, qt.IsNil)

			equal := ipld.DeepEqual(req, expectedMessage)
			if !equal {
				t.Logf("\nwant: %s\n got: %s", mustDagJson(expectedMessage), mustDagJson(req))
			}
			qt.Assert(t, equal, qt.IsTrue)
		}
	}

	t.Run("same-rootcid", runTest(testCid1))
	t.Run("different-rootcid", runTest(testCid2))
}

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}

func mustDagJson(node datamodel.Node) string {
	byts, err := ipld.Encode(node, dagjson.Encode)
	if err != nil {
		panic(err)
	}
	return string(byts)
}
