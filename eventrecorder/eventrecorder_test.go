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
	"github.com/application-research/filclient/rep"
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
	var req datamodel.Node
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		path = r.URL.Path
	}))
	defer ts.Close()

	er := eventrecorder.NewEventRecorder("test-instance", ts.URL)
	id, err := uuid.NewRandom()
	qt.Assert(t, err, qt.IsNil)
	storageProviderId := peer.NewPeerRecord().PeerID
	stats := filclient.RetrievalStats{
		Peer:         storageProviderId,
		Duration:     time.Duration(1010),
		Size:         2020,
		AskPrice:     big.NewInt(3030),
		TotalPayment: big.NewInt(4040),
		NumPayments:  2,
	}
	er.RetrievalSuccess(id, time.Now(), testCid1, storageProviderId, stats)

	qt.Assert(t, req, qt.IsNotNil)

	pathSegments := strings.Split(path, "/")
	qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", storageProviderId.String()})

	timeNode, err := req.LookupByString("time")
	qt.Assert(t, err, qt.IsNil)
	timeStr, err := timeNode.AsString()
	qt.Assert(t, err, qt.IsNil)

	// expect something like this:
	// {
	// 	"askPrice":"0",
	// 	"cid":{"/":"bafkreih65ouyvwap3nx2iv5ff7t6aij45zimnxld7ynbfy25koagya5nyi"},
	// 	"instanceId":"test-instance",
	// 	"numPayments":0,
	// 	"size":262144,
	// 	"stage":"success",
	// 	"time":"2022-07-15T05:12:20.334653376Z",
	// 	"totalPayment":"0"
	// }

	expectedMessage, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "askPrice", qp.String("3030"))
		qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
		qp.MapEntry(ma, "instanceId", qp.String("test-instance"))
		qp.MapEntry(ma, "numPayments", qp.Int(2))
		qp.MapEntry(ma, "size", qp.Int(2020))
		qp.MapEntry(ma, "stage", qp.String("success"))
		qp.MapEntry(ma, "time", qp.String(timeStr))
		qp.MapEntry(ma, "totalPayment", qp.String("4040"))
	})
	qt.Assert(t, err, qt.IsNil)

	equal := ipld.DeepEqual(req, expectedMessage)
	if !equal {
		t.Logf("\nwant: %s\n got: %s", mustDagJson(expectedMessage), mustDagJson(req))
	}
	qt.Assert(t, equal, qt.IsTrue)
}

func TestEventRecorder_Progress(t *testing.T) {
	var req datamodel.Node
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		path = r.URL.Path
	}))
	defer ts.Close()

	er := eventrecorder.NewEventRecorder("test-instance", ts.URL)
	id, err := uuid.NewRandom()
	qt.Assert(t, err, qt.IsNil)
	storageProviderId := peer.NewPeerRecord().PeerID
	er.RetrievalProgress(id, time.Now(), testCid1, storageProviderId, rep.RetrievalEventFirstByte)

	qt.Assert(t, req, qt.IsNotNil)

	pathSegments := strings.Split(path, "/")
	qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", storageProviderId.String()})

	timeNode, err := req.LookupByString("time")
	qt.Assert(t, err, qt.IsNil)
	timeStr, err := timeNode.AsString()
	qt.Assert(t, err, qt.IsNil)

	// expect something like this:
	// {
	// 	"cid":{"/":"bafkreih65ouyvwap3nx2iv5ff7t6aij45zimnxld7ynbfy25koagya5nyi"},
	// 	"instanceId":"test-instance",
	// 	"stage":"first-byte-received",
	// 	"time":"2022-07-15T05:12:20.331482725Z"
	// }

	expectedMessage,
		err := qp.BuildMap(basicnode.Prototype.Any,
		-1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
			qp.MapEntry(ma, "instanceId", qp.String("test-instance"))
			qp.MapEntry(ma, "stage", qp.String("first-byte-received"))
			qp.MapEntry(ma, "time", qp.String(timeStr))
		})
	qt.Assert(t, err, qt.IsNil)

	equal := ipld.DeepEqual(req, expectedMessage)
	if !equal {
		t.Logf("\nwant: %s\n got: %s", mustDagJson(expectedMessage), mustDagJson(req))
	}
	qt.Assert(t, equal, qt.IsTrue)
}

func TestEventRecorder_Start(t *testing.T) {
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

			er := eventrecorder.NewEventRecorder("test-instance", ts.URL)
			id, err := uuid.NewRandom()
			qt.Assert(t, err, qt.IsNil)
			storageProviderId := peer.NewPeerRecord().PeerID
			startTime := time.Now().Add(time.Duration(-500))
			er.RetrievalStart(id, startTime, testCid1, rootCid, storageProviderId)

			qt.Assert(t, req, qt.IsNotNil)

			pathSegments := strings.Split(path, "/")
			qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", storageProviderId.String()})

			// expect something like this:
			// {
			// 	"cid":{"/":"bafkreih65ouyvwap3nx2iv5ff7t6aij45zimnxld7ynbfy25koagya5nyi"},
			// 	"rootCid":{"/":"bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk"},
			// 	"instanceId":"test-instance",
			// 	"stage":"start",
			// 	"time":"2022-07-15T05:12:10.088756251Z"
			// }

			expectedMessage,
				err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
				qp.MapEntry(ma, "instanceId", qp.String("test-instance"))
				if !testCid1.Equals(rootCid) {
					qp.MapEntry(ma, "rootCid", qp.Link(cidlink.Link{Cid: rootCid}))
				}
				qp.MapEntry(ma, "stage", qp.String("start"))
				qp.MapEntry(ma, "time", qp.String(startTime.Format(time.RFC3339Nano)))
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
	var req datamodel.Node
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		path = r.URL.Path
	}))
	defer ts.Close()

	er := eventrecorder.NewEventRecorder("test-instance", ts.URL)
	id, err := uuid.NewRandom()
	qt.Assert(t, err, qt.IsNil)
	storageProviderId := peer.NewPeerRecord().PeerID
	err = errors.New("some error message here")

	er.RetrievalFailure(id, time.Now(), testCid1, storageProviderId, err)

	// expect something like this:
	// {
	//		"event":{"cid":{"/":"bafybeibk5afevfoe3bislt3minhwg52htzmgi356yyrhcvv2lnyzg5cjgi"},
	//		"error":"retrieval failed: data transfer failed: deal rejected: miner is not accepting online retrieval deals",
	//		"instanceId":"test-instance",
	//		"stage":"failure","time":"2022-07-15T05:36:04.218007373Z"
	// }

	qt.Assert(t, req, qt.IsNotNil)

	pathSegments := strings.Split(path, "/")
	qt.Assert(t, pathSegments, qt.DeepEquals, []string{"", "retrieval-event", id.String(), "providers", storageProviderId.String()})

	timeNode, err := req.LookupByString("time")
	qt.Assert(t, err, qt.IsNil)
	timeStr, err := timeNode.AsString()
	qt.Assert(t, err, qt.IsNil)

	expectedMessage, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "cid", qp.Link(cidlink.Link{Cid: testCid1}))
		qp.MapEntry(ma, "error", qp.String("some error message here"))
		qp.MapEntry(ma, "instanceId", qp.String("test-instance"))
		qp.MapEntry(ma, "stage", qp.String("failure"))
		qp.MapEntry(ma, "time", qp.String(timeStr))
	})
	qt.Assert(t, err, qt.IsNil)

	equal := ipld.DeepEqual(req, expectedMessage)
	if !equal {
		t.Logf("\nwant: %s\n got: %s", mustDagJson(expectedMessage), mustDagJson(req))
	}
	qt.Assert(t, equal, qt.IsTrue)
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
