package retriever_test

import (
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/retriever"
	qt "github.com/frankban/quicktest"
	"github.com/ipfs/go-cid"
)

var testCid1 cid.Cid = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")
var testCid2 cid.Cid = mustCid("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")

func TestActiveRetrievalsManager_GetStatusFor(t *testing.T) {
	arm := retriever.NewActiveRetrievalsManager()
	id, err := arm.New(testCid1, 1, 1)
	qt.Assert(t, err, qt.IsNil)

	sid, scid, sqtime, has := arm.GetStatusFor(testCid1, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, time.Since(sqtime).Truncate(time.Millisecond).Milliseconds(), qt.Equals, int64(0))

	_, _, _, has = arm.GetStatusFor(testCid2, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsFalse)

	arm.QueryCandidatedFinished(testCid1)
	_, _, stime, has := arm.GetStatusFor(testCid1, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, stime, qt.Equals, sqtime) // should be identical to the first call, we're still in query phase

	_, _, stime, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, stime, qt.Equals, time.Time{}) // haven't started retrieval phase yet

	// start retrieval phase
	time.Sleep(20 * time.Millisecond)
	arm.SetRetrievalCandidateCount(testCid1, 1)
	sid, scid, srtime, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, time.Since(srtime).Truncate(time.Millisecond*5).Milliseconds(), qt.Equals, int64(0)) // have to be very lax here thanks to windows
	qt.Assert(t, srtime, qt.Not(qt.Equals), sqtime)                                                   // different phase start time

	_, _, stime, has = arm.GetStatusFor(testCid1, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, stime, qt.Equals, sqtime) // should still be the same as original query phase time

	_, _, rtime, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, rtime, qt.Equals, srtime) // should match the original retrieval phase start time we got

	// set a retrieval candidate with a different root CID
	err = arm.SetRetrievalCandidate(testCid1, testCid2, "", 0)
	qt.Assert(t, err, qt.IsNil)

	// statuses with the testCid1 are still the same
	_, _, stime, has = arm.GetStatusFor(testCid1, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, stime, qt.Equals, sqtime)

	_, _, rtime, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, rtime, qt.Equals, srtime)

	// statuses with the testCid2 should also work now
	_, _, stime, has = arm.GetStatusFor(testCid2, eventpublisher.QueryPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, stime, qt.Equals, sqtime)

	_, _, rtime, has = arm.GetStatusFor(testCid2, eventpublisher.RetrievalPhase)
	qt.Assert(t, has, qt.IsTrue)
	qt.Assert(t, sid, qt.Equals, id)
	qt.Assert(t, scid, qt.Equals, testCid1)
	qt.Assert(t, rtime, qt.Equals, srtime)
}

func TestActiveRetrievalsManager_NoRetrievalPhase(t *testing.T) {
	t.Run("in-order", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 1, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 0) // should trigger a clean-up
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})

	t.Run("out-of-order", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 1, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.SetRetrievalCandidateCount(testCid1, 0)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.QueryCandidatedFinished(testCid1) // should trigger a clean-up
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})
}

func TestActiveRetrievalsManager_BothPhases(t *testing.T) {
	t.Run("single-each-success", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 1, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 1)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.RetrievalCandidatedFinished(testCid1, true)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})

	// should work identical to the single-each-success case
	t.Run("single-each-failure", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 1, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 1)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.RetrievalCandidatedFinished(testCid1, false)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})

	t.Run("multiple-each-failure", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 3, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		arm.QueryCandidatedFinished(testCid1)
		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 3)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.RetrievalCandidatedFinished(testCid1, false)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)
		arm.RetrievalCandidatedFinished(testCid1, false)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)
		arm.RetrievalCandidatedFinished(testCid1, false) // expect clean-up
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})

	t.Run("multiple-each-success", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 3, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		arm.QueryCandidatedFinished(testCid1)
		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 3)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.RetrievalCandidatedFinished(testCid1, false)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)
		arm.RetrievalCandidatedFinished(testCid1, true) // expect early clean-up
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})

	// unrealistic, but potential in a very racy situation
	t.Run("multiple-each-out-of-order", func(t *testing.T) {
		arm := retriever.NewActiveRetrievalsManager()
		_, err := arm.New(testCid1, 3, 1)
		qt.Assert(t, err, qt.IsNil)

		arm.QueryCandidatedFinished(testCid1)
		arm.QueryCandidatedFinished(testCid1)
		_, _, _, has := arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.SetRetrievalCandidateCount(testCid1, 3)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.RetrievalCandidatedFinished(testCid1, false)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)
		arm.RetrievalCandidatedFinished(testCid1, true)
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsTrue)

		arm.QueryCandidatedFinished(testCid1) // delayed clean-up
		_, _, _, has = arm.GetStatusFor(testCid1, eventpublisher.RetrievalPhase)
		qt.Assert(t, has, qt.IsFalse)
	})
}

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}
