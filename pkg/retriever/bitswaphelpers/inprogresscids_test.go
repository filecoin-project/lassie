package bitswaphelpers_test

import (
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-test/random"
	"github.com/stretchr/testify/require"
)

func TestInProgressCids(t *testing.T) {
	req := require.New(t)
	cids := random.Cids(3)
	retrievalIDs := testutil.GenerateRetrievalIDs(t, 3)

	inProgressCids := bitswaphelpers.NewInProgressCids()
	// add some values
	inProgressCids.Inc(cids[0], retrievalIDs[0])
	inProgressCids.Inc(cids[0], retrievalIDs[1])
	inProgressCids.Inc(cids[1], retrievalIDs[0])
	inProgressCids.Inc(cids[1], retrievalIDs[2])
	inProgressCids.Inc(cids[2], retrievalIDs[1])
	inProgressCids.Inc(cids[2], retrievalIDs[2])

	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0], retrievalIDs[1]}, inProgressCids.Get(cids[0]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0], retrievalIDs[2]}, inProgressCids.Get(cids[1]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[1], retrievalIDs[2]}, inProgressCids.Get(cids[2]))

	// add some overlapping refs, then remove
	inProgressCids.Inc(cids[0], retrievalIDs[0])
	inProgressCids.Inc(cids[1], retrievalIDs[0])
	inProgressCids.Inc(cids[1], retrievalIDs[2])
	inProgressCids.Inc(cids[2], retrievalIDs[2])
	inProgressCids.Dec(cids[0], retrievalIDs[0])
	inProgressCids.Dec(cids[0], retrievalIDs[1])
	inProgressCids.Dec(cids[1], retrievalIDs[0])
	inProgressCids.Dec(cids[1], retrievalIDs[2])
	inProgressCids.Dec(cids[2], retrievalIDs[1])
	inProgressCids.Dec(cids[2], retrievalIDs[2])

	// only those with 0 refs should be gone
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0]}, inProgressCids.Get(cids[0]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0], retrievalIDs[2]}, inProgressCids.Get(cids[1]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[2]}, inProgressCids.Get(cids[2]))

	// wipe remaining
	inProgressCids.Dec(cids[0], retrievalIDs[0])
	inProgressCids.Dec(cids[1], retrievalIDs[0])
	inProgressCids.Dec(cids[1], retrievalIDs[2])
	inProgressCids.Dec(cids[2], retrievalIDs[2])
	// everything should be empty now
	req.ElementsMatch([]types.RetrievalID{}, inProgressCids.Get(cids[0]))
	req.ElementsMatch([]types.RetrievalID{}, inProgressCids.Get(cids[1]))
	req.ElementsMatch([]types.RetrievalID{}, inProgressCids.Get(cids[2]))

	// add back valus (but different cids)
	inProgressCids.Inc(cids[2], retrievalIDs[0])
	inProgressCids.Inc(cids[2], retrievalIDs[1])
	inProgressCids.Inc(cids[1], retrievalIDs[0])
	inProgressCids.Inc(cids[1], retrievalIDs[2])
	inProgressCids.Inc(cids[0], retrievalIDs[1])
	inProgressCids.Inc(cids[0], retrievalIDs[2])

	// verify matches
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0], retrievalIDs[1]}, inProgressCids.Get(cids[2]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[0], retrievalIDs[2]}, inProgressCids.Get(cids[1]))
	req.ElementsMatch([]types.RetrievalID{retrievalIDs[1], retrievalIDs[2]}, inProgressCids.Get(cids[0]))
}
