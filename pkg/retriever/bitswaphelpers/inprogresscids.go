package bitswaphelpers

import (
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

type InProgressCids struct {
	inProgressCidsLk sync.RWMutex
	inProgressCids   map[cid.Cid]map[types.RetrievalID]uint64
}

func NewInProgressCids() *InProgressCids {
	return &InProgressCids{
		inProgressCids: make(map[cid.Cid]map[types.RetrievalID]uint64),
	}
}

func (ipc *InProgressCids) Get(c cid.Cid) []types.RetrievalID {
	ipc.inProgressCidsLk.RLock()
	defer ipc.inProgressCidsLk.RUnlock()
	retrievalIDMap, ok := ipc.inProgressCids[c]
	if !ok {
		return nil
	}
	retrievalIDs := make([]types.RetrievalID, 0, len(retrievalIDMap))
	for retrievalID := range retrievalIDMap {
		retrievalIDs = append(retrievalIDs, retrievalID)
	}
	return retrievalIDs
}

func (ipc *InProgressCids) Inc(c cid.Cid, retrievalID types.RetrievalID) {
	ipc.inProgressCidsLk.Lock()
	defer ipc.inProgressCidsLk.Unlock()
	retrievalIDMap, ok := ipc.inProgressCids[c]
	if !ok {
		retrievalIDMap = retrievalMapPool.Get().(map[types.RetrievalID]uint64)
		ipc.inProgressCids[c] = retrievalIDMap
	}
	retrievalIDMap[retrievalID]++ // will start at zero value and set if not present
}

func (ipc *InProgressCids) Dec(c cid.Cid, retrievalID types.RetrievalID) {
	ipc.inProgressCidsLk.Lock()
	defer ipc.inProgressCidsLk.Unlock()
	retrievalIDMap, ok := ipc.inProgressCids[c]
	if !ok {
		return
	}
	current, ok := retrievalIDMap[retrievalID]
	if !ok {
		return
	}
	if current <= 1 {
		delete(retrievalIDMap, retrievalID)
		if len(retrievalIDMap) == 0 {
			delete(ipc.inProgressCids, c)
			retrievalMapPool.Put(retrievalIDMap)
		}
		return
	}
	retrievalIDMap[retrievalID] = current - 1
}

var retrievalMapPool = sync.Pool{
	New: func() any {
		return make(map[types.RetrievalID]uint64)
	},
}
