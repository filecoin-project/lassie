package session

import (
	"time"

	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var log = logging.Logger("lassie/session")

type State interface {
	RecordFailure(storageProviderId peer.ID, retrievalId types.RetrievalID) error
	RemoveStorageProviderFromRetrieval(storageProviderId peer.ID, retrievalId types.RetrievalID) error
	IsSuspended(storageProviderId peer.ID) bool
	GetConcurrency(storageProviderId peer.ID) uint
	AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error
	EndRetrieval(retrievalId types.RetrievalID) error
	RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool
}

type Session struct {
	State
	config Config
}

func NewSession(config Config, withState bool) *Session {
	var state State
	if withState {
		state = newSpTracker(nil)
	} else {
		state = &nilstate{}
	}
	return &Session{
		State:  state,
		config: config,
	}
}
func (s *Session) GetStorageProviderTimeout(storageProviderId peer.ID) time.Duration {
	return s.config.getMinerConfig(storageProviderId).RetrievalTimeout
}

// isAcceptableStorageProvider checks whether the storage provider in question
// is acceptable as a retrieval candidate. It checks the blacklists and
// whitelists, the miner monitor for failures and whether we are already at
// concurrency limit for this SP.
func (s *Session) FilterIndexerCandidate(candidate types.RetrievalCandidate) (bool, types.RetrievalCandidate) {
	protocols := candidate.Metadata.Protocols()
	newProtocolMetadata := make([]metadata.Protocol, 0, len(protocols))
	for _, protocol := range protocols {
		includeProtocol := true
		switch protocol {
		case multicodec.TransportGraphsyncFilecoinv1:
			includeProtocol = s.isAcceptableGraphsyncCandidate(candidate.MinerPeer.ID)
		case multicodec.TransportBitswap:
			includeProtocol = s.isAcceptableBitswapCandidate(candidate.MinerPeer.ID)
		}
		if includeProtocol {
			newProtocolMetadata = append(newProtocolMetadata, candidate.Metadata.Get(protocol))
		}
	}
	return len(newProtocolMetadata) != 0, types.RetrievalCandidate{
		MinerPeer: candidate.MinerPeer,
		RootCid:   candidate.RootCid,
		Metadata:  metadata.Default.New(newProtocolMetadata...),
	}
}

func (s *Session) isAcceptableGraphsyncCandidate(storageProviderId peer.ID) bool {
	// Skip blacklist
	if s.config.ProviderBlockList[storageProviderId] {
		return false
	}

	// Skip non-whitelist IF the whitelist isn't empty
	if len(s.config.ProviderAllowList) > 0 && !s.config.ProviderAllowList[storageProviderId] {
		return false
	}

	// Skip suspended SPs from the minerMonitor
	if s.State.IsSuspended(storageProviderId) {
		return false
	}

	// Skip if we are currently at our maximum concurrent retrievals for this SP
	// since we likely won't be able to retrieve from them at the moment even if
	// query is successful
	minerConfig := s.config.getMinerConfig(storageProviderId)
	if minerConfig.MaxConcurrentRetrievals > 0 &&
		s.State.GetConcurrency(storageProviderId) >= minerConfig.MaxConcurrentRetrievals {
		return false
	}

	return true
}

func (state *Session) isAcceptableBitswapCandidate(storageProviderId peer.ID) bool {
	// Skip blacklist
	if state.config.ProviderBlockList[storageProviderId] {
		return false
	}

	// Skip non-whitelist IF the whitelist isn't empty
	if len(state.config.ProviderAllowList) > 0 && !state.config.ProviderAllowList[storageProviderId] {
		return false
	}

	return true
}

// IsAcceptableQueryResponse determines whether a queryResponse is acceptable
// according to the current configuration. For now this is just checking whether
// PaidRetrievals is set and not accepting paid retrievals if so.
func (s *Session) IsAcceptableQueryResponse(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievaltypes.QueryResponse) bool {
	// filter out paid retrievals if necessary

	acceptable := s.config.PaidRetrievals || big.Add(big.Mul(queryResponse.MinPricePerByte, big.NewIntUnsigned(queryResponse.Size)), queryResponse.UnsealPrice).Equals(big.Zero())
	if !acceptable {
		log.Debugf("skipping query response from %s for %s: paid retrieval not allowed", peer, req.Cid)
		s.State.RemoveStorageProviderFromRetrieval(peer, req.RetrievalID)
	}
	return acceptable
}
