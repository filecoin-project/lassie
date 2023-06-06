package session

import (
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

// Session and State both deal with per-storage provider data and usage
// questions. While State is responsible for gathering data of ongoing
// interactions with storage providers and making dynamic decisions based
// on that information, Session extends it and provides harder rules that
// arise from configuration and other static heuristics.
type Session struct {
	State
	config *Config
}

// NewSession constructs a new Session with the given config and with or
// without ongoing dynamic data collection. When withState is false, no
// dynamic data will be collected and decisions will be made solely based
// on the config.
func NewSession(config *Config, withState bool) *Session {
	if config == nil {
		config = DefaultConfig()
	}
	var state State
	if withState {
		state = NewSessionState(config)
	} else {
		state = nilstate{}
	}
	if config == nil {
		config = &Config{}
	}
	return &Session{state, config}
}

// GetStorageProviderTimeout returns the per-retrieval timeout from the
// RetrievalTimeout configuration option.
func (session *Session) GetStorageProviderTimeout(storageProviderId peer.ID) time.Duration {
	return session.config.getProviderConfig(storageProviderId).RetrievalTimeout
}

// FilterIndexerCandidate filters out protocols that are not acceptable for
// the given candidate. It returns a bool indicating whether the candidate
// should be considered at all, and a new candidate with the filtered
// protocols.
func (session *Session) FilterIndexerCandidate(candidate types.RetrievalCandidate) (bool, types.RetrievalCandidate) {
	if !session.isAcceptableCandidate(candidate.MinerPeer.ID) {
		return false, types.RetrievalCandidate{
			MinerPeer: candidate.MinerPeer,
			RootCid:   candidate.RootCid,
			Metadata:  metadata.Default.New(),
		}
	}

	protocols := candidate.Metadata.Protocols()
	newProtocolMetadata := make([]metadata.Protocol, 0, len(protocols))
	seen := make(map[multicodec.Code]struct{})
	for _, protocol := range protocols {
		if _, ok := seen[protocol]; ok {
			continue
		}
		seen[protocol] = struct{}{}
		if session.isAcceptableCandidateForProtocol(candidate.MinerPeer.ID, protocol) {
			newProtocolMetadata = append(newProtocolMetadata, candidate.Metadata.Get(protocol))
		}
	}
	return len(newProtocolMetadata) != 0, types.RetrievalCandidate{
		MinerPeer: candidate.MinerPeer,
		RootCid:   candidate.RootCid,
		Metadata:  metadata.Default.New(newProtocolMetadata...),
	}
}

func (session *Session) isAcceptableCandidate(storageProviderId peer.ID) bool {
	// if blacklisted, candidate is not acceptable
	if session.config.ProviderBlockList[storageProviderId] {
		return false
	}
	// if a whitelist exists and the candidate is not on it, candidate is not acceptable
	if len(session.config.ProviderAllowList) > 0 && !session.config.ProviderAllowList[storageProviderId] {
		return false
	}
	return true
}

func (session *Session) isAcceptableCandidateForProtocol(storageProviderId peer.ID, protocol multicodec.Code) bool {
	if protocol == multicodec.TransportBitswap || protocol == multicodec.TransportIpfsGatewayHttp {
		return true
	}

	// check if we are currently retrieving from the candidate with its maximum
	// concurrency
	minerConfig := session.config.getProviderConfig(storageProviderId)
	if minerConfig.MaxConcurrentRetrievals > 0 &&
		session.State.GetConcurrency(storageProviderId) >= minerConfig.MaxConcurrentRetrievals {
		return false
	}

	return true
}
