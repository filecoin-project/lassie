package events

import (
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

// Identifier returns the peer ID of the storage provider if this retrieval was
// requested via peer ID, or the string "Bitswap" if this retrieval was
// requested via the Bitswap protocol
func Identifier(evt types.RetrievalEvent) string {
	spEvent, spOk := evt.(EventWithSPID)
	if spOk && spEvent.StorageProviderId() != peer.ID("") {
		return spEvent.StorageProviderId().String()
	}
	// we only want to return "Bitswap" if this is an event with a storage provider id using the bitswap protocol
	protocolEvent, pOk := evt.(EventWithProtocol)
	if spOk && pOk && protocolEvent.Protocol() == multicodec.TransportBitswap {
		return types.BitswapIndentifier
	}
	return ""
}

func collectProtocols(candidates []types.RetrievalCandidate) []multicodec.Code {
	allProtocols := make(map[multicodec.Code]struct{})
	for _, candidate := range candidates {
		for _, protocol := range candidate.Metadata.Protocols() {
			allProtocols[protocol] = struct{}{}
		}
	}
	allProtocolsArr := make([]multicodec.Code, 0, len(allProtocols))
	for protocol := range allProtocols {
		allProtocolsArr = append(allProtocolsArr, protocol)
	}
	return allProtocolsArr
}
