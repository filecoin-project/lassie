package events

import (
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

// Identifier returns the HTTP endpoint of the storage provider
func Identifier(evt types.RetrievalEvent) string {
	if epEvent, ok := evt.(EventWithEndpoint); ok {
		return epEvent.Endpoint()
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
