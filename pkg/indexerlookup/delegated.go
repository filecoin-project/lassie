package indexerlookup

import (
	"encoding/json"
	"fmt"

	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// DelegatedRoutingResponse represents the HTTP Routing V1 API response format
// See: https://specs.ipfs.tech/routing/http-routing-v1/#get-routing-v1-providers-cid
type DelegatedRoutingResponse struct {
	Providers []DelegatedProvider `json:"Providers"`
}

// DelegatedProvider represents a single provider in the delegated routing response
type DelegatedProvider struct {
	Schema    string                 `json:"Schema"`
	ID        string                 `json:"ID"`
	Addrs     []string               `json:"Addrs,omitempty"`
	Protocols []string               `json:"Protocols,omitempty"`
	Metadata  map[string]interface{} `json:"-"` // Capture all additional fields
}

// UnmarshalJSON implements custom JSON unmarshaling to capture protocol-specific metadata
func (dp *DelegatedProvider) UnmarshalJSON(data []byte) error {
	// First unmarshal into a map to capture all fields
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Unmarshal known fields
	if schemaRaw, ok := raw["Schema"]; ok {
		json.Unmarshal(schemaRaw, &dp.Schema)
	}
	if idRaw, ok := raw["ID"]; ok {
		json.Unmarshal(idRaw, &dp.ID)
	}
	if addrsRaw, ok := raw["Addrs"]; ok {
		json.Unmarshal(addrsRaw, &dp.Addrs)
	}
	if protocolsRaw, ok := raw["Protocols"]; ok {
		json.Unmarshal(protocolsRaw, &dp.Protocols)
	}

	// Capture protocol-specific metadata (any field that's not a known field)
	dp.Metadata = make(map[string]interface{})
	knownFields := map[string]bool{
		"Schema": true, "ID": true, "Addrs": true, "Protocols": true,
	}
	for key, val := range raw {
		if !knownFields[key] {
			var v interface{}
			json.Unmarshal(val, &v)
			dp.Metadata[key] = v
		}
	}

	return nil
}

// ToAddrInfo converts the delegated provider to a libp2p peer.AddrInfo
func (dp *DelegatedProvider) ToAddrInfo() (*peer.AddrInfo, error) {
	peerID, err := peer.Decode(dp.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode peer ID %s: %w", dp.ID, err)
	}

	var addrs []multiaddr.Multiaddr
	for _, addrStr := range dp.Addrs {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			logger.Debugw("Failed to parse multiaddr, skipping", "addr", addrStr, "err", err)
			continue
		}
		addrs = append(addrs, addr)
	}

	return &peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}, nil
}

// ToMetadata converts the delegated provider's protocol information to metadata.Metadata
func (dp *DelegatedProvider) ToMetadata() (metadata.Metadata, error) {
	if len(dp.Protocols) == 0 {
		return metadata.Metadata{}, fmt.Errorf("no protocols specified")
	}

	var protocols []metadata.Protocol

	for _, protoName := range dp.Protocols {
		switch protoName {
		case "transport-ipfs-gateway-http":
			// HTTP gateway protocol has no additional metadata beyond the protocol ID
			protocols = append(protocols, metadata.IpfsGatewayHttp{})

		default:
			logger.Debugw("Unknown protocol, skipping", "protocol", protoName)
			continue
		}
	}

	if len(protocols) == 0 {
		return metadata.Metadata{}, fmt.Errorf("no supported protocols found")
	}

	return metadata.Default.New(protocols...), nil
}
