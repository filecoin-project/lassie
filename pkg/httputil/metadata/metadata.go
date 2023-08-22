package metadata

import (
	"fmt"
	"io"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	mh "github.com/multiformats/go-multihash"

	_ "embed"
)

//go:embed metadata.ipldsch
var schema []byte

var BindnodeRegistry = bindnoderegistry.NewRegistry()

type CarMetadata struct {
	Metadata *Metadata
}

func (cm CarMetadata) Serialize(w io.Writer) error {
	// TODO: do the same checks we do on Deserialize()
	return BindnodeRegistry.TypeToWriter(&cm, w, dagjson.Encode)
}

func (cm *CarMetadata) Deserialize(r io.Reader) error {
	cmIface, err := BindnodeRegistry.TypeFromReader(r, &CarMetadata{}, dagjson.Decode)
	if err != nil {
		return fmt.Errorf("invalid CarMetadata: %w", err)
	}
	cmm := cmIface.(*CarMetadata) // safe to assume type
	if cmm.Metadata.Properties == nil && cmm.Metadata.Error == nil {
		return fmt.Errorf("invalid CarMetadata: must contain either properties or error fields")
	}
	if (cmm.Metadata.Properties == nil) == (cmm.Metadata.Error == nil) {
		return fmt.Errorf("invalid CarMetadata: must contain either properties or error fields, not both")
	}
	if cmm.Metadata.Properties != nil {
		if _, err := mh.Decode(cmm.Metadata.Properties.ChecksumMultihash); err != nil {
			return fmt.Errorf("invalid CarMetadata: checksum multihash: %w", err)
		}
	}
	// TODO: parse and check EntityBytes format
	*cm = *cmm
	return nil
}

type Metadata struct {
	Request    Request
	Properties *types.CarProperties
	Error      *string
}

type Request struct {
	Root        cid.Cid
	Path        *string
	Scope       types.DagScope
	Duplicates  bool
	EntityBytes *string
}

func init() {
	if err := BindnodeRegistry.RegisterType((*CarMetadata)(nil), string(schema), "CarMetadata"); err != nil {
		panic(err.Error())
	}
}
