package storage

import (
	"context"
	"io"

	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type DuplicateAdder struct {
	ctx      context.Context
	cancel   context.CancelFunc
	root     cid.Cid
	selector ipld.Node
	*DeferredCarWriter
	outgoing *DeferredCarWriter
}

func NewDuplicateAdderCarForStream(ctx context.Context, root cid.Cid, selector ipld.Node, store *DeferredStorageCar, outStream io.Writer) {

}

func (da *DuplicateAdder) addDupes(ctx context.Context) error {

	sel, err := selector.CompileSelector(da.selector)
	if err != nil {
		return nil, err
	}

	cfg := verifiedcar.Config{
		Root:     da.root,
		Selector: sel,
	}

	blockCount, byteCount, err := cfg.Verify(ctx, rdr, retrieval.request.LinkSystem)
	if err != nil {
		return nil, err
	}

}

// OnPut will call a callback when each Put() operation is started. The argument
// to the callback is the number of bytes being written. If once is true, the
// callback will be removed after the first call.
func (da *DuplicateAdder) OnPut(cb func(int), once bool) {

}

// Close closes the underlying file, if one was created.
func (dcw *DeferredCarWriter) Close() error {

}

// BlockWriteOpener returns a BlockWriteOpener that operates on this storage.
func (da *DuplicateAdder) BlockWriteOpener() linking.BlockWriteOpener {
	return da.incoming.BlockWriteOpener()
}
