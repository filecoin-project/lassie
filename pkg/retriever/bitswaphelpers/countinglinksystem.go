package bitswaphelpers

import (
	"io"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/libp2p/go-libp2p/core/peer"
)

type cumulativeCountWriter struct {
	io.Writer
	totalWritten uint64
	committer    linking.BlockWriteCommitter
	cb           func(count uint64)
}

func (ccw *cumulativeCountWriter) Write(p []byte) (n int, err error) {
	written, err := ccw.Writer.Write(p)
	if err != nil {
		return written, err
	}
	ccw.totalWritten += uint64(written)
	return written, nil
}

func (ccw *cumulativeCountWriter) Commit(link datamodel.Link) error {
	err := ccw.committer(link)
	if err != nil {
		return err
	}
	ccw.cb(ccw.totalWritten)
	return nil
}

func NewByteCountingLinkSystem(lsys *linking.LinkSystem, blockWritten func(from *peer.ID, count uint64)) *linking.LinkSystem {
	newLsys := *lsys // copy all values from old system
	oldWriteOpener := lsys.StorageWriteOpener
	newLsys.StorageWriteOpener = func(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		var from *peer.ID = nil
		if p, ok := lctx.Ctx.Value(peerIdContextKey).(peer.ID); ok {
			from = &p
		}
		w, committer, err := oldWriteOpener(lctx)
		if err != nil {
			return w, committer, err
		}
		ccw := &cumulativeCountWriter{w, 0, committer, func(count uint64) { blockWritten(from, count) }}
		return ccw, ccw.Commit, err
	}
	return &newLsys
}
