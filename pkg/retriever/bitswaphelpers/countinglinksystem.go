package bitswaphelpers

import (
	"io"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
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

func NewByteCountingLinkSystem(lsys *linking.LinkSystem, bytesWritten func(count uint64)) *linking.LinkSystem {
	newLsys := *lsys // copy all values from old system
	oldWriteOpener := lsys.StorageWriteOpener
	newLsys.StorageWriteOpener = func(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		w, committer, err := oldWriteOpener(lctx)
		if err != nil {
			return w, committer, err
		}
		ccw := &cumulativeCountWriter{w, 0, committer, bytesWritten}
		return ccw, ccw.Commit, err
	}
	return &newLsys
}
