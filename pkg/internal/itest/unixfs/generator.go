package unixfs

import (
	"bytes"
	"crypto/rand"
	"io"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs/namegen"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func cidCollector(ls *ipld.LinkSystem, cids *[]cid.Cid) (ipld.BlockWriteOpener, func()) {
	swo := ls.StorageWriteOpener
	return func(linkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
			w, c, err := swo(linkCtx)
			if err != nil {
				return nil, nil, err
			}
			return w, func(lnk ipld.Link) error {
				*cids = append(*cids, lnk.(cidlink.Link).Cid)
				return c(lnk)
			}, nil
		}, func() {
			// reset
			ls.StorageWriteOpener = swo
		}
}

func GenerateFile(t *testing.T, linkSys *linking.LinkSystem, randReader io.Reader, size int) DirEntry {
	// a file of `size` random bytes, packaged into unixfs DAGs, stored in the remote blockstore
	delimited := io.LimitReader(randReader, int64(size))
	var buf bytes.Buffer
	buf.Grow(size)
	delimited = io.TeeReader(delimited, &buf)
	// "size-256144" sets the chunker, splitting bytes at 256144b boundaries
	cids := make([]cid.Cid, 0)
	var undo func()
	linkSys.StorageWriteOpener, undo = cidCollector(linkSys, &cids)
	defer undo()
	root, gotSize, err := builder.BuildUnixFSFile(delimited, "size-256144", linkSys)
	require.NoError(t, err)
	srcData := buf.Bytes()
	rootCid := root.(cidlink.Link).Cid
	return DirEntry{
		Path:     "",
		Content:  srcData,
		Root:     rootCid,
		SelfCids: cids,
		TSize:    uint64(gotSize),
	}
}

func rndInt(randReader io.Reader, max int) int {
	coin, err := rand.Int(randReader, big.NewInt(int64(max)))
	if err != nil {
		return 0 // eh, whatever
	}
	return int(coin.Int64())
}

func GenerateDirectory(t *testing.T, linkSys *linking.LinkSystem, randReader io.Reader, targetSize int, rootSharded bool) DirEntry {
	return GenerateDirectoryFrom(t, linkSys, randReader, targetSize, "", rootSharded)
}

func GenerateDirectoryFrom(t *testing.T,
	linkSys *linking.LinkSystem,
	randReader io.Reader,
	targetSize int,
	dir string,
	sharded bool,
) DirEntry {
	var curSize int
	targetFileSize := targetSize / 16
	children := make([]DirEntry, 0)
	for curSize < targetSize {
		switch rndInt(randReader, 6) {
		case 0: // 1 in 6 chance of finishing this directory if not at root
			if dir != "" && len(children) > 0 {
				curSize = targetSize // not really, but we're done with this directory
			} // else at the root we don't get to finish early
		case 1: // 1 in 6 chance of making a new directory
			if targetSize-curSize <= 1024 { // don't make tiny directories
				continue
			}
			var newDir string
			for {
				var err error
				newDir, err = namegen.RandomDirectoryName(randReader)
				require.NoError(t, err)
				if !dupeName(children, newDir) {
					break
				}
			}
			child := GenerateDirectoryFrom(t, linkSys, randReader, targetSize-curSize, dir+"/"+newDir, false)
			children = append(children, child)
			curSize += int(child.TSize)
		default: // 4 in 6 chance of making a new file
			var size int
			for size == 0 { // don't make empty files
				sizeB, err := rand.Int(randReader, big.NewInt(int64(targetFileSize)))
				require.NoError(t, err)
				size = int(sizeB.Int64())
				if size > targetSize-curSize {
					size = targetSize - curSize
				}
			}
			entry := GenerateFile(t, linkSys, randReader, size)
			var name string
			for {
				var err error
				name, err = namegen.RandomFileName(randReader)
				require.NoError(t, err)
				if !dupeName(children, name) {
					break
				}
			}
			entry.Path = dir + "/" + name
			curSize += size
			children = append(children, entry)
		}
	}
	dirEntry := BuildDirectory(t, linkSys, children, sharded)
	dirEntry.Path = dir
	return dirEntry
}

func dupeName(children []DirEntry, name string) bool {
	for _, child := range children {
		if strings.HasSuffix(child.Path, "/"+name) {
			return true
		}
	}
	return false
}

func BuildDirectory(t *testing.T, linkSys *linking.LinkSystem, children []DirEntry, sharded bool) DirEntry {
	// create stable sorted children, which should match the encoded form
	// in dag-pb
	sort.Slice(children, func(i, j int) bool {
		return strings.Compare(children[i].Path, children[j].Path) < 0
	})

	dirLinks := make([]dagpb.PBLink, 0)
	for _, child := range children {
		paths := strings.Split(child.Path, "/")
		name := paths[len(paths)-1]
		lnk, err := builder.BuildUnixFSDirectoryEntry(name, int64(child.TSize), cidlink.Link{Cid: child.Root})
		require.NoError(t, err)
		dirLinks = append(dirLinks, lnk)
	}
	cids := make([]cid.Cid, 0)
	var undo func()
	linkSys.StorageWriteOpener, undo = cidCollector(linkSys, &cids)
	defer undo()
	var root ipld.Link
	var size uint64
	var err error
	if sharded {
		// node arity of 16, quite small to increase collision probability so we actually get sharding
		const width = 16
		const hasher = multihash.MURMUR3X64_64
		root, size, err = builder.BuildUnixFSShardedDirectory(width, hasher, dirLinks, linkSys)
		require.NoError(t, err)
	} else {
		root, size, err = builder.BuildUnixFSDirectory(dirLinks, linkSys)
		require.NoError(t, err)
	}

	return DirEntry{
		Path:     "",
		Root:     root.(cidlink.Link).Cid,
		SelfCids: cids,
		TSize:    size,
		Children: children,
	}
}
