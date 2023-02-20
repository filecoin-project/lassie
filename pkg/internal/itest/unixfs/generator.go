package unixfs

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
	"math/big"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func GenerateFile(t *testing.T, linkSys *linking.LinkSystem, randReader io.Reader, size int) (cid.Cid, []byte) {
	// a file of `size` random bytes, packaged into unixfs DAGs, stored in the remote blockstore
	delimited := io.LimitReader(randReader, int64(size))
	var buf bytes.Buffer
	buf.Grow(size)
	delimited = io.TeeReader(delimited, &buf)
	// "size-256144" sets the chunker, splitting bytes at 256144b boundaries
	root, _, err := builder.BuildUnixFSFile(delimited, "size-256144", linkSys)
	require.NoError(t, err)
	srcData := buf.Bytes()
	rootCid := root.(cidlink.Link).Cid
	return rootCid, srcData
}

func fileName(randReader io.Reader) (string, error) {
	for {
		length, err := rand.Int(randReader, big.NewInt(63))
		if err != nil {
			return "", err
		}
		name := make([]byte, length.Int64()+1)
		_, err = randReader.Read(name)
		if err != nil {
			return "", err
		}

		nameStr := strings.Replace(base64.RawStdEncoding.EncodeToString(name), "/", "", -1)
		if len(nameStr) > 0 {
			return nameStr, nil
		}
	}
}

func rndInt(randReader io.Reader, max int) int {
	coin, err := rand.Int(randReader, big.NewInt(int64(max)))
	if err != nil {
		return 0 // eh, whatever
	}
	return int(coin.Int64())
}

func GenerateDirectory(t *testing.T, linkSys *linking.LinkSystem, randReader io.Reader, targetSize int, rootSharded bool) (cid.Cid, []DirEntry) {
	root, _, entries := generateDirectoryRecursive(t, linkSys, randReader, targetSize, "", rootSharded)
	return root, entries
}

func generateDirectoryRecursive(t *testing.T,
	linkSys *linking.LinkSystem,
	randReader io.Reader,
	targetSize int,
	dir string,
	sharded bool,
) (cid.Cid, int, []DirEntry) {

	var curSize int
	targetFileSize := targetSize / 25
	dirLinks := make([]dagpb.PBLink, 0)
	entries := make([]DirEntry, 0)
	for curSize < targetSize {
		switch rndInt(randReader, 8) {
		case 0:
			if dir != "" {
				curSize = targetSize // not really, but we're done with this directory
			} // else at the root we don't get to finish early
		case 1:
			// make a new directory
			newDir, err := fileName(randReader)
			require.NoError(t, err)
			childRoot, childSize, childEntries := generateDirectoryRecursive(t, linkSys, randReader, targetSize, dir+"/"+newDir, false)
			entries = append(entries, childEntries...)
			lnk, err := builder.BuildUnixFSDirectoryEntry(newDir, int64(childSize), cidlink.Link{Cid: childRoot})
			require.NoError(t, err)
			dirLinks = append(dirLinks, lnk)
			curSize += childSize
		default:
			// make a new file
			sizeB, err := rand.Int(randReader, big.NewInt(int64(targetFileSize)))
			require.NoError(t, err)
			size := int(sizeB.Int64())
			if size > targetSize-curSize {
				size = targetSize - curSize
			}
			root, byts := GenerateFile(t, linkSys, randReader, size)
			name, err := fileName(randReader)
			require.NoError(t, err)
			entry := DirEntry{
				Path:    dir + "/" + name,
				Content: byts,
				Cid:     root,
			}
			curSize += size
			entries = append(entries, entry)
			lnk, err := builder.BuildUnixFSDirectoryEntry(name, int64(size), cidlink.Link{Cid: root})
			require.NoError(t, err)
			dirLinks = append(dirLinks, lnk)
		}
	}
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
	return root.(cidlink.Link).Cid, int(size), entries
}
