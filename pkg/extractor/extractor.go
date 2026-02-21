package extractor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

var logger = log.Logger("lassie/extractor")

func isIdentityCid(c cid.Cid) bool {
	return c.Prefix().MhType == multihash.IDENTITY
}

func extractIdentityData(c cid.Cid) ([]byte, error) {
	hash := c.Hash()
	decoded, err := multihash.Decode(hash)
	if err != nil {
		return nil, err
	}
	return decoded.Digest, nil
}

type FileProgressCallback func(path string, written, total int64)

// Extractor writes UnixFS content to disk as blocks arrive, without
// buffering entire files in memory. Handles chunked files, intermediate
// nodes, and out-of-order block arrival.
type Extractor struct {
	outputDir string
	mu        sync.RWMutex

	openFiles     map[cid.Cid]*fileWriter
	pathContext   map[cid.Cid]string
	fileRoot      map[cid.Cid]cid.Cid // chunk/intermediate CID -> file root CID
	processed     map[cid.Cid]bool
	pendingChunks map[cid.Cid][]byte // blocks that arrived before their parent File node

	onFileStart    FileProgressCallback
	onFileProgress FileProgressCallback
	onFileComplete FileProgressCallback
}

type chunkPosition struct {
	offset int64
	size   int64
}

type fileWriter struct {
	path      string
	file      *os.File
	expected  int64
	written   int64
	positions map[cid.Cid][]chunkPosition
	pending   int
}

type ChildLink struct {
	Cid  cid.Cid
	Name string
}

func New(outputDir string) (*Extractor, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	return &Extractor{
		outputDir:     outputDir,
		openFiles:     make(map[cid.Cid]*fileWriter),
		pathContext:   make(map[cid.Cid]string),
		fileRoot:      make(map[cid.Cid]cid.Cid),
		processed:     make(map[cid.Cid]bool),
		pendingChunks: make(map[cid.Cid][]byte),
	}, nil
}

func (e *Extractor) OnFileStart(cb FileProgressCallback)    { e.onFileStart = cb }
func (e *Extractor) OnFileProgress(cb FileProgressCallback) { e.onFileProgress = cb }
func (e *Extractor) OnFileComplete(cb FileProgressCallback) { e.onFileComplete = cb }

func (e *Extractor) IsProcessed(c cid.Cid) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.processed[c]
}

func (e *Extractor) SetRootPath(rootCid cid.Cid, name string) {
	sanitized := sanitizeName(name)
	if sanitized == "" {
		sanitized = rootCid.String()
	}
	e.mu.Lock()
	e.pathContext[rootCid] = sanitized
	e.mu.Unlock()
}

// ProcessBlock decodes a block and returns child CIDs that should be fetched next.
func (e *Extractor) ProcessBlock(ctx context.Context, block blocks.Block) ([]cid.Cid, error) {
	c := block.Cid()

	e.mu.RLock()
	if e.processed[c] {
		e.mu.RUnlock()
		logger.Debugw("ProcessBlock: skipping duplicate", "cid", c)
		return nil, nil
	}

	rootCid, isFileChild := e.fileRoot[c]
	e.mu.RUnlock()

	node, err := decodeBlock(block)
	if err != nil {
		// not DAG-PB — write as chunk if owned, otherwise standalone raw
		if isFileChild {
			e.mu.Lock()
			e.processed[c] = true
			e.mu.Unlock()
			return nil, e.writeChunk(c, rootCid, block.RawData())
		}
		return nil, e.handleRawBlock(c, block.RawData())
	}

	if !node.FieldData().Exists() {
		// DAG-PB without UnixFS data
		e.mu.Lock()
		e.processed[c] = true
		e.mu.Unlock()
		return e.extractPBLinks(node), nil
	}

	ufsData, err := data.DecodeUnixFSData(node.Data.Must().Bytes())
	if err != nil {
		if isFileChild {
			e.mu.Lock()
			e.processed[c] = true
			e.mu.Unlock()
			return nil, e.writeChunk(c, rootCid, block.RawData())
		}
		return nil, e.handleRawBlock(c, block.RawData())
	}

	dataType := ufsData.FieldDataType().Int()
	switch dataType {
	case data.Data_Directory:
		return e.processDirectory(c, node)
	case data.Data_File:
		if isFileChild {
			return e.processIntermediateFile(c, rootCid, node, ufsData)
		}
		return e.processFile(c, node, ufsData)
	case data.Data_Raw:
		// UnixFS Raw wraps payload in protobuf — extract inner data, not block bytes
		rawData := ufsData.FieldData().Must().Bytes()
		if isFileChild {
			e.mu.Lock()
			e.processed[c] = true
			e.mu.Unlock()
			return nil, e.writeChunk(c, rootCid, rawData)
		}
		return nil, e.handleRawBlock(c, rawData)
	case data.Data_Symlink:
		return nil, e.processSymlink(c, ufsData)
	default:
		logger.Warnw("unsupported UnixFS type", "cid", c, "type", dataType)
		return nil, nil
	}
}

func (e *Extractor) processDirectory(c cid.Cid, node dagpb.PBNode) ([]cid.Cid, error) {
	path := e.getPath(c)
	if path == "" {
		path = c.String()
	}

	fullPath := filepath.Join(e.outputDir, path)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", fullPath, err)
	}
	logger.Debugw("created directory", "path", fullPath)

	var children []cid.Cid
	linksIter := node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		name := ""
		if link.Name.Exists() {
			name = link.Name.Must().String()
		}
		if name == "" {
			name = linkCid.String()
		}
		childPath, err := e.safePath(path, name)
		if err != nil {
			logger.Warnw("skipping unsafe path", "cid", linkCid, "name", name, "err", err)
			continue
		}
		e.mu.Lock()
		e.pathContext[linkCid] = childPath
		e.mu.Unlock()
		children = append(children, linkCid)
	}

	e.mu.Lock()
	e.processed[c] = true
	e.mu.Unlock()
	return children, nil
}

func (e *Extractor) processFile(c cid.Cid, node dagpb.PBNode, ufsData data.UnixFSData) ([]cid.Cid, error) {
	path := e.getPath(c)
	if path == "" {
		path = c.String()
	}

	fullPath := filepath.Join(e.outputDir, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	linksIter := node.Links.Iterator()
	if linksIter.Done() {
		// single-block file
		var fileData []byte
		if ufsData.FieldData().Exists() {
			fileData = ufsData.FieldData().Must().Bytes()
		}
		size := int64(len(fileData))
		if e.onFileStart != nil {
			e.onFileStart(path, 0, size)
		}
		if err := os.WriteFile(fullPath, fileData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write file %s: %w", fullPath, err)
		}
		if e.onFileComplete != nil {
			e.onFileComplete(path, size, size)
		}
		logger.Debugw("wrote single-block file", "path", fullPath, "bytes", len(fileData))
		e.mu.Lock()
		e.processed[c] = true
		e.mu.Unlock()
		return nil, nil
	}

	// chunked file
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}

	var expectedSize int64 = -1
	if ufsData.FieldFileSize().Exists() {
		expectedSize = ufsData.FieldFileSize().Must().Int()
	}

	// write inline data before registering in openFiles
	inlineOffset := int64(0)
	if ufsData.FieldData().Exists() {
		inlineData := ufsData.FieldData().Must().Bytes()
		if len(inlineData) > 0 {
			n, err := f.WriteAt(inlineData, 0)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to write inline data: %w", err)
			}
			inlineOffset = int64(n)
		}
	}

	fw := &fileWriter{
		path:      path,
		file:      f,
		expected:  expectedSize,
		positions: make(map[cid.Cid][]chunkPosition),
	}
	e.mu.Lock()
	e.openFiles[c] = fw
	e.mu.Unlock()

	if e.onFileStart != nil {
		e.onFileStart(path, 0, expectedSize)
	}

	offset := inlineOffset
	linksIter = node.Links.Iterator()
	idx := 0
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid

		var chunkSize int64
		sizes := ufsData.FieldBlockSizes()
		if int64(idx) < sizes.Length() {
			sizeVal, _ := sizes.LookupByIndex(int64(idx))
			if sizeVal != nil {
				chunkSize, _ = sizeVal.AsInt()
			}
		}

		fw.positions[linkCid] = append(fw.positions[linkCid], chunkPosition{offset: offset, size: chunkSize})
		fw.pending++
		e.mu.Lock()
		e.fileRoot[linkCid] = c
		e.mu.Unlock()

		offset += chunkSize
		idx++
	}

	// deduplicate child CIDs; handle identity CIDs inline
	seen := make(map[cid.Cid]bool)
	var children []cid.Cid
	for linkCid := range fw.positions {
		if !seen[linkCid] {
			seen[linkCid] = true

			if isIdentityCid(linkCid) {
				identData, err := extractIdentityData(linkCid)
				if err != nil {
					logger.Warnw("failed to extract identity data", "cid", linkCid, "err", err)
				} else {
					if err := e.writeChunk(linkCid, c, identData); err != nil {
						logger.Warnw("failed to write identity chunk", "cid", linkCid, "err", err)
					}
				}
				continue
			}

			children = append(children, linkCid)
			// drain pending chunks that arrived before this File node
			e.mu.Lock()
			pendingData, ok := e.pendingChunks[linkCid]
			if ok {
				delete(e.pendingChunks, linkCid)
			}
			e.mu.Unlock()
			if ok {
				if err := e.writeChunk(linkCid, c, pendingData); err != nil {
					logger.Warnw("failed to write pending chunk", "cid", linkCid, "err", err)
				}
			}
		}
	}

	logger.Debugw("started chunked file", "path", fullPath, "chunks", fw.pending, "uniqueCIDs", len(children), "expectedSize", expectedSize)
	e.mu.Lock()
	e.processed[c] = true
	e.mu.Unlock()
	return children, nil
}

// processIntermediateFile handles File nodes that are children of another File
// (internal nodes in multi-level chunked files).
func (e *Extractor) processIntermediateFile(c, rootCid cid.Cid, node dagpb.PBNode, ufsData data.UnixFSData) ([]cid.Cid, error) {
	e.mu.RLock()
	fw, ok := e.openFiles[rootCid]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no open file for intermediate node %s (root %s)", c, rootCid)
	}

	myPositions := fw.positions[c]
	if len(myPositions) == 0 {
		return nil, fmt.Errorf("intermediate node %s has no position in file", c)
	}
	baseOffset := myPositions[0].offset

	// intermediate nodes don't carry data themselves, just structure
	delete(fw.positions, c)
	fw.pending -= len(myPositions)

	inlineLen := int64(0)
	if ufsData.FieldData().Exists() {
		inlineData := ufsData.FieldData().Must().Bytes()
		if len(inlineData) > 0 {
			_, err := fw.file.WriteAt(inlineData, baseOffset)
			if err != nil {
				return nil, fmt.Errorf("failed to write intermediate inline data: %w", err)
			}
			inlineLen = int64(len(inlineData))
		}
	}

	offset := baseOffset + inlineLen
	linksIter := node.Links.Iterator()
	idx := 0
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid

		var chunkSize int64
		sizes := ufsData.FieldBlockSizes()
		if int64(idx) < sizes.Length() {
			sizeVal, _ := sizes.LookupByIndex(int64(idx))
			if sizeVal != nil {
				chunkSize, _ = sizeVal.AsInt()
			}
		}

		fw.positions[linkCid] = append(fw.positions[linkCid], chunkPosition{offset: offset, size: chunkSize})
		fw.pending++
		e.mu.Lock()
		e.fileRoot[linkCid] = rootCid
		e.mu.Unlock()

		offset += chunkSize
		idx++
	}

	seen := make(map[cid.Cid]bool)
	var children []cid.Cid
	linksIter = node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		if !seen[linkCid] {
			seen[linkCid] = true

			if isIdentityCid(linkCid) {
				identData, err := extractIdentityData(linkCid)
				if err != nil {
					logger.Warnw("failed to extract identity data", "cid", linkCid, "err", err)
				} else {
					if err := e.writeChunk(linkCid, rootCid, identData); err != nil {
						logger.Warnw("failed to write identity chunk", "cid", linkCid, "err", err)
					}
				}
				continue
			}

			children = append(children, linkCid)
			e.mu.Lock()
			pendingData, ok := e.pendingChunks[linkCid]
			if ok {
				delete(e.pendingChunks, linkCid)
			}
			e.mu.Unlock()
			if ok {
				if err := e.writeChunk(linkCid, rootCid, pendingData); err != nil {
					logger.Warnw("failed to write pending chunk", "cid", linkCid, "err", err)
				}
			}
		}
	}

	e.mu.Lock()
	delete(e.fileRoot, c)
	e.processed[c] = true
	e.mu.Unlock()

	logger.Debugw("processed intermediate file node", "cid", c, "root", rootCid, "newChildren", len(children), "baseOffset", baseOffset)
	return children, nil
}

func (e *Extractor) writeChunk(chunkCid, fileCid cid.Cid, rawData []byte) error {
	e.mu.RLock()
	fw, ok := e.openFiles[fileCid]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no open file for chunk %s (file %s)", chunkCid, fileCid)
	}

	positions := fw.positions[chunkCid]
	if len(positions) == 0 {
		return fmt.Errorf("chunk %s has no positions in file", chunkCid)
	}

	// for DAG-PB chunks, extract the UnixFS data payload
	chunkData := rawData
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, rawData); err == nil {
		pbNode := nb.Build().(dagpb.PBNode)
		if pbNode.FieldData().Exists() {
			ufsData, err := data.DecodeUnixFSData(pbNode.Data.Must().Bytes())
			if err == nil && ufsData.FieldData().Exists() {
				chunkData = ufsData.FieldData().Must().Bytes()
			}
		}
	}

	var bytesWritten int64
	for _, pos := range positions {
		_, err := fw.file.WriteAt(chunkData, pos.offset)
		if err != nil {
			return fmt.Errorf("failed to write chunk at offset %d: %w", pos.offset, err)
		}
		bytesWritten += int64(len(chunkData))
		fw.pending--
	}
	fw.written += bytesWritten

	if e.onFileProgress != nil {
		e.onFileProgress(fw.path, fw.written, fw.expected)
	}

	delete(fw.positions, chunkCid)
	e.mu.Lock()
	delete(e.fileRoot, chunkCid)
	e.mu.Unlock()

	if fw.pending == 0 {
		e.closeFile(fileCid)
	}

	return nil
}

func (e *Extractor) handleRawBlock(c cid.Cid, rawData []byte) error {
	e.mu.RLock()
	rootCid, ok := e.fileRoot[c]
	e.mu.RUnlock()
	if ok {
		e.mu.Lock()
		e.processed[c] = true
		e.mu.Unlock()
		return e.writeChunk(c, rootCid, rawData)
	}

	path := e.getPath(c)
	if path != "" {
		fullPath := filepath.Join(e.outputDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return fmt.Errorf("failed to create parent directory: %w", err)
		}
		size := int64(len(rawData))
		if e.onFileStart != nil {
			e.onFileStart(path, 0, size)
		}
		if err := os.WriteFile(fullPath, rawData, 0644); err != nil {
			return fmt.Errorf("failed to write raw block %s: %w", fullPath, err)
		}
		if e.onFileComplete != nil {
			e.onFileComplete(path, size, size)
		}
		logger.Debugw("wrote raw block", "path", fullPath, "bytes", len(rawData))
		e.mu.Lock()
		e.processed[c] = true
		e.mu.Unlock()
		return nil
	}

	// no path context — block arrived before its parent, stash it
	e.mu.Lock()
	e.pendingChunks[c] = rawData
	e.mu.Unlock()
	return nil
}

func (e *Extractor) processSymlink(c cid.Cid, ufsData data.UnixFSData) error {
	logger.Warnw("symlink extraction not implemented", "cid", c)
	return nil
}

func (e *Extractor) extractPBLinks(node dagpb.PBNode) []cid.Cid {
	var links []cid.Cid
	linksIter := node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		links = append(links, linkCid)
	}
	return links
}

func (e *Extractor) getPath(c cid.Cid) string {
	e.mu.RLock()
	path, ok := e.pathContext[c]
	e.mu.RUnlock()
	if ok {
		return path
	}
	return ""
}

func sanitizeName(name string) string {
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	if name == "." || name == ".." {
		return ""
	}
	name = strings.TrimSpace(name)
	return name
}

func (e *Extractor) safePath(basePath, name string) (string, error) {
	sanitized := sanitizeName(name)
	if sanitized == "" {
		return "", fmt.Errorf("invalid filename: %q", name)
	}
	joined := filepath.Join(basePath, sanitized)
	fullPath := filepath.Join(e.outputDir, joined)
	absOutput, err := filepath.Abs(e.outputDir)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absFull, absOutput+string(filepath.Separator)) && absFull != absOutput {
		return "", fmt.Errorf("path traversal attempt: %q", name)
	}
	return joined, nil
}

func (e *Extractor) closeFile(c cid.Cid) {
	e.mu.Lock()
	fw, ok := e.openFiles[c]
	if ok {
		delete(e.openFiles, c)
	}
	e.mu.Unlock()
	if ok {
		fw.file.Close()
		if e.onFileComplete != nil {
			e.onFileComplete(fw.path, fw.written, fw.expected)
		}
		logger.Debugw("closed file", "path", fw.path, "expected", fw.expected)
	}
}

func (e *Extractor) Close() error {
	e.mu.Lock()
	toClose := make(map[cid.Cid]*fileWriter, len(e.openFiles))
	for c, fw := range e.openFiles {
		toClose[c] = fw
	}
	e.openFiles = make(map[cid.Cid]*fileWriter)
	pendingCount := len(e.pendingChunks)
	e.pendingChunks = make(map[cid.Cid][]byte)
	e.mu.Unlock()

	var errs []error
	for _, fw := range toClose {
		if err := fw.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s: %w", fw.path, err))
		}
		if fw.pending > 0 {
			logger.Warnw("incomplete file", "path", fw.path, "pendingChunks", fw.pending, "expected", fw.expected)
		}
	}
	if pendingCount > 0 {
		logger.Warnw("orphan pending chunks", "count", pendingCount)
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

func decodeBlock(block blocks.Block) (dagpb.PBNode, error) {
	c := block.Cid()
	if c.Prefix().Codec != cid.DagProtobuf {
		return nil, fmt.Errorf("not a DAG-PB block: codec %x", c.Prefix().Codec)
	}

	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, block.RawData()); err != nil {
		return nil, err
	}
	node := nb.Build()
	pbNode, ok := node.(dagpb.PBNode)
	if !ok {
		return nil, fmt.Errorf("decoded node is not PBNode: %T", node)
	}
	return pbNode, nil
}

var _ io.Closer = (*Extractor)(nil)
