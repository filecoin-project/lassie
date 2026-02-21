package extractor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
)

func buildWithLinkSystem(t *testing.T, content []byte) (blocks.Block, *memstore.Store) {
	t.Helper()

	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	link, _, err := builder.BuildUnixFSFile(bytes.NewReader(content), "", &lsys)
	if err != nil {
		t.Fatalf("BuildUnixFSFile failed: %v", err)
	}

	cidLink := link.(cidlink.Link)
	data, err := store.Get(context.Background(), cidLink.Cid.KeyString())
	if err != nil {
		t.Fatalf("failed to get block from store: %v", err)
	}

	block, _ := blocks.NewBlockWithCid(data, cidLink.Cid)
	return block, store
}

func TestExtractSingleBlockFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	ext, err := New(tmpDir)
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}
	defer ext.Close()

	fileContent := []byte("hello world")
	block, _ := buildWithLinkSystem(t, fileContent)

	ext.SetRootPath(block.Cid(), "test.txt")

	children, err := ext.ProcessBlock(ctx, block)
	if err != nil {
		t.Fatalf("ProcessBlock failed: %v", err)
	}
	if len(children) != 0 {
		t.Errorf("expected 0 children, got %d", len(children))
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "test.txt"))
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content mismatch: got %q, want %q", content, "hello world")
	}
}

func TestExtractDirectory(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	ext, err := New(tmpDir)
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}
	defer ext.Close()

	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	fileContent := []byte("file content")
	fileLink, fileSize, err := builder.BuildUnixFSFile(bytes.NewReader(fileContent), "", &lsys)
	if err != nil {
		t.Fatalf("BuildUnixFSFile failed: %v", err)
	}
	fileCidLink := fileLink.(cidlink.Link)

	entry, err := builder.BuildUnixFSDirectoryEntry("myfile.txt", int64(fileSize), fileLink)
	if err != nil {
		t.Fatalf("BuildUnixFSDirectoryEntry failed: %v", err)
	}

	dirLink, _, err := builder.BuildUnixFSDirectory([]dagpb.PBLink{entry}, &lsys)
	if err != nil {
		t.Fatalf("BuildUnixFSDirectory failed: %v", err)
	}
	dirCidLink := dirLink.(cidlink.Link)

	dirData, _ := store.Get(ctx, dirCidLink.Cid.KeyString())
	dirBlock, _ := blocks.NewBlockWithCid(dirData, dirCidLink.Cid)

	fileData, _ := store.Get(ctx, fileCidLink.Cid.KeyString())
	fileBlock, _ := blocks.NewBlockWithCid(fileData, fileCidLink.Cid)

	ext.SetRootPath(dirCidLink.Cid, "testdir")

	children, err := ext.ProcessBlock(ctx, dirBlock)
	if err != nil {
		t.Fatalf("ProcessBlock (dir) failed: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}

	info, err := os.Stat(filepath.Join(tmpDir, "testdir"))
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}

	_, err = ext.ProcessBlock(ctx, fileBlock)
	if err != nil {
		t.Fatalf("ProcessBlock (file) failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "testdir", "myfile.txt"))
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if string(content) != "file content" {
		t.Errorf("content mismatch: got %q", content)
	}
}

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal.txt", "normal.txt"},
		{"../escape", "escape"},
		{"../../etc/passwd", "passwd"},
		{"/absolute/path", "path"},
		{".", ""},
		{"..", ""},
		{"  spaces  ", "spaces"},
		{"", ""},
	}

	for _, tc := range tests {
		got := sanitizeName(tc.input)
		if got != tc.expected {
			t.Errorf("sanitizeName(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestSetRootPathSanitization(t *testing.T) {
	tmpDir := t.TempDir()
	ext, _ := New(tmpDir)
	defer ext.Close()

	maliciousCid := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	ext.SetRootPath(maliciousCid, "../../etc/passwd")

	path := ext.getPath(maliciousCid)
	if path == "../../etc/passwd" {
		t.Error("path traversal not sanitized")
	}
	if path != "passwd" {
		t.Errorf("expected 'passwd', got %q", path)
	}
}

func TestExtractChunkedFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	ext, err := New(tmpDir)
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}
	defer ext.Close()

	// varied content so each 1024-byte chunk gets a unique CID
	fileContent := make([]byte, 3000)
	for i := range fileContent {
		// mix in i/1024 to make each chunk unique
		fileContent[i] = byte(i + i/1024*100)
	}

	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	link, _, err := builder.BuildUnixFSFile(bytes.NewReader(fileContent), "size-1024", &lsys)
	if err != nil {
		t.Fatalf("BuildUnixFSFile failed: %v", err)
	}
	rootCidLink := link.(cidlink.Link)

	ext.SetRootPath(rootCidLink.Cid, "chunked.txt")

	rootData, _ := store.Get(ctx, rootCidLink.Cid.KeyString())
	rootBlock, _ := blocks.NewBlockWithCid(rootData, rootCidLink.Cid)

	children, err := ext.ProcessBlock(ctx, rootBlock)
	if err != nil {
		t.Fatalf("ProcessBlock (root) failed: %v", err)
	}
	if len(children) == 0 {
		t.Fatal("expected children for chunked file")
	}
	t.Logf("root block has %d children", len(children))

	// DFS traversal of child blocks
	queue := children
	for len(queue) > 0 {
		childCid := queue[0]
		queue = queue[1:]

		childData, err := store.Get(ctx, childCid.KeyString())
		if err != nil {
			t.Fatalf("failed to get child block %s: %v", childCid, err)
		}
		childBlock, _ := blocks.NewBlockWithCid(childData, childCid)
		grandchildren, err := ext.ProcessBlock(ctx, childBlock)
		if err != nil {
			t.Fatalf("ProcessBlock (chunk %s) failed: %v", childCid, err)
		}
		queue = append(queue, grandchildren...)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "chunked.txt"))
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if len(content) != len(fileContent) {
		t.Errorf("content length mismatch: got %d, want %d", len(content), len(fileContent))
	}
	if !bytes.Equal(content, fileContent) {
		t.Error("content mismatch")
	}
}

func TestDuplicateBlockHandling(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	ext, _ := New(tmpDir)
	defer ext.Close()

	fileContent := []byte("test content")
	block, _ := buildWithLinkSystem(t, fileContent)

	ext.SetRootPath(block.Cid(), "dup.txt")

	ext.ProcessBlock(ctx, block)
	ext.ProcessBlock(ctx, block) // should be skipped
	info, _ := os.Stat(filepath.Join(tmpDir, "dup.txt"))
	if info.Size() != int64(len(fileContent)) {
		t.Errorf("file size %d, expected %d (duplicate not handled)", info.Size(), len(fileContent))
	}
}

func TestDuplicateChunksInFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	ext, err := New(tmpDir)
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}
	defer ext.Close()

	// 3 identical 1024-byte chunks → same CID repeated in links
	chunk := bytes.Repeat([]byte("x"), 1024)
	fileContent := bytes.Repeat(chunk, 3) // 3072 bytes, 3 identical 1024-byte chunks

	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	link, _, err := builder.BuildUnixFSFile(bytes.NewReader(fileContent), "size-1024", &lsys)
	if err != nil {
		t.Fatalf("BuildUnixFSFile failed: %v", err)
	}
	rootCidLink := link.(cidlink.Link)

	ext.SetRootPath(rootCidLink.Cid, "repeated.txt")

	rootData, _ := store.Get(ctx, rootCidLink.Cid.KeyString())
	rootBlock, _ := blocks.NewBlockWithCid(rootData, rootCidLink.Cid)

	children, err := ext.ProcessBlock(ctx, rootBlock)
	if err != nil {
		t.Fatalf("ProcessBlock (root) failed: %v", err)
	}

	// deduplicated: 1 unique CID for 3 chunk positions
	t.Logf("root returned %d unique children for file with repeated chunks", len(children))

	for _, childCid := range children {
		childData, err := store.Get(ctx, childCid.KeyString())
		if err != nil {
			t.Fatalf("failed to get child block %s: %v", childCid, err)
		}
		childBlock, _ := blocks.NewBlockWithCid(childData, childCid)
		_, err = ext.ProcessBlock(ctx, childBlock)
		if err != nil {
			t.Fatalf("ProcessBlock (chunk %s) failed: %v", childCid, err)
		}
	}

	// all 3072 bytes, not just 1024 — repeated chunks must be expanded
	content, err := os.ReadFile(filepath.Join(tmpDir, "repeated.txt"))
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if len(content) != len(fileContent) {
		t.Errorf("content length mismatch: got %d, want %d (duplicate chunks not expanded)", len(content), len(fileContent))
	}
	if !bytes.Equal(content, fileContent) {
		t.Error("content mismatch")
	}
}
