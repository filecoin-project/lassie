//go:build !race

package itest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/cmd"
	"github.com/ipld/go-car/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesspathing "github.com/ipld/ipld/specs/pkg-go/trustless-pathing"
	"github.com/stretchr/testify/require"
)

const (
	frisbiiReadyMatch      = "Announce() complete"
	indexerReadyMatch      = "Indexer is ready"
	lassieDaemonReadyMatch = "Lassie daemon listening on address"
)

func TestTrustlessGatewayE2E(t *testing.T) {
	switch os.Getenv("CI") {
	case "":
		// skip when not running in a CI environment
		t.Skip("skipping when not in CI environment")
	default:
		if runtime.GOOS == "windows" {
			// skip if windows, just too slow in CI, maybe revisit this later
			t.Skip("skipping on windows in CI")
		} // else in CI and we're good to go
	}

	req := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tr := cmd.NewRunner(t, t.TempDir())

	t.Log("Running in test directory:", tr.Dir)

	// install the lassie cmd, when done in tr.Run() will use the GOPATH/GOBIN
	// in the test directory, so we get a localised `lassie` executable
	lassie := filepath.Join(tr.Dir, "lassie")
	tr.Run(ctx, "go", "install", "../../../cmd/lassie/")

	cwd, err := os.Getwd()
	req.NoError(err)
	err = os.Chdir(tr.Dir)
	req.NoError(err)

	// install the indexer to announce to
	t.Log("Installing indexer")
	indexer := filepath.Join(tr.Dir, "storetheindex")
	tr.Run(ctx, "go", "install", "github.com/ipni/storetheindex@latest")
	// install the ipni cli to inspect the indexer
	t.Log("Installing ipni")
	ipni := filepath.Join(tr.Dir, "ipni")
	tr.Run(ctx, "go", "install", "github.com/ipni/ipni-cli/cmd/ipni@latest")
	// install frisbii to serve the content
	t.Log("Installing frisbii")
	frisbii := filepath.Join(tr.Dir, "frisbii")
	tr.Run(ctx, "go", "install", "github.com/ipld/frisbii/cmd/frisbii@latest")

	err = os.Chdir(cwd)
	req.NoError(err)

	// initialise and start the indexer and adjust the config
	t.Log("Initialising indexer")
	tr.Run(ctx, indexer, "init", "--store", "pebble", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap")
	t.Log("Starting indexer")
	indexerReady := cmd.NewStdoutWatcher(indexerReadyMatch)
	cmdIndexer := tr.Start(ctx, cmd.Args(indexer, "daemon"), indexerReady)
	select {
	case <-indexerReady.Signal():
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer to start")
	}

	testCases, root, err := trustlesspathing.Unixfs20mVarietyCases()
	req.NoError(err)

	carPath := trustlesspathing.Unixfs20mVarietyCARPath()

	// start frisbii with the fixture CAR
	t.Logf("Starting frisbii with CAR [%s] and root [%s]", carPath, root)
	frisbiiReady := cmd.NewStdoutWatcher(frisbiiReadyMatch)
	cmdFrisbii := tr.Start(ctx, cmd.Args(frisbii,
		"--listen", "localhost:37471",
		"--announce", "roots",
		"--announce-url", "http://localhost:3001/announce",
		"--verbose",
		"--car", carPath,
	), frisbiiReady)

	select {
	case <-frisbiiReady.Signal():
	case <-ctx.Done():
		t.Fatal("timed out waiting for frisbii to announce")
	}

	// wait for the CARs to be indexed
	req.Eventually(func() bool {
		mh := root.Hash().B58String()
		findOutput := tr.Run(ctx, ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
		t.Logf("import output:\n%s\n", findOutput)

		if bytes.Contains(findOutput, []byte("not found")) {
			return false
		}
		if !bytes.Contains(findOutput, []byte("Provider:")) {
			t.Logf("mh %s: unexpected error: %s", mh, findOutput)
			return false
		}
		return true
	}, 10*time.Second, time.Second)

	expectedCarPath := root.String() + ".car"

	t.Run("entire CAR fetch", func(t *testing.T) {
		req := require.New(t)

		// fetch the entire CAR
		tr.Run(ctx, lassie,
			"fetch",
			"-vv",
			"--ipni-endpoint", "http://localhost:3000",
			root.String(),
		)

		_, err = os.Stat(expectedCarPath)
		req.NoError(err)
		_, expectedCids := carToCids(t, carPath)
		gotRoot, gotCids := carToCids(t, expectedCarPath)
		req.Equal(root, gotRoot)
		req.ElementsMatch(expectedCids, gotCids)
		req.NoError(os.Remove(expectedCarPath))
	})

	// start lassie daemon
	lassieDaemonReady := cmd.NewStdoutWatcher(lassieDaemonReadyMatch)
	cmdLassieDaemon := tr.Start(ctx, cmd.Args(lassie, "daemon",
		"-vv",
		"--port", "30000",
		"--ipni-endpoint", "http://localhost:3000",
	), lassieDaemonReady)

	select {
	case <-lassieDaemonReady.Signal():
	case <-ctx.Done():
		t.Fatal("timed out waiting for lassie daemon to start")
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Run("lassie CLI fetch", func(t *testing.T) {
				req := require.New(t)

				// TODO: lassie should be able to handle http style queries on the commandline: testCase.AsQuery(),
				args := []string{
					"fetch",
					"-vv",
					"--ipni-endpoint", "http://localhost:3000",
				}
				if testCase.Scope != string(trustlessutils.DagScopeAll) {
					args = append(args, "--dag-scope", testCase.Scope)
				}
				if testCase.ByteRange != "" {
					args = append(args, "--entity-bytes", testCase.ByteRange)
				}
				args = append(args, testCase.Root.String())
				if testCase.Path != "" {
					args[len(args)-1] = args[len(args)-1] + "/" + testCase.Path
				}
				t.Logf("Running lassie %s", strings.Join(args, " "))
				tr.Run(ctx, lassie, args...)

				_, err = os.Stat(expectedCarPath)
				req.NoError(err)
				gotRoot, gotCids := carToCids(t, expectedCarPath)
				req.Equal(testCase.Root, gotRoot)
				req.ElementsMatch(testCase.ExpectedCids, gotCids)
				req.NoError(os.Remove(expectedCarPath))
			})

			t.Run("lassie daemon fetch", func(t *testing.T) {
				req := require.New(t)

				reqUrl, err := url.Parse("http://localhost:30000/" + testCase.AsQuery())
				req.NoError(err)

				t.Logf("Fetching %s", reqUrl.String())

				// download and read all body from URL along with Accept:application/vnd.ipld.car header
				reqReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl.String(), nil)
				req.NoError(err)
				reqReq.Header.Set("Accept", "application/vnd.ipld.car")
				resp, err := http.DefaultClient.Do(reqReq)
				req.NoError(err)
				defer resp.Body.Close()

				gotRoot, gotCids := carReaderToCids(t, resp.Body)
				req.Equal(testCase.Root, gotRoot)
				req.ElementsMatch(testCase.ExpectedCids, gotCids)
			})
		})
	}

	// stop and clean up
	tr.Stop(cmdIndexer, time.Second)
	tr.Stop(cmdFrisbii, time.Second)
	tr.Stop(cmdLassieDaemon, time.Second)
}

func carToCids(t *testing.T, carPath string) (cid.Cid, []cid.Cid) {
	req := require.New(t)

	file, err := os.Open(carPath)
	req.NoError(err)
	defer file.Close()
	return carReaderToCids(t, file)
}

func carReaderToCids(t *testing.T, r io.Reader) (cid.Cid, []cid.Cid) {
	req := require.New(t)

	cr, err := car.NewBlockReader(r)
	req.NoError(err)
	req.Len(cr.Roots, 1)

	cids := make([]cid.Cid, 0)
	for {
		blk, err := cr.Next()
		if err != nil {
			req.ErrorIs(err, io.EOF)
			break
		}
		cids = append(cids, blk.Cid())
	}

	return cr.Roots[0], cids
}
