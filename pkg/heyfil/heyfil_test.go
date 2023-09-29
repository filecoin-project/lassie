package heyfil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/lassie/pkg/heyfil"
	"github.com/stretchr/testify/require"
)

func TestCanHeyfil(t *testing.T) {
	testCase := []struct {
		name  string
		input string
		can   bool
	}{
		{"empty", "", false},
		{"faddr", "f01234", true},
		{"faddr non miner", "f1234", false},
		{"bad faddr", "f00cafebeef", false},
		{"p2p", "12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4", true},
		{"cid p2p", "QmUA9D3H7HeCYsirB3KmPSvZh3dNXMZas6Lwgr4fv1HTTp", true},
		{"cidv1 p2p", "bafzbeicwot2npbkuyjppqaoibbohqemn5dnbidt66mjdfso725vm4lmmlm", true},
		{"cidv1 not p2p", "bafybeicwot2npbkuyjppqaoibbohqemn5dnbidt66mjdfso725vm4lmmlm", false},
		{"multiaddr", "/dns4/dag.w3s.link/tcp/443/https", false},
		{"http addr", "http://dag.w3s.link:443", false},
		{"multiaddr long", "/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4", false},
		{"multiaddr ip4", "/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA", false},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, heyfil.CanHeyfil(tc.input), tc.can)
		})
	}
}

func TestHeyfil(t *testing.T) {
	ts := newHeyfilServer()
	defer ts.Close()

	trans, err := heyfil.Heyfil{Endpoint: ts.URL}.Translate("12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ")
	require.NoError(t, err)
	require.Equal(t, "/ip4/85.11.148.122/tcp/24001/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ", trans)

	trans, err = heyfil.Heyfil{Endpoint: ts.URL}.Translate("f0127896")
	require.NoError(t, err)
	require.Equal(t, "/ip4/85.11.148.122/tcp/24001/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ", trans)

	// no translation, pass-through
	for _, inp := range []string{
		"/dns4/dag.w3s.link/tcp/443/https",
		"http://dag.w3s.link:443",
		"/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA",
		"WOT?", // this is an error for another layer ...
	} {
		trans, err := heyfil.Heyfil{Endpoint: ts.URL}.Translate(inp)
		require.NoError(t, err)
		require.Equal(t, trans, inp)
	}
}

func TestHeyfilTranslateAll(t *testing.T) {
	ts := newHeyfilServer()
	defer ts.Close()

	input := []string{
		"/dns4/dag.w3s.link/tcp/443/https",
		"http://dag.w3s.link:443",
		"12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ",
		"/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA",
		"f0127896",
		"WOT?", // this is an error for another layer ...
	}
	expected := []string{
		"/dns4/dag.w3s.link/tcp/443/https",
		"http://dag.w3s.link:443",
		"/ip4/85.11.148.122/tcp/24001/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ",
		"/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA",
		"/ip4/85.11.148.122/tcp/24001/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ",
		"WOT?",
	}

	trans, err := heyfil.Heyfil{Endpoint: ts.URL}.TranslateAll(input)
	require.NoError(t, err)
	require.Equal(t, trans, expected)

	// same but nothing to translate, make sure we can pass through without even trying
	input = []string{
		"/dns4/dag.w3s.link/tcp/443/https",
		"http://dag.w3s.link:443",
		"/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA",
		"WOT?", // this is an error for another layer ...
	}
	expected = []string{
		"/dns4/dag.w3s.link/tcp/443/https",
		"http://dag.w3s.link:443",
		"/dns4/dag.w3s.link/tcp/443/https/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA",
		"WOT?",
	}

	trans, err = heyfil.Heyfil{Endpoint: ts.URL}.TranslateAll(input)
	require.NoError(t, err)
	require.Equal(t, trans, expected)
}

func newHeyfilServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sp/f0127896" {
			w.Write([]byte(`{"id":"f0127896","status":6,"addr_info":{"ID":"12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ","Addrs":["/ip4/85.11.148.122/tcp/24001"]},"last_checked":"2023-09-29T05:57:19.193226313Z","err":"failed to dial 12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ:\n  * [/ip4/85.11.148.122/tcp/24001] dial tcp4 85.11.148.122:24001: connect: connection refused","head":null,"known_by_indexer":true,"state_miner_power":{"HasMinPower":false,"MinerPower":{"QualityAdjPower":"0","RawBytePower":"0"},"TotalPower":{"QualityAdjPower":"27973870915671523328","RawBytePower":"12090904615566966784"}},"deal_count":2031}`))
			return
		}
		if r.URL.Path == "/sp" && r.URL.RawQuery == "peerid=12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ" {
			w.Write([]byte(`["f0127896"]`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("NO JSON FOR YOU!"))
	}))
}
