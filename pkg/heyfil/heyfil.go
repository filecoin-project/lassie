package heyfil

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

const DefaultHeyfilEndpoint = "https://heyfil.prod.cid.contact/"

var logger = log.Logger("lassie/heyfil")

func isPeerId(s string) bool {
	_, err := peer.Decode(s)
	return err == nil
}

func isFilecoinFaddr(s string) bool {
	// quick check if matches /^f0\d+$/ {
	if len(s) > 2 && s[0] == 'f' && s[1] == '0' {
		for _, c := range s[2:] {
			if c < '0' || c > '9' {
				return false
			}
		}
		return true
	}
	return false
}

type Heyfil struct {
	Endpoint        string
	TranslatePeerId bool
	TranslateFaddr  bool
}

func (h Heyfil) CanTranslate(s string) bool {
	return !strings.Contains(s, "/") && ((h.TranslatePeerId && isPeerId(s)) || (h.TranslateFaddr && isFilecoinFaddr(s)))
}

func (h Heyfil) endpoint() string {
	if h.Endpoint == "" {
		return DefaultHeyfilEndpoint
	}
	return h.Endpoint
}

// TranslateAll performs a Translate on all strings in the input slice. If none
// of the strings can be translated, the input slice is returned as-is.
func (h Heyfil) TranslateAll(ss []string) ([]string, error) {
	translated := make([]string, len(ss))
	var errs []error
	var translatedLk sync.Mutex
	var wg sync.WaitGroup
	for ii, s := range ss {
		if !h.CanTranslate(s) {
			translated[ii] = s
			continue
		}
		wg.Add(1)
		go func(ii int, s string) {
			defer wg.Done()
			res, err := h.Translate(s)
			if err != nil {
				translatedLk.Lock()
				errs = append(errs, err)
				translatedLk.Unlock()
				return
			}
			translatedLk.Lock()
			translated[ii] = res
			translatedLk.Unlock()
		}(ii, s)
	}
	wg.Wait()
	if errs != nil {
		return nil, errors.Join(errs...)
	}
	return translated, nil
}

// Translate will translate an input string to a full multiaddr if the string
// appears to be a Filecoin SP actor address or a peer id using the Heyfil
// service. If the input string is not a Filecoin SP actor address or a peer id,
// it will be returned as-is.
func (h Heyfil) Translate(s string) (string, error) {
	if h.TranslatePeerId && isPeerId(s) {
		res, err := h.translatePeerId(s)
		if err != nil {
			logger.Debugw("failed to translate peer id", "input", s, "err", err)
			return "", err
		}
		return res, nil
	}
	if h.TranslateFaddr && isFilecoinFaddr(s) {
		res, err := h.translateFaddr(s)
		if err != nil {
			logger.Debugw("failed to translate faddr", "input", s, "err", err)
			return "", err
		}
		return res, nil
	}
	return s, nil // pass-through
}

func (h Heyfil) translatePeerId(s string) (string, error) {
	url := h.endpoint() + "/sp?peerid=" + s
	logger.Debugw("translating peer id", "input", s, "url", url)
	got := make([]string, 0)
	if err := httpGet(url, &got); err != nil {
		return "", err
	}
	if len(got) == 0 {
		return "", fmt.Errorf("expected at least 1 response, got %d", len(got))
	}
	return h.translateFaddr(got[0])
}

func (h Heyfil) translateFaddr(s string) (string, error) {
	url := h.endpoint() + "/sp/" + s
	got := make(map[string]interface{})
	if err := httpGet(url, &got); err != nil {
		return "", err
	}
	addrInfo, ok := got["addr_info"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected addr_info to be map[string]interface{}, got %T", got["addr_info"])
	}
	id, ok := addrInfo["ID"].(string)
	if !ok || id == "" {
		return "", fmt.Errorf("expected addr_info.ID to be string")
	}
	addrs, ok := addrInfo["Addrs"].([]interface{})
	if !ok {
		return "", fmt.Errorf("expected addr_info.Addrs to be []interface{}, got %T", addrInfo["Addrs"])
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("expected addr_info.Addrs to be non-empty")
	}
	// TODO: only using the first multiaddr for now, this could be expanded to use
	// all of them, either as separate addrs in the translated string, or, we could
	// allow a translate that spits out a full AddrInfo that maps directly to the
	// value here. TranslateAll([]string) ([]string, error) could become
	// TranslateAll([]string) ([]AddrInfo, []string, error)?
	addr, ok := addrs[0].(string)
	if !ok {
		return "", fmt.Errorf("expected addr_info.Addrs[0] to be string, got %T", addrs[0])
	}
	return addr + "/p2p/" + id, nil
}

func httpGet[T any](url string, result *T) error {
	logger.Debugf("http get: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http get: %s", resp.Status)
	}
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
