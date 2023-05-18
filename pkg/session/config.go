package session

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type ProviderConfig struct {
	RetrievalTimeout        time.Duration
	MaxConcurrentRetrievals uint
}

// All config values should be safe to leave uninitialized
type Config struct {
	// --- Fixed session config

	ProviderBlockList     map[peer.ID]bool
	ProviderAllowList     map[peer.ID]bool
	DefaultProviderConfig ProviderConfig
	ProviderConfigs       map[peer.ID]ProviderConfig
	PaidRetrievals        bool

	// --- Dynamic state config

	MaxFailuresBeforeSuspend uint
	FailureHistoryDuration   time.Duration
	SuspensionDuration       time.Duration

	// ConnectTimeAlpha is the alpha value for the exponential moving average
	// of the connect time for a storage provider. The connect time is the time
	// it takes to connect to a storage provider, it is used to determine the
	// prioritisation of storage providers. The value determines the weight of
	// previous connect times, a lower value will give more weight to recent
	// connect times. A value of 0 will only use the most recent connect time.
	ConnectTimeAlpha float64
}

func DefaultConfig() *Config {
	return &Config{
		MaxFailuresBeforeSuspend: 5,
		SuspensionDuration:       time.Minute * 10,
		FailureHistoryDuration:   time.Second * 30,
		ConnectTimeAlpha:         0.5, // only use most recent connect time
	}
}

func (cfg *Config) getProviderConfig(peer peer.ID) ProviderConfig {
	minerCfg := cfg.DefaultProviderConfig

	if individual, ok := cfg.ProviderConfigs[peer]; ok {
		if individual.MaxConcurrentRetrievals != 0 {
			minerCfg.MaxConcurrentRetrievals = individual.MaxConcurrentRetrievals
		}

		if individual.RetrievalTimeout != 0 {
			minerCfg.RetrievalTimeout = individual.RetrievalTimeout
		}
	}

	return minerCfg
}
