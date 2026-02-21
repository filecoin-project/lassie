package session

import (
	"math/rand/v2"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Random interface{ Float64() float64 }

type ProviderConfig struct {
	MaxConcurrentRetrievals uint
}

// All config values should be safe to leave uninitialized
type Config struct {
	// --- Fixed session config

	ProviderBlockList     map[peer.ID]bool
	ProviderAllowList     map[peer.ID]bool
	DefaultProviderConfig ProviderConfig
	ProviderConfigs       map[peer.ID]ProviderConfig

	// --- Dynamic state config

	// Random is an optional rng, if nil, math/rand will be used.
	Random Random

	// ConnectTimeAlpha is the alpha value for the exponential moving average
	// of the connect time for a storage provider. The connect time is the time
	// it takes to connect to a storage provider, it is used to determine the
	// prioritisation of storage providers. The value determines the weight of
	// previous connect times, a lower value will give more weight to recent
	// connect times. A value of 0 will only use the most recent connect time.
	ConnectTimeAlpha float64
	// FirstByteTimeAlpha is the alpha value for the exponential moving average
	// of the time to first byte for a storage provider. The time to first byte
	// is the time it takes to receive the first byte of data from a storage
	// provider, it is used to determine the prioritisation of storage
	// providers. The value determines the weight of previous time to first
	// bytes, a lower value will give more weight to recent time to first bytes.
	// A value of 0 will only use the most recent time to first byte.
	FirstByteTimeAlpha float64
	// OverallConnectTimeAlpha is the alpha value for the exponential moving
	// average of the overall connect time. The overall connect time is the
	// average connect time of *all* storage providers, used to normalise the
	// connect time metric.
	OverallConnectTimeAlpha float64
	// OverallFirstByteTimeAlpha is the alpha value for the exponential moving
	// average of the overall time to first byte. The overall time to first byte
	// is the average time to first byte of *all* storage providers, used to
	// normalise the time to first byte metric.
	OverallFirstByteTimeAlpha float64
	// BandwidthAlpha is the alpha value for the exponential moving average of
	// the bandwidth metric for a storage provider. The bandwidth metric is
	// measured in bytes per second. The value of BandwidthAlpha determines the
	// weight of previous bandwidth metrics, a lower value will give more weight
	// to recent bandwidth metrics. A value of 0 will only use the most recent
	// bandwidth metric.
	BandwidthAlpha float64
	// OverallBandwidthAlpha is the alpha value for the exponential moving
	// average of the overall bandwidth metric. The overall bandwidth metric is
	// the average bandwidth of *all* storage providers, used to normalise the
	// bandwidth metric.
	OverallBandwidthAlpha float64
	// SuccessAlpha is the alpha value for the exponential moving average of
	// the success metric for a storage provider. The success metric is the
	// ratio of successful retrievals to total retrievals. The value determines
	// the weight of previous success metrics, a lower value will give more
	// weight to recent success metrics. A value of 0 will only use the most
	// recent success metric.
	SuccessAlpha float64

	// ConnectTimeWeight is the scoring weight applied to the connect time
	// exponential moving average for the candidate at time of scoring. The
	// weight is a multiplier the base value, which should be a normalised
	// score in the range of [0, 1].
	ConnectTimeWeight float64
	// FirstByteTimeWeight is the scoring weight applied to the time to first
	// byte exponential moving average for the candidate at time of scoring. The
	// weight is a multiplier the base value, which should be a normalised
	// score in the range of [0, 1].
	FirstByteTimeWeight float64
	// BandwidthWeight is the scoring weight applied to the bandwidth
	// exponential moving average for the candidate at time of scoring. The
	// weight is a multiplier the base value, which should be a normalised
	// score in the range of [0, 1].
	BandwidthWeight float64
	// SuccessWeight is the scoring weight applied to the success exponential
	// moving average for the candidate at time of scoring. The weight is a
	// multiplier the base value, which should be a normalised score in the
	// range of [0, 1] (where each failure contributes a 0 and each success
	// contributes a 1).
	SuccessWeight float64
}

// DefaultConfig returns a default config with usable alpha and weight values.
func DefaultConfig() *Config {
	return &Config{
		ConnectTimeAlpha:          0.5,
		OverallConnectTimeAlpha:   0.8,
		FirstByteTimeAlpha:        0.5,
		OverallFirstByteTimeAlpha: 0.8,
		BandwidthAlpha:            0.5,
		OverallBandwidthAlpha:     0.8,
		SuccessAlpha:              0.5,
		ConnectTimeWeight:         1.0,
		FirstByteTimeWeight:       1.0,
		BandwidthWeight:           0.5,
		SuccessWeight:             1.0,
	}
}

// WithProviderBlockList sets the provider blocklist.
func (cfg Config) WithProviderBlockList(blocklist map[peer.ID]bool) *Config {
	cfg.ProviderBlockList = blocklist
	return &cfg
}

// WithProviderAllowList sets the provider allowlist.
func (cfg Config) WithProviderAllowList(allowlist map[peer.ID]bool) *Config {
	cfg.ProviderAllowList = allowlist
	return &cfg
}

// WithDefaultProviderConfig sets the default provider config.
func (cfg Config) WithDefaultProviderConfig(providerConfig ProviderConfig) *Config {
	cfg.DefaultProviderConfig = providerConfig
	return &cfg
}

// WithProviderConfigs sets the provider configs.
func (cfg Config) WithProviderConfigs(providerConfigs map[peer.ID]ProviderConfig) *Config {
	cfg.ProviderConfigs = providerConfigs
	return &cfg
}

// WithConnectTimeAlpha sets the connect time alpha.
func (cfg Config) WithConnectTimeAlpha(alpha float64) *Config {
	cfg.ConnectTimeAlpha = alpha
	return &cfg
}

// WithOverallConnectTimeAlpha sets the overall connect time alpha.
func (cfg Config) WithOverallConnectTimeAlpha(alpha float64) *Config {
	cfg.OverallConnectTimeAlpha = alpha
	return &cfg
}

// WithFirstByteTimeAlpha sets the time to first byte alpha.
func (cfg Config) WithFirstByteTimeAlpha(alpha float64) *Config {
	cfg.FirstByteTimeAlpha = alpha
	return &cfg
}

// WithOverallFirstByteTimeAlpha sets the overall time to first byte alpha.
func (cfg Config) WithOverallFirstByteTimeAlpha(alpha float64) *Config {
	cfg.OverallFirstByteTimeAlpha = alpha
	return &cfg
}

// WithSuccessAlpha sets the success alpha.
func (cfg Config) WithSuccessAlpha(alpha float64) *Config {
	cfg.SuccessAlpha = alpha
	return &cfg
}

// WithoutRandomness removes the dice roll for choosing the best peer, with this
// set, it will always choose the peer with the highest score.
func (cfg Config) WithoutRandomness() *Config {
	cfg.Random = nonRandom{}
	return &cfg
}

// WithConnectTimeWeight sets the connect time weight.
func (cfg Config) WithConnectTimeWeight(weight float64) *Config {
	cfg.ConnectTimeWeight = weight
	return &cfg
}

// WithFirstByteTimeWeight sets the time to first byte weight.
func (cfg Config) WithFirstByteTimeWeight(weight float64) *Config {
	cfg.FirstByteTimeWeight = weight
	return &cfg
}

// WithSuccessWeight sets the success weight.
func (cfg Config) WithSuccessWeight(weight float64) *Config {
	cfg.SuccessWeight = weight
	return &cfg
}

// roll returns a random float64 between 0 and 1.
func (c *Config) roll() float64 {
	if c.Random == nil {
		return rand.Float64()
	}
	return c.Random.Float64()
}

// getProviderConfig returns the provider config for a given peer.
// If no config is set for the peer, the default config is returned.
func (cfg *Config) getProviderConfig(peer peer.ID) ProviderConfig {
	minerCfg := cfg.DefaultProviderConfig
	if individual, ok := cfg.ProviderConfigs[peer]; ok {
		if individual.MaxConcurrentRetrievals != 0 {
			minerCfg.MaxConcurrentRetrievals = individual.MaxConcurrentRetrievals
		}
	}
	return minerCfg
}

// nonRandom will always roll a 0.5, meaning that the peer with the highest
// score of two being compared will always be chosen.

var _ Random = nonRandom{}

type nonRandom struct{}

func (nonRandom) Float64() float64 { return 0 }
