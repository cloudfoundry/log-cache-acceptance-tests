package lca

import (
	"log"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
)

type TestConfig struct {
	LogCacheAddr   string `env:"LOG_CACHE_ADDR,   required"`
	LogEmitterAddr string `env:"LOG_EMITTER_ADDR, required"`

	TLS TLS

	DefaultTimeout     time.Duration `env:"DEFAULT_TIMEOUT"`
	WaitForLogsTimeout time.Duration `env:"LOG_EMIT_TIMEOUT"`
}

var config *TestConfig

func LoadConfig() (*TestConfig, error) {
	config := &TestConfig{
		DefaultTimeout:     10 * time.Second,
		WaitForLogsTimeout: 10 * time.Second,
	}
	err := envstruct.Load(config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func Config() *TestConfig {
	if config != nil {
		return config
	}

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load drain test config: %s", err)
	}
	config = cfg
	return config
}
