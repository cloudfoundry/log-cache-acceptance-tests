package lca

import (
	"log"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
)

type TestConfig struct {
	LogCacheAddr           string `env:"LOG_CACHE_ADDR,    required"`
	GroupReaderAddr        string `env:"GROUP_READER_ADDR, required"`
	LogEmitterAddr         string `env:"LOG_EMITTER_ADDR,  required"`
	LogCacheCFAuthProxyURL string `env:"LOG_CACHE_CF_AUTH_PROXY_URL",  required"`

	TLS TLS

	UAAURL       string `env:"UAA_URL"`
	ClientID     string `env:"CLIENT_ID"`
	ClientSecret string `env:"CLIENT_SECRET, noreport"`

	SkipCertVerify bool `env:"SKIP_CERT_VERIFY"`

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
