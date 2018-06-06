package tests

import (
	"fmt"
	"net/http"
	"time"

	"code.cloudfoundry.org/log-cache-acceptance-tests/lca"

	"github.com/cloudfoundry-incubator/cf-test-helpers/cf"
	"github.com/cloudfoundry-incubator/cf-test-helpers/generator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("v1", func() {
	Context("under heavy ingress load", func() {
		// log emitter setup
		logEmitterApp := PushLogEmitter("")
		fmt.Println(logEmitterApp)

		Describe("/read/sourceID", func() {
			It("returns a 2xx response", func() {
			})

			It("returns envelopes in valid json format", func() {
			})
		})

		// Describe("/meta", func() {

		// })
	})
})

func PushLogEmitter(path string) string {
	appName := generator.PrefixedRandomName("LOG-EMITTER", "")
	EventuallyWithOffset(1, cf.Cf(
		"push",
		appName,
		"--no-start",
		"-p", path,
	), 45*time.Second).Should(Exit(0), "Failed to push log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"set-env",
		appName,
		"ADDR",
		lca.Config().LogEmitterAddr,
	), 45*time.Second).Should(Exit(0), "Failed to restart log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"set-env",
		appName,
		"LOG_CACHE_ADDR",
		lca.Config().LogCacheAddr,
	), 45*time.Second).Should(Exit(0), "Failed to LOG_CACHE_ADDR env var for log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"set-env",
		appName,
		"CA_PATH",
		lca.Config().CAPath,
	), 45*time.Second).Should(Exit(0), "Failed to CA_PATH env var for log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"set-env",
		appName,
		"CERT_PATH",
		lca.Config().CertPath,
	), 45*time.Second).Should(Exit(0), "Failed to CERT_PATH env var for log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"set-env",
		appName,
		"KEY_PATH",
		lca.Config().KeyPath,
	), 45*time.Second).Should(Exit(0), "Failed to KEY_PATH env var for log emitter app")

	EventuallyWithOffset(1, cf.Cf(
		"restart",
		appName,
	), 45*time.Second).Should(Exit(0), "Failed to restart log emitter app")

	return appName
}

func WriteToLogsApp(doneChan chan struct{}, sourceIDs []string, logEmitterAppName string) {
	cfg := lca.Config()
	logUrl := fmt.Sprintf("http://%s.%s/emit/%s", logEmitterAppName, cfg.CFDomain, sourceIDs)
	defer GinkgoRecover()
	for {
		select {
		case <-doneChan:
			return
		default:
			resp, err := http.Get(logUrl)
			ExpectWithOffset(1, err).ToNot(HaveOccurred())
			ExpectWithOffset(1, resp.StatusCode).To(Equal(http.StatusOK))
			time.Sleep(3 * time.Second)
		}
	}
}
