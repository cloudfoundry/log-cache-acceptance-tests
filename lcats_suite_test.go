package lca_test

import (
	"testing"

	"github.com/cloudfoundry-incubator/cf-test-helpers/cf"
	"github.com/cloudfoundry-incubator/cf-test-helpers/generator"

	"code.cloudfoundry.org/log-cache-acceptance-tests/lca"
	_ "code.cloudfoundry.org/log-cache-acceptance-tests/lca/tests"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

func TestLogCacheAcceptanceTests(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LogCacheAcceptanceTests Suite")
}

var (
	TestPrefix = "LCA"

	org           string
	space         string
	cliBinaryPath string
)

var _ = BeforeSuite(func() {
	cfg := lca.Config()

	targetAPI(cfg)
	login(cfg)

	createOrgAndSpace(cfg)
	cfTarget(cfg)
})

var _ = AfterSuite(func() {
	cfg := lca.Config()

	deleteOrg(cfg)
})

func targetAPI(cfg *lca.TestConfig) {
	commandArgs := []string{"api", "https://api." + cfg.CFDomain}

	if cfg.SkipCertVerify {
		commandArgs = append(commandArgs, "--skip-ssl-validation")
	}

	Eventually(cf.Cf(commandArgs...), cfg.DefaultTimeout).Should(Exit(0))
}

func login(cfg *lca.TestConfig) {
	Eventually(
		cf.Cf("auth",
			cfg.CFAdminUser,
			cfg.CFAdminPassword,
		), cfg.DefaultTimeout).Should(Exit(0))
}

func createOrgAndSpace(cfg *lca.TestConfig) {
	org = generator.PrefixedRandomName(TestPrefix, "org")
	space = generator.PrefixedRandomName(TestPrefix, "space")

	Eventually(cf.Cf("create-org", org), cfg.DefaultTimeout).Should(Exit(0))
	Eventually(cf.Cf("create-space", space, "-o", org), cfg.DefaultTimeout).Should(Exit(0))
}

func cfTarget(cfg *lca.TestConfig) {
	Eventually(cf.Cf("target", "-o", org, "-s", space), cfg.DefaultTimeout).Should(Exit(0))
}

func deleteOrg(cfg *lca.TestConfig) {
	Eventually(cf.Cf("delete-org", org, "-f"), cfg.DefaultTimeout).Should(Exit(0))
}
