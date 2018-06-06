package lca_test

import (
	"testing"

	_ "code.cloudfoundry.org/log-cache-acceptance-tests/tests"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

func TestLogCacheAcceptanceTests(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LogCacheAcceptanceTests Suite")
}
