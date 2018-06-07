package tests

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	lca "github.com/cloudfoundry/log-cache-acceptance-tests"
	uuid "github.com/nu7hatch/gouuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("LogCache", func() {
	It("makes emitted logs available", func() {
		s := sourceID()

		start := time.Now()
		emitLogs([]string{s})
		waitForLogs()
		end := time.Now()

		received := countEnvelopes(start, end, s)
		Expect(received).To(BeNumerically(">=", 9900))
	})

	It("source ids and counts available from meta", func() {
		s := sourceID()

		emitLogs([]string{s})
		waitForLogs()
		m := meta()

		Expect(m).To(HaveKey(s))

		count := m[s].GetCount()
		Expect(count).To(BeNumerically(">=", 9900))
	})
})

func emitLogs(sourceIDs []string) {
	cfg := lca.Config()
	query := strings.Join(sourceIDs, "&sourceIDs=")
	logUrl := fmt.Sprintf("http://%s/emit?sourceIDs=%s", cfg.LogEmitterAddr, query)

	resp, err := http.Get(logUrl)

	Expect(err).ToNot(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
}

func waitForLogs() {
	cfg := lca.Config()
	time.Sleep(cfg.WaitForLogsTimeout)
}

func countEnvelopes(start, end time.Time, sourceID string) int {
	cfg := lca.Config()
	client := logcache.NewClient(
		cfg.LogCacheAddr,
		logcache.WithViaGRPC(
			grpc.WithTransportCredentials(
				cfg.TLS.Credentials("log-cache"),
			),
		),
	)

	var receivedCount int
	logcache.Walk(
		context.Background(),
		sourceID,
		func(envelopes []*loggregator_v2.Envelope) bool {
			receivedCount += len(envelopes)
			return receivedCount < 10000
		},
		client.Read,
		logcache.WithWalkStartTime(start),
		logcache.WithWalkEndTime(end),
		logcache.WithWalkBackoff(logcache.NewRetryBackoff(50*time.Millisecond, 100)),
	)

	return receivedCount
}

func meta() map[string]*logcache_v1.MetaInfo {
	cfg := lca.Config()
	client := logcache.NewClient(
		cfg.LogCacheAddr,
		logcache.WithViaGRPC(
			grpc.WithTransportCredentials(
				cfg.TLS.Credentials("log-cache"),
			),
		),
	)
	meta, err := client.Meta(context.Background())
	Expect(err).ToNot(HaveOccurred())
	return meta
}

func sourceID() string {
	u, err := uuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	return u.String()
}
