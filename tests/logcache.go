package tests

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	lca "github.com/cloudfoundry/log-cache-acceptance-tests"
	uuid "github.com/nu7hatch/gouuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("LogCache", func() {
	var (
		c *logcache.Client
	)

	Context("with grpc client", func() {
		BeforeEach(func() {
			cfg := lca.Config()
			c = logcache.NewClient(
				cfg.LogCacheAddr,
				logcache.WithViaGRPC(
					grpc.WithTransportCredentials(
						cfg.TLS.Credentials("log-cache"),
					),
				),
			)
		})

		It("makes emitted logs available", func() {
			s := newUUID()

			start := time.Now()
			emitLogs([]string{s})
			end := time.Now()

			received := countEnvelopes(start, end, c.Read, s, 10000)
			Expect(received).To(BeNumerically(">=", 9900))
		})

		It("lists the available source ids that log cache has persisted", func() {
			s := newUUID()

			emitLogs([]string{s})

			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			meta, err := c.Meta(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(meta).To(HaveKey(s))

			count := meta[s].GetCount()
			Expect(count).To(BeNumerically(">=", 9900))
		})

		It("can query for emitted metrics with PromQL™ Instant Queries©", func() {
			s := newUUID()

			emitGauges([]string{s})

			query := fmt.Sprintf("metric{source_id=%q}", s)
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			result, err := c.PromQL(ctx, query)
			Expect(err).ToNot(HaveOccurred())

			vector := result.GetVector()
			Expect(vector.Samples).To(HaveLen(1))
			Expect(vector.Samples[0].Point.GetValue()).To(Equal(10.0))
		})

		It("can do math on emitted metrics with PromQL™ Instant Queries©", func() {
			s := newUUID()
			s2 := newUUID()

			emitGauges([]string{s, s2})

			query := fmt.Sprintf("metric{source_id=%q} + metric{source_id=%q}", s, s2)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := c.PromQL(ctx, query)
			Expect(err).ToNot(HaveOccurred())

			vector := result.GetVector()
			Expect(vector.Samples).To(HaveLen(1))
			Expect(vector.Samples[0].Point.GetValue()).To(Equal(20.0))
		})

		It("performs aggregations with PromQL™ Range Queries©", func() {
			s := newUUID()
			now := time.Now()

			emitGauges([]string{s})

			Consistently(func() float64 {
				query := fmt.Sprintf("sum_over_time(metric{source_id=%q}[5m])", s)
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				result, err := c.PromQLRange(
					ctx,
					query,
					WithPromQLStart(now.Add(-time.Minute)),
					WithPromQLEnd(now.Add(time.Minute)),
					WithPromQLStep("15s"),
				)
				Expect(err).ToNot(HaveOccurred())

				vector := result.GetVector()
				Expect(vector.Samples).To(HaveLen(1))
				return vector.Samples[0].Point.GetValue()
			}, 30).Should(BeEquivalentTo(100000.0))
		})
	})

	Context("with http client", func() {
		BeforeEach(func() {
			cfg := lca.Config()
			c = logcache.NewClient(
				cfg.LogCacheCFAuthProxyURL,
				logcache.WithHTTPClient(newOauth2HTTPClient(cfg)),
			)
		})

		It("makes emitted logs available", func() {
			s := newUUID()

			start := time.Now()
			emitLogs([]string{s})
			end := time.Now()

			received := countEnvelopes(start, end, c.Read, s, 10000)
			Expect(received).To(BeNumerically(">=", 9000))
		})

		It("lists the available source ids that log cache has persisted", func() {
			s := newUUID()

			emitLogs([]string{s})

			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			meta, err := c.Meta(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(meta).To(HaveKey(s))

			count := meta[s].GetCount()
			Expect(count).To(BeNumerically(">=", 9900))
		})

		It("can query for emitted metrics with PromQL™", func() {
			s := newUUID()

			emitGauges([]string{s})

			query := fmt.Sprintf("metric{source_id=%q}", s)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := c.PromQL(ctx, query)
			Expect(err).ToNot(HaveOccurred())

			vector := result.GetVector()
			Expect(vector.Samples).To(HaveLen(1))
			Expect(vector.Samples[0].Point.GetValue()).To(Equal(10.0))
		})

		It("can do math on emitted metrics with PromQL™", func() {
			s := newUUID()
			s2 := newUUID()

			emitGauges([]string{s, s2})

			query := fmt.Sprintf("metric{source_id=%q} + metric{source_id=%q}", s, s2)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := c.PromQL(ctx, query)
			Expect(err).ToNot(HaveOccurred())

			vector := result.GetVector()
			Expect(vector.Samples).To(HaveLen(1))
			Expect(vector.Samples[0].Point.GetValue()).To(Equal(20.0))
		})

		It("performs aggregations on range queries with PromQL™", func() {
			s := newUUID()

			emitGauges([]string{s})

			Consistently(func() float64 {
				query := fmt.Sprintf("sum_over_time(metric{source_id=%q}[5m])", s)
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				result, err := c.PromQL(ctx, query)
				Expect(err).ToNot(HaveOccurred())

				vector := result.GetVector()
				Expect(vector.Samples).To(HaveLen(1))
				return vector.Samples[0].Point.GetValue()
			}, 30).Should(BeEquivalentTo(100000.0))
		})

		It("performs a range query over a range of time with PromQL™", func() {
			sourceID := newUUID()

			emitGauges([]string{sourceID})

			start := time.Now()
			emitGauges([]string{sourceID})
			end := time.Now()

			emitGauges([]string{sourceID})

			query := fmt.Sprintf("metric{source_id=%q}", sourceID)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := c.PromQLRange(ctx, query, start, end, 15*time.Second)

			// Expect(vector.Samples).To(HaveLen(10000))
			Expect(err).ToNot(HaveOccurred())
		})
	})

})

func newOauth2HTTPClient(cfg *lca.TestConfig) *logcache.Oauth2HTTPClient {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: cfg.SkipCertVerify,
			},
		},
		Timeout: 5 * time.Second,
	}

	return logcache.NewOauth2HTTPClient(
		cfg.UAAURL,
		cfg.ClientID,
		cfg.ClientSecret,
		logcache.WithOauth2HTTPClient(client),
	)
}

func emitLogs(sourceIDs []string) {
	cfg := lca.Config()
	query := strings.Join(sourceIDs, "&sourceIDs=")
	logUrl := fmt.Sprintf("http://%s/emit-logs?sourceIDs=%s", cfg.LogEmitterAddr, query)

	res, err := http.Get(logUrl)

	Expect(err).ToNot(HaveOccurred())
	Expect(res.StatusCode).To(Equal(http.StatusOK))
	waitForLogs()
}

func emitGauges(sourceIDs []string) {
	cfg := lca.Config()
	query := strings.Join(sourceIDs, "&sourceIDs=")
	logurl := fmt.sprintf("http://%s/emit-gauges?sourceids=%s", cfg.logemitteraddr, query)

	res, err := http.Get(logUrl)

	Expect(err).ToNot(HaveOccurred())
	Expect(res.StatusCode).To(Equal(http.StatusOK))
	waitForLogs()
}

func waitForLogs() {
	cfg := lca.Config()
	time.Sleep(cfg.WaitForLogsTimeout)
}

func countEnvelopes(start, end time.Time, reader logcache.Reader, sourceID string, totalEmitted int) int {
	var receivedCount int
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	logcache.Walk(
		ctx,
		sourceID,
		func(envelopes []*loggregator_v2.Envelope) bool {
			receivedCount += len(envelopes)
			return receivedCount < totalEmitted
		},
		reader,
		logcache.WithWalkStartTime(start),
		logcache.WithWalkEndTime(end),
		logcache.WithWalkBackoff(logcache.NewRetryBackoff(50*time.Millisecond, 100)),
	)

	return receivedCount
}

func newUUID() string {
	u, err := uuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	return u.String()
}
