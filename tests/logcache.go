package tests

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
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
	var (
		c   *logcache.Client
		grc *logcache.ShardGroupReaderClient
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

			//grc = logcache.NewShardGroupReaderClient(
			//	cfg.GroupReaderAddr,
			//	logcache.WithViaGRPC(
			//		grpc.WithTransportCredentials(
			//			cfg.TLS.Credentials("log-cache"),
			//		),
			//	),
			//)
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

		XIt("creates a group and reads from it", func() {
			s1 := newUUID()
			s2 := newUUID()

			groupName := newUUID()
			createGroup(grc, groupName, []string{s1, s2})

			start := time.Now()
			emitLogs([]string{s1, s2})
			end := time.Now()

			reader := grc.BuildReader(rand.Uint64())
			received := countEnvelopes(start, end, reader, groupName, 20000)
			Expect(received).To(BeNumerically(">=", 2*7500))
		})

		XIt("can get metadata from a shard group", func() {
			s1 := newUUID()
			s2 := newUUID()

			groupName := newUUID()
			createGroup(grc, groupName, []string{s1, s2})

			emitLogs([]string{s1, s2})

			requestorID := rand.Uint64()
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			grc.Read(ctx, groupName, time.Time{}, requestorID)

			ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
			shardGroup, err := grc.ShardGroup(ctx, groupName)
			Expect(err).ToNot(HaveOccurred())

			Expect(shardGroup.RequesterIDs).To(ConsistOf(requestorID))
			Expect(shardGroup.SubGroups).To(ConsistOf(
				logcache.SubGroup{SourceIDs: []string{s1}},
				logcache.SubGroup{SourceIDs: []string{s2}},
			))
		})

		It("can query for emitted metrics with PromQL™", func() {
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
	})

	Context("with http client", func() {
		BeforeEach(func() {
			cfg := lca.Config()
			c = logcache.NewClient(
				cfg.LogCacheCFAuthProxyURL,
				logcache.WithHTTPClient(newOauth2HTTPClient(cfg)),
			)

			//grc = logcache.NewShardGroupReaderClient(
			//	cfg.LogCacheCFAuthProxyURL,
			//	logcache.WithHTTPClient(newOauth2HTTPClient(cfg)),
			//)
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

		XIt("creates a group and reads from it", func() {
			s1 := newUUID()
			s2 := newUUID()

			groupName := newUUID()
			createGroup(grc, groupName, []string{s1, s2})

			start := time.Now()
			emitLogs([]string{s1, s2})
			end := time.Now()

			reader := grc.BuildReader(rand.Uint64())
			received := countEnvelopes(start, end, reader, groupName, 20000)
			Expect(received).To(BeNumerically(">=", 2*7500))
		})

		XIt("can get metadata from a shard group", func() {
			s1 := newUUID()
			s2 := newUUID()

			groupName := newUUID()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go maintainGroup(ctx, groupName, []string{s1, s2}, grc)

			requestorID := rand.Uint64()
			reader := grc.BuildReader(requestorID)

			start := time.Now()
			emitLogs([]string{s1, s2})
			end := time.Now()

			var receivedCount int
			wctx, _ := context.WithTimeout(ctx, time.Minute)
			logcache.Walk(
				wctx,
				groupName,
				func(envelopes []*loggregator_v2.Envelope) bool {
					for _, e := range envelopes {
						if strings.Contains(string(e.GetLog().GetPayload()), "log message") {
							receivedCount++
						}
					}
					return receivedCount < 20000
				},
				reader,
				logcache.WithWalkStartTime(start),
				logcache.WithWalkEndTime(end),
				logcache.WithWalkBackoff(logcache.NewRetryBackoff(time.Second, 30)),
				logcache.WithWalkEnvelopeTypes(logcache_v1.EnvelopeType_LOG),
			)

			Expect(receivedCount).To(BeNumerically(">=", 100))

			shardGroup, err := grc.ShardGroup(ctx, groupName)
			Expect(err).ToNot(HaveOccurred())

			Expect(shardGroup.RequesterIDs).To(ConsistOf(requestorID))
			Expect(shardGroup.SubGroups).To(ConsistOf(
				logcache.SubGroup{SourceIDs: []string{s1}},
				logcache.SubGroup{SourceIDs: []string{s2}},
			))
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

func maintainGroup(
	ctx context.Context,
	groupName string,
	sourceIDs []string,
	client *logcache.ShardGroupReaderClient,
) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		for _, sID := range sourceIDs {
			shardGroupCtx, _ := context.WithTimeout(ctx, time.Second)
			err := client.SetShardGroup(shardGroupCtx, groupName, sID)
			if err != nil {
				fmt.Printf("unable to set shard group: %s\n", err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

func createGroup(client *logcache.ShardGroupReaderClient, groupName string, sourceIDs []string) {
	for _, sid := range sourceIDs {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		err := client.SetShardGroup(ctx, groupName, sid)
		Expect(err).ToNot(HaveOccurred())
	}
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
