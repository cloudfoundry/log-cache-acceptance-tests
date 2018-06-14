package tests

import (
	"context"
	"fmt"
	"math/rand"
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
		c   *logcache.Client
		grc *logcache.ShardGroupReaderClient
	)

	BeforeEach(func() {
		cfg := lca.Config()
		c = logcache.NewClient(
			cfg.LogCacheAddr,
			logcache.WithViaGRPC(
				grpc.WithTransportCredentials(
					cfg.TLS.Credentials("log-cache"),
				),
			),
			logcache.WithHTTPClient(http.DefaultClient),
		)

		grc = logcache.NewShardGroupReaderClient(
			cfg.GroupReaderAddr,
			logcache.WithViaGRPC(
				grpc.WithTransportCredentials(
					cfg.TLS.Credentials("log-cache"),
				),
			),
			logcache.WithHTTPClient(http.DefaultClient),
		)
	})

	It("makes emitted logs available", func() {
		s := newUUID()

		start := time.Now()
		emitLogs([]string{s})
		waitForLogs()
		end := time.Now()

		received := countEnvelopes(start, end, c.Read, s, 10000)
		Expect(received).To(BeNumerically(">=", 9900))
	})

	It("lists the available source ids that log cache has persisted", func() {
		s := newUUID()

		emitLogs([]string{s})
		waitForLogs()

		meta, err := c.Meta(context.Background())
		Expect(err).ToNot(HaveOccurred())
		Expect(meta).To(HaveKey(s))

		count := meta[s].GetCount()
		Expect(count).To(BeNumerically(">=", 9900))
	})

	It("creates a group and reads from it", func() {
		s1 := newUUID()
		s2 := newUUID()

		groupName := newUUID()
		createGroup(grc, groupName, []string{s1, s2})

		start := time.Now()
		emitLogs([]string{s1, s2})
		waitForLogs()
		end := time.Now()

		reader := grc.BuildReader(rand.Uint64())
		received := countEnvelopes(start, end, reader, groupName, 20000)
		Expect(received).To(BeNumerically(">=", 2*9900))
	})

	It("can get metadata from a shard group", func() {
		s1 := newUUID()
		s2 := newUUID()

		groupName := newUUID()
		createGroup(grc, groupName, []string{s1, s2})

		emitLogs([]string{s1, s2})
		waitForLogs()

		requestorID := rand.Uint64()
		grc.Read(context.Background(), groupName, time.Time{}, requestorID)

		shardGroup, err := grc.ShardGroup(context.Background(), groupName)
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
		waitForLogs()

		query := fmt.Sprintf("metric{source_id=%q}", s)
		result, err := c.PromQL(context.Background(), query)
		Expect(err).ToNot(HaveOccurred())

		vector := result.GetVector()
		Expect(vector.Samples).To(HaveLen(1))
		Expect(vector.Samples[0].Point.GetValue()).To(Equal(10.0))
	})

	It("can do math on emitted metrics with PromQL™", func() {
		s := newUUID()
		s2 := newUUID()

		emitGauges([]string{s, s2})
		waitForLogs()

		query := fmt.Sprintf("metric{source_id=%q} + metric{source_id=%q}", s, s2)
		result, err := c.PromQL(context.Background(), query)
		Expect(err).ToNot(HaveOccurred())

		vector := result.GetVector()
		Expect(vector.Samples).To(HaveLen(1))
		Expect(vector.Samples[0].Point.GetValue()).To(Equal(20.0))
	})

	It("performs aggregations on range queries with PromQL™", func() {
		s := newUUID()

		emitGauges([]string{s})
		waitForLogs()

		Consistently(func() float64 {
			query := fmt.Sprintf("sum_over_time(metric{source_id=%q}[5m])", s)
			result, err := c.PromQL(context.Background(), query)
			Expect(err).ToNot(HaveOccurred())

			vector := result.GetVector()
			Expect(vector.Samples).To(HaveLen(1))
			return vector.Samples[0].Point.GetValue()
		}, 30).Should(BeEquivalentTo(100000.0))
	})
})

func createGroup(client *logcache.ShardGroupReaderClient, groupName string, sourceIDs []string) {
	for _, sid := range sourceIDs {
		err := client.SetShardGroup(context.Background(), groupName, sid)
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
}

func emitGauges(sourceIDs []string) {
	cfg := lca.Config()
	query := strings.Join(sourceIDs, "&sourceIDs=")
	logUrl := fmt.Sprintf("http://%s/emit-gauges?sourceIDs=%s", cfg.LogEmitterAddr, query)

	res, err := http.Get(logUrl)

	Expect(err).ToNot(HaveOccurred())
	Expect(res.StatusCode).To(Equal(http.StatusOK))
}

func waitForLogs() {
	cfg := lca.Config()
	time.Sleep(cfg.WaitForLogsTimeout)
}

func countEnvelopes(start, end time.Time, reader logcache.Reader, sourceID string, totalEmitted int) int {
	var receivedCount int
	logcache.Walk(
		context.Background(),
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
