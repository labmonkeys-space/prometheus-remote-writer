/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;

/**
 * The arbiter half of the tier: which tier a flusher takes its next batch
 * from, and what each tier's failure handling is.
 *
 * <p>The failure asymmetry is the point. A memory batch that exhausts its
 * retries is dropped, because there is nowhere to put it back. A disk batch is
 * not: the bucket rewinds and re-ships it, and its segment stays until the
 * backend has taken it. That is the whole reason a batch is drawn from exactly
 * one tier.
 */
class OverflowFlusherTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    private MockWebServer server;
    private RemoteWriteHttpClient http;
    private SampleQueue queue;
    private OverflowBucket bucket;
    private Flusher flusher;
    private PluginMetrics metrics;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(2);
        c.setOverflowMaxSizeBytes(0);
        c.validate();
        http    = new RemoteWriteHttpClient(c);
        queue   = new SampleQueue(100);
        metrics = new PluginMetrics();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flusher != null) flusher.stop(1_000);
        if (bucket != null) bucket.close();
        http.shutdown();
        server.shutdown();
    }

    private void openBucket(Path dir) throws IOException {
        bucket = OverflowBucket.open(dir, 1L << 20, 64 * 1024,
                OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH, MAX_PAYLOAD, "shard-0");
    }

    /** A shard with a disk tier and no rescue path, so a failed memory batch
     *  is dropped exactly as it was before 0.8.0. */
    private Flusher flusher(int batchSize) {
        return new Flusher(queue, bucket, null, http, batchSize, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1), "test-flusher");
    }

    private Flusher concurrentFlusher(int batchSize) {
        return new Flusher(queue, bucket, null, http, batchSize, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1), "test-flusher",
                OverflowBucket.DrainPolicy.CONCURRENT);
    }

    private static MappedSample sample(long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut");
        return new MappedSample(labels, ts, (double) ts);
    }

    private long counter(String name) {
        return metrics.snapshot().get(name).longValue();
    }

    @Test
    void the_disk_tier_drains_before_the_memory_tier(@TempDir Path dir) throws Exception {
        openBucket(dir);
        // Two samples on disk, two in memory. Disk goes first.
        bucket.append(sample(1));
        bucket.append(sample(2));
        queue.tryEnqueue(sample(3));
        queue.tryEnqueue(sample(4));

        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 4);

        assertThat(bucket.isEmpty()).isTrue();
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW))
                .as("only the two disk samples count as drained").isEqualTo(2);
    }

    @Test
    void a_request_never_mixes_tiers(@TempDir Path dir) throws Exception {
        openBucket(dir);
        // The bucket holds fewer than batch.size; memory holds more. The
        // request must carry the bucket's samples alone rather than topping up.
        bucket.append(sample(1));
        queue.tryEnqueue(sample(2));
        queue.tryEnqueue(sample(3));

        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 3);

        assertThat(server.getRequestCount())
                .as("one request for the disk sample, another for the memory ones")
                .isGreaterThanOrEqualTo(2);
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW)).isEqualTo(1);
    }

    @Test
    void a_failed_disk_batch_is_retried_not_dropped(@TempDir Path dir) throws Exception {
        openBucket(dir);
        bucket.append(sample(1));
        bucket.append(sample(2));

        // Enough 503s to exhaust the retries on the first cycle, then success.
        for (int i = 0; i < 4; i++) server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(204));

        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(15))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 2);

        assertThat(counter(PluginMetrics.SAMPLES_DROPPED_5XX))
                .as("a disk batch is never dropped for a 5xx — the bucket holds it")
                .isZero();
        assertThat(bucket.isEmpty()).as("and it leaves the bucket only once accepted").isTrue();
    }

    @Test
    void a_failed_memory_batch_is_dropped_when_there_is_nowhere_to_put_it(@TempDir Path dir)
            throws Exception {
        // No rescue path: the pre-0.8.0 behaviour, and what a deployment with
        // overflow.max-size-bytes=0 still gets.
        openBucket(dir);
        queue.tryEnqueue(sample(1));
        queue.tryEnqueue(sample(2));
        for (int i = 0; i < 6; i++) server.enqueue(new MockResponse().setResponseCode(503));

        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(15))
                .until(() -> counter(PluginMetrics.SAMPLES_DROPPED_5XX) >= 2);

        assertThat(counter(PluginMetrics.SAMPLES_WRITTEN)).isZero();
    }

    @Test
    void a_failed_memory_batch_goes_to_the_disk_tier_rather_than_being_lost(@TempDir Path dir) {
        // With a tier configured, a sample is durable from the first failure
        // rather than only once the queue happens to fill. Without this, up to
        // queue.capacity samples would still be lost while the bucket sat
        // empty, and "zero loss while the disk tier has room" would be false.
        //
        // Driven through a real Shards because the rescue is a collaboration
        // between the flusher and its shard: the batch has to be registered as
        // in flight, atomically with leaving the queue, for the shard to know
        // it can take it back.
        for (int i = 0; i < 4; i++) server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(new MockResponse().setResponseCode(204));

        try (Shards shards = new Shards(1, 4, http, 10, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        return OverflowBucket.open(dir.resolve("shard-" + shard), 1L << 20,
                                1 << 17, OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH,
                                MAX_PAYLOAD, "shard-" + shard);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                })) {
            shards.accept(sample(1));
            shards.accept(sample(2));
            shards.start();

            await().atMost(Duration.ofSeconds(15))
                    .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 2);

            assertThat(counter(PluginMetrics.SAMPLES_DROPPED_5XX))
                    .as("nothing is dropped — the batch went to disk and was retried")
                    .isZero();
            assertThat(counter(PluginMetrics.SAMPLES_DROPPED_TRANSPORT)).isZero();
            shards.stop(1_000);
        }
    }

    @Test
    void a_4xx_on_a_disk_batch_advances_past_it(@TempDir Path dir) throws Exception {
        // Permanent rejection: retrying forever would wedge the bucket, so the
        // checkpoint moves past the batch and the samples are counted dropped.
        openBucket(dir);
        bucket.append(sample(1));
        bucket.append(sample(2));
        server.enqueue(new MockResponse().setResponseCode(400));

        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> counter(PluginMetrics.SAMPLES_DROPPED_4XX) >= 2);

        await().atMost(Duration.ofSeconds(10)).until(() -> bucket.isEmpty());
        assertThat(counter(PluginMetrics.WAL_BATCHES_DROPPED_4XX)).isEqualTo(1);
    }

    @Test
    void a_rewound_disk_batch_is_not_overtaken_by_the_next_one(@TempDir Path dir)
            throws Exception {
        // The builder runs one payload ahead of the sender (D2). For the disk
        // tier that is only safe if it stops at one: the reader position is
        // shared state coupled to send outcomes. Read batch 2 while batch 1 is
        // in flight, and batch 1's rewind is undone by batch 2's
        // acknowledgement advancing the checkpoint straight past batch 1's
        // frames — which are then never sent, and whose series arrives out of
        // order behind batch 2's.
        openBucket(dir);
        for (int i = 1; i <= 6; i++) bucket.append(sample(i));

        // Exhaust the retries on the first request, then accept everything.
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(503));
        for (int i = 0; i < 20; i++) server.enqueue(new MockResponse().setResponseCode(204));

        flusher = flusher(2);   // three batches of two
        flusher.start();

        await().atMost(Duration.ofSeconds(20))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 6);

        assertThat(counter(PluginMetrics.SAMPLES_WRITTEN))
                .as("every sample ships, including the batch that was rewound")
                .isEqualTo(6);
        assertThat(counter(PluginMetrics.SAMPLES_DROPPED_5XX)).isZero();
        await().atMost(Duration.ofSeconds(10)).until(() -> bucket.isEmpty());
    }

    @Test
    void the_bucket_is_fsynced_on_the_flush_interval(@TempDir Path dir) throws Exception {
        // overflow.fsync=batch is documented as "loses at most one
        // flush-interval window". The segment layer only forces under
        // `always`, so that promise is only true if something calls flush() on
        // the boundary — and for a while nothing did, making the real RPO the
        // whole unflushed segment.
        OverflowBucket real = OverflowBucket.open(dir, 1L << 20, 64 * 1024,
                OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH, MAX_PAYLOAD, "shard-0");
        bucket = org.mockito.Mockito.spy(real);

        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                org.mockito.Mockito.verify(bucket, org.mockito.Mockito.atLeastOnce()).flush());
    }

    @Test
    void latency_of_a_spilled_sample_counts_from_its_original_store(@TempDir Path dir)
            throws Exception {
        // The stamp rides in the frame, so time on disk is inside the budget
        // rather than invisible to it.
        openBucket(dir);
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        long stampedAt = System.currentTimeMillis() - 500;
        bucket.append(new MappedSample(labels, 1_000L, 1.0, stampedAt));

        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = flusher(10);
        flusher.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 1);

        assertThat(counter(PluginMetrics.SAMPLE_LATENCY_MS))
                .as("at least the 500 ms the sample was already old")
                .isGreaterThanOrEqualTo(500);
    }

    @Test
    void concurrent_alternates_the_tiers_it_drains(@TempDir Path dir) throws Exception {
        // Under `ordered` the bucket is emptied before a single memory sample
        // goes out, so a fresh sample waits out the whole backlog. Alternating
        // is what stops that — and alternating rather than memory-first is
        // what stops the backlog becoming permanent when the offered rate
        // matches the drain rate.
        openBucket(dir);
        for (int i = 1; i <= 6; i++) bucket.append(sample(i));
        for (int i = 101; i <= 106; i++) queue.tryEnqueue(sample(i));

        for (int i = 0; i < 20; i++) server.enqueue(new MockResponse().setResponseCode(204));

        flusher = concurrentFlusher(2);
        flusher.start();

        await().atMost(Duration.ofSeconds(15))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 12);

        // Both tiers made progress rather than one being drained to empty
        // first: the disk half is counted separately from the total.
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW)).isEqualTo(6);
        assertThat(counter(PluginMetrics.SAMPLES_WRITTEN)).isEqualTo(12);
        assertThat(bucket.isEmpty()).isTrue();
        assertThat(queue.depth()).isZero();
    }

    @Test
    void concurrent_does_not_starve_the_memory_tier(@TempDir Path dir) throws Exception {
        // The property that distinguishes alternation from disk-first: with a
        // deep backlog, memory samples still go out early rather than after
        // the whole bucket.
        openBucket(dir);
        for (int i = 1; i <= 40; i++) bucket.append(sample(i));
        queue.tryEnqueue(sample(999));

        for (int i = 0; i < 60; i++) server.enqueue(new MockResponse().setResponseCode(204));

        flusher = concurrentFlusher(2);
        flusher.start();

        // The memory sample is one of the first handful out, not the 21st.
        await().atMost(Duration.ofSeconds(15)).until(() ->
                counter(PluginMetrics.SAMPLES_WRITTEN)
                        - counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW) >= 1);
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW))
                .as("memory got a turn well before the 40-sample backlog was gone")
                .isLessThan(40);
    }

    @Test
    void ordered_drains_the_bucket_before_any_memory_sample(@TempDir Path dir) throws Exception {
        // The default, unchanged: the counterpart of the test above.
        openBucket(dir);
        for (int i = 1; i <= 40; i++) bucket.append(sample(i));
        queue.tryEnqueue(sample(999));

        for (int i = 0; i < 60; i++) server.enqueue(new MockResponse().setResponseCode(204));

        flusher = flusher(2);
        flusher.start();

        await().atMost(Duration.ofSeconds(15))
                .until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) >= 41);

        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW))
                .as("every disk sample shipped, and only then the memory one")
                .isEqualTo(40);
    }

    @Test
    void concurrent_alternates_through_the_shipped_wiring(@TempDir Path dir) throws Exception {
        // The other concurrent cases here build a Flusher directly, which
        // leaves memoryTier null — a combination Shards never produces. That
        // path took a different branch, so those tests were asserting totals
        // over a flusher that did not actually alternate. This one goes
        // through Shards, so it covers what ships.
        for (int i = 0; i < 60; i++) server.enqueue(new MockResponse().setResponseCode(204));

        try (Shards shards = new Shards(1, 40, http, 2, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        return OverflowBucket.open(dir.resolve("shard-" + shard), 1L << 20,
                                1 << 17, OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH,
                                MAX_PAYLOAD, "shard-" + shard);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }, OverflowBucket.DrainPolicy.CONCURRENT)) {

            // A deep disk backlog and a little memory. Under ordered the
            // memory sample goes last; under concurrent it goes early.
            OverflowBucket bucket = shards.bucketForTesting(0);
            for (int i = 1; i <= 30; i++) bucket.append(sample(i));
            for (int i = 900; i <= 903; i++) shards.accept(sample(i));

            shards.start();

            await().atMost(Duration.ofSeconds(20)).until(() ->
                    counter(PluginMetrics.SAMPLES_WRITTEN)
                            - counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW) >= 4);

            assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW))
                    .as("the memory samples went out well before the 30-sample "
                        + "disk backlog was exhausted")
                    .isLessThan(30);
            shards.stop(2_000);
        }
    }
}
