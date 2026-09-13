/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;

/**
 * A shard that spills while running drains through the flusher it already
 * has, with no restart (#177).
 *
 * <p>v0.8.0 drained a bucket only when it was found non-empty at start-up.
 * A spill during operation could leave the shard sending nothing until the
 * process restarted. This pins the requirement end to end: many spills, each
 * drained to empty under a sustained offered rate the flusher can keep up
 * with. The race behind #177 is pinned deterministically in
 * {@link FlusherPipelineAccountingTest}; this test rarely hits it by itself.
 */
class OverflowLiveDrainTest {

    private static final int CYCLES = 10;

    private static MappedSample sample(long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut");
        return new MappedSample(labels, ts, (double) ts);
    }

    @ParameterizedTest
    @EnumSource(OverflowBucket.DrainPolicy.class)
    void a_shard_that_spills_while_running_drains_without_a_restart(
            OverflowBucket.DrainPolicy drain, @TempDir Path dir) throws Exception {
        MockWebServer server = new MockWebServer();
        server.setDispatcher(new Dispatcher() {
            @Override public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(204);
            }
        });
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        c.setOverflowMaxSizeBytes(0);
        c.validate();
        RemoteWriteHttpClient http = new RemoteWriteHttpClient(c);
        PluginMetrics metrics = new PluginMetrics();
        Shards shards = new Shards(1, 1_000, http, 500, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        return OverflowBucket.open(dir.resolve("shard-" + shard), 64L << 20,
                                8L << 20, OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH,
                                64 * 1024, "shard-" + shard);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, drain);
        shards.start();
        long ts = 1;
        try {
            for (int cycle = 0; cycle < CYCLES; cycle++) {
                // Offer faster than one flusher drains until the shard spills.
                long drainedBefore = drained(metrics);
                long spilledBefore = shards.totalSamplesSpilled();
                while (shards.totalSamplesSpilled() == spilledBefore) {
                    assertThat(shards.accept(sample(ts++))).isNotEqualTo(Shards.Acceptance.REFUSED);
                }

                // Keep offering at about 1,000 samples/s, well under the
                // flusher's ceiling, and wait for the bucket to empty. Each
                // disk batch waits for a checkpoint fsync, so a large batch
                // keeps the drain comfortably ahead of the offered rate. Under
                // `ordered` the new samples land in the bucket too, and it is
                // empty only once the reader catches up between two arrivals,
                // which is why a drain here can take a few seconds.
                long writtenAtSpill = written(metrics);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (shards.recoveringShards() > 0) {
                    if (System.nanoTime() > deadline) {
                        fail("cycle %d under %s: bucket still holds %d sample(s) after 10 s; "
                                + "written %d since the spill, %d requests in total",
                                cycle, drain, shards.totalOverflowPending(),
                                written(metrics) - writtenAtSpill, server.getRequestCount());
                    }
                    for (int i = 0; i < 5; i++) {
                        assertThat(shards.accept(sample(ts++))).isNotEqualTo(Shards.Acceptance.REFUSED);
                    }
                    Thread.sleep(5);
                }
                int thisCycle = cycle;
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(drained(metrics)).as("cycle %d: drained from the bucket", thisCycle)
                                .isGreaterThan(drainedBefore));
                assertThat(shards.totalOverflowPending()).isZero();
            }
        } finally {
            shards.stop(1_000);
            shards.close();
            http.shutdown();
            server.shutdown();
        }
    }

    private static long drained(PluginMetrics metrics) {
        return metrics.snapshot().get(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW).longValue();
    }

    private static long written(PluginMetrics metrics) {
        return metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue();
    }
}
