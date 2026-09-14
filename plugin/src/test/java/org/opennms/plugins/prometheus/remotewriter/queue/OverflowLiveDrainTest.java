/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.fail;

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
 * process restarted. This pins the requirement end to end, in two steps per
 * cycle: the bucket is drained while samples keep arriving (the drained
 * counter advances under an offered rate the flusher can keep up with), and
 * once the offering stops the bucket ends up empty. The two are proved one
 * after the other on purpose: under {@code ordered} the new samples land in
 * the bucket too, so demanding "empty" while still offering is a race against
 * the offered rate that a loaded machine can lose (#200). The race behind
 * #177 is pinned deterministically in {@link FlusherPipelineAccountingTest};
 * this test rarely hits it by itself.
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

                // First: the bucket drains while samples keep arriving. Offer
                // at about 1,000 samples/s, well under the flusher's ceiling,
                // until the drained counter has moved past its pre-spill
                // value. A flusher that never drains a live spill (#177)
                // fails here, with the diagnostic; one that parks after a
                // batch fails in the second step.
                long writtenAtSpill = written(metrics);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (drained(metrics) <= drainedBefore) {
                    if (System.nanoTime() > deadline) {
                        fail("cycle %d under %s: nothing drained from the bucket after 10 s of offering; "
                                + "bucket holds %d sample(s), written %d since the spill, %d requests in total",
                                cycle, drain, shards.totalOverflowPending(),
                                written(metrics) - writtenAtSpill, server.getRequestCount());
                    }
                    for (int i = 0; i < 5; i++) {
                        assertThat(shards.accept(sample(ts++))).isNotEqualTo(Shards.Acceptance.REFUSED);
                    }
                    Thread.sleep(5);
                }

                // Second: with nothing more arriving, the bucket ends up empty
                // and every spilled sample has been drained. Under `ordered`
                // the samples offered above landed in the bucket behind the
                // backlog, so this is bounded by the backlog at the flusher's
                // rate, not by a race with new arrivals; the bound only turns
                // a hang into a failure. Waiting for the drained counter to
                // catch up with the spilled one, not only for the bucket to
                // read empty, also means the next cycle's drainedBefore is
                // read after this cycle's last increment has landed.
                int thisCycle = cycle;
                await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(5)).untilAsserted(() -> {
                    assertThat(shards.recoveringShards())
                            .as("cycle %d under %s: bucket still holds %d sample(s) 30 s after offering stopped; "
                                + "written %d since the spill, %d requests in total",
                                thisCycle, drain, shards.totalOverflowPending(),
                                written(metrics) - writtenAtSpill, server.getRequestCount())
                            .isZero();
                    assertThat(drained(metrics))
                            .as("cycle %d under %s: drained %d of %d spilled", thisCycle, drain,
                                drained(metrics), shards.totalSamplesSpilled())
                            .isEqualTo(shards.totalSamplesSpilled());
                });
                assertThat(drained(metrics) - drainedBefore)
                        .as("cycle %d under %s: this cycle's spill was drained in full", thisCycle, drain)
                        .isEqualTo(shards.totalSamplesSpilled() - spilledBefore);
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
