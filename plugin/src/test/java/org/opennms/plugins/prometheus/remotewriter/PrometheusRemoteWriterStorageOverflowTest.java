/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

/**
 * {@code store()} against a configured disk tier.
 *
 * <p>The headline change this pins: a full memory queue is no longer a
 * refusal. Before 0.8.0 the cases below threw {@code StorageException} and
 * ticked {@code samples_dropped_queue_full_total}; now they return normally
 * and the samples are on disk. Refusal has moved to a full bucket.
 *
 * <p>The flusher is parked on a {@code NO_RESPONSE} server throughout, so
 * nothing drains and the tier a sample lands in is stable to assert on.
 */
class PrometheusRemoteWriterStorageOverflowTest {

    private MockWebServer server;
    private PrometheusRemoteWriterStorage storage;

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) storage.stop();
        if (server != null) server.shutdown();
    }

    /** Queue of {@code capacity}, a disk tier of {@code bucketBytes}, and a
     *  flusher that parks forever on its first write so nothing drains. */
    private PrometheusRemoteWriterConfig config(Path dir, int capacity, long bucketBytes,
                                                String full) throws Exception {
        server = new MockWebServer();
        server.start();
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        c.setQueueCapacity(capacity);
        c.setWriterShards(1);   // these cases size one queue deliberately; 0.8.0 defaults to 4
        c.setStorePolicy("partial");
        c.setBatchSize(1);
        c.setFlushIntervalMs(50);
        c.setHttpReadTimeoutMs(60_000);
        c.setHttpWriteTimeoutMs(60_000);
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(1);
        c.setShutdownGracePeriodMs(100);
        c.setOverflowDir(dir.toString());
        c.setMetadataCadenceMs(0);      // these cases count offered samples and tiers exactly
        c.setOverflowMaxSizeBytes(bucketBytes);
        c.setOverflowFull(full);
        return c;
    }

    private long metric(String name) {
        return storage.getMetrics().snapshot().get(name).longValue();
    }

    private static Sample sample(String id) {
        return ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", "t")
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .externalTag("id", id)
                        .build())
                .time(Instant.now())
                .value(1.0)
                .build();
    }

    @Test
    void a_full_queue_spills_instead_of_throwing(@TempDir Path dir) throws Exception {
        storage = new PrometheusRemoteWriterStorage(config(dir, 1, 1L << 20, "refuse"));
        storage.start();
        // Let the flusher take the first sample and park on the write.
        storage.store(List.of(sample("a")));
        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).as("flusher never parked").isNotNull();

        // Far more than the single memory slot can hold.
        assertThatCode(() -> {
            for (int i = 0; i < 50; i++) storage.store(List.of(sample("s" + i)));
        }).doesNotThrowAnyException();

        assertThat(metric(PluginMetrics.SAMPLES_SPILLED)).isPositive();
        assertThat(metric(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL))
                .as("a configured disk tier means a full queue never drops")
                .isZero();
        assertThat(metric(PluginMetrics.OVERFLOW_PENDING_SAMPLES)).isPositive();
    }

    @Test
    void a_full_bucket_refuses_and_names_the_overflow_counter(@TempDir Path dir) throws Exception {
        // Tiny bucket plus a parked flusher: both tiers fill, and only then
        // does store() throw.
        storage = new PrometheusRemoteWriterStorage(config(dir, 1, 8 * 1024, "refuse"));
        storage.start();

        assertThatThrownBy(() -> {
            for (int i = 0; i < 10_000; i++) storage.store(List.of(sample("s" + i)));
        }).isInstanceOf(StorageException.class);

        assertThat(metric(PluginMetrics.SAMPLES_DROPPED_OVERFLOW_FULL)).isPositive();
        assertThat(metric(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL)).isZero();
    }

    @Test
    void drop_oldest_keeps_accepting(@TempDir Path dir) throws Exception {
        storage = new PrometheusRemoteWriterStorage(config(dir, 1, 8 * 1024, "drop-oldest"));
        storage.start();

        assertThatCode(() -> {
            for (int i = 0; i < 500; i++) storage.store(List.of(sample("s" + i)));
        }).doesNotThrowAnyException();

        assertThat(metric(PluginMetrics.SAMPLES_EVICTED_OVERFLOW)).isPositive();
        assertThat(metric(PluginMetrics.OVERFLOW_BYTES)).isLessThanOrEqualTo(8 * 1024);
    }

    @Test
    void all_or_nothing_counts_the_bucket_as_room(@TempDir Path dir) throws Exception {
        // Under all-or-nothing a shard short of memory used to refuse the whole
        // call. With a bucket behind it, the call is accepted.
        PrometheusRemoteWriterConfig c = config(dir, 1, 1L << 20, "refuse");
        c.setStorePolicy("all-or-nothing");
        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();

        assertThatCode(() -> storage.store(List.of(
                sample("a"), sample("b"), sample("c"), sample("d"))))
                .doesNotThrowAnyException();
        assertThat(metric(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL)).isZero();
    }

    @Test
    void the_overflow_gauges_are_registered(@TempDir Path dir) throws Exception {
        storage = new PrometheusRemoteWriterStorage(config(dir, 4, 1L << 20, "refuse"));
        storage.start();

        assertThat(storage.getMetrics().snapshot())
                .containsKeys(PluginMetrics.OVERFLOW_PENDING_SAMPLES,
                        PluginMetrics.OVERFLOW_BYTES,
                        PluginMetrics.SAMPLES_SPILLED,
                        PluginMetrics.SAMPLES_DROPPED_OVERFLOW_FULL,
                        PluginMetrics.SAMPLES_EVICTED_OVERFLOW,
                        PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW);
    }

    @Test
    void no_gauges_and_no_directory_when_the_tier_is_disabled(@TempDir Path dir) throws Exception {
        PrometheusRemoteWriterConfig c = config(dir, 4, 0, "refuse");
        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();

        assertThat(storage.getMetrics().snapshot())
                .doesNotContainKeys(PluginMetrics.OVERFLOW_PENDING_SAMPLES,
                        PluginMetrics.OVERFLOW_BYTES);
        assertThat(dir.toFile().list()).as("nothing is written when the tier is off").isEmpty();
    }

    @Test
    void nothing_offered_is_lost_when_the_backend_is_down_from_the_start(@TempDir Path dir)
            throws Exception {
        // Reconciliation is the trap here. An earlier version of the rescue
        // path inferred "already on disk" from a batch not being registered as
        // in flight, which was true for the shutdown residual drain — so those
        // samples were discarded, counted nowhere, and offered no longer
        // equalled written + dropped + pending. Nothing in the metrics said so.
        server = new MockWebServer();
        server.start();
        server.shutdown();   // dead endpoint: every write fails at connect
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        c.setWriterShards(1);   // one queue, deliberately tiny; 0.8.0 defaults to 4
        c.setQueueCapacity(1);
        c.setBatchSize(1);
        c.setStorePolicy("partial");
        c.setFlushIntervalMs(50);
        c.setRetryMaxAttempts(1);
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setShutdownGracePeriodMs(500);
        c.setOverflowDir(dir.toString());
        c.setOverflowMaxSizeBytes(1L << 20);
        c.setOverflowFull("refuse");
        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();

        for (int i = 0; i < 10; i++) storage.store(List.of(sample("s" + i)));
        Thread.sleep(300);   // let the flusher try, fail, and rescue

        long offered  = metric(PluginMetrics.STORE_SAMPLES_OFFERED);
        long written  = metric(PluginMetrics.SAMPLES_WRITTEN);
        long pending  = metric(PluginMetrics.OVERFLOW_PENDING_SAMPLES);
        long dropped  = metric(PluginMetrics.SAMPLES_DROPPED_4XX)
                      + metric(PluginMetrics.SAMPLES_DROPPED_5XX)
                      + metric(PluginMetrics.SAMPLES_DROPPED_TRANSPORT)
                      + metric(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL)
                      + metric(PluginMetrics.SAMPLES_DROPPED_OVERFLOW_FULL)
                      + metric(PluginMetrics.SAMPLES_DROPPED_NONFINITE)
                      + metric(PluginMetrics.SAMPLES_DROPPED_DUPLICATE)
                      + metric(PluginMetrics.SAMPLES_DROPPED_UNMAPPED)
                      + metric(PluginMetrics.SAMPLES_DROPPED_SHUTDOWN);

        assertThat(offered).isEqualTo(10);
        assertThat(written).as("the backend never came up").isZero();
        assertThat(dropped)
                .as("the bucket had room the whole time, so nothing may be dropped")
                .isZero();
        assertThat(pending + written + dropped)
                .as("offered = written + dropped + pending — nothing may vanish uncounted")
                .isEqualTo(offered);
    }

    @Test
    void samples_pending_on_disk_survive_a_restart(@TempDir Path dir) throws Exception {
        storage = new PrometheusRemoteWriterStorage(config(dir, 1, 1L << 20, "refuse"));
        storage.start();
        storage.store(List.of(sample("a")));
        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).isNotNull();
        for (int i = 0; i < 20; i++) storage.store(List.of(sample("s" + i)));
        long pendingBefore = metric(PluginMetrics.OVERFLOW_PENDING_SAMPLES);
        assertThat(pendingBefore).isPositive();
        storage.stop();
        server.shutdown();

        // Fresh pipeline over the same directory.
        storage = new PrometheusRemoteWriterStorage(config(dir, 1, 1L << 20, "refuse"));
        storage.start();
        assertThat(metric(PluginMetrics.WAL_REPLAY_SAMPLES))
                .as("the recovered samples are reported at startup").isPositive();
    }
}
