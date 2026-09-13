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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
 * A receiver that answers 2xx but reports writing fewer samples than it was
 * sent is contradicting its own acknowledgement. The plugin counts the
 * difference so an operator can see it, and changes nothing else (#187).
 */
class FlusherReceiverShortfallTest {

    private static final String HEADER = "X-Prometheus-Remote-Write-Samples-Written";

    private MockWebServer server;
    private RemoteWriteHttpClient http;
    private PluginMetrics metrics;
    private Flusher flusher;
    private OverflowBucket bucket;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/write").toString());
        c.setReadUrl(server.url("/").toString());
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(1);
        c.setOverflowMaxSizeBytes(0);
        c.validate();
        http = new RemoteWriteHttpClient(c);
        metrics = new PluginMetrics();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flusher != null) flusher.stop(1_000);
        if (bucket != null) bucket.close();
        http.shutdown();
        server.shutdown();
    }

    private static List<MappedSample> tenSamples() {
        List<MappedSample> out = new ArrayList<>();
        for (long t = 1; t <= 10; t++) {
            Map<String, String> labels = new LinkedHashMap<>();
            labels.put("__name__", "shortfall_metric");
            labels.put("instance", "ut");
            out.add(new MappedSample(labels, t, (double) t));
        }
        return out;
    }

    private long counter(String name) {
        return metrics.snapshot().get(name).longValue();
    }

    private Flusher memoryFlusher() {
        return new Flusher(new SampleQueue(100), http, 100, 50, metrics);
    }

    @Test
    void the_counter_is_exported_and_starts_at_zero() {
        assertThat(counter(PluginMetrics.SAMPLES_UNCONFIRMED_BY_RECEIVER)).isZero();
    }

    @Test
    void a_short_count_on_a_2xx_is_counted_and_nothing_else_changes() {
        server.enqueue(new MockResponse().setResponseCode(204).setHeader(HEADER, "7"));
        memoryFlusher().flushBatch(tenSamples());
        assertThat(counter(PluginMetrics.SAMPLES_WRITTEN)).isEqualTo(10);
        assertThat(counter(PluginMetrics.SAMPLES_UNCONFIRMED_BY_RECEIVER)).isEqualTo(3);
    }

    @Test
    void a_matching_count_or_no_header_counts_nothing() {
        server.enqueue(new MockResponse().setResponseCode(204).setHeader(HEADER, "10"));
        server.enqueue(new MockResponse().setResponseCode(204));
        Flusher f = memoryFlusher();
        f.flushBatch(tenSamples());
        f.flushBatch(tenSamples());
        assertThat(counter(PluginMetrics.SAMPLES_WRITTEN)).isEqualTo(20);
        assertThat(counter(PluginMetrics.SAMPLES_UNCONFIRMED_BY_RECEIVER)).isZero();
    }

    @Test
    void a_disk_batch_with_a_short_count_is_still_acknowledged(@TempDir Path dir) throws Exception {
        bucket = OverflowBucket.open(dir, 1L << 20, 64 * 1024, OverflowBucket.FullPolicy.REFUSE,
                FsyncPolicy.BATCH, 64 * 1024, "shard-0");
        for (MappedSample s : tenSamples()) bucket.append(s);
        server.enqueue(new MockResponse().setResponseCode(204).setHeader(HEADER, "6"));
        flusher = new Flusher(new SampleQueue(100), bucket, null, http, 100, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1), "test-flusher");
        flusher.start();
        await().atMost(Duration.ofSeconds(5)).until(bucket::isEmpty);
        await().atMost(Duration.ofSeconds(5))
               .until(() -> counter(PluginMetrics.SAMPLES_UNCONFIRMED_BY_RECEIVER) == 4);
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW)).isEqualTo(10);
    }
}
