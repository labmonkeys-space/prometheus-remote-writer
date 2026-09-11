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
import java.time.Duration;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class FlusherTest {

    private MockWebServer server;
    private RemoteWriteHttpClient http;
    private SampleQueue queue;
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
        c.validate();
        http    = new RemoteWriteHttpClient(c);
        queue   = new SampleQueue(100);
        metrics = new PluginMetrics();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flusher != null) flusher.stop(1_000);
        http.shutdown();
        server.shutdown();
    }

    @Test
    void flush_batch_sends_compressed_payload_to_the_http_client() {
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 10, 10_000, metrics);

        flusher.flushBatch(java.util.List.of(sample(1), sample(2)));

        assertThat(http.getWritesSuccessful()).isEqualTo(1);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void background_thread_flushes_on_sample_arrival() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 100, 50, metrics);

        // Pre-load before start() so the background thread's first pollBatch
        // sees all three samples immediately and coalesces them into one
        // batch. Enqueueing after start() races the flusher: on a busy
        // runner, pollBatch can unblock on sample 1 before 2 and 3 land,
        // producing two batches.
        queue.tryEnqueue(sample(1));
        queue.tryEnqueue(sample(2));
        queue.tryEnqueue(sample(3));

        flusher.start();

        await().atMost(Duration.ofSeconds(2))
                .until(() -> http.getWritesSuccessful() == 1);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void background_thread_caps_each_flush_at_batch_size() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 5, 50, metrics);
        flusher.start();

        for (int i = 0; i < 10; i++) queue.tryEnqueue(sample(i));

        // 10 samples / batch=5 = 2 HTTP calls.
        await().atMost(Duration.ofSeconds(2))
                .until(() -> http.getWritesSuccessful() == 2);
    }

    @Test
    void stop_flushes_residual_samples_within_grace_period() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 100, 10_000, metrics); // long interval → relies on shutdown drain
        flusher.start();

        queue.tryEnqueue(sample(1));
        // Don't wait — call stop immediately. The run-loop may or may not have
        // picked the sample up yet; the residual-drain path in run() must
        // still flush it.
        flusher.stop(2_000);

        assertThat(http.getWritesSuccessful()).isEqualTo(1);
    }

    private static MappedSample sample(int i) {
        return new MappedSample(
                Map.of("__name__", "t", "i", Integer.toString(i)),
                1_000_000L + i,
                (double) i);
    }
}
