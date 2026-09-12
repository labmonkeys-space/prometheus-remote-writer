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
    void linger_turns_a_trickle_into_one_request() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 100, 1000, 300, metrics,
                org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders.forVersion(1),
                "test-flusher");
        flusher.start();
        for (int i = 0; i < 10; i++) {
            queue.tryEnqueue(sample(i));
            Thread.sleep(10);
        }
        await().atMost(Duration.ofSeconds(3)).until(() -> http.getWritesSuccessful() >= 1);
        Thread.sleep(400); // longer than the linger: a split second batch would have landed by now
        assertThat(server.getRequestCount()).isEqualTo(1);
        assertThat(queue.depth()).isZero();
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue()).isEqualTo(10L);
        assertThat(metrics.snapshot().get(PluginMetrics.FLUSHER_LINGER_MS).longValue()).isGreaterThanOrEqualTo(200L);
    }

    @Test
    void linger_time_is_not_idle_time() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 100, 10_000, 200, metrics,
                org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders.forVersion(1),
                "test-flusher");
        long started = System.nanoTime();
        flusher.start();
        Thread.sleep(100); // idle: nothing queued
        queue.tryEnqueue(sample(1));
        long headWaitMs = (System.nanoTime() - started) / 1_000_000; // upper bound on legitimate idle
        await().atMost(Duration.ofSeconds(3)).until(() -> http.getWritesSuccessful() == 1);
        long idle = metrics.snapshot().get(PluginMetrics.FLUSHER_IDLE_MS).longValue();
        assertThat(metrics.snapshot().get(PluginMetrics.FLUSHER_LINGER_MS).longValue()).isGreaterThanOrEqualTo(150L);
        assertThat(idle).as("idle is the head wait only; the 200 ms linger must not be in it").isLessThanOrEqualTo(headWaitMs + 50);
    }

    @Test
    void waiting_on_an_empty_queue_counts_as_idle_time() throws Exception {
        flusher = new Flusher(queue, http, 100, 100, metrics);
        flusher.start();

        await().atMost(Duration.ofSeconds(2)).until(() ->
                metrics.snapshot().get(PluginMetrics.FLUSHER_IDLE_MS).longValue() >= 100L);
        assertThat(server.getRequestCount()).isZero();
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

    // ---------- #163: pipelined builder ------------------------------------

    private static final java.util.function.Function<java.util.Collection<MappedSample>,
            org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult> V1 =
            org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders.forVersion(1);

    /** First response held until released; every later one immediate 204. */
    private static okhttp3.mockwebserver.Dispatcher holdFirst(java.util.concurrent.CountDownLatch release) {
        java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        return new okhttp3.mockwebserver.Dispatcher() {
            @Override public MockResponse dispatch(okhttp3.mockwebserver.RecordedRequest r) {
                if (first.getAndSet(false)) {
                    try { release.await(10, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                return new MockResponse().setResponseCode(204);
            }
        };
    }

    @Test
    void next_batch_is_built_while_the_previous_request_is_in_flight() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        server.setDispatcher(holdFirst(release));
        for (int i = 0; i < 4; i++) queue.tryEnqueue(sample(i)); // two batches of two
        flusher = new Flusher(queue, http, 2, 10_000, 0L, metrics, V1, "test-flusher");
        flusher.start();
        try {
            assertThat(server.takeRequest(5, java.util.concurrent.TimeUnit.SECONDS)).as("first request sent").isNotNull();
            // With the first response held, a serial flusher leaves the second
            // batch in the queue; a pipelined one has taken and built it.
            await().atMost(Duration.ofSeconds(2)).until(() -> queue.depth() == 0);
            assertThat(server.getRequestCount()).as("second request must wait for the first response").isEqualTo(1);
            assertThat(metrics.snapshot().get(PluginMetrics.FLUSHER_BUILD_MS)).isNotNull();
        } finally {
            release.countDown();
        }
        await().atMost(Duration.ofSeconds(3)).until(() -> http.getWritesSuccessful() == 2);
    }

    @Test
    void per_series_order_holds_across_the_handoff() throws Exception {
        for (int i = 0; i < 40; i++) server.enqueue(new MockResponse().setResponseCode(204));
        flusher = new Flusher(queue, http, 3, 10_000, 0L, metrics, V1, "test-flusher");
        flusher.start();
        for (int t = 1; t <= 30; t++) {
            queue.tryEnqueue(new MappedSample(Map.of("__name__", "s", "k", "v"), 1_000_000L + t, t));
            if (t % 7 == 0) Thread.sleep(15);
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() == 30L);
        long last = 0; int requests = server.getRequestCount();
        for (int r = 0; r < requests; r++) {
            okhttp3.mockwebserver.RecordedRequest rr = server.takeRequest(1, java.util.concurrent.TimeUnit.SECONDS);
            org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest wr =
                    org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest.parseFrom(
                            org.xerial.snappy.Snappy.uncompress(rr.getBody().readByteArray()));
            for (var ts : wr.getTimeseriesList()) for (var s : ts.getSamplesList()) {
                assertThat(s.getTimestamp()).as("monotonic across requests").isGreaterThan(last);
                last = s.getTimestamp();
            }
        }
        assertThat(last).isEqualTo(1_000_030L);
    }

    @Test
    void stop_sends_the_built_payload_and_the_queued_remainder() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        server.setDispatcher(holdFirst(release));
        for (int i = 0; i < 6; i++) queue.tryEnqueue(sample(i)); // three batches of two
        flusher = new Flusher(queue, http, 2, 10_000, 0L, metrics, V1, "test-flusher");
        flusher.start();
        assertThat(server.takeRequest(5, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
        await().atMost(Duration.ofSeconds(2)).until(() -> queue.depth() <= 2); // second batch built and handed off
        Thread stopper = new Thread(() -> flusher.stop(5_000));
        stopper.start();
        Thread.sleep(100);
        release.countDown();
        stopper.join(10_000);
        assertThat(stopper.isAlive()).as("stop returned").isFalse();
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue()).isEqualTo(6L);
        assertThat(queue.depth()).isZero();
    }

    private static MappedSample sample(int i) {
        return new MappedSample(
                Map.of("__name__", "t", "i", Integer.toString(i)),
                1_000_000L + i,
                (double) i);
    }
}
