/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class ShardsTest {

    private MockWebServer server;
    private RemoteWriteHttpClient http;
    private PluginMetrics metrics;
    private Shards shards;

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
        metrics = new PluginMetrics();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (shards != null) shards.stop(1_000);
        http.shutdown();
        server.shutdown();
    }

    // ---------- router --------------------------------------------------------

    @Test
    void shard_assignment_is_stable_and_insertion_order_independent() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("__name__", "ifHCInOctets");
        a.put("node", "42");
        Map<String, String> b = new TreeMap<>();
        b.put("node", "42");
        b.put("__name__", "ifHCInOctets");

        for (int n : new int[] {1, 2, 4, 7, 64}) {
            assertThat(Shards.shardFor(a, n))
                    .as("same series, different map insertion order, n=%d", n)
                    .isEqualTo(Shards.shardFor(b, n));
        }
    }

    @Test
    void single_shard_routes_everything_to_shard_zero() {
        for (int i = 0; i < 100; i++) {
            assertThat(Shards.shardFor(Map.of("__name__", "m" + i), 1)).isZero();
        }
    }

    @Test
    void distinct_series_spread_across_shards() {
        int[] hits = new int[4];
        for (int i = 0; i < 400; i++) {
            hits[Shards.shardFor(Map.of("__name__", "metric", "node", "n" + i), 4)]++;
        }
        // Not a statistical test — just proof the router is not degenerate.
        for (int h : hits) {
            assertThat(h).isGreaterThan(0);
        }
    }

    // ---------- composition ---------------------------------------------------

    @Test
    void capacity_is_split_across_shards_without_losing_slots() {
        shards = new Shards(3, 100, http, 10, 10_000, metrics, batch -> failBuild());
        int total = 0;
        for (SampleQueue q : shards.queuesForTesting()) {
            total += q.capacity();
        }
        assertThat(total).isEqualTo(100);
    }

    @Test
    void shard_overflow_throws_without_touching_siblings() throws Exception {
        // Two shards, tiny capacity, flushers never started — queues only.
        shards = new Shards(2, 4, http, 2, 10_000, metrics, batch -> failBuild());
        // Find two label sets landing on different shards.
        Map<String, String> shard0 = null;
        Map<String, String> shard1 = null;
        for (int i = 0; i < 64 && (shard0 == null || shard1 == null); i++) {
            Map<String, String> labels = Map.of("__name__", "m", "node", "n" + i);
            if (Shards.shardFor(labels, 2) == 0) shard0 = shard0 == null ? labels : shard0;
            else                                 shard1 = shard1 == null ? labels : shard1;
        }
        assertThat(shard0).isNotNull();
        assertThat(shard1).isNotNull();

        // Fill shard0 to its capacity (2 of the 4 total slots).
        shards.enqueue(new MappedSample(shard0, 1, 1.0));
        shards.enqueue(new MappedSample(shard0, 2, 1.0));
        final Map<String, String> full = shard0;
        assertThatThrownBy(() -> shards.enqueue(new MappedSample(full, 3, 1.0)))
                .isInstanceOf(StorageException.class);
        // Sibling shard still accepts.
        shards.enqueue(new MappedSample(shard1, 1, 1.0));
        assertThat(shards.totalDepth()).isEqualTo(3);
    }

    @Test
    void skew_is_100_when_empty_and_maximal_when_one_shard_owns_everything() throws Exception {
        shards = new Shards(4, 400, http, 10, 10_000, metrics, batch -> failBuild());
        assertThat(shards.skewPct()).isEqualTo(100);

        // Enqueue several samples of ONE series — all land on one shard.
        Map<String, String> series = Map.of("__name__", "m", "node", "n1");
        for (int t = 1; t <= 8; t++) {
            shards.enqueue(new MappedSample(series, t, 1.0));
        }
        // max=8, mean=2 → 400%.
        assertThat(shards.skewPct()).isEqualTo(400);
    }

    // ---------- parallelism (spec: one slow shard does not stall the others) --

    @Test
    void slow_shard_does_not_stall_sibling_shards() throws Exception {
        // Dispatcher holds the FIRST request on a latch; all later requests
        // answer 204 immediately. If flushing were serialized, no second
        // request could complete while the first is held.
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                if (requests.incrementAndGet() == 1) {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return new MockResponse().setResponseCode(204);
            }
        });

        shards = new Shards(2, 100, http, 1, 20, metrics,
                org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders.forVersion(1));

        Map<String, String> s0 = null;
        Map<String, String> s1 = null;
        for (int i = 0; i < 64 && (s0 == null || s1 == null); i++) {
            Map<String, String> labels = Map.of("__name__", "m", "node", "n" + i);
            if (Shards.shardFor(labels, 2) == 0) s0 = s0 == null ? labels : s0;
            else                                 s1 = s1 == null ? labels : s1;
        }
        shards.enqueue(new MappedSample(s0, 1, 1.0));
        shards.enqueue(new MappedSample(s1, 1, 1.0));
        shards.start();

        try {
            // Both flushers must go in flight: one is held by the latch, the
            // other completes — proof of parallel in-flight requests over
            // disjoint shards.
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> http.getWritesSuccessful() >= 1 && requests.get() >= 2);
        } finally {
            release.countDown();
        }
        await().atMost(Duration.ofSeconds(5))
                .until(() -> http.getWritesSuccessful() == 2);
    }

    @Test
    void stop_drains_all_shards_under_a_shared_grace_budget() throws Exception {
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(204);
            }
        });
        shards = new Shards(2, 100, http, 10, 10_000, metrics,
                org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders.forVersion(1));
        for (int i = 0; i < 10; i++) {
            shards.enqueue(new MappedSample(Map.of("__name__", "m", "node", "n" + i), i, 1.0));
        }
        shards.start();
        shards.stop(5_000);
        assertThat(shards.totalDepth()).isZero();
    }

    // ---------- helpers -------------------------------------------------------

    private static org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult failBuild() {
        throw new AssertionError("builder must not be invoked in this test");
    }
}
