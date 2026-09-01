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
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.HttpHeadersConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

// Force sequential execution: several tests here share static state
// (INSTANCE_ID_UNSET_WARNED, INSTANCE_ID_UNSET_WARN_COUNT, LAST_ACTIVE)
// that is intentionally JVM-scoped. Parallel execution within this class
// would race the @BeforeEach reset against a concurrent test's start().
// @Isolated additionally locks against *other* test classes running in
// parallel under a future project-wide parallel-tests switch — any other
// class that touches the plugin storage would race the same static state.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class PrometheusRemoteWriterStorageTest {

    @BeforeEach
    void resetWarnGates() {
        // The WARN gates are static one-shots; tests that start more than one
        // storage bean need a clean slate so the gates can be observed flipping.
        PrometheusRemoteWriterStorage.resetInstanceIdWarnedForTesting();
        PrometheusRemoteWriterStorage.resetWireV2WarnedForTesting();
    }

    // ---------- instance.id startup WARN ------------------------------------

    @Test
    void instance_id_unset_trips_the_warn_gate_on_start() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
        assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isZero();
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();
            // Count is the stronger assertion — a refactor that keeps the gate
            // correct but moves LOG.warn outside the CAS-success branch would
            // pass the boolean check and fail this one.
            assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isEqualTo(1);
        } finally {
            s.stop();
        }
    }

    @Test
    void instance_id_set_does_not_trip_the_warn_gate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("opennms-us-east");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
            assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isZero();
        } finally {
            s.stop();
        }
    }

    @Test
    void warn_gate_stays_tripped_across_hot_reload_activations() {
        // Simulates the blueprint rebuild-on-config-update path. The gate is
        // static so the WARN fires exactly once per JVM regardless of how many
        // times the blueprint container re-activates the bean with unset
        // instance.id.
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(minimal());
        first.start();
        first.stop();
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();
        assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isEqualTo(1);

        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(minimal());
        second.start();
        try {
            // CAS already happened; a second start() cannot re-flip the gate.
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();
            // And — the stronger check — the WARN emission did NOT recur.
            assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isEqualTo(1);
        } finally {
            second.stop();
        }
    }

    @Test
    void warn_count_does_not_increment_when_hot_reload_sets_instance_id() {
        // The specific hot-reload scenario: an operator runs without
        // instance.id (WARN fires once), then discovers the recommendation and
        // sets instance.id in config. The blueprint rebuild triggers a fresh
        // storage bean — the WARN MUST NOT fire a second time regardless of
        // which way the knob is flipped.
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(minimal());
        first.start();
        first.stop();
        assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isEqualTo(1);

        PrometheusRemoteWriterConfig withInstanceId = minimal();
        withInstanceId.setInstanceId("opennms-us-east");
        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(withInstanceId);
        second.start();
        try {
            // Different branch of warnIfInstanceIdUnset — instance.id is set,
            // the WARN path isn't taken. Count unchanged.
            assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isEqualTo(1);
        } finally {
            second.stop();
        }
    }

    @Test
    void warn_gate_not_tripped_when_config_is_invalid() {
        // If validate() fails, warnIfInstanceIdUnset never runs — we don't
        // want spurious WARNs while ConfigAdmin is still settling.
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        // deliberately leave write.url / read.url unset so validate() throws
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        s.start();
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
        assertThat(PrometheusRemoteWriterStorage.getInstanceIdWarnCountForTesting()).isZero();
    }

    @Test
    void reserved_rename_target_leaves_service_inactive() {
        // A labels.rename whose target collides with a default-allowlist name
        // is rejected by validate(); start() catches the IllegalStateException
        // and leaves `active` null so OpenNMS sees no writable TSS until the
        // operator corrects the cfg.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> __name__");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        assertThatCode(s::start).doesNotThrowAnyException();
        assertThatThrownBy(() -> s.store(List.of()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("not accepting writes");
    }

    @Test
    void start_tolerates_invalid_config_and_stays_inactive() {
        // start() must not throw even when config is bad — throwing would
        // permanently kill the blueprint container and defeat the
        // reload-on-config-update strategy (see PrometheusRemoteWriterStorage#start).
        // Instead the bundle registers the service but refuses writes until
        // a later reload delivers valid config.
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        c.setBasicPassword("p");
        c.setBearerToken("t");

        PrometheusRemoteWriterStorage storage = new PrometheusRemoteWriterStorage(c);
        assertThatCode(storage::start).doesNotThrowAnyException();
        assertThatThrownBy(() -> storage.store(List.of()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("not accepting writes");
    }

    @Test
    void start_and_stop_with_minimum_valid_config() {
        PrometheusRemoteWriterStorage storage = new PrometheusRemoteWriterStorage(minimal());
        assertThatCode(storage::start).doesNotThrowAnyException();
        assertThatCode(storage::stop).doesNotThrowAnyException();
    }

    @Test
    void supports_aggregation_is_true_only_for_none() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThat(s.supportsAggregation(Aggregation.NONE)).isTrue();
        assertThat(s.supportsAggregation(Aggregation.AVERAGE)).isFalse();
        assertThat(s.supportsAggregation(Aggregation.MIN)).isFalse();
        assertThat(s.supportsAggregation(Aggregation.MAX)).isFalse();
    }

    @Test
    void delete_is_a_noop_that_counts_and_never_throws() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        var metric = ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .build();

        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();
        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();
        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();

        assertThat(s.getDeleteNoopTotal()).isEqualTo(3);
    }

    @Test
    void hot_reload_emits_diff_without_throwing() {
        // Activate once.
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(minimal());
        first.start();

        // Activate a second time with a modified config — simulates Blueprint
        // rebuilding the bean after a ConfigAdmin update. LAST_ACTIVE is
        // shared state inside the storage, so the second start() should see
        // the diff and log it.
        PrometheusRemoteWriterConfig c2 = minimal();
        c2.setBatchSize(500);
        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(c2);
        assertThatCode(second::start).doesNotThrowAnyException();
    }

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }

    // ---------- wire.protocol-version=2 startup WARN ------------------------

    @Test
    void wire_v2_trips_the_warn_gate_on_start() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWireProtocolVersion("2");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        assertThat(PrometheusRemoteWriterStorage.getWireV2WarnCountForTesting()).isZero();
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.getWireV2WarnCountForTesting()).isEqualTo(1);
        } finally {
            s.stop();
        }
    }

    @Test
    void wire_v1_default_does_not_trip_the_v2_warn_gate() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.getWireV2WarnCountForTesting()).isZero();
        } finally {
            s.stop();
        }
    }

    @Test
    void wire_v2_warn_count_does_not_increment_on_hot_reload() {
        // The static one-shot semantic: even if blueprint rebuilds the
        // bean repeatedly with wire.protocol-version=2, the WARN fires
        // exactly once per JVM lifetime.
        PrometheusRemoteWriterConfig c = minimal();
        c.setWireProtocolVersion("2");
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(c);
        first.start();
        first.stop();
        assertThat(PrometheusRemoteWriterStorage.getWireV2WarnCountForTesting()).isEqualTo(1);

        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(c);
        second.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.getWireV2WarnCountForTesting()).isEqualTo(1);
        } finally {
            second.stop();
        }
    }

    // ---------- end-to-end store() flow ------------------------------------

    @Test
    void store_maps_enqueues_and_flushes_end_to_end() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setResponseCode(204));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                s.store(List.of(
                        ImmutableSample.builder()
                                .metric(ImmutableMetric.builder()
                                        .intrinsicTag("name", "ifHCInOctets")
                                        .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                                        .externalTag("nodeId", "1")
                                        .build())
                                .time(Instant.ofEpochMilli(1_000_000L))
                                .value(42.0)
                                .build()));

                PluginMetrics m = s.getMetrics();
                await().atMost(Duration.ofSeconds(2))
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() == 1L);
            } finally {
                s.stop();
            }
            assertThat(server.getRequestCount()).isEqualTo(1);
        }
    }

    /**
     * Pins the Blueprint-shaped wiring: headers configured on the bean must
     * survive the whole composition and reach the wire. Every other test here
     * uses the one-arg constructor, which substitutes
     * {@code HttpHeadersConfig.empty()} — so without this test, dropping
     * {@code httpHeadersConfig} from {@code startQueueMode()} (or from the
     * Blueprint {@code <argument ref>}) leaves the suite green while the
     * feature silently no-ops in production.
     */
    @Test
    void configured_http_headers_reach_the_write_endpoint_through_storage() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setResponseCode(204));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            HttpHeadersConfig h = HttpHeadersConfig.empty();
            h.updated(Map.of("http.headers.cf-access-client-id", "abc123"));

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c, h);
            s.start();
            try {
                s.store(List.of(
                        ImmutableSample.builder()
                                .metric(ImmutableMetric.builder()
                                        .intrinsicTag("name", "ifHCInOctets")
                                        .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                                        .externalTag("nodeId", "1")
                                        .build())
                                .time(Instant.ofEpochMilli(1_000_000L))
                                .value(42.0)
                                .build()));

                PluginMetrics m = s.getMetrics();
                await().atMost(Duration.ofSeconds(2))
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() == 1L);
            } finally {
                s.stop();
            }
            assertThat(server.takeRequest().getHeader("cf-access-client-id"))
                .isEqualTo("abc123");
        }
    }

    /**
     * The resolved round-3 decision: an invalid {@code http.headers.*} entry
     * must make the plugin inert rather than silently unauthenticated. The
     * storage declines to activate, so SPI calls are rejected instead of
     * going out without the headers the operator configured.
     */
    @Test
    void invalid_http_headers_leave_the_storage_inert() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("http://localhost:9090/api/v1/push");
        c.setReadUrl("http://localhost:9090/prometheus");

        HttpHeadersConfig h = HttpHeadersConfig.empty();
        // Authorization is reserved: updated() records the error and rethrows.
        assertThatThrownBy(() -> h.updated(Map.of("http.headers.Authorization", "HMAC sig")))
            .isInstanceOf(IllegalStateException.class);
        assertThat(h.validationError()).contains("Authorization");

        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c, h);
        s.start();
        try {
            assertThatThrownBy(() -> s.store(List.of(
                    ImmutableSample.builder()
                            .metric(ImmutableMetric.builder()
                                    .intrinsicTag("name", "ifHCInOctets")
                                    .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                                    .build())
                            .time(Instant.ofEpochMilli(1_000_000L))
                            .value(42.0)
                            .build())))
                .isInstanceOf(StorageException.class);
        } finally {
            s.stop();
        }
    }

    @Test
    void store_throws_when_not_started() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThatThrownBy(() -> s.store(List.of()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("not accepting writes");
    }

    @Test
    void second_start_on_same_bean_is_a_no_op() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThatCode(s::start).doesNotThrowAnyException();
        try {
            PluginMetrics first = s.getMetrics();
            assertThatCode(s::start).doesNotThrowAnyException();
            PluginMetrics second = s.getMetrics();
            // Idempotent: the existing pipeline is preserved, not rebuilt.
            assertThat(second).isSameAs(first);
        } finally {
            s.stop();
        }
    }

    @Test
    void stop_then_start_produces_a_fresh_pipeline() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        s.start();
        PluginMetrics first = s.getMetrics();
        s.stop();
        assertThat(s.getMetrics()).isNull();

        s.start();
        try {
            PluginMetrics second = s.getMetrics();
            assertThat(second).isNotNull();
            assertThat(second).isNotSameAs(first);
        } finally {
            s.stop();
        }
    }

    @Test
    void http_in_flight_gauge_is_registered() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        s.start();
        try {
            assertThat(s.getMetrics().snapshot()).containsKey(PluginMetrics.HTTP_IN_FLIGHT);
        } finally {
            s.stop();
        }
    }

    @Test
    void backend_returning_4xx_increments_drop_counter() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setResponseCode(400).setBody("bad labels"));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                s.store(List.of(ImmutableSample.builder()
                        .metric(ImmutableMetric.builder()
                                .intrinsicTag("name", "t")
                                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                                .build())
                        .time(Instant.ofEpochMilli(1_000_000L))
                        .value(1.0)
                        .build()));

                PluginMetrics m = s.getMetrics();
                await().atMost(Duration.ofSeconds(2))
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue() == 1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void queue_overflow_throws_storage_exception_with_counter_increment() throws Exception {
        // Stall the flusher inside its first HTTP write so queued samples
        // accumulate until the queue is full. NO_RESPONSE keeps the socket
        // open indefinitely; OkHttp's read timeout is deliberately long so it
        // doesn't rescue us during the test window.
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse()
                    .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setQueueCapacity(2);
            c.setBatchSize(1);
            c.setFlushIntervalMs(50);
            c.setHttpReadTimeoutMs(60_000);
            c.setHttpWriteTimeoutMs(60_000);
            c.setRetryInitialBackoffMs(1);
            c.setRetryMaxBackoffMs(2);
            c.setRetryMaxAttempts(1);
            c.setShutdownGracePeriodMs(100);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                // First store: flusher picks it up immediately, hits NO_RESPONSE,
                // hangs. Wait for the flusher to be parked in the HTTP call.
                s.store(List.of(sample("a")));
                server.takeRequest(5, TimeUnit.SECONDS);

                // Now fill the queue to capacity.
                s.store(List.of(sample("b")));
                s.store(List.of(sample("c")));

                assertThatThrownBy(() -> s.store(List.of(sample("d"))))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("queue full");

                PluginMetrics m = s.getMetrics();
                assertThat(m.snapshot().get(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL).longValue())
                        .isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    private static org.opennms.integration.api.v1.timeseries.Sample sample(String id) {
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
}
