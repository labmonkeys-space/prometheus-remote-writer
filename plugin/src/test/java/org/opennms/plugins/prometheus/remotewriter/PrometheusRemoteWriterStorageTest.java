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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest;
import org.xerial.snappy.Snappy;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
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
        try {
            assertThatCode(second::start).doesNotThrowAnyException();
        } finally {
            second.stop();
            first.stop();
        }
    }

    /**
     * A memory-only pipeline: no disk tier, which is what the cases in this
     * class assert. The queue-full contract they pin — a full shard refuses
     * and throws — is exactly the {@code overflow.max-size-bytes=0} behaviour
     * after 0.8.0; spilling is covered in {@code OverflowTierTest} and the
     * integration suites, which configure a directory.
     */
    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        c.setOverflowMaxSizeBytes(0);
        return c;
    }

    // ---------- effective-authentication startup line ------------------------
    //
    // Asserted as a pure function rather than through a log-capture appender —
    // same approach as HttpHeadersConfig.formatActivationMessage, and for the
    // same reason: this repo has declined to take a logging-backend test
    // dependency. The .cfg and the docs both tell operators to read this line
    // after changing an ${env:} reference, so its content is a contract.

    @Test
    void effective_auth_reports_none_when_nothing_is_configured() {
        assertThat(new PrometheusRemoteWriterStorage(minimal()).effectiveAuthDescription())
                .startsWith("none")
                .contains("auth.authorization.*");
    }

    @Test
    void effective_auth_reports_basic() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        c.setBasicPassword("p");
        assertThat(new PrometheusRemoteWriterStorage(c).effectiveAuthDescription())
                .startsWith("basic")
                .contains("auth.basic.username");
    }

    @Test
    void effective_auth_reports_bearer() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        assertThat(new PrometheusRemoteWriterStorage(c).effectiveAuthDescription())
                .startsWith("bearer")
                .contains("auth.bearer.token");
    }

    @Test
    void effective_auth_reports_custom_scheme_without_echoing_the_keyword() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setAuthorizationType("Token");
        c.setAuthorizationCredentials("s3cret-in-the-wrong-key");
        String desc = new PrometheusRemoteWriterStorage(c).effectiveAuthDescription();
        assertThat(desc)
                .contains("custom scheme")
                .contains("auth.authorization.type")
                .contains("auth.authorization.credentials");
        // The type is operator-supplied text one line above the credentials in
        // the same file. A paste into the wrong key must not reach the log.
        assertThat(desc).doesNotContain("Token")
                        .doesNotContain("s3cret-in-the-wrong-key");
    }

    @Test
    void effective_auth_reports_none_for_an_incomplete_authorization_block() {
        // The description must mirror the emitters exactly: they gate on a
        // COMPLETE block, so a credentials-only config sends no header and the
        // line must not claim otherwise. (validate() rejects this config; the
        // description is still defined on it.)
        PrometheusRemoteWriterConfig c = minimal();
        c.setAuthorizationCredentials("tok");   // no type — nothing is emitted
        String desc = new PrometheusRemoteWriterStorage(c).effectiveAuthDescription();
        assertThat(desc).startsWith("none");
        assertThat(desc).doesNotContain("custom scheme");
    }

    @Test
    void effective_auth_reports_none_for_a_username_only_basic_block() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");                // no password — nothing is emitted
        assertThat(new PrometheusRemoteWriterStorage(c).effectiveAuthDescription())
                .startsWith("none");
    }

    @Test
    void effective_auth_line_appends_the_tenant_suffix() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        c.setTenantOrgId("team-a");
        assertThat(new PrometheusRemoteWriterStorage(c).effectiveAuthLine())
                .startsWith("bearer")
                .endsWith("; tenant.org-id set");
    }

    @Test
    void effective_auth_line_omits_the_tenant_suffix_when_unset() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        assertThat(new PrometheusRemoteWriterStorage(c).effectiveAuthLine())
                .doesNotContain("tenant.org-id set");
    }

    @Test
    void start_actually_emits_the_authentication_line() {
        // Asserting effectiveAuthDescription() alone would let a refactor that
        // drops logEffectiveAuth() from start() ship with a green suite. The
        // counter lives beside the LOG.info, so it proves the call happened.
        PrometheusRemoteWriterStorage.resetAuthLineCountForTesting();
        assertThat(PrometheusRemoteWriterStorage.getAuthLineCountForTesting()).isZero();

        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        try {
            s.start();
            assertThat(PrometheusRemoteWriterStorage.getAuthLineCountForTesting()).isEqualTo(1);
        } finally {
            s.stop();
        }
    }

    @Test
    void a_config_rejected_by_validate_never_reaches_the_authentication_line() {
        // start() bails before logEffectiveAuth() when validate() throws, so an
        // inert plugin must not log an authentication mode it will never use.
        PrometheusRemoteWriterStorage.resetAuthLineCountForTesting();
        PrometheusRemoteWriterConfig c = minimal();
        c.setAuthorizationCredentials("tok");   // type missing — validate() throws
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        try {
            s.start();
            assertThat(PrometheusRemoteWriterStorage.getAuthLineCountForTesting()).isZero();
        } finally {
            s.stop();
        }
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
            c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
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
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);
            } finally {
                s.stop();
            }
            // The data path's request, decoded: one series, the one stored.
            // Request counting would race the metadata emitter, whose first
            // rows for this resource can follow within a second.
            WriteRequest first = decode(server.takeRequest(5, TimeUnit.SECONDS));
            assertThat(first.getTimeseriesCount()).isEqualTo(1);
            assertThat(labelsOf(first.getTimeseries(0))).containsEntry("__name__", "ifHCInOctets");
        }
    }

    /**
     * The metadata series reach the backend through the same pipeline as
     * the data: registry, emitter, shards, HTTP. Nothing else exercises that
     * whole path, and a resource's attributes only ever become rows here.
     */
    @Test
    void resource_attributes_reach_the_backend_as_rows() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.setDispatcher(new okhttp3.mockwebserver.Dispatcher() {
                @Override public MockResponse dispatch(okhttp3.mockwebserver.RecordedRequest r) {
                    return new MockResponse().setResponseCode(204);
                }
            });
            server.start();
            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setOverflowMaxSizeBytes(0);
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
                                .intrinsicTag("name", "ifHCInOctets")
                                .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                                .externalTag("ifName", "eth0")
                                .externalTag("ifAlias", "uplink to core")
                                .externalTag("categories", "Routers")
                                .build())
                        .time(Instant.ofEpochMilli(1_000_000L))
                        .value(42.0)
                        .build()));

                Map<String, String> dataSeries = null;
                Map<String, String> aliasRow = null;
                Map<String, String> categoryRow = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while ((aliasRow == null || categoryRow == null || dataSeries == null) && System.nanoTime() < deadline) {
                    okhttp3.mockwebserver.RecordedRequest r = server.takeRequest(5, TimeUnit.SECONDS);
                    if (r == null) break;
                    for (org.opennms.plugins.prometheus.remotewriter.wire.proto.TimeSeries ts : decode(r).getTimeseriesList()) {
                        Map<String, String> l = labelsOf(ts);
                        switch (l.get("__name__")) {
                            case "ifHCInOctets" -> dataSeries = l;
                            case "onms_resource_attr" -> { if ("ifAlias".equals(l.get("key"))) aliasRow = l; }
                            case "onms_resource_category" -> categoryRow = l;
                            default -> { }
                        }
                    }
                }
                assertThat(dataSeries).as("data series").isNotNull();
                assertThat(aliasRow).as("ifAlias row").isNotNull()
                        .containsEntry("value", "uplink to core")
                        .containsEntry("resourceId", dataSeries.get("resourceId"));
                assertThat(aliasRow.keySet()).containsExactlyInAnyOrder("__name__", "resourceId", "key", "value");
                assertThat(categoryRow).as("category row").isNotNull().containsEntry("category", "Routers");
                assertThat(dataSeries).doesNotContainKey("if_alias");

                Map<String, Number> m = s.getMetrics().snapshot();
                assertThat(m.get(PluginMetrics.METADATA_RESOURCES).longValue()).isEqualTo(1);
                assertThat(m.get(PluginMetrics.METADATA_SERIES_EMITTED).longValue()).isGreaterThanOrEqualTo(3);
            } finally {
                s.stop();
            }
        }
    }

    private static WriteRequest decode(okhttp3.mockwebserver.RecordedRequest r) throws java.io.IOException {
        return WriteRequest.parseFrom(Snappy.uncompress(r.getBody().readByteArray()));
    }

    private static Map<String, String> labelsOf(org.opennms.plugins.prometheus.remotewriter.wire.proto.TimeSeries ts) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        ts.getLabelsList().forEach(l -> out.put(l.getName(), l.getValue()));
        return out;
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
            c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            HttpHeadersConfig h = HttpHeadersConfig.empty();
            h.applyProperties(Map.of("http.headers.cf-access-client-id", "abc123"));

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
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);
            } finally {
                s.stop();
            }
            assertThat(server.takeRequest().getHeader("cf-access-client-id"))
                .isEqualTo("abc123");
        }
    }

    /**
     * The read half of the symmetric-application claim. Zero-Trust gateways
     * gate both endpoints with the same credentials, so a write-only
     * implementation 403s on every dashboard query — which the CHANGELOG
     * calls out by name as "a hard bug to spot in production".
     *
     * <p>Without this, swapping {@code startQueueMode}'s read client for
     * {@code HttpHeadersConfig.empty()} leaves the whole suite green:
     * {@code BlueprintWiringTest} checks XML, {@code PrometheusReadClientTest}
     * supplies its own header config, and the write test above only inspects
     * the POST.
     */
    @Test
    void configured_http_headers_reach_the_read_endpoint_through_storage() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setBody("{\"status\":\"success\",\"data\":["
                           + "{\"__name__\":\"ifHCInOctets\",\"node\":\"1:1\"}"
                           + "]}"));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("").toString());
            c.setShutdownGracePeriodMs(1_000);

            HttpHeadersConfig h = HttpHeadersConfig.empty();
            h.applyProperties(Map.of("http.headers.cf-access-client-id", "abc123"));

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c, h);
            s.start();
            try {
                s.findMetrics(List.of(ImmutableTagMatcher.builder()
                        .type(TagMatcher.Type.EQUALS)
                        .key("name").value("ifHCInOctets").build()));
            } finally {
                s.stop();
            }
            assertThat(server.takeRequest().getHeader("cf-access-client-id"))
                .isEqualTo("abc123");
        }
    }

    /**
     * "Never delivered" and "operator configured no headers" both leave the
     * header map empty. Only the delivery flag separates them, and the first
     * is the state this plugin shipped in twice.
     */
    @Test
    void storage_refuses_to_start_when_header_config_was_never_delivered() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
        c.setWriteUrl("http://localhost:9090/api/v1/push");
        c.setReadUrl("http://localhost:9090");

        // Blueprint construction whose init() never ran — the wiring regression.
        HttpHeadersConfig undelivered = new HttpHeadersConfig(Map.of(
                "http.headers.cf-access-client-id", "abc123"));
        assertThat(undelivered.isDelivered()).isFalse();

        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c, undelivered);
        s.start();
        try {
            assertThatThrownBy(() -> s.findMetrics(List.of()))
                .isInstanceOf(StorageException.class);
        } finally {
            s.stop();
        }
    }

    @Test
    void storage_starts_when_no_headers_are_configured_at_all() throws Exception {
        // The legitimate empty case must NOT be mistaken for undelivered.
        HttpHeadersConfig none = HttpHeadersConfig.empty();
        assertThat(none.isDelivered()).isTrue();
        assertThat(none.headers()).isEmpty();

        try (MockWebServer server = new MockWebServer()) {
            server.start();
            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setShutdownGracePeriodMs(1_000);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c, none);
            s.start();
            try {
                assertThat(s.getMetrics()).isNotNull();
            } finally {
                s.stop();
            }
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
        c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
        c.setWriteUrl("http://localhost:9090/api/v1/push");
        c.setReadUrl("http://localhost:9090/prometheus");

        HttpHeadersConfig h = HttpHeadersConfig.empty();
        // Authorization is reserved: updated() records the error and rethrows.
        assertThatThrownBy(() -> h.applyProperties(Map.of("http.headers.Authorization", "HMAC sig")))
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
            c.setOverflowMaxSizeBytes(0);   // memory-only pipeline
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

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, 2));
            s.start();
            try {
                parkFlusher(s, server);

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

    @org.junit.jupiter.params.ParameterizedTest(name = "capacity {0}: {1} dropped, depth {2}")
    @org.junit.jupiter.params.provider.CsvSource({
            "1, 4, 1, refused 4 of 4",   // queue already full: all four samples of the call are refused
            "2, 3, 2, refused 3 of 4",   // one slot free: first enqueued, the other three are lost
    })
    void multi_sample_store_against_full_queue_counts_every_lost_sample(
            int capacity, long expectedDropped, long expectedDepth, String expectedMessage) throws Exception {
        // Issue #154: the counter must mean samples, not failed store() calls.
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse()
                    .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, capacity));
            s.start();
            try {
                parkFlusher(s, server);
                s.store(List.of(sample("b")));

                assertThatThrownBy(() -> s.store(List.of(sample("c"), sample("d"), sample("e"), sample("f"))))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("queue full")
                        .hasMessageContaining(expectedMessage);

                PluginMetrics m = s.getMetrics();
                assertThat(m.snapshot().get(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL).longValue())
                        .isEqualTo(expectedDropped);
                assertThat(m.snapshot().get(PluginMetrics.QUEUE_DEPTH).longValue())
                        .isEqualTo(expectedDepth);
            } finally {
                s.stop();
            }
        }
    }

    // ---------- #156: partial acceptance across shards --------------------

    @Test
    void full_shard_does_not_block_sample_bound_for_sibling_shard() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            PrometheusRemoteWriterConfig c = twoShardConfig(server, 2, "partial");
            PrometheusRemoteWriterStorage s = started(c);
            try {
                TwoShardSamples t = parkBothFlushers(s, c, server);
                s.store(List.of(t.shard0()));                     // shard 0 now full (capacity 1)
                long before = dropped(s);

                assertThatThrownBy(() -> s.store(List.of(t.shard0(), t.shard1())))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("refused 1 of 2");

                assertThat(depth(s)).isEqualTo(2);                // shard 0: 1, shard 1: 1
                assertThat(dropped(s) - before).isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void refusal_in_the_middle_of_a_call_does_not_stop_later_samples() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            PrometheusRemoteWriterConfig c = twoShardConfig(server, 4, "partial");
            PrometheusRemoteWriterStorage s = started(c);
            try {
                TwoShardSamples t = parkBothFlushers(s, c, server);
                s.store(List.of(t.shard0(), t.shard0()));         // shard 0 full (capacity 2)
                long before = dropped(s);

                assertThatThrownBy(() -> s.store(List.of(t.shard1(), t.shard0(), t.shard1())))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("refused 1 of 3");

                assertThat(depth(s)).isEqualTo(4);                // shard 0: 2, shard 1: 2
                assertThat(dropped(s) - before).isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void all_or_nothing_refuses_the_whole_call_when_one_shard_is_short() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            PrometheusRemoteWriterConfig c = twoShardConfig(server, 2, "all-or-nothing");
            PrometheusRemoteWriterStorage s = started(c);
            try {
                TwoShardSamples t = parkBothFlushers(s, c, server);
                s.store(List.of(t.shard0()));                     // shard 0 full
                long before = dropped(s);

                assertThatThrownBy(() -> s.store(List.of(t.shard0(), t.shard1())))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("refused 2 of 2");

                assertThat(depth(s)).isEqualTo(1);                // nothing enqueued
                assertThat(dropped(s) - before).isEqualTo(2L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void all_or_nothing_accepts_a_call_that_fits() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            PrometheusRemoteWriterConfig c = twoShardConfig(server, 4, "all-or-nothing");
            PrometheusRemoteWriterStorage s = started(c);
            try {
                TwoShardSamples t = parkBothFlushers(s, c, server);
                s.store(List.of(t.shard0(), t.shard1()));
                assertThat(depth(s)).isEqualTo(2);
                assertThat(dropped(s)).isZero();
            } finally {
                s.stop();
            }
        }
    }

    // ---------- #155: store() counters, high water, JMX ---------------------

    @Test
    void store_calls_and_offered_samples_are_counted_whatever_the_outcome() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse()
                    .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));
            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, 1));
            s.start();
            try {
                parkFlusher(s, server);
                long calls = metric(s, PluginMetrics.STORE_CALLS);
                long failed = metric(s, PluginMetrics.STORE_CALLS_FAILED);
                long offered = metric(s, PluginMetrics.STORE_SAMPLES_OFFERED);

                s.store(List.of(sample("b")));
                assertThatThrownBy(() -> s.store(List.of(sample("c"), sample("d"), sample("e"), sample("f"))))
                        .isInstanceOf(StorageException.class);

                assertThat(metric(s, PluginMetrics.STORE_CALLS) - calls).isEqualTo(2L);
                assertThat(metric(s, PluginMetrics.STORE_CALLS_FAILED) - failed).isEqualTo(1L);
                assertThat(metric(s, PluginMetrics.STORE_SAMPLES_OFFERED) - offered).isEqualTo(5L);

                // Duration: one large call maps thousands of samples before the
                // queue refuses, which takes measurable wall time.
                long duration0 = metric(s, PluginMetrics.STORE_CALL_DURATION_MS);
                java.util.List<org.opennms.integration.api.v1.timeseries.Sample> big = new java.util.ArrayList<>();
                for (int i = 0; i < 20_000; i++) big.add(sample("big" + i, "m" + (i % 50)));
                assertThatThrownBy(() -> s.store(big)).isInstanceOf(StorageException.class);
                assertThat(metric(s, PluginMetrics.STORE_CALL_DURATION_MS) - duration0)
                        .as("mapping 20k samples inside store() is milliseconds, not zero").isGreaterThanOrEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void queue_depth_high_water_keeps_the_maximum_after_the_queue_drains() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            // First write blocks until the test releases it; every later
            // write answers at once so the queue drains.
            java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
            server.setDispatcher(new okhttp3.mockwebserver.Dispatcher() {
                @Override public okhttp3.mockwebserver.MockResponse dispatch(okhttp3.mockwebserver.RecordedRequest r) {
                    if (first.getAndSet(false)) {
                        try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                    return new okhttp3.mockwebserver.MockResponse().setResponseCode(204);
                }
            });
            server.start();
            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, 10));
            s.start();
            try {
                parkFlusher(s, server);
                s.store(List.of(sample("b"), sample("c")));
                assertThat(depth(s)).isEqualTo(2L);

                release.countDown();
                await().atMost(Duration.ofSeconds(5)).until(() -> depth(s) == 0L);
                assertThat(metric(s, PluginMetrics.QUEUE_DEPTH_HIGH_WATER)).isEqualTo(2L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void unmapped_samples_are_counted_and_the_call_succeeds() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse()
                    .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));
            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, 10));
            s.start();
            try {
                parkFlusher(s, server);
                org.opennms.integration.api.v1.timeseries.Sample nameless = ImmutableSample.builder()
                        .metric(ImmutableMetric.builder()
                                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                                .build())
                        .time(Instant.now())
                        .value(1.0)
                        .build();

                s.store(List.of(nameless, sample("b")));

                assertThat(metric(s, PluginMetrics.SAMPLES_DROPPED_UNMAPPED)).isEqualTo(1L);
                assertThat(metric(s, PluginMetrics.STORE_SAMPLES_OFFERED)).isEqualTo(5L); // a, a2, a3 (fixture), nameless, b
                assertThat(depth(s)).isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void metrics_are_registered_as_mbeans_while_active_and_removed_on_stop() throws Exception {
        javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        // Dropwizard names MBeans <domain>:name=<metric>,type=<counters|gauges>.
        javax.management.ObjectName depth = new javax.management.ObjectName(
                PluginMetrics.JMX_DOMAIN + ":name=" + PluginMetrics.QUEUE_DEPTH + ",*");
        javax.management.ObjectName all = new javax.management.ObjectName(PluginMetrics.JMX_DOMAIN + ":*");
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            PrometheusRemoteWriterConfig c = stalledFlusherConfig(server, 10);
            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);

            s.start();
            assertThat(mbs.queryNames(depth, null)).as("registered after start").hasSize(1);
            s.stop();
            assertThat(mbs.queryNames(depth, null)).as("removed after stop").isEmpty();
            assertThat(mbs.queryNames(all, null)).as("domain empty after stop").isEmpty();
        }
    }

    @Test
    void http_error_counts_and_duration_buckets_are_exported() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse().setResponseCode(204));
            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(stalledFlusherConfig(server, 10));
            s.start();
            try {
                s.store(List.of(sample("a")));
                await().atMost(Duration.ofSeconds(3)).until(() -> metric(s, PluginMetrics.SAMPLES_WRITTEN) == 1L);
                java.util.Map<String, Number> snap = s.getMetrics().snapshot();
                assertThat(snap).containsKeys(PluginMetrics.HTTP_WRITES_4XX, PluginMetrics.HTTP_WRITES_5XX,
                        PluginMetrics.HTTP_WRITES_TRANSPORT, PluginMetrics.FLUSHER_BUILD_MS,
                        "http_write_duration_bucket_le_5", "http_write_duration_bucket_le_inf");
                assertThat(snap.get("http_write_duration_bucket_le_inf").longValue()).isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    private static long metric(PrometheusRemoteWriterStorage s, String name) {
        return s.getMetrics().snapshot().get(name).longValue();
    }

    private record TwoShardSamples(org.opennms.integration.api.v1.timeseries.Sample shard0,
                                   org.opennms.integration.api.v1.timeseries.Sample shard1) {}

    /** Two shards, {@code capacity} total, batch size 1, flushers stall on
     *  their first write. Both NO_RESPONSE answers are queued up front. */
    private static PrometheusRemoteWriterConfig twoShardConfig(MockWebServer server, int capacity, String policy)
            throws Exception {
        server.start();
        server.enqueue(new okhttp3.mockwebserver.MockResponse()
                .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));
        server.enqueue(new okhttp3.mockwebserver.MockResponse()
                .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));
        PrometheusRemoteWriterConfig c = stalledFlusherConfig(server, capacity);
        c.setWriterShards(2);
        c.setStorePolicy(policy);
        return c;
    }

    private static PrometheusRemoteWriterStorage started(PrometheusRemoteWriterConfig c) {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        s.start();
        return s;
    }

    /** Find one sample per shard, store each so both flushers take theirs
     *  and park inside NO_RESPONSE, then return fresh samples on the same
     *  two shards for the test body to use. */
    private static TwoShardSamples parkBothFlushers(PrometheusRemoteWriterStorage s,
                                                    PrometheusRemoteWriterConfig c,
                                                    MockWebServer server) throws Exception {
        org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper lm =
                new org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper(c, new PluginMetrics());
        org.opennms.integration.api.v1.timeseries.Sample s0 = null, s1 = null;
        for (int i = 0; i < 64 && (s0 == null || s1 == null); i++) {
            org.opennms.integration.api.v1.timeseries.Sample x = sample("p" + i, "probe_" + i);
            int shard = org.opennms.plugins.prometheus.remotewriter.queue.Shards.shardFor(lm.map(x).labels(), 2);
            if (shard == 0 && s0 == null) s0 = x;
            if (shard == 1 && s1 == null) s1 = x;
        }
        assertThat(s0).as("no sample routed to shard 0").isNotNull();
        assertThat(s1).as("no sample routed to shard 1").isNotNull();
        s.store(List.of(s0));
        s.store(List.of(s1));
        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).as("flusher 0 never parked").isNotNull();
        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).as("flusher 1 never parked").isNotNull();
        // Two more per shard: each builder's handoff and its in-hand batch (see parkFlusher).
        s.store(List.of(s0, s1));
        await().atMost(Duration.ofSeconds(3)).alias("both builders should take their handoff batch").until(() -> depth(s) == 0L);
        s.store(List.of(s0, s1));
        await().atMost(Duration.ofSeconds(3)).alias("both builders should hold their in-hand batch").until(() -> depth(s) == 0L);
        return new TwoShardSamples(s0, s1);
    }

    private static long dropped(PrometheusRemoteWriterStorage s) { return metric(s, PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL); }

    private static long depth(PrometheusRemoteWriterStorage s)   { return metric(s, PluginMetrics.QUEUE_DEPTH); }

    /** Store one sample and wait until the flusher has taken it and is
     *  parked inside the NO_RESPONSE write. Fails loudly instead of letting
     *  a slow runner turn into a confusing off-by-one on the counter. */
    private static void parkFlusher(PrometheusRemoteWriterStorage s, MockWebServer server) throws Exception {
        // Sender parks on the first (unanswered) request. The builder then
        // fills its depth-one handoff with a second batch and blocks holding
        // a third. Feed both, so the queue is empty and every later sample
        // counts against capacity.
        s.store(List.of(sample("a")));
        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).as("flusher never parked").isNotNull();
        s.store(List.of(sample("a2")));
        await().atMost(Duration.ofSeconds(3)).alias("builder should take the handoff batch").until(() -> depth(s) == 0L);
        s.store(List.of(sample("a3")));
        await().atMost(Duration.ofSeconds(3)).alias("builder should hold the in-hand batch").until(() -> depth(s) == 0L);
    }

    /** Config whose flusher parks forever on its first write (NO_RESPONSE
     *  server, long read timeout), so the queue fills deterministically. */
    private static PrometheusRemoteWriterConfig stalledFlusherConfig(MockWebServer server, int queueCapacity) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setOverflowMaxSizeBytes(0);   // memory-only: these cases pin the refusal contract
        c.setMetadataCadenceMs(0);      // these cases count offered samples and queue slots exactly
        c.setReadUrl(server.url("/prometheus").toString());
        c.setQueueCapacity(queueCapacity);
        c.setWriterShards(1);   // these cases size one queue deliberately; 0.8.0 defaults to 4
        c.setStorePolicy("partial"); // pin: AUTO would read a JVM-global property another test may set
        c.setBatchSize(1);
        c.setFlushIntervalMs(50);
        c.setHttpReadTimeoutMs(60_000);
        c.setHttpWriteTimeoutMs(60_000);
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(1);
        c.setShutdownGracePeriodMs(100);
        return c;
    }

    private static org.opennms.integration.api.v1.timeseries.Sample sample(String id) {
        return sample(id, "t");
    }

    /** Distinct metric names map to distinct label sets, which is what the
     *  two-shard tests need to find samples on both shards. */
    private static org.opennms.integration.api.v1.timeseries.Sample sample(String id, String metricName) {
        return ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .externalTag("id", id)
                        .build())
                .time(Instant.now())
                .value(1.0)
                .build();
    }
}
