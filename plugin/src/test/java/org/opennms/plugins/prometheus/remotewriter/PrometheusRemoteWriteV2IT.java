/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test for the Remote Write v2 wire format
 * against a Prometheus 2.50+ instance running in a Testcontainers-
 * managed container. Same shape as {@link PrometheusRemoteWriteIT}
 * (which exercises the v1 default) but with
 * {@code wire.protocol-version=2}.
 *
 * <p>Verifies the full pipeline: store → label-mapping → flush →
 * v2 builder (with string interning) → snappy → HTTP POST with
 * v2 headers → Prometheus ingest → query API readback.
 */
@Testcontainers
class PrometheusRemoteWriteV2IT {

    // Prometheus 3.0 ships stable v2 receiver support (2.50 was
    // experimental; 2.55 stabilised; 3.0 promoted to default-enabled).
    // The IT pins 3.0.1 to give v2 a known-supported environment;
    // PrometheusRemoteWriteIT continues to exercise v1 against the
    // older 2.53.2 image.
    @Container
    static GenericContainer<?> prometheus =
            new GenericContainer<>(DockerImageName.parse("prom/prometheus:v3.0.1"))
                    .withExposedPorts(9090)
                    .withCommand(
                            "--config.file=/etc/prometheus/prometheus.yml",
                            "--storage.tsdb.path=/prometheus",
                            "--web.console.libraries=/usr/share/prometheus/console_libraries",
                            "--web.console.templates=/usr/share/prometheus/consoles",
                            "--web.enable-remote-write-receiver")
                    .waitingFor(Wait.forHttp("/-/ready").forStatusCode(200));

    private PrometheusRemoteWriterStorage storage;

    @BeforeEach
    void start() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setOverflowMaxSizeBytes(0);   // memory-only: these suites pin the queue path
        c.setReadUrl(base);
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        c.setWireProtocolVersion("2");

        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();
    }

    @AfterEach
    void stop() {
        if (storage != null) storage.stop();
    }

    @Test
    void v2_sample_round_trips_through_prometheus() throws Exception {
        Instant now = Instant.now();
        String metricName = "onms_v2_it_" + System.nanoTime();

        Sample sample = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId", "nodeSource[NOC:v2-it].interfaceSnmp[eth0]")
                        .externalTag("foreignSource", "NOC")
                        .externalTag("foreignId", "v2-it")
                        .externalTag("location", "lab")
                        .externalTag("ifName", "eth0")
                        .build())
                .time(now)
                .value(42.0)
                .build();

        storage.store(List.of(sample));

        // Wait for v2 ingest path to land the sample.
        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(20))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

        // Verify the sample is queryable through the read path (which
        // uses the standard Prometheus query API, independent of the
        // write-side protocol version).
        TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value(metricName)
                .build();

        List<Metric> found = await().atMost(Duration.ofSeconds(20))
                .until(() -> storage.findMetrics(List.of(nameMatcher)),
                       list -> !list.isEmpty());
        assertThat(found).hasSize(1);

        // Default labels must round-trip — confirms label interning
        // didn't drop anything on the wire.
        var keys = found.get(0).getMetaTags().stream().map(t -> t.getKey()).toList();
        assertThat(keys).contains(
                "job", "instance", "node", "foreign_source", "foreign_id",
                "location", "if_name");
    }

    @Test
    void v2_multiple_series_with_shared_labels_all_land() throws Exception {
        // Exercise the symbol-interning advantage: many series sharing
        // the same default label NAMES but different __name__ values.
        // If interning is broken, Prometheus would either reject the
        // payload or return wrong-shaped series.
        Instant now = Instant.now();
        String prefix = "onms_v2_intern_" + System.nanoTime() + "_";
        for (int i = 0; i < 5; i++) {
            String metricName = prefix + i;
            storage.store(List.of(ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "nodeSource[NOC:v2-i" + i + "].interfaceSnmp[eth0]")
                            .externalTag("foreignSource", "NOC")
                            .externalTag("foreignId", "v2-i" + i)
                            .build())
                    .time(now.plusMillis(i))
                    .value((double) i)
                    .build()));
        }

        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(20))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 5L);

        // Confirm each series exists in Prometheus (each via its unique name).
        for (int i = 0; i < 5; i++) {
            String metricName = prefix + i;
            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();
            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> storage.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);
        }
    }
}
