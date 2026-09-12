/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
 * End-to-end integration tests for the bottom-right corner of the IT
 * matrix: {@code wal.enabled=true} combined with
 * {@code wire.protocol-version=2}, against a Prometheus 3.0+ container
 * that natively accepts Remote Write v2.
 *
 * <p>Two scenarios:
 * <ol>
 *   <li><b>WAL replay under v2</b> — samples queued in the WAL while the
 *       endpoint is unreachable, then replayed under v2 after a restart
 *       against a real Prometheus 3.0.1.</li>
 *   <li><b>Wire-version flipped mid-WAL</b> — samples written under
 *       {@code wire.protocol-version=1}, replayed under
 *       {@code wire.protocol-version=2} after a restart. Pins the
 *       wire-version-agnostic-WAL invariant in
 *       {@code add-remote-write-v2-support}.</li>
 * </ol>
 *
 * <p>Helpers are duplicated from {@link PrometheusRemoteWriteWalIT} to
 * avoid premature abstraction across two ITs. If a third WAL IT lands,
 * extract a shared utility then.
 */
@Testcontainers
class PrometheusRemoteWriteWalV2IT {

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

    @AfterEach
    void stop() {
        if (storage != null) try { storage.stop(); } catch (Exception ignored) {}
        storage = null;
    }

    @Test
    void wal_v2_persists_and_replays_on_restart(@TempDir Path walDir) throws Exception {
        String metricName = "onms_it_wal_v2_replay_" + System.nanoTime();
        Instant now = Instant.now();

        // Phase 1: WAL+v2, broken endpoint. Samples queue, never ship.
        {
            PrometheusRemoteWriterConfig c = walConfig(walDir);
            c.setWriteUrl("http://127.0.0.1:1/api/v1/write"); // port 1 → refused
            c.setReadUrl("http://127.0.0.1:1");
            c.setRetryMaxAttempts(1);
            c.setRetryInitialBackoffMs(10);
            c.setRetryMaxBackoffMs(50);
            storage = new PrometheusRemoteWriterStorage(c);
            storage.start();
            storage.store(List.of(sample(metricName, now, 1.0)));
            storage.store(List.of(sample(metricName, now.plusMillis(1), 2.0)));
            storage.store(List.of(sample(metricName, now.plusMillis(2), 3.0)));
            Thread.sleep(500); // let the flusher attempt and fail at least once
            // ECONNREFUSED is a transport error: the checkpoint must NOT
            // advance, and SAMPLES_WRITTEN must stay at 0. If this ever
            // regresses (transport reclassified as 4xx → drop), phase 2
            // would see an empty replay and the failure mode would point
            // at replay rather than the actual classification bug.
            assertThat(storage.getMetrics().snapshot()
                    .get(PluginMetrics.SAMPLES_WRITTEN).longValue()).isZero();
            storage.stop();
            storage = null;
        }

        // Phase 2: same wal.path, real Prom 3.0.1 URL, still v2.
        // Replay must hit the v2 builder and reach Prometheus.
        {
            PrometheusRemoteWriterConfig c = walConfig(walDir);
            setRealPrometheusUrls(c);
            storage = new PrometheusRemoteWriterStorage(c);
            storage.start();

            // Approximate by design: the counter sums per-segment index
            // counts. Delivery below is the assertion that matters.
            long replayed = storage.getMetrics().snapshot()
                    .get(PluginMetrics.WAL_REPLAY_SAMPLES).longValue();
            assertThat(replayed).isPositive();

            awaitMetricInPrometheus(metricName, 3);

            // Positive proof the v2 wire path was used: if v1 bytes had
            // been sent under v2 headers, Prom 3.0.1 would 4xx the batch
            // and SAMPLES_WRITTEN would never reach 3 (so awaitMetric
            // would time out). Pinning samples_dropped_4xx==0 turns that
            // failure mode from "await timeout" into a direct
            // wire-format-mismatch signal.
            assertThat(storage.getMetrics().snapshot()
                    .get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue()).isZero();
        }
    }

    @Test
    void wire_version_flipped_mid_wal_replays_under_new_version(@TempDir Path walDir)
            throws Exception {
        String metricName = "onms_it_wal_v2_flip_" + System.nanoTime();
        Instant now = Instant.now();

        // Phase 1: WAL+v1, broken endpoint. Samples land in WAL as
        // wire-version-agnostic MappedSample (the WAL doesn't cache
        // protobuf bytes per entry).
        {
            PrometheusRemoteWriterConfig c = walConfig(walDir);
            c.setWireProtocolVersion("1"); // override the helper default
            c.setWriteUrl("http://127.0.0.1:1/api/v1/write");
            c.setReadUrl("http://127.0.0.1:1");
            c.setRetryMaxAttempts(1);
            c.setRetryInitialBackoffMs(10);
            c.setRetryMaxBackoffMs(50);
            storage = new PrometheusRemoteWriterStorage(c);
            storage.start();
            storage.store(List.of(sample(metricName, now, 1.0)));
            storage.store(List.of(sample(metricName, now.plusMillis(1), 2.0)));
            storage.store(List.of(sample(metricName, now.plusMillis(2), 3.0)));
            Thread.sleep(500);
            // Same transport-classification check as scenario 1 — phase
            // 1 must not have advanced the checkpoint.
            assertThat(storage.getMetrics().snapshot()
                    .get(PluginMetrics.SAMPLES_WRITTEN).longValue()).isZero();
            storage.stop();
            storage = null;
        }

        // Phase 2: same wal.path, but now v2 + real Prom 3.0.1 URL.
        // The WAL must replay the v1-era samples under v2 headers; if
        // anything in the WAL had cached v1 wire bytes, Prom 3.0.1 would
        // reject the request (it only knows v2).
        {
            PrometheusRemoteWriterConfig c = walConfig(walDir); // defaults to v2
            setRealPrometheusUrls(c);
            storage = new PrometheusRemoteWriterStorage(c);
            storage.start();

            // Approximate by design: the counter sums per-segment index
            // counts. Delivery below is the assertion that matters.
            long replayed = storage.getMetrics().snapshot()
                    .get(PluginMetrics.WAL_REPLAY_SAMPLES).longValue();
            assertThat(replayed).isPositive();

            awaitMetricInPrometheus(metricName, 3);

            // Positive proof v2 was used (see scenario 1 for the
            // reasoning). Especially load-bearing here: this scenario's
            // raison d'être is that v1-era WAL samples must replay
            // cleanly under v2; a 4xx tick would indicate the wire
            // layer somehow leaked v1 bytes.
            assertThat(storage.getMetrics().snapshot()
                    .get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue()).isZero();
        }
    }

    // --- helpers ----------------------------------------------------------------
    // Duplicated from PrometheusRemoteWriteWalIT — see class javadoc for why.

    private static PrometheusRemoteWriterConfig walConfig(Path walDir) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("http://example.invalid/api/v1/write");
        c.setReadUrl("http://example.invalid");
        c.setBatchSize(1);   // must fit the one-slot memory tier; disk batches are unaffected
        c.setFlushIntervalMs(50);
        c.setRetryMaxAttempts(5);
        c.setRetryInitialBackoffMs(50);
        c.setRetryMaxBackoffMs(200);
        c.setShutdownGracePeriodMs(2_000);
        // Degenerate tiered configuration: one memory slot, so everything
        // spills and the disk tier is what the test actually exercises.
        c.setQueueCapacity(1);
        c.setOverflowDir(walDir.toString());
        c.setOverflowMaxSizeBytes(1L << 20); // 1 MiB
        c.setOverflowFsync("batch");
        c.setOverflowFull("refuse");
        c.setWireProtocolVersion("2"); // default for this IT; flip-test overrides
        return c;
    }

    private void setRealPrometheusUrls(PrometheusRemoteWriterConfig c) {
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setRetryMaxAttempts(10);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
    }

    private static Sample sample(String metricName, Instant t, double value) {
        return ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId",
                                "nodeSource[NOC:wal-v2-it].interfaceSnmp[eth0]")
                        .externalTag("foreignSource", "NOC")
                        .externalTag("foreignId", "wal-v2-it")
                        .build())
                .time(t)
                .value(value)
                .build();
    }

    private void awaitMetricInPrometheus(String metricName, long minSamples) throws Exception {
        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(30))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue()
                       >= minSamples);

        TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value(metricName)
                .build();
        List<Metric> found = await().atMost(Duration.ofSeconds(20))
                .until(() -> storage.findMetrics(List.of(nameMatcher)), list -> !list.isEmpty());
        assertThat(found).isNotEmpty();
    }
}
