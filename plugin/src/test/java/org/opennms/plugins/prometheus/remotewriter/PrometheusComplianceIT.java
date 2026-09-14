/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.AbstractStorageIntegrationTest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the OpenNMS-supplied TSS compliance suite
 * ({@link AbstractStorageIntegrationTest}) against this plugin backed by a
 * real Prometheus container.
 *
 * <p><b>Known-deviating tests</b> that conflict with deliberate v0.1 design
 * decisions are overridden with {@link Ignore}:
 * <ul>
 *   <li><b>Partition-lossy read path</b> (design.md §6). Prometheus does not
 *       model the OpenNMS intrinsic/meta/external tag partition, so the
 *       compliance suite's whole-{@code Metric} equality assertions fail
 *       by design. The plugin's own {@link PrometheusRemoteWriteIT} covers
 *       equivalent round-trip coverage without that assumption.</li>
 *   <li><b>delete() is a no-op</b> (design.md §7). Remote Write has no
 *       delete semantic; the compliance suite's {@code shouldDeleteMetrics}
 *       verifies that deletion actually removes data.</li>
 * </ul>
 *
 * <p>The compliance tests that remain exercise {@code findMetrics} with
 * empty/null matchers (argument validation, which is storage-agnostic).
 * Extending coverage beyond that requires either:
 * (a) relaxing the compliance-suite assertions upstream, or
 * (b) accepting partition-preservation on the read side (rejected in v0.1
 * to keep PromQL labels clean).
 *
 * <p>Runs in the {@code verify} phase; requires Docker.
 */
public class PrometheusComplianceIT extends AbstractStorageIntegrationTest {

    private static GenericContainer<?> prometheus;
    private static PrometheusRemoteWriterStorage activeStorage;

    @Override
    protected TimeSeriesStorage createStorage() {
        if (prometheus == null) {
            prometheus = new GenericContainer<>(DockerImageName.parse(PrometheusImages.V1_REFERENCE))
                    .withExposedPorts(9090)
                    .withCommand(
                            "--config.file=/etc/prometheus/prometheus.yml",
                            "--storage.tsdb.path=/prometheus",
                            "--web.console.libraries=/usr/share/prometheus/console_libraries",
                            "--web.console.templates=/usr/share/prometheus/consoles",
                            "--web.enable-remote-write-receiver")
                    .waitingFor(Wait.forHttp("/-/ready").forStatusCode(200));
            prometheus.start();
        }

        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setOverflowMaxSizeBytes(0);   // memory-only: these suites pin the queue path
        c.setReadUrl(base);
        // Surface every tag as a Prom label so the round-trip keys match.
        c.setLabelsInclude("*");
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);

        if (activeStorage != null) activeStorage.stop();
        activeStorage = new PrometheusRemoteWriterStorage(c);
        activeStorage.start();
        return activeStorage;
    }

    @Override
    protected void waitForPersistingChanges() throws InterruptedException {
        // Give the flusher a beat to drain + Prometheus to ingest.
        Thread.sleep(3_000);
    }

    // --- overrides that conflict with deliberate v0.1 design choices ------

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldLoadMultipleMetricsWithSameTag() {}

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldFindOneMetricWithUniqueTag() {}

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldFindOneMetricWithRegexMatching() {}

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldFindWithNotEquals() {}

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldFindOneMetricWithRegexNotMatching() {}

    @Override @Test @Ignore("partition-lossy read path — design.md §6; see javadoc")
    public void shouldGetSamplesForMetric() {}

    @Override @Test @Ignore("delete() is a no-op by design — design.md §7")
    public void shouldDeleteMetrics() {}

    // shouldThrowExceptionWhenFindCalledWithoutTagMatcher stays — pure argument
    // validation, works unchanged against our implementation.

    @AfterClass
    public static void stopAll() {
        if (activeStorage != null) { activeStorage.stop(); activeStorage = null; }
        if (prometheus != null)    { prometheus.stop();    prometheus = null; }
    }
}
