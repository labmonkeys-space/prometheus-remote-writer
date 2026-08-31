/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

class StatsCommandTest {

    @Test
    void renders_message_when_storage_is_not_active() {
        StatsCommand cmd = new StatsCommand((PrometheusRemoteWriterStorage) null);
        assertThat(capture(cmd)).contains("not active");
    }

    @Test
    void renders_message_when_storage_has_not_been_started() {
        PrometheusRemoteWriterStorage storage = mock(PrometheusRemoteWriterStorage.class);
        when(storage.getMetrics()).thenReturn(null);
        StatsCommand cmd = new StatsCommand(storage);
        assertThat(capture(cmd)).contains("not been started");
    }

    @Test
    void renders_all_counters_and_gauges_from_snapshot() {
        PluginMetrics metrics = new PluginMetrics();
        metrics.samplesWritten(42);
        metrics.samplesDropped4xx(3);
        metrics.samplesUnparseableResourceId(5);
        metrics.registerLongGauge(PluginMetrics.QUEUE_DEPTH, () -> 7L);

        PrometheusRemoteWriterStorage storage = mock(PrometheusRemoteWriterStorage.class);
        when(storage.getMetrics()).thenReturn(metrics);

        String out = capture(new StatsCommand(storage));

        assertThat(out).contains("samples_written_total");
        assertThat(out).contains("42");
        assertThat(out).contains("samples_dropped_4xx_total");
        assertThat(out).contains("3");
        assertThat(out).contains("samples_unparseable_resource_id_total");
        assertThat(out).contains("5");
        assertThat(out).contains("queue_depth");
        assertThat(out).contains("7");
        // Header
        assertThat(out).contains("prometheus-remote-writer metrics");
    }

    @Test
    void renders_wal_counters_and_gauges() {
        // WAL-enabled deployments see the new wal_* metrics in the shell
        // output alongside the existing counters. This pins that
        // PluginMetrics.snapshot() enumerates every counter registered
        // in the constructor — a regression that added a wal_* counter
        // but forgot to register it would silently drop from stats.
        PluginMetrics metrics = new PluginMetrics();
        metrics.walBytesWritten(10_000);
        metrics.walBytesCheckpointed(8_192);
        metrics.walReplaySamples(4);
        metrics.walBatchesDropped4xx(1);
        metrics.samplesDroppedWalFull(123);
        metrics.registerLongGauge(PluginMetrics.WAL_DISK_USAGE_BYTES, () -> 65_536L);
        metrics.registerLongGauge(PluginMetrics.WAL_SEGMENTS_ACTIVE, () -> 3L);

        PrometheusRemoteWriterStorage storage = mock(PrometheusRemoteWriterStorage.class);
        when(storage.getMetrics()).thenReturn(metrics);

        String out = capture(new StatsCommand(storage));

        assertThat(out).contains("wal_bytes_written_total");
        assertThat(out).contains("10000");
        assertThat(out).contains("wal_bytes_checkpointed_total");
        assertThat(out).contains("8192");
        assertThat(out).contains("wal_replay_samples_total");
        assertThat(out).contains("wal_batches_dropped_4xx_total");
        assertThat(out).contains("samples_dropped_wal_full_total");
        assertThat(out).contains("123");
        assertThat(out).contains("wal_disk_usage_bytes");
        assertThat(out).contains("65536");
        assertThat(out).contains("wal_segments_active");
    }

    // ---------- registration contract (#113) --------------------------------
    // Karaf's CommandExtender registers the command only if the class carries
    // BOTH @Command and @Service, and injects @Reference fields. The manifest
    // header half of the contract lives in plugin/pom.xml (Karaf-Commands)
    // and is exercised by the e2e smoke's stats-command step; this test pins
    // the class-level half so a refactor can't silently drop an annotation.

    @Test
    void command_carries_the_karaf_registration_annotations() throws Exception {
        org.apache.karaf.shell.api.action.Command cmd =
                StatsCommand.class.getAnnotation(org.apache.karaf.shell.api.action.Command.class);
        assertThat(cmd).as("@Command present").isNotNull();
        assertThat(cmd.scope()).isEqualTo("opennms");
        assertThat(cmd.name()).isEqualTo("prometheus-writer-stats");

        assertThat(StatsCommand.class.getAnnotation(
                org.apache.karaf.shell.api.action.lifecycle.Service.class))
                .as("@Service present — without it the extender skips the class")
                .isNotNull();

        java.lang.reflect.Field storage = StatsCommand.class.getDeclaredField("storage");
        assertThat(storage.getType()).isEqualTo(PrometheusRemoteWriterStorage.class);
        org.apache.karaf.shell.api.action.lifecycle.Reference ref =
                storage.getAnnotation(org.apache.karaf.shell.api.action.lifecycle.Reference.class);
        assertThat(ref).as("@Reference present on storage field").isNotNull();
        assertThat(ref.optional())
                .as("optional=true — a mandatory reference delay-activates the "
                    + "command into 'Command not found' while the plugin is unconfigured")
                .isTrue();
    }

    private static String capture(StatsCommand cmd) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cmd.render(new PrintStream(baos, true, StandardCharsets.UTF_8));
        return baos.toString(StandardCharsets.UTF_8);
    }
}
