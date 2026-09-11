/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.Test;

class PluginMetricsTest {

    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    /** Dropwizard names MBeans {@code <domain>:name=<metric>,type=<counters|gauges>}. */
    static ObjectName mbean(String metric) throws Exception {
        java.util.Set<ObjectName> found = MBS.queryNames(
                new ObjectName(PluginMetrics.JMX_DOMAIN + ":name=" + metric + ",*"), null);
        assertThat(found).as("exactly one MBean for %s", metric).hasSize(1);
        return found.iterator().next();
    }

    @Test
    void jmx_reporter_registers_every_metric_and_unregisters_on_stop() throws Exception {
        ObjectName all = new ObjectName(PluginMetrics.JMX_DOMAIN + ":*");
        PluginMetrics m = new PluginMetrics();
        m.registerLongGauge("unit_test_gauge", () -> 7L);

        m.startJmxReporter();
        try {
            ObjectName written = mbean(PluginMetrics.SAMPLES_WRITTEN);
            assertThat(written.getKeyProperty("type")).isEqualTo("counters");
            assertThat(MBS.getAttribute(written, "Count")).isEqualTo(0L);
            ObjectName gauge = mbean("unit_test_gauge");
            assertThat(gauge.getKeyProperty("type")).isEqualTo("gauges");
            assertThat(MBS.getAttribute(gauge, "Value")).isEqualTo(7L);
        } finally {
            m.stopJmxReporter();
        }
        assertThat(MBS.queryNames(all, null)).isEmpty();
    }

    @Test
    void restarting_the_reporter_on_a_fresh_registry_does_not_duplicate_mbeans() throws Exception {
        ObjectName all = new ObjectName(PluginMetrics.JMX_DOMAIN + ":*");
        PluginMetrics first = new PluginMetrics();
        first.startJmxReporter();
        first.stopJmxReporter();
        PluginMetrics second = new PluginMetrics();
        second.startJmxReporter();
        try {
            long copies = MBS.queryNames(all, null).stream()
                    .filter(n -> PluginMetrics.SAMPLES_WRITTEN.equals(n.getKeyProperty("name"))).count();
            assertThat(copies).isEqualTo(1L);
        } finally {
            second.stopJmxReporter();
        }
    }

    @Test
    void start_and_stop_are_idempotent() throws Exception {
        PluginMetrics m = new PluginMetrics();
        m.startJmxReporter();
        m.startJmxReporter();
        m.stopJmxReporter();
        m.stopJmxReporter();
        assertThat(MBS.queryNames(new ObjectName(PluginMetrics.JMX_DOMAIN + ":*"), null)).isEmpty();
    }
}
