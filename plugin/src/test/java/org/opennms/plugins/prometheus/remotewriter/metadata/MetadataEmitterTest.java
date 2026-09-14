/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * What the emitter puts on the wire for a resource: the rows, pinned per
 * resource type, and the budget that bounds them.
 *
 * <p>Rows have three label names whatever OpenNMS supplies, so an attribute
 * key nobody has seen before cannot grow the backend's label-name index.
 */
class MetadataEmitterTest {

    private static final long CADENCE = 15 * 60_000L;

    private final AtomicLong clock = new AtomicLong(1_700_000_000_000L);
    private final MetadataRegistry registry = new MetadataRegistry(clock::get);
    private final PluginMetrics metrics = new PluginMetrics();
    private final List<List<MappedSample>> batches = new ArrayList<>();
    /** What the sink refuses on its next call; the pipeline's answer. */
    private int refuseNext = 0;

    private MetadataEmitter emitter(int budget, String instanceId) {
        return new MetadataEmitter(registry, new MetadataEmitter.Settings(CADENCE, budget, instanceId),
                batch -> { batches.add(batch); int r = refuseNext; refuseNext = 0; return r; },
                metrics, clock::get);
    }

    private List<MappedSample> emitted() {
        List<MappedSample> out = new ArrayList<>();
        batches.forEach(out::addAll);
        return out;
    }

    private static List<Map<String, String>> labelsOf(List<MappedSample> samples) {
        return samples.stream().map(MappedSample::labels).toList();
    }

    private static Metric metric(String resourceId, String... kv) {
        ImmutableMetric.MetricBuilder b = ImmutableMetric.builder()
                .intrinsicTag("name", "x").intrinsicTag("resourceId", resourceId);
        for (int i = 0; i < kv.length; i += 2) b.externalTag(kv[i], kv[i + 1]);
        return b.build();
    }

    @Test
    void an_interface_becomes_one_row_per_attribute_and_per_category() {
        String rid = "nodeSource[NOC:router-42].interfaceSnmp[eth0]";
        registry.observe(rid, metric(rid,
                "nodeLabel", "router-42", "ifName", "eth0", "ifDescr", "GigabitEthernet0/0",
                "ifAlias", "uplink", "ifHighSpeed", "1000", "categories", "Routers,ProductionSites"));
        int n = emitter(16, null).emitDue();

        assertThat(n).isEqualTo(7);
        assertThat(labelsOf(emitted())).containsExactly(
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifAlias", "value", "uplink"),
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifDescr", "value", "GigabitEthernet0/0"),
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifName", "value", "eth0"),
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "nodeLabel", "value", "router-42"),
                Map.of("__name__", "onms_resource_category", "resourceId", rid, "category", "ProductionSites"),
                Map.of("__name__", "onms_resource_category", "resourceId", rid, "category", "Routers"),
                Map.of("__name__", "onms_resource_ifspeed", "resourceId", rid));
        List<MappedSample> all = emitted();
        assertThat(all.subList(0, 6)).allSatisfy(s -> assertThat(s.value()).isEqualTo(1.0));
        // ifHighSpeed=1000 Mbit/s as a gauge in bits per second: a multiplier as-is.
        assertThat(all.get(6).value()).isEqualTo(1_000_000_000.0);
        assertThat(all).allSatisfy(s -> assertThat(s.timestampMs()).isEqualTo(clock.get()));
    }

    @Test
    void a_speed_only_resource_emits_the_gauge_and_no_rows() {
        registry.observe("r", metric("r", "ifSpeed", "100000000"));
        assertThat(emitter(16, null).emitDue()).isEqualTo(1);
        assertThat(emitted().get(0).labels()).isEqualTo(Map.of("__name__", "onms_resource_ifspeed", "resourceId", "r"));
        assertThat(emitted().get(0).value()).isEqualTo(100_000_000.0);
    }

    @Test
    void a_refused_batch_is_retried_next_tick_and_a_taken_one_is_not() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        MetadataEmitter e = emitter(16, null);
        refuseNext = 1;
        assertThat(e.emitDue()).isZero();
        assertThat(e.emitDue()).isEqualTo(1);   // retried, now taken
        assertThat(e.emitDue()).isZero();       // and not again before the cadence
        assertThat(metrics.snapshot().get(PluginMetrics.METADATA_SERIES_EMITTED).longValue()).isEqualTo(1);
    }

    @Test
    void a_batch_the_pipeline_throws_on_is_retried_next_tick() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        MetadataEmitter e = new MetadataEmitter(registry, new MetadataEmitter.Settings(CADENCE, 16, null),
                batch -> { if (fail.getAndSet(false)) throw new IllegalStateException("disk"); batches.add(batch); return 0; },
                metrics, clock::get);
        e.tick();                                // swallowed and counted as a sink error
        assertThat(batches).isEmpty();
        e.tick();
        assertThat(batches).hasSize(1);
    }

    @Test
    void a_tick_expires_resources_unseen_for_a_day() {
        registry.observe("old", metric("old", "ifAlias", "a"));
        MetadataEmitter e = emitter(16, null);
        e.tick();
        clock.addAndGet(25 * 3_600_000L);
        registry.observe("new", metric("new", "ifAlias", "b"));
        e.tick();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void storage_jmx_jdbc_and_icmp_resources_pin_their_rows() {
        registry.observe("node[1].hrStorageIndex[3]", metric("node[1].hrStorageIndex[3]",
                "hrStorageDescr", "/var/lib/postgresql", "hrStorageAllocationUnits", "4096"));
        registry.observe("node[1].opennms-eventd[0]", metric("node[1].opennms-eventd[0]",
                "name", "Eventd Processing Stats"));
        registry.observe("node[1].pgDatabase[opennms]", metric("node[1].pgDatabase[opennms]",
                "datname", "opennms", "spcname", "pg_default"));
        registry.observe("node[1].responseTime[10.0.0.1]", metric("node[1].responseTime[10.0.0.1]",
                "ICMP/10.0.0.1", "latency ICMP/10.0.0.1"));
        emitter(16, null).emitDue();

        List<Map<String, String>> rows = labelsOf(emitted());
        assertThat(rows).contains(
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].hrStorageIndex[3]", "key", "hrStorageDescr", "value", "/var/lib/postgresql"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].hrStorageIndex[3]", "key", "hrStorageAllocationUnits", "value", "4096"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].opennms-eventd[0]", "key", "name", "value", "Eventd Processing Stats"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].pgDatabase[opennms]", "key", "datname", "value", "opennms"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].pgDatabase[opennms]", "key", "spcname", "value", "pg_default"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].responseTime[10.0.0.1]", "key", "ICMP/10.0.0.1", "value", "latency ICMP/10.0.0.1"));
        // Three label names on every row, whatever the key.
        assertThat(rows).allSatisfy(l -> assertThat(l.keySet()).containsExactlyInAnyOrder("__name__", "resourceId", "key", "value"));
    }

    @Test
    void the_budget_keeps_the_lowest_keys_and_counts_the_rest() {
        String[] kv = new String[40];
        for (int i = 0; i < 20; i++) { kv[2 * i] = String.format("attr%02d", i); kv[2 * i + 1] = "v" + i; }
        registry.observe("r", metric("r", kv));
        int n = emitter(16, null).emitDue();

        assertThat(n).isEqualTo(16);
        assertThat(emitted().stream().map(s -> s.labels().get("key")))
                .containsExactly("attr00", "attr01", "attr02", "attr03", "attr04", "attr05", "attr06", "attr07",
                        "attr08", "attr09", "attr10", "attr11", "attr12", "attr13", "attr14", "attr15");
        assertThat(metrics.snapshot().get(PluginMetrics.METADATA_ATTRS_DROPPED).longValue()).isEqualTo(4);
        assertThat(metrics.snapshot().get(PluginMetrics.METADATA_SERIES_EMITTED).longValue()).isEqualTo(16);
    }

    @Test
    void emission_follows_the_registry_schedule() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        MetadataEmitter e = emitter(16, null);
        assertThat(e.emitDue()).isEqualTo(1);
        assertThat(e.emitDue()).isZero();
        clock.addAndGet(CADENCE);
        assertThat(e.emitDue()).isEqualTo(1);
        registry.observe("r", metric("r", "ifAlias", "b"));
        assertThat(e.emitDue()).isEqualTo(1);
        assertThat(emitted().get(2).labels()).containsEntry("value", "b");
    }

    @Test
    void the_instance_id_travels_with_the_rows_when_configured() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        emitter(16, "core-01").emitDue();
        assertThat(emitted().get(0).labels()).containsEntry("onms_instance_id", "core-01");
    }

    @Test
    void label_values_are_sanitised_like_the_data_series() {
        String raw = "uplink \u0000 to core";
        registry.observe("r", metric("r", "ifAlias", raw));
        emitter(16, null).emitDue();
        assertThat(emitted().get(0).labels().get("value"))
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer.labelValue(raw));
    }

    @Test
    void a_resource_with_nothing_to_say_emits_nothing() {
        registry.observe("r", metric("r"));
        assertThat(emitter(16, null).emitDue()).isZero();
        assertThat(batches).isEmpty();
    }
}
