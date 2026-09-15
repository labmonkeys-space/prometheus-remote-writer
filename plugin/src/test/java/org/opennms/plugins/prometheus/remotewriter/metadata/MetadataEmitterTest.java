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
 * key nobody has seen before cannot grow the backend's label-name index; the
 * info series carries the configured columns, and the speed is a gauge.
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
        return emitter(new MetadataEmitter.Settings(CADENCE, budget, instanceId,
                InfoColumns.parse(InfoColumns.DEFAULT_SPEC)));
    }

    private MetadataEmitter emitter(MetadataEmitter.Settings settings) {
        return new MetadataEmitter(registry, settings,
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

        // nodeLabel is on every data series as node_label, so it is no row.
        assertThat(n).isEqualTo(7);
        assertThat(labelsOf(emitted())).containsExactly(
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifAlias", "value", "uplink"),
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifDescr", "value", "GigabitEthernet0/0"),
                Map.of("__name__", "onms_resource_attr", "resourceId", rid, "key", "ifName", "value", "eth0"),
                Map.of("__name__", "onms_resource_category", "resourceId", rid, "category", "ProductionSites"),
                Map.of("__name__", "onms_resource_category", "resourceId", rid, "category", "Routers"),
                Map.of("__name__", "onms_resource_info", "resourceId", rid, "if_alias", "uplink", "if_descr", "GigabitEthernet0/0"),
                Map.of("__name__", "onms_resource_ifspeed", "resourceId", rid));
        List<MappedSample> all = emitted();
        assertThat(all.subList(0, 6)).allSatisfy(s -> assertThat(s.value()).isEqualTo(1.0));
        // ifHighSpeed=1000 Mbit/s as a gauge in bits per second: a multiplier as-is.
        assertThat(all.get(6).value()).isEqualTo(1_000_000_000.0);
        assertThat(all).allSatisfy(s -> assertThat(s.timestampMs()).isEqualTo(clock.get()));
    }

    @Test
    void the_default_columns_give_storage_and_jdbc_resources_their_names() {
        registry.observe("node[1].hrStorageIndex[3]", metric("node[1].hrStorageIndex[3]",
                "hrStorageDescr", "/var/lib/postgresql", "hrStorageAllocationUnits", "4096"));
        registry.observe("node[1].pgDatabase[opennms]", metric("node[1].pgDatabase[opennms]",
                "name", "opennms", "datname", "opennms", "spcname", "pg_default"));
        emitter(16, null).emitDue();
        assertThat(labelsOf(emitted())).contains(
                Map.of("__name__", "onms_resource_info", "resourceId", "node[1].hrStorageIndex[3]",
                        "hr_storage_descr", "/var/lib/postgresql"),
                Map.of("__name__", "onms_resource_info", "resourceId", "node[1].pgDatabase[opennms]",
                        "resource_name", "opennms", "datname", "opennms", "spcname", "pg_default"));
        // The collector attribute `name` is never a label called `name`.
        assertThat(labelsOf(emitted())).noneSatisfy(l -> assertThat(l).containsKey("name"));
    }

    @Test
    void an_empty_attribute_makes_no_row_and_no_column() {
        // An unaliased interface: ifAlias is present and empty.
        registry.observe("r", metric("r", "ifAlias", "", "ifName", "eth0"));
        emitter(16, null).emitDue();
        List<Map<String, String>> l = labelsOf(emitted());
        assertThat(l).noneSatisfy(m -> assertThat(m).containsEntry("key", "ifAlias"));
        assertThat(l).noneSatisfy(m -> assertThat(m).containsKey("if_alias"));
        assertThat(l).noneSatisfy(m -> assertThat(m).containsEntry("__name__", "onms_resource_info"));
    }

    @Test
    void a_resource_with_none_of_the_configured_keys_gets_no_info_series() {
        registry.observe("r", metric("r", "sysContact", "noc@example.com"));
        emitter(16, null).emitDue();
        assertThat(labelsOf(emitted())).noneSatisfy(l -> assertThat(l).containsEntry("__name__", "onms_resource_info"));
    }

    @Test
    void an_operator_defined_column_is_read_from_its_key() {
        registry.observe("r", metric("r", "sysContact", "noc@example.com", "ifAlias", "x"));
        emitter(new MetadataEmitter.Settings(CADENCE, 16, null, InfoColumns.parse("contact=sysContact"))).emitDue();
        assertThat(labelsOf(emitted())).contains(
                Map.of("__name__", "onms_resource_info", "resourceId", "r", "contact", "noc@example.com"));
    }

    @Test
    void columns_read_the_whole_attribute_set_not_the_budgeted_rows() {
        // 20 attributes, budget 4: the rows stop at attr03 but a column keyed
        // on a later attribute still gets its value.
        String[] kv = new String[42];
        for (int i = 0; i < 20; i++) { kv[2 * i] = String.format("attr%02d", i); kv[2 * i + 1] = "v" + i; }
        kv[40] = "zzz"; kv[41] = "last";
        registry.observe("r", metric("r", kv));
        emitter(new MetadataEmitter.Settings(CADENCE, 4, null, InfoColumns.parse("z=zzz"))).emitDue();
        assertThat(labelsOf(emitted())).contains(Map.of("__name__", "onms_resource_info", "resourceId", "r", "z", "last"));
    }

    @Test
    void the_instance_id_travels_with_the_info_series_too() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        emitter(16, "core-01").emitDue();
        assertThat(labelsOf(emitted())).contains(
                Map.of("__name__", "onms_resource_info", "resourceId", "r", "onms_instance_id", "core-01", "if_alias", "a"));
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
        registry.observe("r", metric("r", "sysContact", "a"));
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
        MetadataEmitter e = new MetadataEmitter(registry, new MetadataEmitter.Settings(CADENCE, 16, null, Map.of()),
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
        // A latency resource carries only the metric's own identity, which
        // is not a property of the resource and becomes no row (#223).
        registry.observe("node[1].responseTime[10.0.0.1]", metric("node[1].responseTime[10.0.0.1]",
                "ICMP/10.0.0.1", "latency ICMP/10.0.0.1"));
        emitter(16, null).emitDue();

        List<Map<String, String>> rows = labelsOf(emitted());
        assertThat(rows).contains(
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].hrStorageIndex[3]", "key", "hrStorageDescr", "value", "/var/lib/postgresql"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].hrStorageIndex[3]", "key", "hrStorageAllocationUnits", "value", "4096"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].opennms-eventd[0]", "key", "name", "value", "Eventd Processing Stats"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].pgDatabase[opennms]", "key", "datname", "value", "opennms"),
                Map.of("__name__", "onms_resource_attr", "resourceId", "node[1].pgDatabase[opennms]", "key", "spcname", "value", "pg_default"));
        assertThat(rows)
                .as("a metric identity is no row, so the latency resource emits nothing")
                .noneMatch(l -> "node[1].responseTime[10.0.0.1]".equals(l.get("resourceId")));
        // Three label names on every row, whatever the key.
        assertThat(rows).filteredOn(l -> "onms_resource_attr".equals(l.get("__name__")))
                .isNotEmpty()
                .allSatisfy(l -> assertThat(l.keySet()).containsExactlyInAnyOrder("__name__", "resourceId", "key", "value"));
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
        // ifAlias is a default column, so each emission is a row plus an info series.
        assertThat(e.emitDue()).isEqualTo(2);
        assertThat(e.emitDue()).isZero();
        clock.addAndGet(CADENCE);
        assertThat(e.emitDue()).isEqualTo(2);
        registry.observe("r", metric("r", "ifAlias", "b"));
        assertThat(e.emitDue()).isEqualTo(2);
        assertThat(emitted().get(4).labels()).containsEntry("value", "b");
    }

    @Test
    void the_instance_id_travels_with_the_rows_when_configured() {
        registry.observe("r", metric("r", "ifAlias", "a"));
        emitter(16, "core-01").emitDue();
        assertThat(emitted().get(0).labels()).containsEntry("onms_instance_id", "core-01");
        assertThat(emitted().get(0).labels()).containsEntry("__name__", "onms_resource_attr");
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
