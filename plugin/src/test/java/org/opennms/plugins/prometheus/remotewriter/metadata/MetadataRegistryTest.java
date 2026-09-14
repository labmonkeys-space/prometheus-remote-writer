/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

/**
 * The per-resource registry the write path feeds: what it keeps, when it
 * reports a change, what is due for emission, and what it forgets.
 */
class MetadataRegistryTest {

    private static final long HOUR = 3_600_000L;
    private static final long CADENCE = 15 * 60_000L;

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final MetadataRegistry registry = new MetadataRegistry(clock::get);

    private static Metric interfaceMetric(String alias) {
        return ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]")
                .metaTag("mtype", "counter")
                .externalTag("nodeLabel", "router-42.example.com")
                .externalTag("ifName", "eth0")
                .externalTag("ifDescr", "GigabitEthernet0/0")
                .externalTag("ifAlias", alias)
                .externalTag("ifHighSpeed", "1000")
                .externalTag("ifSpeed", "4294967295")
                .externalTag("categories", "ProductionSites,Routers")
                .externalTag("requisition:location", "Pittsboro")
                .externalTag("snmp-community", "public")
                .build();
    }

    private static final String RID = "nodeSource[NOC:router-42].interfaceSnmp[eth0]";

    @Test
    void the_first_sight_of_a_resource_is_a_change() {
        assertThat(registry.observe(RID, interfaceMetric("uplink"))).isTrue();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void the_same_metadata_again_is_not_a_change() {
        registry.observe(RID, interfaceMetric("uplink"));
        assertThat(registry.observe(RID, interfaceMetric("uplink"))).isFalse();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void a_resources_counter_and_gauge_metrics_are_the_same_metadata() {
        // mtype differs per metric of one resource; it is not metadata, so the
        // second metric must not read as a change (or the resource would be
        // re-emitted on every sample).
        Metric counter = ImmutableMetric.builder()
                .intrinsicTag("name", "cpu").intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .metaTag("mtype", "counter").externalTag("nodeLabel", "n1").build();
        Metric gauge = ImmutableMetric.builder()
                .intrinsicTag("name", "loadavg").intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .metaTag("mtype", "gauge").externalTag("nodeLabel", "n1").build();
        assertThat(registry.observe("node[1].nodeSnmp[]", counter)).isTrue();
        assertThat(registry.observe("node[1].nodeSnmp[]", gauge)).isFalse();
        assertThat(registry.observe("node[1].nodeSnmp[]", counter)).isFalse();
    }

    @Test
    void marking_emitted_keeps_a_change_that_arrived_meanwhile() {
        registry.observe(RID, interfaceMetric("uplink"));
        List<ResourceMetadata> due = registry.dueForEmission(CADENCE);
        registry.observe(RID, interfaceMetric("renamed while in flight"));
        registry.markEmitted(due);
        List<ResourceMetadata> again = registry.dueForEmission(CADENCE);
        assertThat(again).hasSize(1);
        assertThat(again.get(0).attributes()).containsEntry("ifAlias", "renamed while in flight");
    }

    @Test
    void a_changed_attribute_is_a_change() {
        registry.observe(RID, interfaceMetric("uplink"));
        assertThat(registry.observe(RID, interfaceMetric("uplink to core-sw-1"))).isTrue();
        ResourceMetadata r = registry.dueForEmission(CADENCE).get(0);
        assertThat(r.attributes()).containsEntry("ifAlias", "uplink to core-sw-1");
    }

    @Test
    void only_eligible_tags_are_attributes() {
        registry.observe(RID, interfaceMetric("uplink"));
        ResourceMetadata r = registry.dueForEmission(CADENCE).get(0);
        // Intrinsics, mtype, categories, the speed pair, the keys the data
        // series carry as labels, context keys and secrets are not
        // attributes; keys keep their OpenNMS spelling.
        assertThat(r.attributes()).containsOnlyKeys("ifAlias", "ifDescr", "ifName");
        assertThat(r.categories()).containsExactly("ProductionSites", "Routers");
        assertThat(r.ifSpeedBps()).isEqualTo(1_000_000_000L);
        assertThat(r.resourceId()).isEqualTo(RID);
    }

    @Test
    void attributes_are_sorted_by_key() {
        registry.observe(RID, interfaceMetric("uplink"));
        ResourceMetadata r = registry.dueForEmission(CADENCE).get(0);
        assertThat(r.attributes().keySet()).containsExactly("ifAlias", "ifDescr", "ifName");
    }

    @Test
    void what_the_data_series_already_say_is_not_a_row() {
        // Every data series carries these as node_label, foreign_source,
        // foreign_id, location and node; the categories are rows of their
        // own, and OpenNMS's cat_<Name> tags repeat them. ifName stays: the
        // flow reports dereference {ifName}.
        Metric m = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets").intrinsicTag("resourceId", RID)
                .externalTag("nodeLabel", "core-sw-1").externalTag("foreignSource", "NOC")
                .externalTag("foreignId", "core-sw-1").externalTag("location", "Default")
                .externalTag("nodeId", "42")
                .externalTag("categories", "Production,Routers")
                .externalTag("cat_Production", "Production").externalTag("cat_Routers", "Routers")
                .externalTag("ifName", "Et1").externalTag("ifDescr", "Ethernet1")
                .build();
        registry.observe(RID, m);
        ResourceMetadata r = registry.dueForEmission(CADENCE).get(0);
        // nodeId stays: its label `node` carries foreignSource:foreignId, not the id.
        assertThat(r.attributes()).containsOnlyKeys("ifDescr", "ifName", "nodeId");
        assertThat(r.categories()).containsExactly("Production", "Routers");
        assertThat(MetadataRegistry.LABEL_KEYS).containsOnlyKeys("nodeLabel", "foreignSource", "foreignId", "location");
        java.util.Set<String> all = MetadataRegistry.LABEL_KEYS.keySet();
        assertThat(MetadataRegistry.whyNotARow("nodeLabel", all)).contains("node_label");
        assertThat(MetadataRegistry.whyNotARow("cat_Routers", all)).contains("onms_resource_category");
        assertThat(MetadataRegistry.whyNotARow("ifName", all)).isNull();
        assertThat(MetadataRegistry.whyNotARow("nodeLabel", java.util.Set.of())).isNull();
    }

    @Test
    void a_skipped_key_is_a_row_again_when_its_label_is_not_on_the_wire() {
        // labels.exclude = node_label: the value would otherwise be nowhere.
        MetadataRegistry r = new MetadataRegistry(clock::get, java.util.Set.of("foreignSource", "foreignId", "location"));
        r.observe(RID, interfaceMetric("uplink"));
        assertThat(r.dueForEmission(CADENCE).get(0).attributes()).containsKeys("nodeLabel", "ifAlias");
    }

    @Test
    void only_opennms_category_mirrors_are_skipped_not_every_cat_key() {
        Metric m = ImmutableMetric.builder()
                .intrinsicTag("name", "m").intrinsicTag("resourceId", RID)
                .externalTag("cat_Routers", "Routers")      // OpenNMS's mirror of a category
                .externalTag("cat_number", "7")             // an operator's own attribute
                .build();
        registry.observe(RID, m);
        assertThat(registry.dueForEmission(CADENCE).get(0).attributes()).containsOnlyKeys("cat_number");
    }

    @Test
    void a_change_to_a_skipped_key_alone_is_not_a_change() {
        Metric before = ImmutableMetric.builder()
                .intrinsicTag("name", "m").intrinsicTag("resourceId", RID)
                .externalTag("nodeLabel", "old-name").externalTag("ifAlias", "uplink").build();
        Metric after = ImmutableMetric.builder()
                .intrinsicTag("name", "m").intrinsicTag("resourceId", RID)
                .externalTag("nodeLabel", "new-name").externalTag("ifAlias", "uplink").build();
        assertThat(registry.observe(RID, before)).isTrue();
        assertThat(registry.observe(RID, after)).isFalse();
    }

    @Test
    void a_new_resource_is_due_at_once_and_then_only_after_the_cadence() {
        registry.observe(RID, interfaceMetric("uplink"));
        List<ResourceMetadata> due = registry.dueForEmission(CADENCE);
        assertThat(due).hasSize(1);
        assertThat(registry.dueForEmission(CADENCE)).as("still due until marked").hasSize(1);
        registry.markEmitted(due);
        assertThat(registry.dueForEmission(CADENCE)).isEmpty();
        clock.addAndGet(CADENCE - 1);
        assertThat(registry.dueForEmission(CADENCE)).isEmpty();
        clock.addAndGet(1);
        assertThat(registry.dueForEmission(CADENCE)).hasSize(1);
    }

    @Test
    void a_change_makes_a_resource_due_before_the_cadence() {
        registry.observe(RID, interfaceMetric("uplink"));
        registry.markEmitted(registry.dueForEmission(CADENCE));
        clock.addAndGet(60_000);
        registry.observe(RID, interfaceMetric("renamed"));
        List<ResourceMetadata> due = registry.dueForEmission(CADENCE);
        assertThat(due).hasSize(1);
        assertThat(due.get(0).attributes()).containsEntry("ifAlias", "renamed");
    }

    @Test
    void a_resource_unseen_for_a_day_is_forgotten() {
        registry.observe(RID, interfaceMetric("uplink"));
        registry.observe("node[1].nodeSnmp[]", ImmutableMetric.builder()
                .intrinsicTag("name", "sysUpTime").intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .externalTag("nodeLabel", "n1").build());
        clock.addAndGet(23 * HOUR);
        registry.observe(RID, interfaceMetric("uplink"));   // seen again, stays
        clock.addAndGet(2 * HOUR);
        assertThat(registry.expire(24 * HOUR)).isEqualTo(1);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void a_metric_without_metadata_registers_the_resource_with_nothing_to_say() {
        Metric bare = ImmutableMetric.builder()
                .intrinsicTag("name", "x").intrinsicTag("resourceId", "node[1].nodeSnmp[]").build();
        assertThat(registry.observe("node[1].nodeSnmp[]", bare)).isTrue();
        ResourceMetadata r = registry.dueForEmission(CADENCE).get(0);
        assertThat(r.attributes()).isEmpty();
        assertThat(r.categories()).isEmpty();
        assertThat(r.ifSpeedBps()).isNull();
    }

    @Test
    void attribute_values_are_kept_verbatim() {
        Metric m = ImmutableMetric.builder()
                .intrinsicTag("name", "x").intrinsicTag("resourceId", "r")
                .externalTag("ICMP/10.0.0.1", "latency ICMP/10.0.0.1")
                .externalTag("hrStorageDescr", "/var/lib/postgresql")
                .build();
        registry.observe("r", m);
        Map<String, String> a = registry.dueForEmission(CADENCE).get(0).attributes();
        assertThat(a).containsEntry("ICMP/10.0.0.1", "latency ICMP/10.0.0.1")
                     .containsEntry("hrStorageDescr", "/var/lib/postgresql");
    }
}
