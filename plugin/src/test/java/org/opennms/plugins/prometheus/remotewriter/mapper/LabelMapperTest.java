/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class LabelMapperTest {

    private static final LabelMapper DEFAULT_MAPPER = new LabelMapper(defaultConfig());

    // ---------- defaults ----------------------------------------------------

    @Test
    void emits_name_and_resource_id_from_intrinsic_tags() {
        Sample s = interfaceSample();
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out).isNotNull();
        assertThat(out.labels()).containsEntry("__name__", "ifHCInOctets");
        assertThat(out.labels()).containsEntry("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]");
    }

    @Test
    void emits_parsed_resource_components() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("resource_type", "interfaceSnmp");
        assertThat(out.labels()).containsEntry("resource_instance", "eth0");
    }

    @Test
    void node_uses_fs_qualified_identity_when_available() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("foreign_source", "NOC");
        assertThat(out.labels()).containsEntry("foreign_id", "router-42");
    }

    @Test
    void node_falls_back_to_parsed_id_when_no_foreign_source_is_present() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "node[42].interfaceSnmp[eth0]")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "42");
    }

    @Test
    void node_falls_back_to_node_id_tag_when_no_resource_id() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .externalTag("nodeId", "7")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "7");
    }

    @Test
    void unparseable_resource_id_emits_only_raw_label() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "not-a-resource-id")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("resourceId", "not-a-resource-id");
        assertThat(out.labels()).doesNotContainKey("resource_type");
        assertThat(out.labels()).doesNotContainKey("resource_instance");
    }

    @Test
    void node_label_and_location_emitted_when_present() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("node_label", "router-42.example.com");
        assertThat(out.labels()).containsEntry("location", "default");
    }

    @Test
    void if_name_is_a_label_and_the_other_interface_attributes_are_not() {
        // ifDescr, ifSpeed/ifHighSpeed and categories travel as metadata
        // series (onms_resource_attr, onms_resource_ifspeed,
        // onms_resource_category), never as labels on the data series.
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("if_name", "eth0");
        assertThat(out.labels().keySet())
                .doesNotContain("if_descr", "if_speed", "ifSpeed", "ifHighSpeed", "categories")
                .noneMatch(k -> k.startsWith("onms_cat_"));
    }

    // ---------- labels.if-speed-mode = raw ----------------------------------

    // ---------- labels.categories-mode --------------------------------------

    @Test
    void mtype_meta_tag_emits_mtype_label() {
        // mtype is load-bearing for OpenNMS late-aggregation. The OpenNMS
        // writer always sets MetaTagNames.mtype on every Sample handed to
        // store(); here we verify the value rides through to a Prometheus
        // label of the same name.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                .metaTag(MetaTagNames.mtype, "counter"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("mtype", "counter");
    }

    @Test
    void mtype_label_absent_when_source_meta_tag_missing() {
        // Non-OpenNMS test fixtures (and v0.3.x writes) have no mtype; the
        // mapper does not synthesize on the write side. The read-side
        // MtypeFallback covers the read direction.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "x")
                .intrinsicTag("resourceId", "node[1].nodeSnmp[]"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).doesNotContainKey("mtype");
    }

    @Test
    void mtype_marked_consumed_so_labels_include_star_does_not_double_emit() {
        // labels.include = * walks every source-tag and emits any not already
        // consumed by the default allowlist. mtype is in the consumed set, so
        // it must appear exactly once with value "gauge" — not also under a
        // snake-cased alias that duplicates the entry.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "x")
                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .metaTag(MetaTagNames.mtype, "gauge"));
        MappedSample out = new LabelMapper(c).map(s);
        assertThat(out.labels()).containsEntry("mtype", "gauge");
        long mtypeEntries = out.labels().keySet().stream()
                .filter(k -> k.equals("mtype"))
                .count();
        assertThat(mtypeEntries).isEqualTo(1);
    }

    @Test
    void mtype_label_value_passes_through_sanitizer_byte_cap() {
        // Pin the sanitizer call path by feeding an over-long value and
        // asserting it's truncated to the label-value byte cap. The Mtype
        // enum names (gauge, counter, count, rate, timestamp) all fit
        // trivially, but if a future refactor bypasses Sanitizer.labelValue
        // for the mtype emission, an over-long value would slip through to
        // the wire and Prometheus would reject the whole batch. Sanitizer
        // semantics: truncate at MAX_LABEL_VALUE_BYTES (codepoint-safe).
        String oversized = "a".repeat(Sanitizer.MAX_LABEL_VALUE_BYTES + 100);
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "x")
                .metaTag(MetaTagNames.mtype, oversized));
        MappedSample out = DEFAULT_MAPPER.map(s);
        String emitted = out.labels().get("mtype");
        assertThat(emitted.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .hasSizeLessThanOrEqualTo(Sanitizer.MAX_LABEL_VALUE_BYTES);
    }

    @Test
    void returns_null_when_metric_has_no_name() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                .externalTag("nodeId", "1"));
        assertThat(DEFAULT_MAPPER.map(s)).isNull();
    }

    @Test
    void metric_prefix_prepended_when_configured() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetricPrefix("onms_");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("__name__", "onms_ifHCInOctets");
    }

    @Test
    void timestamp_and_value_round_trip_from_sample() {
        Instant when = Instant.ofEpochMilli(1_742_000_000L);
        Sample s = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", "foo")
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .build())
                .time(when)
                .value(3.14)
                .build();
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.timestampMs()).isEqualTo(when.toEpochMilli());
        assertThat(out.value()).isEqualTo(3.14);
    }

    // ---------- onms_instance_id --------------------------------------------

    @Test
    void onms_instance_id_is_emitted_when_instance_id_is_set() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("onms_instance_id", "opennms-us-east");
    }

    @Test
    void onms_instance_id_is_absent_when_instance_id_is_unset() {
        // The default fixture config doesn't set instance.id.
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
        // Other defaults still emitted.
        assertThat(out.labels()).containsKey("__name__");
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void onms_instance_id_is_absent_when_instance_id_is_whitespace() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("   ");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_honors_labels_exclude() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        c.setLabelsExclude("onms_instance_id");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_honors_labels_rename() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        c.setLabelsRename("onms_instance_id -> cluster");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("cluster", "opennms-us-east");
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_is_emitted_first_in_iteration_order() {
        // Design decision §3 pins emission position: onms_instance_id goes in
        // first. Iteration order of the emitted Map is load-bearing for the
        // exclude/include/rename passes downstream, so guard against a future
        // refactor that silently moves the put() call.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        List<String> keys = new ArrayList<>(out.labels().keySet());
        assertThat(keys.get(0)).isEqualTo("onms_instance_id");
        assertThat(keys.get(1)).isEqualTo("__name__");
    }

    @Test
    void onms_instance_id_value_is_sanitized_to_label_value_rules() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        // Sanitizer.labelValue truncates values exceeding 2048 bytes;
        // we just assert the label is emitted with the literal value
        // for typical operator-supplied identifiers.
        c.setInstanceId("opennms-us-east-1");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("onms_instance_id", "opennms-us-east-1");
    }

    // ---------- override globs ----------------------------------------------

    @Test
    void exclude_glob_removes_default_label() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("node_label");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("node_label");
        // Other defaults still present.
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void exclude_glob_with_wildcard_removes_matching_labels() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("foreign_*");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("foreign_source");
        assertThat(out.labels()).doesNotContainKey("foreign_id");
    }

    @Test
    void include_glob_surfaces_non_default_tag_snake_cased() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("ifAlias");
        Sample s = interfaceSampleWith("ifAlias", "uplink-to-core");
        MappedSample out = new LabelMapper(c).map(s);
        // Matches the default allowlist's convention (ifName -> if_name).
        assertThat(out.labels()).containsEntry("if_alias", "uplink-to-core");
    }

    @Test
    void include_glob_does_not_overwrite_default_label() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        // 'ifName' is a source tag key AND the default emits 'if_name'.
        // Include match on 'ifName' must not clobber the default value.
        c.setLabelsInclude("ifName");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        // Default label from ifName source.
        assertThat(out.labels()).containsEntry("if_name", "eth0");
    }

    // ---------- metadata passthrough (integration with LabelMapper) ---------

    @Test
    void metadata_is_emitted_alongside_defaults_when_enabled() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetadataEnabled(true);
        c.setMetadataInclude("requisition:*");
        Sample s = interfaceSampleWith("requisition:location", "Pittsboro");
        MappedSample out = new LabelMapper(c).map(s);
        assertThat(out.labels()).containsEntry("onms_meta_requisition_location", "Pittsboro");
        // Defaults still present.
        assertThat(out.labels()).containsKey("__name__");
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void metadata_denylist_blocks_reflect_in_counter() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetadataEnabled(true);
        c.setMetadataInclude("requisition:*");
        LabelMapper mapper = new LabelMapper(c);
        Sample s = interfaceSampleWith("requisition:snmp-community", "public");
        MappedSample out = mapper.map(s);
        assertThat(out.labels()).doesNotContainKey("onms_meta_requisition_snmp_community");
        assertThat(mapper.getMetadataDenylistBlockedCount()).isEqualTo(1);
    }

    @Test
    void rename_applies_after_include_and_exclude() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsRename("node_label->hostname, location->region");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("hostname", "router-42.example.com");
        assertThat(out.labels()).containsEntry("region", "default");
        assertThat(out.labels()).doesNotContainKey("node_label");
        assertThat(out.labels()).doesNotContainKey("location");
    }

    // ---------- consumed-keys dedup (v0.2) ----------------------------------

    @Test
    void labels_include_star_does_not_re_emit_consumed_source_keys() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        // Five duplicates v0.1 produced under `labels.include = *` are gone:
        // the source-key snake-cased forms that did NOT collide with a default
        // label name, and therefore slipped past v0.1's putIfAbsent dedup.
        assertThat(out.labels()).doesNotContainKey("name");
        assertThat(out.labels()).doesNotContainKey("resource_id");
        assertThat(out.labels()).doesNotContainKey("if_high_speed");
        assertThat(out.labels()).doesNotContainKey("node_id");
        assertThat(out.labels()).doesNotContainKey("categories");
    }

    @Test
    void labels_include_star_preserves_default_allowlist_labels() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels()).containsEntry("__name__", "ifHCInOctets");
        assertThat(out.labels()).containsKey("resourceId");
        assertThat(out.labels()).containsKey("node");
        assertThat(out.labels()).containsEntry("foreign_source", "NOC");
        assertThat(out.labels()).containsEntry("foreign_id", "router-42");
        assertThat(out.labels()).containsEntry("node_label", "router-42.example.com");
        assertThat(out.labels()).containsEntry("location", "default");
        assertThat(out.labels()).containsEntry("if_name", "eth0");
        // Consumed but not labels: the include glob must not surface them.
        assertThat(out.labels().keySet())
                .doesNotContain("if_descr", "if_speed", "if_high_speed", "categories")
                .noneMatch(k -> k.startsWith("onms_cat_"));
    }

    @Test
    void labels_include_star_does_not_introduce_collide_on_name_duplicates() {
        // Regression: v0.1's putIfAbsent already blocked these 7 labels from
        // being duplicated under `labels.include = *` because their
        // snake-cased source-key forms collide with a default label name.
        // The consumed-keys mechanism must preserve that single-value property.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels().get("foreign_source")).isEqualTo("NOC");
        assertThat(out.labels().get("foreign_id")).isEqualTo("router-42");
        assertThat(out.labels().get("node_label")).isEqualTo("router-42.example.com");
        assertThat(out.labels().get("location")).isEqualTo("default");
        assertThat(out.labels().get("if_name")).isEqualTo("eth0");
    }

    @Test
    void labels_include_narrow_globs_surface_non_default_source_tags() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("sys*, asset*");
        ImmutableMetric.MetricBuilder mb = fullFixtureBuilder()
                .externalTag("sysDescription", "Linux 5.15")
                .externalTag("assetRegion", "us-east");
        MappedSample out = new LabelMapper(c).map(sample(mb));
        assertThat(out.labels()).containsEntry("sys_description", "Linux 5.15");
        assertThat(out.labels()).containsEntry("asset_region", "us-east");
    }

    @Test
    void rename_of_default_plus_include_star_does_not_re_emit_source_key() {
        // In v0.1 this pair produced both `foreign_source_raw` (renamed
        // default) and `foreign_source` (re-surfaced from the `foreignSource`
        // source tag via labels.include = *). Under consumed-keys the source
        // key is skipped, so only `foreign_source_raw` remains.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsRename("foreign_source -> foreign_source_raw");
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels()).containsEntry("foreign_source_raw", "NOC");
        assertThat(out.labels()).doesNotContainKey("foreign_source");
    }

    // ---------- labels.copy --------------------------------------------------

    @Test
    void copy_emits_source_under_additional_name() {
        // Targets `cluster` (non-default, non-reserved) so the assertion
        // genuinely exercises the copy stage. Using `instance` here would
        // pass even if copy were broken — `instance` is a v0.4 default.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsCopy("node -> cluster");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("cluster", "NOC:router-42");
    }

    @Test
    void copy_multiple_targets_from_same_source_emits_all() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsCopy("node -> cluster, node -> host");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("cluster", "NOC:router-42");
        assertThat(out.labels()).containsEntry("host", "NOC:router-42");
    }

    @Test
    void copy_is_one_pass_and_does_not_chain() {
        // `copy = node -> a, a -> b` must NOT produce `b` — the second
        // directive's source `a` did not exist at copy-stage entry.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsCopy("node -> a, a -> b");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("a", "NOC:router-42");
        assertThat(out.labels()).doesNotContainKey("b");
    }

    @Test
    void copy_composes_with_rename_on_same_source() {
        // `copy = node -> cluster, rename = node -> host` — copy runs first
        // on pre-rename `node`, then rename moves `node` to `host`. Final:
        // `host` (from rename) + `cluster` (from copy), no `node`.
        // `cluster` (not `instance`) so the copy stage is actually exercised;
        // `instance` is a v0.4 default emission.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsCopy("node -> cluster");
        c.setLabelsRename("node -> host");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("host", "NOC:router-42");
        assertThat(out.labels()).containsEntry("cluster", "NOC:router-42");
        assertThat(out.labels()).doesNotContainKey("node");
    }

    @Test
    void copy_sees_labels_surfaced_by_include() {
        // `include = ifAlias` creates `if_alias`; copy = if_alias -> port_description
        // must then produce both.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("ifAlias");
        c.setLabelsCopy("if_alias -> port_description");
        Sample s = interfaceSampleWith("ifAlias", "uplink-to-core");
        MappedSample out = new LabelMapper(c).map(s);
        assertThat(out.labels()).containsEntry("if_alias", "uplink-to-core");
        assertThat(out.labels()).containsEntry("port_description", "uplink-to-core");
    }

    @Test
    void copy_does_not_resurrect_excluded_label() {
        // `exclude = foreign_source, copy = foreign_source -> my_fs`:
        // foreign_source is gone before copy runs, so my_fs is not created
        // either. The scenario in the delta spec requires exactly one startup
        // WARN naming the excluded label as an unknown copy source.
        //
        // NOTE: the original test used `node -> instance` for this, but v0.4
        // now emits `instance` as a default label (mirror of node), so that
        // pair no longer demonstrates the "copy can't resurrect excluded"
        // invariant — `instance` is present regardless of copy. We pick a
        // different source whose target is not a default emission.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("foreign_source");
        c.setLabelsCopy("foreign_source -> my_fs");
        LabelMapper mapper = new LabelMapper(c);
        MappedSample out = mapper.map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("foreign_source");
        assertThat(out.labels()).doesNotContainKey("my_fs");
        assertThat(mapper.warnedUnknownCopySourcesForTesting()).containsExactly("foreign_source");
    }

    @Test
    void copy_unknown_source_is_noop_and_warns_exactly_once() {
        // `copy = nonexistent -> fooo` — source never appears. Must not fail,
        // must not emit `fooo`. The WARN fires exactly once for the missing
        // source, regardless of how many samples flow through.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsCopy("nonexistent -> fooo");
        LabelMapper mapper = new LabelMapper(c);

        MappedSample out1 = mapper.map(interfaceSample());
        assertThat(out1.labels()).doesNotContainKey("fooo");
        assertThat(mapper.warnedUnknownCopySourcesForTesting()).containsExactly("nonexistent");

        // Second call must also not throw and must produce no `fooo`; gate
        // already flipped, no additional sources recorded.
        MappedSample out2 = mapper.map(interfaceSample());
        assertThat(out2.labels()).doesNotContainKey("fooo");
        assertThat(mapper.warnedUnknownCopySourcesForTesting()).containsExactly("nonexistent");
    }

    @Test
    void copy_target_clobbering_include_surfaced_label_warns_once() {
        // labels.include = customTag surfaces `custom_tag` from a source tag.
        // labels.copy = node -> custom_tag then overwrites it. Startup
        // validation cannot catch this (include globs resolve dynamically);
        // runtime WARN records the clobber once per mapper instance.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("customTag");
        c.setLabelsCopy("node -> custom_tag");
        Sample s = interfaceSampleWith("customTag", "include-value");
        LabelMapper mapper = new LabelMapper(c);

        MappedSample out = mapper.map(s);
        // Copy wins over include at runtime (same key, copy's put overwrites).
        assertThat(out.labels()).containsEntry("custom_tag", "NOC:router-42");
        assertThat(mapper.warnedCopyTargetClobbersForTesting()).containsExactly("custom_tag");

        // Second sample: clobber still happens, but no additional WARN.
        mapper.map(s);
        assertThat(mapper.warnedCopyTargetClobbersForTesting()).containsExactly("custom_tag");
    }

    @Test
    void copy_input_map_is_read_only_during_stage() {
        // The hardest test of the one-pass-reads-from-input invariant: the
        // second copy directive's source is a label that was CLOBBERED by the
        // first directive's target. If applyCopy were reading from its own
        // `out` map (the naive chained-copy), the second directive would see
        // the clobbered value. Reading from the input `labels` map, it sees
        // the original include-surfaced value.
        //
        // Test setup depends on `sourceX` NOT being consumed by the default
        // allowlist — so the pre-assertion below confirms the include pass
        // actually surfaces `source_x` before we layer copy on top. If a
        // future commit adds `sourceX` to buildDefaults' consumed-keys set,
        // this test fails here rather than silently passing for the wrong
        // reason below.

        // ---- Pre-assert: include-only baseline ----
        PrometheusRemoteWriterConfig baselineCfg = defaultConfig();
        baselineCfg.setLabelsInclude("sourceX");
        Sample s = interfaceSampleWith("sourceX", "include-value");
        MappedSample baseline = new LabelMapper(baselineCfg).map(s);
        assertThat(baseline.labels())
                .as("`sourceX` must not be consumed by defaults; include must surface `source_x`")
                .containsEntry("source_x", "include-value");

        // ---- Main scenario ----
        // Config: labels.include surfaces source tag `sourceX` as label
        // `source_x="include-value"`. labels.copy then runs two directives:
        //   1. node -> source_x (clobbers the include-surfaced label)
        //   2. source_x -> new_target (must see the ORIGINAL include-value,
        //      not the clobbered node value)
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("sourceX");
        c.setLabelsCopy("node -> source_x, source_x -> new_target");
        LabelMapper mapper = new LabelMapper(c);

        MappedSample out = mapper.map(s);

        // First directive: source_x is the clobber target and ends up with
        // node's value (the documented clobber behavior).
        assertThat(out.labels()).containsEntry("source_x", "NOC:router-42");
        // Second directive: source_x as source reads from the ORIGINAL
        // labels map, not from the partial `out` — so new_target holds
        // the include-surfaced value. This is the primary invariant under
        // test.
        assertThat(out.labels()).containsEntry("new_target", "include-value");
        // Node is still present (not renamed).
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        // Clobber WARN behavior (secondary, but worth pinning alongside):
        // `source_x` was clobbered by the first directive; WARN fires
        // exactly once for it. Other warns (e.g. unknown sources) stay clear.
        assertThat(mapper.warnedCopyTargetClobbersForTesting()).contains("source_x");
        assertThat(mapper.warnedUnknownCopySourcesForTesting()).isEmpty();
    }

    @Test
    void copy_source_with_empty_value_is_treated_as_absent() {
        // An include-surfaced source tag with an empty value would, without
        // this guard, be copied as-is — and Prometheus treats empty-valued
        // labels as absent, so the operator's intent is almost certainly
        // satisfied by skipping the copy. Same WARN path as a missing source.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("emptyTag");
        c.setLabelsCopy("empty_tag -> mirror");
        Sample s = interfaceSampleWith("emptyTag", "");
        LabelMapper mapper = new LabelMapper(c);

        MappedSample out = mapper.map(s);
        assertThat(out.labels()).doesNotContainKey("mirror");
        assertThat(mapper.warnedUnknownCopySourcesForTesting()).containsExactly("empty_tag");
    }

    // ---------- job label derivation (v0.4) ---------------------------------

    @Test
    void job_bracketed_resource_id_derives_to_snmp() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("job", "snmp");
    }

    @Test
    void job_slash_db_resource_id_derives_to_snmp() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/42/hrStorageIndex/1"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "snmp");
    }

    @Test
    void job_slash_fs_jmx_minion_derives_to_jmx() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/fs/selfmonitor/1/jmx-minion/OpenNMS_Name_Notifd"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "jmx");
    }

    @Test
    void job_slash_fs_opennms_jvm_derives_to_jmx() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/fs/selfmonitor/1/opennms-jvm/Heap"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "jmx");
    }

    @Test
    void job_slash_fs_other_group_derives_to_snmp() {
        // Fixture matches the delta-spec scenario literal (hrStorage, not
        // hrStorageIndex). Both resolve to a non-jmx / non-opennms-jvm
        // group, so both derive to "snmp" — alignment is for spec fidelity
        // rather than behavior.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/fs/prod/server-01/hrStorage/1"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "snmp");
    }

    @Test
    void job_unparseable_resource_id_derives_to_opennms() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "not-a-valid-resource-id"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "opennms");
    }

    @Test
    void job_absent_resource_id_derives_to_opennms() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .externalTag("nodeId", "1"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("job", "opennms");
    }

    @Test
    void job_name_override_replaces_derivation() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setJobName("my-opennms-fleet");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        // Would otherwise derive to "snmp" from the bracketed resourceId.
        assertThat(out.labels()).containsEntry("job", "my-opennms-fleet");
    }

    @Test
    void job_name_override_replaces_opennms_catchall() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setJobName("my-fleet");
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage"));
        MappedSample out = new LabelMapper(c).map(s);
        // Would otherwise fall back to "opennms"; operator override wins.
        assertThat(out.labels()).containsEntry("job", "my-fleet");
    }

    @Test
    void job_blank_override_uses_derivation() {
        // setJobName with blank normalises to null; derivation applies.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setJobName("   ");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("job", "snmp");
    }

    // ---------- samples_unparseable_resource_id counter (v0.5) ---------------

    @Test
    void defaults_flags_unparseable_resource_id() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "foo");
        tags.put(IntrinsicTagNames.resourceId, "not-a-valid-shape");
        LabelMapper.Defaults d = LabelMapper.buildDefaults("foo", tags, null, null);
        assertThat(d.resourceIdWasUnparseable()).isTrue();
    }

    @Test
    void defaults_flags_missing_resource_id_as_unparseable() {
        // No resourceId tag at all — same catch-all branch in deriveJob, same
        // flag for the counter.
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "foo");
        LabelMapper.Defaults d = LabelMapper.buildDefaults("foo", tags, null, null);
        assertThat(d.resourceIdWasUnparseable()).isTrue();
    }

    @Test
    void defaults_does_not_flag_bracketed_resource_id() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "ifHCInOctets");
        tags.put(IntrinsicTagNames.resourceId, "nodeSource[NOC:router-42].interfaceSnmp[eth0]");
        LabelMapper.Defaults d = LabelMapper.buildDefaults("ifHCInOctets", tags, null, null);
        assertThat(d.resourceIdWasUnparseable()).isFalse();
    }

    @Test
    void defaults_does_not_flag_slash_db_resource_id() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "foo");
        tags.put(IntrinsicTagNames.resourceId, "snmp/42/hrStorageIndex/1");
        LabelMapper.Defaults d = LabelMapper.buildDefaults("foo", tags, null, null);
        assertThat(d.resourceIdWasUnparseable()).isFalse();
    }

    @Test
    void defaults_does_not_flag_slash_fs_resource_id() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "foo");
        tags.put(IntrinsicTagNames.resourceId, "snmp/fs/selfmonitor/1/jmx-minion/Heap");
        LabelMapper.Defaults d = LabelMapper.buildDefaults("foo", tags, null, null);
        assertThat(d.resourceIdWasUnparseable()).isFalse();
    }

    @Test
    void counter_increments_on_unparseable_resource_id_at_map_time() {
        PluginMetrics metrics = new PluginMetrics();
        LabelMapper mapper = new LabelMapper(defaultConfig(), metrics);

        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isZero();

        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage"));
        mapper.map(s);
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isEqualTo(1);

        // A parseable sample must NOT bump the counter.
        mapper.map(interfaceSample());
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isEqualTo(1);

        // Another unparseable sample continues to bump.
        mapper.map(s);
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isEqualTo(2);
    }

    @Test
    void counter_increments_for_sample_with_no_resource_id_at_all() {
        // Delta spec scenario: "Counter increments for samples with no
        // resourceId tag at all". Distinct from "unparseable resourceId" —
        // the flag is set the same way (parsed == null) but the scenario
        // deserves its own end-to-end bump-the-counter test so a future
        // refactor that treats absent-tag differently from failed-parse
        // surfaces as a named failure.
        PluginMetrics metrics = new PluginMetrics();
        LabelMapper mapper = new LabelMapper(defaultConfig(), metrics);

        Sample s = sample(ImmutableMetric.builder().intrinsicTag("name", "foo"));
        mapper.map(s);

        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isEqualTo(1);
    }

    @Test
    void counter_increments_even_when_job_name_override_is_set() {
        // Counter tracks parser fallthrough rate — independent of whether the
        // derivation's "opennms" fallback value is actually surfaced as the
        // `job` label. Operators want the raw fallthrough signal even when
        // they've overridden `job.name`.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setJobName("my-fleet");
        PluginMetrics metrics = new PluginMetrics();
        LabelMapper mapper = new LabelMapper(c, metrics);

        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage"));
        MappedSample out = mapper.map(s);

        // Operator's override wins for the label value.
        assertThat(out.labels()).containsEntry("job", "my-fleet");
        // But the counter still increments — it's the parser-fallthrough
        // signal, not the surfaced-value signal.
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_UNPARSEABLE_RESOURCE_ID).longValue())
                .isEqualTo(1);
    }

    // ---------- labels.exclude honors job and instance (v0.4) ---------------

    @Test
    void labels_exclude_removes_default_job_and_instance() {
        // Delta spec scenario "`job` and `instance` honor `labels.exclude`".
        // Operator opts out of the two v0.4 defaults; all other default
        // labels continue to emit.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("job, instance");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("job");
        assertThat(out.labels()).doesNotContainKey("instance");
        // Other defaults still present.
        assertThat(out.labels()).containsKey("__name__");
        assertThat(out.labels()).containsKey("node");
        assertThat(out.labels()).containsKey("foreign_source");
    }

    @Test
    void job_name_with_sanitizable_characters_passes_through_sanitizer() {
        // Sanitizer.labelValue accepts non-label-grammar characters as-is
        // (label VALUES are less restricted than label NAMES; only
        // byte-length and UTF-8-codepoint rules apply). So `job.name` with
        // spaces, punctuation, etc. round-trips unchanged — no silent
        // mangling of operator input.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setJobName("ops team / production");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("job", "ops team / production");
    }

    // ---------- instance mirrors node (v0.4) --------------------------------

    @Test
    void instance_mirrors_node_for_fs_qualified_identity() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("instance", "NOC:router-42");
    }

    @Test
    void instance_mirrors_node_for_parsed_slash_fs_identity() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "jvm_memory")
                .intrinsicTag("resourceId", "snmp/fs/selfmonitor/1/jmx-minion/OpenNMS_Name_Notifd"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "selfmonitor:1");
        assertThat(out.labels()).containsEntry("instance", "selfmonitor:1");
    }

    @Test
    void instance_mirrors_node_for_numeric_db_id_fallback() {
        // Fixture matches the delta-spec scenario value ("42") and its
        // condition (unparseable resourceId + external nodeId).
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage-not-a-resource-id")
                .externalTag("nodeId", "42"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "42");
        assertThat(out.labels()).containsEntry("instance", "42");
    }

    @Test
    void instance_absent_when_no_identity_source() {
        // No FS tags, unparseable resourceId, no nodeId tag — neither `node`
        // nor `instance` should be emitted.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).doesNotContainKey("node");
        assertThat(out.labels()).doesNotContainKey("instance");
    }

    // ---------- node-label precedence (slash-path) --------------------------

    @Test
    void slash_fs_resource_id_alone_emits_node_and_parsed_components() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "jvm_memory_used_bytes")
                .intrinsicTag("resourceId", "snmp/fs/selfmonitor/1/jmx-minion/java.lang_type_Memory"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "selfmonitor:1");
        assertThat(out.labels()).containsEntry("resource_type", "jmx-minion");
        assertThat(out.labels()).containsEntry("resource_instance", "java.lang_type_Memory");
    }

    @Test
    void external_fs_tags_win_over_parsed_slash_fs_resource_id() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "jvm_metric")
                .intrinsicTag("resourceId", "snmp/fs/other-fs/other-fid/jmx-minion/OpenNMS_Name_Notifd")
                .externalTag("foreignSource", "real-fs")
                .externalTag("foreignId", "real-fid"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "real-fs:real-fid");
        // Parsed resource components still come from the resourceId.
        assertThat(out.labels()).containsEntry("resource_type", "jmx-minion");
        assertThat(out.labels()).containsEntry("resource_instance", "OpenNMS_Name_Notifd");
    }

    @Test
    void parsed_slash_fs_wins_over_nonexistent_slash_db_context() {
        // Sample has only a slash-FS resourceId, no external FS tags, no nodeId.
        // Precedence: parsed slash-FS provides `node=<fs>:<fid>`, not falling
        // through to external `nodeId` (which isn't present either).
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/fs/selfmonitor/1/grp/inst"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "selfmonitor:1");
    }

    @Test
    void parsed_slash_db_wins_over_external_node_id_tag() {
        // Slash-DB resourceId and an unrelated external nodeId — parser wins
        // because the resourceId is the authoritative identity source when
        // FS tags are absent.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "snmp/42/grp/inst")
                .externalTag("nodeId", "99"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "42");
    }

    @Test
    void node_uses_external_nodeId_when_no_fs_and_unparseable_resourceId() {
        // True third-level fall-through: no FS tags, resourceId is unparseable
        // (so `parsed == null`), external `nodeId` tag must win. Guards
        // against a refactor that accidentally returns early when the parser
        // misses.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "garbage-not-a-resource-id")
                .externalTag("nodeId", "99"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "99");
    }

    // ---------- drift guard -------------------------------------------------

    @Test
    void consumed_keys_covers_all_buildDefaults_source_reads() {
        // Given a fixture carrying every source key buildDefaults currently
        // reads, the returned consumedSourceKeys set must equal that set
        // exactly. Reviewers adding a new source-key read to buildDefaults
        // must update this test — that's the point.
        //
        // Use IntrinsicTagNames constants for the two intrinsic keys so that
        // an upstream IAPI rename (e.g. `name` → `__name__`) would surface
        // as a test failure rather than silently bypassing the consumed-keys
        // dedup in production.
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(IntrinsicTagNames.name, "ifHCInOctets");
        tags.put(IntrinsicTagNames.resourceId, "nodeSource[NOC:router-42].interfaceSnmp[eth0]");
        tags.put("foreignSource", "NOC");
        tags.put("foreignId", "router-42");
        tags.put("nodeLabel", "router-42.example.com");
        tags.put("location", "default");
        tags.put("ifName", "eth0");
        tags.put("ifDescr", "GigabitEthernet0/0");
        tags.put("ifHighSpeed", "1000");
        tags.put("ifSpeed", "4294967295");
        tags.put("nodeId", "42");
        tags.put("categories", "Routers, ProductionSites");

        LabelMapper.Defaults defaults = LabelMapper.buildDefaults("ifHCInOctets", tags, null, null);

        assertThat(defaults.consumedSourceKeys()).containsExactlyInAnyOrder(
                IntrinsicTagNames.name,
                IntrinsicTagNames.resourceId,
                "foreignSource",
                "foreignId",
                "nodeLabel",
                "location",
                "ifName",
                "ifDescr",
                "ifHighSpeed",
                "ifSpeed",
                "nodeId",
                "categories",
                MetaTagNames.mtype);
    }

    // ---------- ${name} resource-label substitution round-trip --------------
    // OpenNMS substitutes ${name} / ${datname} / ${spcname} placeholders in
    // resource-type labels from string attributes carried as Sample meta tags.
    // For substitution to work, the source key/value must survive the
    // round-trip Sample → Prom label → Metric.metaTags. The plugin emits
    // them as `onms_attr_<key>` so collisions with intrinsic tag keys
    // (notably `name`) don't drop the meta value.

    // ---------- onms_extattr_ — external-partition round-trip --------------
    // OpenNMS-core's TimeseriesPersistOperationBuilder attaches resource
    // string attributes (the values ${name} / ${datname} / ${spcname}
    // resolve against) to Metric.getExternalTags(). The plugin emits them
    // under onms_extattr_<key> so the read side can deposit them on the
    // external partition where TimeseriesResourceStorageDao.getStringAttributes()
    // actually looks for placeholder substitution.

    // ---------- fixtures ----------------------------------------------------

    /** A well-populated interface sample: FS-qualified node, 2 categories, 1 Gbps. */
    private static Sample interfaceSample() {
        return interfaceSampleWith(null, null);
    }

    private static Sample interfaceSampleWith(String extraKey, String extraValue) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]")
                .externalTag("nodeLabel", "router-42.example.com")
                .externalTag("foreignSource", "NOC")
                .externalTag("foreignId", "router-42")
                .externalTag("location", "default")
                .externalTag("ifName", "eth0")
                .externalTag("ifDescr", "GigabitEthernet0/0")
                .externalTag("ifHighSpeed", "1000")
                .externalTag("ifSpeed", "4294967295")
                .externalTag("categories", "ProductionSites,Routers"); // OpenNMS pre-sorts; comma, no space
        if (extraKey != null) {
            mb.externalTag(extraKey, extraValue);
        }
        return sample(mb);
    }

    /** Fixture carrying every source key buildDefaults consults, including
     *  `nodeId` (redundant with FS-qualified identity but exercised by the
     *  consumed-keys dedup tests). */
    private static Sample fullFixtureSample() {
        return sample(fullFixtureBuilder());
    }

    private static ImmutableMetric.MetricBuilder fullFixtureBuilder() {
        return ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]")
                .externalTag("nodeLabel", "router-42.example.com")
                .externalTag("foreignSource", "NOC")
                .externalTag("foreignId", "router-42")
                .externalTag("location", "default")
                .externalTag("ifName", "eth0")
                .externalTag("ifDescr", "GigabitEthernet0/0")
                .externalTag("ifHighSpeed", "1000")
                .externalTag("ifSpeed", "4294967295")
                .externalTag("nodeId", "42")
                .externalTag("categories", "ProductionSites,Routers"); // OpenNMS pre-sorts; comma, no space
    }

    private static Sample sample(ImmutableMetric.MetricBuilder metricBuilder) {
        return sample(metricBuilder.build());
    }

    private static Sample sample(org.opennms.integration.api.v1.timeseries.Metric m) {
        return ImmutableSample.builder()
                .metric(m)
                .time(Instant.ofEpochMilli(1_000_000L))
                .value(42.0)
                .build();
    }

    // ---------- labels.profile / labels.attr-mode (v0.5 defaults) -----------

    @Test
    void default_config_emits_no_attr_or_extattr_labels() {
        // A sample carrying meta AND external string attributes emits neither
        // v0.x prefix: attributes are metadata rows, and the data series keep
        // the bounded schema (issue #112).
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "events_processed")
                .intrinsicTag("resourceId", "node[1].eventdProcessingStat[Logger]")
                .metaTag("name", "Eventd_Logger_Receiver")
                .metaTag("custom", "from_meta")
                .externalTag("name", "eventlogs.process")
                .externalTag("datname", "customers_db"));
        MappedSample out = new LabelMapper(defaultConfig()).map(s);
        assertThat(out.labels().keySet())
                .noneMatch(k -> k.startsWith("onms_attr_"))
                .noneMatch(k -> k.startsWith("onms_extattr_"));
    }

    @Test
    void default_config_metatag_aliases_emit_once_under_the_builtin_name() {
        // Spec "Metatag aliases of built-in labels emit once": nodeLabel /
        // ifName arrive as meta tags; only the snake_case built-ins appear.
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "snmp/fs/Servers/router1/eth0/mib2-interfaces")
                .metaTag("nodeLabel", "router1")
                .metaTag("ifName", "eth0"));
        MappedSample out = new LabelMapper(defaultConfig()).map(s);
        assertThat(out.labels()).containsEntry("node_label", "router1");
        assertThat(out.labels()).containsEntry("if_name", "eth0");
        assertThat(out.labels()).doesNotContainKeys(
                "nodeLabel", "ifName", "onms_attr_nodeLabel", "onms_attr_ifName");
    }

    @Test
    void native_profile_label_names_stay_within_the_bounded_schema() {
        // Spec "Bounded label-name set": whatever the sample carries, the
        // native default emits only names from the fixed schema — the
        // property that keeps the backend's label-name index flat (#112).
        java.util.Set<String> allowed = java.util.Set.of(
                "__name__", "resourceId", "mtype", "node", "resource_type",
                "resource_instance", "job", "instance", "onms_instance_id",
                "node_label", "location", "foreign_source", "foreign_id",
                "if_name", "geohash");
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "snmp/fs/Servers/router1/eth0/mib2-interfaces")
                .metaTag("nodeLabel", "router1")
                .metaTag("foreignSource", "Servers")
                .metaTag("foreignId", "router1")
                .metaTag("location", "Default")
                .metaTag("ifName", "eth0")
                .metaTag("ifDescr", "GigabitEthernet0/0")
                .metaTag("ifSpeed", "1000000000")
                .metaTag("ifHighSpeed", "1000")
                .metaTag("mtype", "counter")
                .metaTag("arbitraryAttribute", "should_not_leak")
                .externalTag("ICMP/10.42.0.1", "should_not_leak"));
        MappedSample out = new LabelMapper(defaultConfig()).map(s);
        assertThat(out.labels().keySet()).allMatch(allowed::contains);
    }

    private static PrometheusRemoteWriterConfig defaultConfig() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
