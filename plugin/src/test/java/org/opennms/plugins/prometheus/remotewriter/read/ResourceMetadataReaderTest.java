/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;

/**
 * The read path restores a resource's attributes and categories from the
 * metadata rows, so OpenNMS graph placeholders resolve from what is on the
 * wire and nothing has to be curated for correctness (#189).
 *
 * <p>One instant query per batch of resources, never one per metric; the
 * selector always carries the resource list.
 */
class ResourceMetadataReaderTest {

    private final List<String> urls = new ArrayList<>();
    private final List<String> forms = new ArrayList<>();
    private static final String EMPTY = "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";
    private String response = EMPTY;
    /** What the speed query (the second per batch) gets back. */
    private String speedResponse = EMPTY;

    private PrometheusRemoteWriterConfig config(long cadenceMs, int batchSize) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("http://backend/api/v1/write");
        c.setReadUrl("http://backend");
        c.setMetadataCadenceMs(cadenceMs);
        c.setDiscoveryBatchSize(batchSize);
        return c;
    }

    private ResourceMetadataReader reader(long cadenceMs, int batchSize) {
        return new ResourceMetadataReader(config(cadenceMs, batchSize), this::record);
    }

    private String record(String url, String form) {
        urls.add(url);
        forms.add(form);
        return form.contains("onms_resource_ifspeed") ? speedResponse : response;
    }

    /** A gauge sample as {@code last_over_time(onms_resource_ifspeed[w])} returns it. */
    private static String speed(String resourceId, String bps) {
        return "{\"metric\":{\"resourceId\":\"" + resourceId + "\"},\"value\":[1700000100,\"" + bps + "\"]}";
    }

    private static Metric metric(String name, String resourceId) {
        return ImmutableMetric.builder()
                .intrinsicTag("name", name)
                .intrinsicTag("resourceId", resourceId)
                .metaTag("mtype", "counter")
                .build();
    }

    /** A row as {@code timestamp(...) or max_over_time(...)} returns it: no
     *  {@code __name__} (a function result drops it), and the value is the
     *  row's newest sample timestamp in seconds. */
    private static String row(String metricName, String resourceId, String... kv) {
        return rowAt(1_700_000_000.000, metricName, resourceId, kv);
    }

    private static String rowAt(double stampSec, String metricName, String resourceId, String... kv) {
        StringBuilder b = new StringBuilder("{\"metric\":{\"resourceId\":\"").append(resourceId).append('"');
        for (int i = 0; i < kv.length; i += 2) b.append(",\"").append(kv[i]).append("\":\"").append(kv[i + 1]).append('"');
        return b.append("},\"value\":[1700000100,\"").append(stampSec).append("\"]}").toString();
    }

    private static String vector(String... rows) {
        return "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[" + String.join(",", rows) + "]}}";
    }

    private static Optional<String> external(Metric m, String key) {
        return m.getExternalTags().stream().filter(t -> t.getKey().equals(key)).map(Tag::getValue).findFirst();
    }

    private static Optional<String> meta(Metric m, String key) {
        return m.getMetaTags().stream().filter(t -> t.getKey().equals(key)).map(Tag::getValue).findFirst();
    }

    private static String decoded(String form) {
        return URLDecoder.decode(form, StandardCharsets.UTF_8);
    }

    @Test
    void rows_become_external_tags_and_categories_a_meta_tag() throws StorageException {
        String rid = "nodeSource[NOC:r1].interfaceSnmp[eth0]";
        response = vector(
                row("onms_resource_attr", rid, "key", "ifAlias", "value", "uplink to core"),
                row("onms_resource_attr", rid, "key", "ifName", "value", "eth0"),
                row("onms_resource_category", rid, "category", "Routers"),
                row("onms_resource_category", rid, "category", "Production"));
        List<Metric> out = reader(900_000, 50).enrich(List.of(metric("ifHCInOctets", rid)));

        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(external(m, "ifAlias")).contains("uplink to core");
        assertThat(external(m, "ifName")).contains("eth0");
        assertThat(meta(m, "categories")).contains("Production,Routers");
        // What was there stays there.
        assertThat(meta(m, "mtype")).contains("counter");
        assertThat(m.getIntrinsicTags()).hasSize(2);
    }

    @Test
    void one_query_per_batch_of_resources_not_per_metric() throws StorageException {
        List<Metric> in = new ArrayList<>();
        for (int r = 0; r < 30; r++) {
            for (int n = 0; n < 4; n++) in.add(metric("m" + n, "node[" + r + "].nodeSnmp[]"));
        }
        reader(900_000, 50).enrich(in);
        // Two POSTs per batch: the speed gauge, then the rows; bodies, not request lines.
        assertThat(urls).containsExactly("http://backend/api/v1/query", "http://backend/api/v1/query");
        assertThat(decoded(forms.get(0))).startsWith("query=last_over_time(")
                .contains("__name__=\"onms_resource_ifspeed\"").contains("[86400s])");
        String q = decoded(forms.get(1));
        assertThat(q).startsWith("query=timestamp(");
        assertThat(q).contains(") or max_over_time(timestamp(");
        assertThat(q).contains("__name__=~\"onms_resource_attr|onms_resource_category\"");
        assertThat(q).contains("[86400s:5m]");   // the registry's expiry, so an idle resource keeps its rows
        assertThat(q).doesNotContain("onms_instance_id");
        // resourceIds are regex-escaped and then PromQL-string-escaped in the
        // alternation, so the wire carries a doubled backslash before each
        // bracket (the query is URL-decoded here; that does not touch it).
        assertThat(q).contains("node\\\\[0\\\\]").contains("nodeSnmp\\\\[\\\\]");
    }

    @Test
    void more_resources_than_the_batch_size_means_more_queries() throws StorageException {
        List<Metric> in = new ArrayList<>();
        for (int r = 0; r < 120; r++) in.add(metric("m", "node[" + r + "].nodeSnmp[]"));
        reader(900_000, 50).enrich(in);
        assertThat(urls).hasSize(6);   // three batches, rows and speed each
    }

    @Test
    void the_gauge_becomes_the_ifSpeed_and_ifHighSpeed_strings_opennms_reports_read() throws StorageException {
        // mib2.traffic and mib2.HCtraffic-inout dereference {ifSpeed} and
        // {ifHighSpeed}; the wire carries only the gauge, so the read path
        // derives the pair the SNMP collector would have stored.
        speedResponse = vector(speed("ten-gig", "10000000000"), speed("fast-e", "100000000"));
        List<Metric> out = reader(900_000, 50).enrich(List.of(metric("m", "ten-gig"), metric("m", "fast-e")));
        assertThat(external(out.get(0), "ifHighSpeed")).contains("10000");
        assertThat(external(out.get(0), "ifSpeed")).contains("4294967295");   // the OID saturates
        assertThat(external(out.get(1), "ifHighSpeed")).contains("100");
        assertThat(external(out.get(1), "ifSpeed")).contains("100000000");
        assertThat(ResourceMetadataReader.speedStrings(1_000_000_000)).containsExactly(
                Map.entry("ifHighSpeed", "1000"), Map.entry("ifSpeed", "1000000000"));
    }

    @Test
    void what_the_data_series_carry_as_labels_comes_back_as_attributes_too() throws StorageException {
        // A custom report may dereference {nodeLabel}: it is no row, but the
        // label is right there. Rows still win over it.
        String rid = "node[1].interfaceSnmp[eth0]";
        Metric m = ImmutableMetric.builder()
                .intrinsicTag("name", "m").intrinsicTag("resourceId", rid)
                .metaTag("node_label", "core-sw-1").metaTag("foreign_source", "NOC")
                .metaTag("foreign_id", "core-sw-1").metaTag("location", "Default").metaTag("node", "NOC:core-sw-1")
                .build();
        Metric out = reader(900_000, 50).enrich(List.of(m)).get(0);
        assertThat(external(out, "nodeLabel")).contains("core-sw-1");
        assertThat(external(out, "foreignSource")).contains("NOC");
        assertThat(external(out, "foreignId")).contains("core-sw-1");
        assertThat(external(out, "location")).contains("Default");
        assertThat(external(out, "nodeId")).isEmpty();   // `node` is not the numeric id
        response = vector(row("onms_resource_attr", rid, "key", "nodeLabel", "value", "from-a-row"));
        assertThat(external(reader(900_000, 50).enrich(List.of(m)).get(0), "nodeLabel")).contains("from-a-row");
    }

    @Test
    void the_cap_counts_both_queries_of_a_batch() {
        List<Metric> in = new ArrayList<>();
        for (int r = 0; r < 51; r++) in.add(metric("m", "node[" + r + "].nodeSnmp[]"));
        assertThat(reader(900_000, 1).enrich(in)).isEqualTo(in);   // 51 batches = 102 calls > 100
        assertThat(urls).isEmpty();
        for (int r = 0; r < 50; r++) reader(900_000, 1).enrich(List.of(metric("m", "node[" + r + "].nodeSnmp[]")));
        assertThat(urls).hasSize(100);
    }

    @Test
    void a_failed_speed_query_leaves_the_rows_and_a_failed_rows_query_leaves_the_speed() {
        String rid = "node[1].interfaceSnmp[eth0]";
        ResourceMetadataReader speedDown = new ResourceMetadataReader(config(900_000, 50), (url, form) -> {
            if (form.contains("onms_resource_ifspeed")) throw new StorageException("timeout");
            return vector(row("onms_resource_attr", rid, "key", "ifAlias", "value", "uplink"));
        });
        Metric m = speedDown.enrich(List.of(metric("m", rid))).get(0);
        assertThat(external(m, "ifAlias")).contains("uplink");
        assertThat(external(m, "ifHighSpeed")).isEmpty();

        ResourceMetadataReader rowsDown = new ResourceMetadataReader(config(900_000, 50), (url, form) -> {
            if (form.contains("onms_resource_ifspeed")) return vector(speed(rid, "1000000000"));
            throw new StorageException("timeout");
        });
        m = rowsDown.enrich(List.of(metric("m", rid))).get(0);
        assertThat(external(m, "ifAlias")).isEmpty();
        assertThat(external(m, "ifHighSpeed")).contains("1000");
    }

    @Test
    void a_resource_without_rows_is_returned_unchanged() throws StorageException {
        Metric m = metric("m", "node[1].nodeSnmp[]");
        List<Metric> out = reader(900_000, 50).enrich(List.of(m));
        assertThat(out).containsExactly(m);
    }

    @Test
    void nothing_is_queried_for_an_empty_result_or_when_metadata_is_off() throws StorageException {
        reader(900_000, 50).enrich(List.of());
        assertThat(urls).isEmpty();
        Metric m = metric("m", "node[1].nodeSnmp[]");
        assertThat(reader(0, 50).enrich(List.of(m))).containsExactly(m);
        assertThat(urls).isEmpty();
    }

    @Test
    void rows_of_another_resource_do_not_leak() throws StorageException {
        response = vector(row("onms_resource_attr", "other", "key", "ifAlias", "value", "not mine"));
        Metric m = metric("m", "mine");
        assertThat(external(reader(900_000, 50).enrich(List.of(m)).get(0), "ifAlias")).isEmpty();
    }

    @Test
    void the_newest_emission_wins_over_an_older_row_in_the_window() throws StorageException {
        // A rename: the old row's series still has a sample inside the window.
        String rid = "node[1].interfaceSnmp[eth0]";
        response = vector(
                rowAt(1_700_000_000.000, "onms_resource_attr", rid, "key", "ifAlias", "value", "aaa-old"),
                rowAt(1_700_000_000.000, "onms_resource_category", rid, "category", "Retired"),
                rowAt(1_700_000_900.000, "onms_resource_attr", rid, "key", "ifAlias", "value", "zzz-new"),
                rowAt(1_700_000_900.000, "onms_resource_category", rid, "category", "Routers"));
        Metric m = reader(900_000, 50).enrich(List.of(metric("m", rid))).get(0);
        assertThat(external(m, "ifAlias")).contains("zzz-new");
        assertThat(meta(m, "categories")).contains("Routers");
    }

    @Test
    void rows_win_over_tags_the_series_already_carries() throws StorageException {
        String rid = "node[1].interfaceSnmp[eth0]";
        Metric stale = ImmutableMetric.builder()
                .intrinsicTag("name", "m").intrinsicTag("resourceId", rid)
                .externalTag("ifAlias", "from a label as old as the series lookback")
                .metaTag("categories", "Old").build();
        response = vector(row("onms_resource_attr", rid, "key", "ifAlias", "value", "current"),
                          row("onms_resource_category", rid, "category", "New"));
        Metric m = reader(900_000, 50).enrich(List.of(stale)).get(0);
        assertThat(external(m, "ifAlias")).contains("current");
        assertThat(m.getExternalTags()).filteredOn(t -> t.getKey().equals("ifAlias")).hasSize(1);
        assertThat(meta(m, "categories")).contains("New");
    }

    @Test
    void the_query_is_scoped_to_this_instance_when_configured() throws StorageException {
        PrometheusRemoteWriterConfig c = config(900_000, 50);
        c.setInstanceId("core-01");
        new ResourceMetadataReader(c, this::record).enrich(List.of(metric("m", "r")));
        assertThat(decoded(forms.get(0))).contains("onms_instance_id=\"core-01\"");
    }

    @Test
    void a_failed_query_returns_the_metrics_unenriched() {
        Metric m = metric("m", "node[1].nodeSnmp[]");
        ResourceMetadataReader r = new ResourceMetadataReader(config(900_000, 50),
                (url, form) -> { throw new StorageException("backend down"); });
        assertThat(r.enrich(List.of(m))).containsExactly(m);
    }

    @Test
    void a_failed_or_malformed_batch_leaves_only_its_own_resources_unenriched() {
        List<Metric> in = List.of(metric("m", "first"), metric("m", "second"));
        ResourceMetadataReader r = new ResourceMetadataReader(config(900_000, 1), (url, form) -> {
            if (form.contains("first")) return "{\"status\":\"error\",\"error\":\"nope\"}";
            return vector(row("onms_resource_attr", "second", "key", "ifAlias", "value", "still enriched"));
        });
        List<Metric> out = r.enrich(in);
        assertThat(external(out.get(0), "ifAlias")).isEmpty();
        assertThat(external(out.get(1), "ifAlias")).contains("still enriched");
    }

    @Test
    void rows_landing_moments_apart_are_one_emission_and_the_newest_row_per_key_wins() throws StorageException {
        // The rows of one emission shard independently and a refused batch is
        // retried, so they can carry stamps seconds apart; the value before
        // a rename in the same emission window loses to the newer row.
        String rid = "node[1].interfaceSnmp[eth0]";
        response = vector(
                rowAt(1_700_000_000.000, "onms_resource_attr", rid, "key", "ifAlias", "value", "old"),
                rowAt(1_700_000_000.000, "onms_resource_attr", rid, "key", "ifName", "value", "eth0"),
                rowAt(1_700_000_003.000, "onms_resource_attr", rid, "key", "ifAlias", "value", "new"),
                rowAt(1_700_000_005.000, "onms_resource_category", rid, "category", "Routers"));
        Metric m = reader(900_000, 50).enrich(List.of(metric("m", rid))).get(0);
        assertThat(external(m, "ifAlias")).contains("new");
        assertThat(external(m, "ifName")).contains("eth0");
        assertThat(meta(m, "categories")).contains("Routers");
    }

    @Test
    void a_malformed_response_returns_the_metrics_unenriched() {
        response = "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"nope\"}";
        Metric m = metric("m", "node[1].nodeSnmp[]");
        assertThat(reader(900_000, 50).enrich(List.of(m))).containsExactly(m);
    }

    @Test
    void too_many_batches_skips_enrichment_rather_than_flooding_the_backend() {
        List<Metric> in = new ArrayList<>();
        for (int r = 0; r < 101; r++) in.add(metric("m", "node[" + r + "].nodeSnmp[]"));
        assertThat(reader(900_000, 1).enrich(in)).isEqualTo(in);
        assertThat(urls).isEmpty();
    }

    @Test
    void the_window_is_never_shorter_than_two_cadences() {
        reader(TimeUnit_HOURS_24 + 1, 50).enrich(List.of(metric("m", "r")));
        long window = 2 * (TimeUnit_HOURS_24 + 1) / 1000;
        assertThat(decoded(forms.get(0))).contains("[" + window + "s])");    // the speed gauge
        assertThat(decoded(forms.get(1))).contains("[" + window + "s:5m]");  // the rows
    }

    private static final long TimeUnit_HOURS_24 = 24L * 3_600_000L;
}
