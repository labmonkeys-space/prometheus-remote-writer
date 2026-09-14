/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

class PromResponseParserTest {

    // ---------- /series response -------------------------------------------

    @Test
    void series_response_reconstructs_metric_with_intrinsic_name_and_resource_id() throws Exception {
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "nodeSource[NOC:router-42].interfaceSnmp[eth0]",
                  "node": "NOC:router-42",
                  "if_name": "eth0"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);

        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue()).isEqualTo("ifHCInOctets");
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("nodeSource[NOC:router-42].interfaceSnmp[eth0]");
        assertThat(m.getIntrinsicTags()).hasSize(2);
        // "node" and "if_name" go into meta (partition-lossy on round-trip).
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsOnly("node", "if_name");
    }

    @Test
    void series_response_empty_data_returns_empty_list() throws Exception {
        String json = "{\"status\":\"success\",\"data\":[]}";
        assertThat(PromResponseParser.parseSeriesResponse(json)).isEmpty();
    }

    @Test
    void series_response_error_status_is_wrapped_in_storage_exception() {
        String json = "{\"status\":\"error\",\"error\":\"bad request\"}";
        assertThatThrownBy(() -> PromResponseParser.parseSeriesResponse(json))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("bad request");
    }

    @Test
    void series_response_malformed_json_is_wrapped_in_storage_exception() {
        assertThatThrownBy(() -> PromResponseParser.parseSeriesResponse("<html>gateway oops</html>"))
                .isInstanceOf(StorageException.class);
    }

    @Test
    void series_response_empty_body_is_wrapped_in_storage_exception() {
        assertThatThrownBy(() -> PromResponseParser.parseSeriesResponse(""))
                .isInstanceOf(StorageException.class);
    }

    // ---------- /query_range response --------------------------------------

    @Test
    void range_response_reconstructs_data_points() throws Exception {
        String json = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [
                  {
                    "metric": { "__name__": "x" },
                    "values": [
                      [1700000000, "1.5"],
                      [1700000060, "2.5"],
                      [1700000120, "3.5"]
                    ]
                  }
                ]
              }
            }""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);

        assertThat(pts).hasSize(3);
        assertThat(pts.get(0).getTime().getEpochSecond()).isEqualTo(1_700_000_000L);
        assertThat(pts.get(0).getValue()).isEqualTo(1.5);
        assertThat(pts.get(2).getValue()).isEqualTo(3.5);
    }

    @Test
    void range_response_empty_result_returns_empty_list() throws Exception {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[]}}""";
        assertThat(PromResponseParser.parseRangeResponse(json)).isEmpty();
    }

    @Test
    void range_response_parses_fractional_timestamps() throws Exception {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{"__name__":"x"},"values":[[1700000000.5,"42"]]}
            ]}}""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);
        assertThat(pts).hasSize(1);
        // 500 ms offset from the integer second
        assertThat(pts.get(0).getTime().toEpochMilli()).isEqualTo(1_700_000_000_500L);
    }

    @Test
    void range_response_merges_multiple_series_and_dedups_by_timestamp() throws Exception {
        // Two series for the same selector — points across both should be
        // merged into one timeline ordered by timestamp, with same-timestamp
        // collisions collapsing via last-write-wins.
        String json = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [
                  {"metric":{"__name__":"x","a":"1"},"values":[[1700000000,"1.0"],[1700000060,"2.0"]]},
                  {"metric":{"__name__":"x","a":"2"},"values":[[1700000030,"1.5"],[1700000060,"2.5"]]}
                ]
              }
            }""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);
        assertThat(pts).hasSize(3);
        assertThat(pts).extracting(p -> p.getTime().getEpochSecond())
                .containsExactly(1_700_000_000L, 1_700_000_030L, 1_700_000_060L);
        // Last-write-wins on the shared 1700000060 timestamp — ordering of
        // results[0]/results[1] in the JSON dictates which value wins; the
        // parser sees results[1]'s 2.5 last.
        assertThat(pts.get(2).getValue()).isEqualTo(2.5);
    }

    @Test
    void range_response_parses_prometheus_non_finite_value_strings() throws Exception {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{"__name__":"x"},"values":[
                [1700000000,"NaN"],[1700000060,"+Inf"],[1700000120,"-Inf"]
              ]}
            ]}}""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);
        assertThat(pts).hasSize(3);
        assertThat(pts.get(0).getValue()).isNaN();
        assertThat(pts.get(1).getValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(pts.get(2).getValue()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void range_response_malformed_value_is_wrapped_in_storage_exception() {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{"__name__":"x"},"values":[[1700000000,"not-a-number"]]}
            ]}}""";
        assertThatThrownBy(() -> PromResponseParser.parseRangeResponse(json))
                .isInstanceOf(StorageException.class);
    }

    // ---------- onms_attr_ prefix round-trip --------------------------------

    // ---------- onms_extattr_ — external-partition round-trip --------------

    @Test
    void mtype_label_round_trips_into_meta_tags() throws Exception {
        // The catch-all branch in labelObjectToMetric routes any non-name,
        // non-resourceId label into a meta tag. mtype is the load-bearing
        // example: OpenNMS-core dereferences MetaTagNames.mtype on every
        // returned Metric, so this round-trip must work without special-casing.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "mtype": "counter"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("counter");
        assertThat(m.getMetaTags()).extracting("key").containsOnly("mtype");
    }

    // ---------- /query_range with Metric reconstruction ---------------------

    @Test
    void range_with_metric_reconstructs_first_series_labels() throws Exception {
        String json = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [
                  {
                    "metric": {
                      "__name__": "ifHCInOctets",
                      "resourceId": "node[1].interfaceSnmp[eth0]",
                      "mtype": "counter"
                    },
                    "values": [[1700000000,"1.0"],[1700000060,"2.0"]]
                  }
                ]
              }
            }""";
        Metric fallback = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "ignored")
                .build();
        PromResponseParser.RangeResult r =
                PromResponseParser.parseRangeResponseWithMetric(json, fallback);

        assertThat(r.points()).hasSize(2);
        assertThat(r.metric().getFirstTagByKey(IntrinsicTagNames.name).getValue())
                .isEqualTo("ifHCInOctets");
        assertThat(r.metric().getFirstTagByKey(MetaTagNames.mtype).getValue())
                .isEqualTo("counter");
    }

    @Test
    void range_with_metric_falls_back_when_matrix_is_empty() throws Exception {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[]}}""";
        Metric fallback = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "fallback-name")
                .build();
        PromResponseParser.RangeResult r =
                PromResponseParser.parseRangeResponseWithMetric(json, fallback);

        assertThat(r.points()).isEmpty();
        assertThat(r.metric()).isSameAs(fallback);
    }

    @Test
    void range_with_metric_falls_back_when_first_series_has_empty_metric_object() throws Exception {
        // Synthetic ranges (recording rules, etc.) sometimes serialize with
        // `"metric": {}`. Nothing to reconstruct from — use the fallback.
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{},"values":[[1700000000,"1.0"]]}
            ]}}""";
        Metric fallback = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "fallback-name")
                .build();
        PromResponseParser.RangeResult r =
                PromResponseParser.parseRangeResponseWithMetric(json, fallback);

        assertThat(r.points()).hasSize(1);
        assertThat(r.metric()).isSameAs(fallback);
    }

    @Test
    void range_with_metric_uses_first_series_when_multiple_series_match() throws Exception {
        // Two series share the same selector — the first series' labels are
        // used for the Metric (in practice every matched series has the same
        // mtype, so this is correct).
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{"__name__":"x","a":"1","mtype":"gauge"},"values":[[1700000000,"1.0"]]},
              {"metric":{"__name__":"x","a":"2","mtype":"gauge"},"values":[[1700000030,"1.5"]]}
            ]}}""";
        Metric fallback = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "ignored")
                .build();
        PromResponseParser.RangeResult r =
                PromResponseParser.parseRangeResponseWithMetric(json, fallback);

        assertThat(r.points()).hasSize(2);
        assertThat(r.metric().getMetaTags()).extracting("key").contains("a", "mtype");
        assertThat(r.metric().getFirstTagByKey("a").getValue()).isEqualTo("1");
    }

    @Test
    void parse_prom_value_translates_prometheus_wire_form() {
        assertThat(PromResponseParser.parsePromValue("NaN")).isNaN();
        assertThat(PromResponseParser.parsePromValue("+Inf")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(PromResponseParser.parsePromValue("-Inf")).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(PromResponseParser.parsePromValue("3.14")).isEqualTo(3.14);
    }

    // ---------- the 1.0.0 data series ------------------------------------------

    @Test
    void a_data_series_parses_with_everything_but_name_and_resource_id_as_meta_tags() throws Exception {
        // The bounded 1.0.0 schema: read requires only __name__ + resourceId;
        // everything else lands as meta tags, and the resource's attributes
        // come from the metadata rows, not from the series labels.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "snmp/fs/Servers/router1/eth0/mib2-interfaces",
                  "node": "Servers:router1",
                  "node_label": "router1",
                  "if_name": "eth0",
                  "job": "snmp",
                  "instance": "Servers:router1",
                  "mtype": "counter"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("snmp/fs/Servers/router1/eth0/mib2-interfaces");
        assertThat(m.getExternalTags()).isEmpty();
    }

    @Test
    void legacy_plugin_written_series_parses_into_a_readable_metric() throws Exception {
        // Migration guarantee (spec "Read path functions under every
        // profile"): history written by the AGPL
        // opennms-prometheus-remotewrite-plugin — its 8-label wire schema
        // observed on a live deployment — parses with resourceId intact and
        // the remaining labels deposited as meta tags. Label names from the
        // captured schema; values here are representative shapes only (the
        // value-format fixture lands with change task 4.2).
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "snmp/512/HundredGigE0_1_0_36/mib2-X-interfaces-pkts",
                  "node": "512",
                  "ifName": "HundredGigE0/1/0/36",
                  "ifDescr": "HundredGigE0/1/0/36",
                  "location": "Default",
                  "mtype": "counter",
                  "geohash": "u336xps"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue()).isEqualTo("ifHCInOctets");
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("snmp/512/HundredGigE0_1_0_36/mib2-X-interfaces-pkts");
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsExactlyInAnyOrder("node", "ifName", "ifDescr", "location", "mtype", "geohash");
        assertThat(m.getExternalTags()).isEmpty();
    }
}
