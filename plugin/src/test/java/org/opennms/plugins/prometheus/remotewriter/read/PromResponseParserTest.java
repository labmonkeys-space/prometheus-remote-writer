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

    @Test
    void onms_attr_label_becomes_meta_tag_with_prefix_stripped() throws Exception {
        // Resource string attribute: OpenNMS substitutes ${name} placeholders
        // against meta tags whose key matches the placeholder. The wire form
        // is `onms_attr_<key>`; the parser must recover the meta tag with the
        // bare key on read.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "events_processed",
                  "resourceId": "node[1].eventdProcessingStat[Logger]",
                  "onms_attr_name": "Eventd_Logger_Receiver"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue())
                .isEqualTo("events_processed");
        // Meta tag with the stripped key — the prefixed form must NOT also
        // appear as a meta tag (single source of truth on the read side).
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsOnly("name");
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("name");
                    assertThat(t.getValue()).isEqualTo("Eventd_Logger_Receiver");
                });
    }

    @Test
    void onms_attr_with_empty_suffix_falls_through_to_meta_tag() throws Exception {
        // A label literally named `onms_attr_` (empty suffix after the
        // prefix) is pathological — emitting a meta tag with an empty key
        // would be worse than preserving the verbatim label name. The parser
        // routes the bare prefix through the catch-all branch.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "onms_attr_": "anomalous"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsOnly("onms_attr_");
        assertThat(m.getMetaTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEmpty());
        // Symmetric with the onms_extattr_ twin: no leak into the external
        // partition either.
        assertThat(m.getExternalTags()).isEmpty();
    }

    @Test
    void onms_attr_label_with_sanitized_key_recovers_the_sanitized_form() throws Exception {
        // Spec scenario: "meta-tag key with non-identifier characters is
        // sanitized into the label name" — write side maps `rack-unit` to
        // `onms_attr_rack_unit`; read side strips the prefix and recovers
        // `rack_unit` (the sanitized form, not the original `rack-unit`).
        // Pins the documented one-way round-trip-fidelity caveat.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "onms_attr_rack_unit": "14"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("rack_unit");
                    assertThat(t.getValue()).isEqualTo("14");
                });
        assertThat(m.getMetaTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("rack-unit"));
    }

    @Test
    void onms_attr___name__exotica_lands_as_meta_tag_keyed_double_underscore_name() throws Exception {
        // Defensive: the parser checks branches in order — __name__ first,
        // then resourceId, then onms_attr_*. A hand-crafted backend label
        // literally named `onms_attr___name__` (operator misuse, or a
        // third-party exporter squatting the namespace) hits the prefix
        // branch and produces a meta tag with key `__name__`. The intrinsic
        // is set independently when `__name__` itself is also present.
        // Pin this so a regression that swaps branch order would surface here.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "onms_attr___name__": "exotic"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        // Intrinsic is populated correctly from the dedicated __name__ branch.
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue())
                .isEqualTo("ifHCInOctets");
        // The exotica reconstructs as a meta tag keyed `__name__` — coherent
        // partition co-population, no collision since meta and intrinsic
        // partitions are independent in the OpenNMS Metric model.
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("__name__");
                    assertThat(t.getValue()).isEqualTo("exotic");
                });
    }

    @Test
    void onms_attr_resourceId_exotica_lands_as_meta_tag_alongside_intrinsic() throws Exception {
        // Same defensive shape as above for the second special-cased
        // intrinsic. A label named `onms_attr_resourceId` reconstructs as a
        // meta tag keyed `resourceId` while the dedicated `resourceId`
        // branch independently populates the intrinsic.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "onms_attr_resourceId": "fake-resource-id"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("node[1].interfaceSnmp[eth0]");
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("resourceId");
                    assertThat(t.getValue()).isEqualTo("fake-resource-id");
                });
    }

    // ---------- onms_extattr_ — external-partition round-trip --------------

    @Test
    void onms_extattr_label_becomes_external_tag_with_prefix_stripped() throws Exception {
        // OpenNMS-core's TimeseriesResourceStorageDao.getStringAttributes()
        // reads ONLY from getExternalTags() for placeholder substitution
        // (${name} / ${datname} / ${spcname}). The parser must deposit the
        // recovered tag on the external partition, not meta.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "EventProcess50",
                  "resourceId": "snmp/fs/selfmonitor/1/Eventlogs/eventlogs.process.expand/x",
                  "onms_extattr_name": "eventlogs.process"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue())
                .isEqualTo("EventProcess50");
        // External partition carries the recovered string attribute.
        assertThat(m.getExternalTags())
                .extracting("key")
                .containsOnly("name");
        assertThat(m.getExternalTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("name");
                    assertThat(t.getValue()).isEqualTo("eventlogs.process");
                });
        // Partition fidelity — meta must NOT also carry it.
        assertThat(m.getMetaTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("name"));
        // No raw prefixed form leaks anywhere.
        assertThat(m.getMetaTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("onms_extattr_name"));
        assertThat(m.getExternalTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("onms_extattr_name"));
    }

    @Test
    void onms_extattr_with_empty_suffix_falls_through_to_meta_tag() throws Exception {
        // Defensive: bare `onms_extattr_` (empty suffix) is pathological and
        // must fall through to the catch-all branch — emitting an external
        // tag with an empty key would be worse than preserving the raw name.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "onms_extattr_": "anomalous"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsOnly("onms_extattr_");
        assertThat(m.getMetaTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEmpty());
        // Did NOT land on external partition with an empty key.
        assertThat(m.getExternalTags()).isEmpty();
    }

    @Test
    void onms_attr_and_onms_extattr_in_same_response_route_to_their_own_partitions() throws Exception {
        // Mixed-prefix response: ensure each prefix routes to its own
        // partition; no cross-pollination.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "EventProcess50",
                  "onms_attr_x":    "meta_value",
                  "onms_extattr_y": "external_value"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        // Meta partition has only x; external partition has only y.
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("x");
                    assertThat(t.getValue()).isEqualTo("meta_value");
                })
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("y"));
        assertThat(m.getExternalTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("y");
                    assertThat(t.getValue()).isEqualTo("external_value");
                })
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("x"));
    }

    @Test
    void onms_extattr_label_with_sanitized_key_recovers_the_sanitized_form() throws Exception {
        // Parity with onms_attr_: read-side recovery yields the SANITIZED
        // key (rack_unit), not the original (rack-unit).
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "node[1].interfaceSnmp[eth0]",
                  "onms_extattr_rack_unit": "14"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getExternalTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("rack_unit");
                    assertThat(t.getValue()).isEqualTo("14");
                });
        assertThat(m.getExternalTags())
                .noneSatisfy(t -> assertThat(t.getKey()).isEqualTo("rack-unit"));
    }

    @Test
    void onms_attr_label_round_trips_alongside_intrinsic_name() throws Exception {
        // The motivating case: intrinsic `name` (metric-name) and meta `name`
        // (resource string attribute) both reach the read side, in their
        // proper partitions, via the onms_attr_ namespace.
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "events_processed",
                  "onms_attr_name": "Eventd_Logger_Receiver"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue())
                .isEqualTo("events_processed");
        assertThat(m.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("name");
                    assertThat(t.getValue()).isEqualTo("Eventd_Logger_Receiver");
                });
    }

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
}
