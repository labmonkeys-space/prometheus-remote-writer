/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;

class PrometheusReadClientTest {

    private MockWebServer server;
    private PrometheusReadClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("").toString().replaceAll("/$", ""));
        c.validate();
        client = new PrometheusReadClient(c);
    }

    @AfterEach
    void tearDown() throws IOException {
        client.shutdown();
        server.shutdown();
    }

    @Test
    void find_metrics_hits_series_endpoint_with_expected_query() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"status\":\"success\",\"data\":["
                       + "{\"__name__\":\"ifHCInOctets\",\"node\":\"1:1\"}"
                       + "]}"));

        TagMatcher m = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value("ifHCInOctets")
                .build();
        List<Metric> out = client.findMetrics(List.of(m));

        assertThat(out).hasSize(1);
        RecordedRequest req = server.takeRequest();
        // OkHttp leaves [ and ] as literals in the path since they're in
        // RFC 3986's gen-delims; inner special chars (= and ") are escaped.
        assertThat(req.getPath()).startsWith("/api/v1/series?match[]=");
        assertThat(req.getPath()).contains("__name__%3D%22ifHCInOctets%22");
        assertThat(req.getPath()).contains("&start=");
    }

    @Test
    void find_metrics_rejects_null_or_empty_matchers() {
        assertThatThrownBy(() -> client.findMetrics(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.findMetrics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void get_time_series_data_hits_query_range_with_derived_step() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                       + "{\"metric\":{},\"values\":[[1700000000,\"1.0\"],[1700000060,\"2.0\"]]}"
                       + "]}}"));

        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(1_700_000_000L),
                Instant.ofEpochSecond(1_700_000_120L),
                Duration.ofSeconds(60));
        TimeSeriesData data = client.getTimeSeriesData(request);

        List<DataPoint> pts = data.getDataPoints();
        assertThat(pts).hasSize(2);
        assertThat(pts.get(1).getValue()).isEqualTo(2.0);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).startsWith("/api/v1/query_range?");
        assertThat(req.getPath()).contains("step=60s");
    }

    @Test
    void step_derivation_respects_explicit_step() {
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(3600),
                Duration.ofSeconds(15));
        assertThat(PrometheusReadClient.stepSeconds(request)).isEqualTo(15);
    }

    @Test
    void step_derivation_falls_back_to_range_over_600_when_no_step_provided() {
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(6000),
                null);
        // 6000s / 600 points = 10s step
        assertThat(PrometheusReadClient.stepSeconds(request)).isEqualTo(10);
    }

    @Test
    void step_derivation_clamps_to_points_per_query_cap() {
        // 1 year range, 1s step would exceed 11000 point cap
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(31_536_000L),
                Duration.ofSeconds(1));
        long step = PrometheusReadClient.stepSeconds(request);
        assertThat(31_536_000L / step).isLessThanOrEqualTo(11_000L);
    }

    // ---------- two-phase resource discovery -------------------------------

    @Test
    void two_phase_fires_when_strategy_on_and_resourceid_regex_present() throws Exception {
        // Phase 1 returns 2 resourceIds → 1 phase-2 batch.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"node[1].nodeSnmp[]\",\"node[2].nodeSnmp[]\"]}"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            c.findMetrics(List.of(eq("name", "ifHCInOctets"), regex("resourceId", "node\\[.*\\]\\..*")));
        } finally {
            c.shutdown();
        }

        assertThat(server.getRequestCount()).isEqualTo(2);
        RecordedRequest p1 = server.takeRequest();
        assertThat(p1.getPath()).startsWith("/api/v1/label/resourceId/values?");
        RecordedRequest p2 = server.takeRequest();
        assertThat(p2.getPath()).startsWith("/api/v1/series?");
    }

    @Test
    void two_phase_skipped_when_strategy_on_but_no_resourceid_regex() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            // Exact-match resourceId → single-pass even with strategy on.
            c.findMetrics(List.of(eq("resourceId", "node[1].interfaceSnmp[en0]")));
        } finally {
            c.shutdown();
        }

        assertThat(server.getRequestCount()).isEqualTo(1);
        RecordedRequest only = server.takeRequest();
        assertThat(only.getPath()).startsWith("/api/v1/series?");
    }

    @Test
    void two_phase_skipped_when_strategy_off_even_with_resourceid_regex() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        // Default strategy = SINGLE_PASS — even with regex on resourceId.
        client.findMetrics(List.of(regex("resourceId", "node\\[.*\\]")));

        assertThat(server.getRequestCount()).isEqualTo(1);
        RecordedRequest only = server.takeRequest();
        assertThat(only.getPath()).startsWith("/api/v1/series?");
    }

    @Test
    void phase1_forwards_non_resourceid_matchers_as_match_param() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            c.findMetrics(List.of(eq("name", "ifHCInOctets"), regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        RecordedRequest p1 = server.takeRequest();
        assertThat(p1.getPath()).contains("match[]=");
        assertThat(p1.getPath()).contains("__name__%3D%22ifHCInOctets%22");
        // The resourceId regex is dropped from phase 1 — would over-constrain
        // the enumeration. The path itself contains "resourceId" (it's the
        // label being enumerated), so check the URL-decoded match[] selector
        // instead.
        String decoded = java.net.URLDecoder.decode(p1.getPath(), java.nio.charset.StandardCharsets.UTF_8);
        int matchStart = decoded.indexOf("match[]=") + "match[]=".length();
        int matchEnd = decoded.indexOf('&', matchStart);
        String selector = matchEnd > 0 ? decoded.substring(matchStart, matchEnd) : decoded.substring(matchStart);
        assertThat(selector).doesNotContain("resourceId");
    }

    @Test
    void phase1_omits_match_param_when_no_non_resourceid_matchers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            // Only matcher is regex on resourceId — phase 1 has nothing else
            // to forward. URL must not carry an empty match[]={} that would
            // 400 on strict backends.
            c.findMetrics(List.of(regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        RecordedRequest p1 = server.takeRequest();
        assertThat(p1.getPath()).doesNotContain("match[]=");
        // No match[] means start= is the only query parameter — preceded by
        // '?', not '&'.
        assertThat(p1.getPath()).contains("?start=");
    }

    @Test
    void phase1_empty_response_short_circuits_with_no_phase2_call() throws Exception {
        // Phase 1 returns empty data → no phase-2 calls, fast empty result.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        List<Metric> out;
        try {
            out = c.findMetrics(List.of(regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        assertThat(out).isEmpty();
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void phase2_chunks_resourceids_by_discovery_batch_size() throws Exception {
        // 5 resourceIds, batch-size 2 → 1 + ceil(5/2) = 1 + 3 = 4 HTTP calls.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"a\",\"b\",\"c\",\"d\",\"e\"]}"));
        for (int i = 0; i < 3; i++) {
            server.enqueue(new MockResponse().setResponseCode(200).setBody(
                    "{\"status\":\"success\",\"data\":[]}"));
        }

        PrometheusReadClient c = twoPhaseClient(2);
        try {
            c.findMetrics(List.of(regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        assertThat(server.getRequestCount()).isEqualTo(4);
    }

    @Test
    void phase2_selector_includes_non_resourceid_matchers_and_anchors_alternation() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"a\",\"b\"]}"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            c.findMetrics(List.of(eq("name", "ifHCInOctets"), regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        // Discard phase 1
        server.takeRequest();
        RecordedRequest p2 = server.takeRequest();
        // URL-decode just the match[] selector substring for readability.
        String decoded = java.net.URLDecoder.decode(p2.getPath(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(decoded).contains("__name__=\"ifHCInOctets\"");
        assertThat(decoded).contains("resourceId=~\"^(a|b)$\"");
    }

    @Test
    void phase2_alternation_escapes_regex_meta_in_resourceids() throws Exception {
        // OpenNMS bracketed grammar — must be regex-escaped byte-for-byte.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"node[1].interfaceSnmp[en0]\",\"node[2].nodeSnmp[]\"]}"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            c.findMetrics(List.of(regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        server.takeRequest();
        RecordedRequest p2 = server.takeRequest();
        String decoded = java.net.URLDecoder.decode(p2.getPath(), java.nio.charset.StandardCharsets.UTF_8);
        // Two layers of backslash on the wire:
        //   regex-literal escape:  '['  → '\['     (one backslash per meta)
        //   PromQL value escape:   '\'  → '\\'     (each backslash doubled
        //                                          because it's inside "..."
        //                                          in the selector)
        // Net wire bytes per resourceId-bracket: '\\['. The regex compiler
        // strips the PromQL layer to '\[' which matches a literal '['.
        assertThat(decoded).contains("\\\\[1\\\\]");
        assertThat(decoded).contains("\\\\.interfaceSnmp");
        assertThat(decoded).contains("\\\\[en0\\\\]");
    }

    @Test
    void phase2_results_are_deduplicated_across_batches() throws Exception {
        // 3 resourceIds → batch-size 2 → 2 phase-2 batches. Each batch
        // returns the SAME series (defensive — Thanos can return duplicates).
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"a\",\"b\",\"c\"]}"));
        String dup = "{\"status\":\"success\",\"data\":["
                + "{\"__name__\":\"foo\",\"resourceId\":\"r1\"}]}";
        server.enqueue(new MockResponse().setResponseCode(200).setBody(dup));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(dup));

        PrometheusReadClient c = twoPhaseClient(2);
        List<Metric> out;
        try {
            out = c.findMetrics(List.of(regex("resourceId", ".*")));
        } finally {
            c.shutdown();
        }

        assertThat(out).hasSize(1);
    }

    @Test
    void phase1_4xx_propagates_as_storage_exception() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(400).setBody(
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid match[]\"}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            assertThatThrownBy(() ->
                    c.findMetrics(List.of(regex("resourceId", ".*"))))
                    .isInstanceOf(org.opennms.integration.api.v1.timeseries.StorageException.class)
                    .hasMessageContaining("400");
        } finally {
            c.shutdown();
        }
        // Exactly one HTTP call — no fallback to single-pass, no phase-2.
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void phase2_4xx_in_one_batch_aborts_the_call_and_discards_partial_results() throws Exception {
        // Phase 1 returns 3 resourceIds → batch-size 1 → 3 phase-2 calls.
        // First batch 200, second 503; third must NOT be issued.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"a\",\"b\",\"c\"]}"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":["
                + "{\"__name__\":\"ok\",\"resourceId\":\"a\"}]}"));
        server.enqueue(new MockResponse().setResponseCode(503).setBody(
                "{\"status\":\"error\",\"error\":\"upstream\"}"));

        PrometheusReadClient c = twoPhaseClient(1);
        try {
            assertThatThrownBy(() -> c.findMetrics(List.of(regex("resourceId", ".*"))))
                    .isInstanceOf(org.opennms.integration.api.v1.timeseries.StorageException.class);
        } finally {
            c.shutdown();
        }
        // Phase 1 + 2 phase-2 batches (the third never issued).
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void decide_strategy_is_pure_function_of_matchers() {
        PrometheusReadClient c = twoPhaseClient(50);
        try {
            // strategy = LABEL_VALUES_FIRST (set by twoPhaseClient)
            assertThat(c.decideStrategy(List.of(regex("resourceId", ".*"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.LABEL_VALUES_FIRST);
            // Exact-match resourceId → single-pass
            assertThat(c.decideStrategy(List.of(eq("resourceId", "node[1]"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
            // Regex on a non-resourceId label → single-pass
            assertThat(c.decideStrategy(List.of(regex("name", "if.*"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
            // NOT_EQUALS_REGEX on resourceId → falls through to single-pass
            // (intentional — the alternation form would silently invert the
            // operator's exclusion intent; see decideStrategy javadoc).
            assertThat(c.decideStrategy(List.of(notEqualsRegex("resourceId", ".*ifb.*"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
            // EQUALS_REGEX + NOT_EQUALS_REGEX on resourceId in the same call
            // → ALSO falls through to single-pass. Code-review round 2
            // (DN-R2-1): without this fall-through, the trigger fires on the
            // EQUALS_REGEX, but `fromMatchersWithResourceIdAlternation` strips
            // ALL resourceId matchers including the negative regex —
            // operator's exclusion intent silently dropped.
            assertThat(c.decideStrategy(List.of(
                    regex("resourceId", "node\\[1\\]\\..*"),
                    notEqualsRegex("resourceId", ".*ifb.*"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
            // Order-independence: same shape, NOT_EQUALS_REGEX listed first.
            assertThat(c.decideStrategy(List.of(
                    notEqualsRegex("resourceId", ".*ifb.*"),
                    regex("resourceId", "node\\[1\\]\\..*"))))
                    .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
        } finally {
            c.shutdown();
        }
    }

    @Test
    void phase2_call_count_above_cap_throws_storage_exception_and_skips_phase2() throws Exception {
        // The cap is on phase-2 CALL COUNT, not resourceId count (round 2,
        // DN-R2-2). At batch-size 50, the 100-call cap admits exactly
        // 5000 resourceIds; 5001 resourceIds → 101 batches → throws.
        // The cap kicks in BEFORE any phase-2 call is issued — operator gets
        // an actionable error rather than a sequential storm.
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < 5001; i++) {
            if (i > 0) data.append(',');
            data.append('"').append("rid_").append(i).append('"');
        }
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[" + data + "]}"));

        PrometheusReadClient c = twoPhaseClient(50);
        try {
            assertThatThrownBy(() -> c.findMetrics(List.of(regex("resourceId", ".*"))))
                    .isInstanceOf(org.opennms.integration.api.v1.timeseries.StorageException.class)
                    .hasMessageContaining("101") // batchCount = ceil(5001/50)
                    .hasMessageContaining("100") // cap value
                    .hasMessageContaining("Broaden")
                    .hasMessageContaining("single-pass");
        } finally {
            c.shutdown();
        }
        // Exactly one HTTP call — phase 1 only, no phase-2 fan-out.
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void phase2_call_count_cap_kicks_in_at_small_batch_sizes() throws Exception {
        // The whole point of switching from "resourceId-count cap" to
        // "call-count cap" (DN-R2-2): at batch-size 1, even 101 resourceIds
        // would trigger 101 sequential phase-2 calls — the very storm the
        // cap is meant to prevent.
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < 101; i++) {
            if (i > 0) data.append(',');
            data.append('"').append("rid_").append(i).append('"');
        }
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[" + data + "]}"));

        PrometheusReadClient c = twoPhaseClient(1); // batch-size 1
        try {
            assertThatThrownBy(() -> c.findMetrics(List.of(regex("resourceId", ".*"))))
                    .isInstanceOf(org.opennms.integration.api.v1.timeseries.StorageException.class)
                    .hasMessageContaining("101");
        } finally {
            c.shutdown();
        }
        // Phase 1 only — phase 2 was never started.
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void mtype_fallback_runs_once_per_merged_metric_in_two_phase_path() throws Exception {
        // 3 phase-2 batches, each returning the SAME series without `mtype`.
        // After dedup the merged result has 1 Metric — mtype fallback must
        // synthesize exactly once, not three times.
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"status\":\"success\",\"data\":[\"a\",\"b\",\"c\"]}"));
        String dup = "{\"status\":\"success\",\"data\":["
                + "{\"__name__\":\"foo\",\"resourceId\":\"r1\"}]}";
        server.enqueue(new MockResponse().setResponseCode(200).setBody(dup));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(dup));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(dup));

        // Construct a metrics-tracking client (the test-friendly two-arg
        // ctor passes null and skips synthesis-counter increments).
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("").toString().replaceAll("/$", ""));
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryBatchSize(1);
        c.validate();
        org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics m =
                new org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics();
        PrometheusReadClient client = new PrometheusReadClient(c, m);
        List<Metric> out;
        try {
            out = client.findMetrics(List.of(regex("resourceId", ".*")));
            assertThat(out).hasSize(1);
        } finally {
            client.shutdown();
        }

        // Synthesis fires exactly once (per merged Metric), not three times
        // (per phase-2 batch). This is the load-bearing contract: the
        // counter must agree with the result-set size, not with the work done.
        long synthesized = m.snapshot().get(
                org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics
                        .SAMPLES_SYNTHESIZED_MTYPE).longValue();
        assertThat(synthesized).isEqualTo(1L);
        // The synthesized mtype tag must actually land on the returned
        // Metric — round 2 (P-R2-4): a refactor that calls
        // `mtypeFallback.apply(m)` but discards its return would still pass
        // the counter assertion above. Pin the result-side contract too.
        assertThat(out.get(0).getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo(
                            org.opennms.integration.api.v1.timeseries.MetaTagNames.mtype);
                    assertThat(t.getValue()).isEqualTo("gauge");
                });
    }

    // ---------- helpers ----------------------------------------------------

    /** Builds a dedicated client with strategy=label-values-first and the
     *  given batch-size, sharing the test's MockWebServer. */
    private PrometheusReadClient twoPhaseClient(int batchSize) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("").toString().replaceAll("/$", ""));
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryBatchSize(batchSize);
        c.validate();
        return new PrometheusReadClient(c);
    }

    private static TagMatcher eq(String key, String value) {
        return ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS).key(key).value(value).build();
    }

    private static TagMatcher regex(String key, String value) {
        return ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS_REGEX).key(key).value(value).build();
    }

    private static TagMatcher notEqualsRegex(String key, String value) {
        return ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.NOT_EQUALS_REGEX).key(key).value(value).build();
    }

    /** Lightweight in-test TimeSeriesFetchRequest since the API bundle exposes
     *  only the interface. */
    private record FakeFetchRequest(
            Metric metric, Instant start, Instant end, Duration step)
            implements TimeSeriesFetchRequest {

        @Override public Metric getMetric()  { return metric; }
        @Override public Instant getStart()  { return start; }
        @Override public Instant getEnd()    { return end; }
        @Override public Duration getStep()  { return step; }
        @Override public org.opennms.integration.api.v1.timeseries.Aggregation getAggregation() {
            return org.opennms.integration.api.v1.timeseries.Aggregation.NONE;
        }
    }
}
