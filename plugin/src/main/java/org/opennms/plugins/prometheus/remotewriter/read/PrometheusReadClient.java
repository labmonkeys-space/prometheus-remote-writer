/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesData;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig.DiscoveryStrategy;
import org.opennms.plugins.prometheus.remotewriter.http.TlsConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

/**
 * Read-side client that talks to the Prometheus HTTP query API. Implements
 * the two SPI queries ({@code findMetrics}, {@code getTimeSeriesData}) as
 * {@code GET /api/v1/series} and {@code GET /api/v1/query_range} calls.
 *
 * <p>Shares authentication, tenant, and TLS settings with the write client;
 * each call attaches the same headers so a single credential / tenant
 * configuration covers both directions.
 */
public final class PrometheusReadClient {

    private static final int MAX_POINTS_PER_RANGE = 11_000;
    /** Cap on the bytes we'll read from a single Prom-API response.
     *  Protects the plugin from a misbehaving backend returning a
     *  multi-megabyte error page or a pathologically large matrix. */
    private static final long MAX_RESPONSE_BYTES = 8L * 1024 * 1024; // 8 MiB

    /** Maximum number of phase-2 HTTP calls the two-phase path will issue
     *  before refusing to proceed. Bounds the actual cost (sequential HTTP
     *  round-trips on the OpenNMS request thread) rather than the
     *  resourceId enumeration size — at small batch sizes (e.g.,
     *  {@code read.discovery-batch-size=1} for debugging), an enumeration of
     *  even 5000 resourceIds would mean 5000 sequential calls. Code-review
     *  round 2 (DN-R2-2): the original {@code MAX_PHASE1_RESOURCE_IDS=5000}
     *  cap bounded the wrong quantity — it allowed the call-count storm
     *  it was meant to prevent at small batch sizes. With the default
     *  batch-size 50, 100 calls × 5000 = 5000 resourceIds (parity with the
     *  v0.5.0 round-1 cap); at batch-size 1, the same 100-call cap admits
     *  only 100 resourceIds. */
    private static final int MAX_PHASE2_CALLS = 100;

    private final OkHttpClient http;
    private final PrometheusRemoteWriterConfig config;
    private final MtypeFallback mtypeFallback;
    private final PluginMetrics metrics;

    /** Test-friendly constructor — no metrics sink, so the synthesis counter
     *  and the find_metrics_* counters are not driven. Production code uses
     *  the two-arg constructor. */
    public PrometheusReadClient(PrometheusRemoteWriterConfig config) {
        this(config, null);
    }

    public PrometheusReadClient(PrometheusRemoteWriterConfig config, PluginMetrics metrics) {
        this.config = Objects.requireNonNull(config);
        OkHttpClient.Builder b = new OkHttpClient.Builder()
                .connectTimeout(config.getHttpConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getHttpReadTimeoutMs(),       TimeUnit.MILLISECONDS)
                .writeTimeout(config.getHttpWriteTimeoutMs(),     TimeUnit.MILLISECONDS);
        TlsConfig.configure(b, config);
        this.http = b.build();
        this.mtypeFallback = new MtypeFallback(metrics);
        this.metrics = metrics;
    }

    /** Visible for tests — exposes the WARN-tracking set so tests can assert
     *  one-shot semantics and LRU eviction without depending on a logging
     *  framework. */
    java.util.Set<String> warnedMtypeMetricsForTesting() {
        return mtypeFallback.warnedMetricsForTesting();
    }

    public void shutdown() {
        java.util.concurrent.ExecutorService exec = http.dispatcher().executorService();
        exec.shutdown();
        try {
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
                exec.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
        http.connectionPool().evictAll();
        if (config.isTlsInsecureSkipVerify()) {
            org.opennms.plugins.prometheus.remotewriter.http.TlsConfig.stopInsecureWarn();
        }
    }

    /**
     * Discover metrics by translating the {@code TagMatcher} collection to one
     * of two paths:
     *
     * <ul>
     *   <li><b>Single-pass</b> (default) — exactly one
     *       {@code GET /api/v1/series}. Preserves v0.5.0 behavior bit-for-bit.</li>
     *   <li><b>Two-phase</b> — opt-in via
     *       {@code read.discovery-strategy = label-values-first}; fires only
     *       when the matcher collection contains a regex on {@code resourceId}.
     *       Phase 1 enumerates {@code resourceId} values via
     *       {@code /api/v1/label/resourceId/values}; phase 2 issues batched
     *       series queries with exact-match alternation per
     *       {@code read.discovery-batch-size}. See
     *       {@link #decideStrategy(Collection)}.</li>
     * </ul>
     */
    public List<Metric> findMetrics(Collection<TagMatcher> matchers) throws StorageException {
        if (matchers == null) {
            throw new NullPointerException("matchers");
        }
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("matchers must not be empty");
        }
        if (decideStrategy(matchers) == DiscoveryStrategy.LABEL_VALUES_FIRST) {
            if (metrics != null) metrics.findMetricsTwoPhase();
            return findMetricsTwoPhase(matchers);
        }
        if (metrics != null) metrics.findMetricsSinglePass();
        return findMetricsSinglePass(matchers);
    }

    /**
     * Pure predicate over the matcher collection: returns
     * {@link DiscoveryStrategy#LABEL_VALUES_FIRST} when the operator opted in
     * AND at least one matcher is an {@code EQUALS_REGEX} on
     * {@code resourceId} AND no {@code NOT_EQUALS_REGEX} on {@code resourceId}
     * is present; otherwise {@link DiscoveryStrategy#SINGLE_PASS}.
     *
     * <p>{@code NOT_EQUALS_REGEX} on {@code resourceId} is intentionally
     * excluded from the trigger — even when paired with an
     * {@code EQUALS_REGEX} — because the phase-2 alternation
     * ({@code resourceId=~"^(v1|…)$"}) is an inclusion form, while
     * {@code NOT_EQUALS_REGEX} encodes an exclusion. The phase-2 helper
     * strips ALL {@code resourceId}-keyed matchers, so any negative regex
     * present alongside an {@code EQUALS_REGEX} would be silently dropped —
     * the operator's exclusion intent inverted into an inclusion of every
     * enumerated value. Falling through to single-pass keeps the negative
     * regex correct, at the cost of the optimization for the
     * {@code EQUALS_REGEX + NOT_EQUALS_REGEX} mixed-shape input.
     *
     * <p>Visible for testing.
     */
    DiscoveryStrategy decideStrategy(Collection<TagMatcher> matchers) {
        if (config.getDiscoveryStrategy() != DiscoveryStrategy.LABEL_VALUES_FIRST) {
            return DiscoveryStrategy.SINGLE_PASS;
        }
        boolean hasEqualsRegex = false;
        for (TagMatcher m : matchers) {
            if (!IntrinsicTagNames.resourceId.equals(m.getKey())) continue;
            TagMatcher.Type t = m.getType();
            if (t == TagMatcher.Type.NOT_EQUALS_REGEX) {
                // Short-circuit — any NOT_EQUALS_REGEX on resourceId disables
                // two-phase regardless of what else is in the collection.
                return DiscoveryStrategy.SINGLE_PASS;
            }
            if (t == TagMatcher.Type.EQUALS_REGEX) {
                hasEqualsRegex = true;
            }
        }
        return hasEqualsRegex
                ? DiscoveryStrategy.LABEL_VALUES_FIRST
                : DiscoveryStrategy.SINGLE_PASS;
    }

    private List<Metric> findMetricsSinglePass(Collection<TagMatcher> matchers) throws StorageException {
        String selector = PromQLBuilder.fromMatchers(matchers);
        long startEpoch = Instant.now().getEpochSecond() - config.getMaxSeriesLookbackSeconds();

        String url = config.getReadUrl()
                + "/api/v1/series?match[]=" + urlEncode(selector)
                + "&start=" + startEpoch;
        String body = executeGet(url);
        List<Metric> raw = PromResponseParser.parseSeriesResponse(body);
        // Apply the mtype fallback per-Metric — the read path's contract with
        // OpenNMS-core requires every returned Metric carry an mtype meta tag.
        List<Metric> out = new ArrayList<>(raw.size());
        for (Metric m : raw) out.add(mtypeFallback.apply(m));
        return out;
    }

    /**
     * Two-phase resource discovery. Phase 1 enumerates {@code resourceId}
     * values via {@code /api/v1/label/resourceId/values?match[]=…}; phase 2
     * issues batched {@code /api/v1/series} calls with exact-match
     * alternation over the resourceIds, chunked by
     * {@code read.discovery-batch-size}.
     *
     * <p>Phase 1 forwards the non-resourceId portion of the matcher
     * collection as its {@code match[]} parameter for correctness — without
     * it, phase 1 would enumerate every resourceId in the lookback window
     * regardless of the other constraints. When stripping leaves no
     * matchers, the {@code match[]} parameter is omitted entirely.
     *
     * <p>Phase 2 results are merged across batches and de-duplicated by
     * intrinsic-tag identity (defensive against backend quirks like Thanos
     * returning duplicate series across replicas without {@code dedup=true}).
     * The {@code mtype} fallback runs once per merged Metric, not per batch
     * — preserving the {@code samples_synthesized_mtype_total} counter
     * contract across strategies.
     */
    private List<Metric> findMetricsTwoPhase(Collection<TagMatcher> matchers) throws StorageException {
        long startEpoch = Instant.now().getEpochSecond() - config.getMaxSeriesLookbackSeconds();

        // ---- Phase 1: enumerate resourceIds ------------------------------
        StringBuilder p1 = new StringBuilder(config.getReadUrl())
                .append("/api/v1/label/")
                .append(IntrinsicTagNames.resourceId)
                .append("/values?");
        String phase1Selector = PromQLBuilder.fromMatchersExcludingResourceId(matchers);
        if (phase1Selector != null) {
            p1.append("match[]=").append(urlEncode(phase1Selector)).append('&');
        }
        p1.append("start=").append(startEpoch);

        String phase1Body = executeGet(p1.toString());
        List<String> resourceIds = PromResponseParser.parseLabelValuesResponse(phase1Body);
        if (resourceIds.isEmpty()) {
            // No resourceIds match the non-resourceId constraints — short-circuit
            // to an empty result without issuing any phase-2 calls. Operator
            // sees a fast empty response.
            return List.of();
        }
        // ---- Phase 2: batched series queries with alternation ------------
        int batchSize = config.getDiscoveryBatchSize();
        // Bound the actual cost — sequential HTTP calls on the request
        // thread — rather than the resourceId-enumeration size. At
        // batch-size 50, 100 calls × 50 = 5000 resourceIds; at batch-size 1,
        // 100 calls × 1 = 100 resourceIds. The OpenNMS request thread
        // shouldn't be blocked for thousands of round-trips regardless of
        // how the operator tunes batch-size.
        int batchCount = (resourceIds.size() + batchSize - 1) / batchSize;
        if (batchCount > MAX_PHASE2_CALLS) {
            throw new StorageException(
                "Two-phase resource discovery would issue " + batchCount
                + " phase-2 calls (cap " + MAX_PHASE2_CALLS + "; phase-1 returned "
                + resourceIds.size() + " resourceIds at read.discovery-batch-size="
                + batchSize + "). Broaden your matchers to narrow the resourceId set, "
                + "increase read.discovery-batch-size, or set "
                + "read.discovery-strategy=single-pass to bypass the two-phase path "
                + "for this query shape.");
        }
        // LinkedHashMap preserves discovery order across batches; the dedup
        // key is the sorted intrinsic-tag set (matches the single-pass
        // parser's series identity contract).
        Map<String, Metric> deduped = new LinkedHashMap<>();
        int batches = 0;
        for (int from = 0; from < resourceIds.size(); from += batchSize) {
            int to = Math.min(from + batchSize, resourceIds.size());
            List<String> chunk = resourceIds.subList(from, to);
            String selector = PromQLBuilder.fromMatchersWithResourceIdAlternation(matchers, chunk);
            String url = config.getReadUrl()
                    + "/api/v1/series?match[]=" + urlEncode(selector)
                    + "&start=" + startEpoch;
            String body = executeGet(url);
            for (Metric m : PromResponseParser.parseSeriesResponse(body)) {
                deduped.putIfAbsent(intrinsicIdentityKey(m), m);
            }
            batches++;
        }
        if (metrics != null) metrics.findMetricsPhase2Batches(batches);

        // mtype fallback runs ONCE per deduped Metric, not per phase-2 batch.
        List<Metric> out = new ArrayList<>(deduped.size());
        for (Metric m : deduped.values()) out.add(mtypeFallback.apply(m));
        return out;
    }

    /**
     * Build a stable string key from a Metric's intrinsic-tag set for use as
     * a dedup-map key in the two-phase merge. Intrinsic tags in TSS are the
     * series identity (name + resourceId); two Metrics with equal intrinsic
     * sets refer to the same series. Sort by tag key so the produced string
     * is order-stable across input orderings.
     *
     * <p>Encoding uses {@code } as the inter-tag separator and
     * {@code } as the intra-tag (key/value) separator — both are
     * non-printable C0 controls that cannot appear inside an OpenNMS
     * resourceId or metric-name. Using {@code TreeMap.toString()} would be
     * tempting but produces ambiguous output: a key {@code "a"} with value
     * {@code "b, c=1"} renders as {@code "{a=b, c=1}"} — indistinguishable
     * from two separate tags {@code a=b} and {@code c=1}. Code-review round
     * 1 (P1): the OpenNMS resourceId grammar legitimately allows {@code ,}
     * and {@code =} (e.g. {@code nodeSource[Org=Foo,Bar].interfaceSnmp[en0]}),
     * so the toString encoding could collide on legal inputs.
     */
    private static String intrinsicIdentityKey(Metric m) {
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Tag t : m.getIntrinsicTags()) {
            sorted.put(t.getKey(), t.getValue());
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (!first) sb.append('');
            sb.append(e.getKey()).append('').append(e.getValue());
            first = false;
        }
        return sb.toString();
    }

    /** Translates a fetch request to a {@code /api/v1/query_range} call. */
    public TimeSeriesData getTimeSeriesData(TimeSeriesFetchRequest request) throws StorageException {
        Objects.requireNonNull(request, "request");
        Metric requestMetric = Objects.requireNonNull(request.getMetric(), "request.metric");
        String selector = PromQLBuilder.fromIntrinsicTags(requestMetric.getIntrinsicTags());
        long startSec = request.getStart().getEpochSecond();
        long endSec   = request.getEnd().getEpochSecond();
        long stepSec  = stepSeconds(request);

        String url = config.getReadUrl()
                + "/api/v1/query_range?query=" + urlEncode(selector)
                + "&start=" + startSec
                + "&end=" + endSec
                + "&step=" + stepSec + "s";

        String body = executeGet(url);
        // Reconstruct the Metric from the response's actual labels (where
        // mtype lives), not from the request Metric (which OpenNMS builds
        // with only resourceId+name — no mtype). When the matrix is empty,
        // skip the synthesis fallback altogether: OpenNMS streams the
        // empty datapoint list and never dereferences mtype on it, so a
        // synthesis tick here would just inflate the counter for graphs
        // that won't render anyway.
        PromResponseParser.RangeResult result =
                PromResponseParser.parseRangeResponseWithMetric(body, requestMetric);
        Metric metric = result.points().isEmpty()
                ? result.metric()
                : mtypeFallback.apply(result.metric());
        return new ImmutableTimeSeriesData(metric, result.points());
    }

    /**
     * Derive a step in seconds. Uses the request's explicit step if given;
     * otherwise falls back to {@code (end - start) / 600}, clamped to at
     * least 1s and at most a value that would exceed Prometheus's 11 000
     * points-per-query ceiling.
     *
     * <p>Overflow-safe: computes {@code end - start} with
     * {@link Math#subtractExact}. A range that wraps {@code Long.MAX_VALUE}
     * is rejected with {@link IllegalArgumentException}, which surfaces to
     * the caller rather than producing a garbage step that Prometheus will
     * 422 on.
     */
    static long stepSeconds(TimeSeriesFetchRequest request) {
        Duration explicit = request.getStep();
        long start = request.getStart().getEpochSecond();
        long end   = request.getEnd().getEpochSecond();
        long rangeSec;
        try {
            rangeSec = Math.max(1, Math.subtractExact(end, start));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                "fetch-request range overflows Long: start=" + start + ", end=" + end, overflow);
        }

        long step = explicit != null && !explicit.isZero()
                ? Math.max(1, explicit.getSeconds())
                : Math.max(1, rangeSec / 600);

        long minStepForPointsCap = (rangeSec + MAX_POINTS_PER_RANGE - 1) / MAX_POINTS_PER_RANGE;
        if (step < minStepForPointsCap) step = minStepForPointsCap;
        return step;
    }

    // -- HTTP ---------------------------------------------------------------

    private String executeGet(String url) throws StorageException {
        Request.Builder rb = new Request.Builder().url(url).get();
        if (config.hasBasicAuth()) {
            String creds = config.getBasicUsername() + ":" + config.getBasicPassword();
            rb.addHeader("Authorization", "Basic "
                + java.util.Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8)));
        } else if (config.hasBearerAuth()) {
            rb.addHeader("Authorization", "Bearer " + config.getBearerToken());
        }
        if (config.hasTenant()) {
            rb.addHeader("X-Scope-OrgID", config.getTenantOrgId());
        }

        try (Response resp = http.newCall(rb.build()).execute()) {
            String text = readBodyCapped(resp);
            if (!resp.isSuccessful()) {
                throw new StorageException(
                    "Prometheus query failed: " + resp.code() + " " + resp.message()
                        + " — " + text);
            }
            return text;
        } catch (IOException e) {
            throw new StorageException("Prometheus query transport error: " + e.getMessage(), e);
        }
    }

    /** Read the response body, capped at {@link #MAX_RESPONSE_BYTES} to
     *  protect against a misbehaving backend returning a huge payload.
     *  When the cap is exceeded, throws a {@link StorageException} rather
     *  than silently truncating — a truncated JSON would parse as
     *  malformed and produce a less actionable error. */
    private static String readBodyCapped(Response resp) throws IOException, StorageException {
        ResponseBody body = resp.body();
        if (body == null) return "";
        long declared = body.contentLength();
        if (declared > MAX_RESPONSE_BYTES) {
            throw new StorageException(
                "Prometheus response too large: " + declared + " bytes (cap "
                    + MAX_RESPONSE_BYTES + ")");
        }
        try (okio.BufferedSource src = body.source()) {
            if (!src.request(MAX_RESPONSE_BYTES + 1)) {
                // fits within the cap — read it all
                return body.string();
            }
            throw new StorageException(
                "Prometheus response exceeded cap of " + MAX_RESPONSE_BYTES + " bytes");
        }
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
