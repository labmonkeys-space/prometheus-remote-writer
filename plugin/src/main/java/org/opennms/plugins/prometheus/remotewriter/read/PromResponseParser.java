/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableDataPoint;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;

/**
 * Parses Prometheus HTTP API responses into OpenNMS TSS domain objects.
 *
 * <p>The two shapes handled:
 * <ul>
 *   <li>{@code /api/v1/series} — a JSON array of label objects, each
 *       reconstructed as a {@link Metric}.</li>
 *   <li>{@code /api/v1/query_range} — a matrix response with
 *       {@code result[].values = [[unix_ts, value_string], ...]} pairs
 *       reconstructed as {@link DataPoint}s.</li>
 * </ul>
 *
 * <p>On the reconstruction side, the plugin is deliberately partition-lossy:
 * {@code __name__} and {@code resourceId} land as intrinsic tags; everything
 * else becomes a meta tag. The round-trip through the query API does not
 * preserve which labels were originally intrinsic/meta/external on the
 * write side.
 */
public final class PromResponseParser {

    private PromResponseParser() {}

    /**
     * Parse a {@code /api/v1/series} response into a list of Metric objects.
     * Every label pair becomes a tag; {@code __name__} becomes intrinsic
     * {@link IntrinsicTagNames#name}; {@code resourceId} becomes intrinsic
     * {@link IntrinsicTagNames#resourceId}; everything else becomes a meta
     * tag.
     */
    public static List<Metric> parseSeriesResponse(String json) throws StorageException {
        try {
            JSONObject root = parseJson(json);
            requireSuccess(root);
            JSONArray data = root.optJSONArray("data");
            if (data == null) return List.of();
            List<Metric> out = new ArrayList<>(data.length());
            for (int i = 0; i < data.length(); i++) {
                JSONObject labels = data.getJSONObject(i);
                out.add(labelObjectToMetric(labels));
            }
            return out;
        } catch (JSONException | IllegalStateException e) {
            throw new StorageException(
                "failed to parse Prometheus /series response: " + e.getMessage(), e);
        }
    }

    /**
     * Result of {@link #parseRangeResponseWithMetric}: the merged datapoints
     * plus a Metric reconstructed from the first matched series' labels (or
     * the supplied fallback Metric when the matrix is empty).
     *
     * <p>The Metric is the load-bearing reason this method exists — OpenNMS-
     * core's {@code NewtsConverterUtils.dataPointToRow} dereferences
     * {@code MetaTagNames.mtype} on the returned Metric, so we must surface
     * the actual Prometheus-stored mtype label rather than echoing the
     * inbound request Metric (which OpenNMS constructs with only
     * {@code resourceId} and {@code name} — no mtype).
     *
     * <p>{@code points} is wrapped in {@link List#copyOf} at construction to
     * make the result effectively immutable — callers can't mutate the
     * merged datapoint list and accidentally corrupt what we hand to OpenNMS.
     */
    public record RangeResult(Metric metric, List<DataPoint> points) {
        public RangeResult {
            points = List.copyOf(points);
        }
    }

    /**
     * Parse a {@code /api/v1/query_range} matrix response into a list of
     * DataPoints. When the response contains multiple series (selector was not
     * unique), points from all series are merged in order, with duplicates
     * removed (same-timestamp entries keep the last value — matching the
     * write-side last-write-wins dedup). Prometheus returns non-finite values
     * as the literal strings {@code "NaN"}, {@code "+Inf"}, {@code "-Inf"},
     * which this method translates to {@link Double#NaN} /
     * {@link Double#POSITIVE_INFINITY} / {@link Double#NEGATIVE_INFINITY}.
     *
     * <p>Prefer {@link #parseRangeResponseWithMetric} for production code —
     * it surfaces the response's actual labels (which carry {@code mtype})
     * rather than discarding them.
     */
    public static List<DataPoint> parseRangeResponse(String json) throws StorageException {
        try {
            JSONObject root = parseJson(json);
            requireSuccess(root);
            JSONObject data = root.optJSONObject("data");
            if (data == null) return List.of();
            JSONArray results = data.optJSONArray("result");
            if (results == null || results.isEmpty()) return List.of();

            // Merge datapoints across all series in the matrix. Duplicate
            // timestamps collapse via last-write-wins to match the write-side
            // contract.
            java.util.TreeMap<Long, Double> byTimestamp = new java.util.TreeMap<>();
            for (int r = 0; r < results.length(); r++) {
                JSONArray values = results.getJSONObject(r).optJSONArray("values");
                if (values == null) continue;
                for (int i = 0; i < values.length(); i++) {
                    JSONArray pair = values.getJSONArray(i);
                    double unixTs = pair.getDouble(0);
                    double value  = parsePromValue(pair.getString(1));
                    long millis   = Math.round(unixTs * 1_000.0);
                    byTimestamp.put(millis, value);
                }
            }

            List<DataPoint> out = new ArrayList<>(byTimestamp.size());
            for (java.util.Map.Entry<Long, Double> e : byTimestamp.entrySet()) {
                out.add(new ImmutableDataPoint(Instant.ofEpochMilli(e.getKey()), e.getValue()));
            }
            return out;
        } catch (JSONException | IllegalStateException | NumberFormatException e) {
            throw new StorageException(
                "failed to parse Prometheus /query_range response: " + e.getMessage(), e);
        }
    }

    /**
     * Parse a {@code /api/v1/query_range} matrix response into a Metric +
     * merged DataPoints. The Metric is reconstructed from the first matched
     * series' labels via {@link #labelObjectToMetric}; when the matrix is
     * empty the supplied {@code fallbackMetric} is returned unchanged.
     *
     * <p>DataPoint merge semantics match {@link #parseRangeResponse}: across
     * multiple series, points are merged with last-write-wins on duplicate
     * timestamps. In practice every series matched by a single OpenNMS
     * resource+attribute query carries the same {@code mtype} on the write
     * side, so taking the first series' labels for the Metric is correct.
     */
    public static RangeResult parseRangeResponseWithMetric(String json, Metric fallbackMetric)
            throws StorageException {
        try {
            JSONObject root = parseJson(json);
            requireSuccess(root);
            JSONObject data = root.optJSONObject("data");
            if (data == null) return new RangeResult(fallbackMetric, List.of());
            JSONArray results = data.optJSONArray("result");
            if (results == null || results.isEmpty()) {
                return new RangeResult(fallbackMetric, List.of());
            }

            // Reconstruct the Metric from the first series' labels. Empty
            // label objects (synthetic ranges that Prometheus serializes
            // with `"metric": {}`) fall back to the request Metric — there's
            // nothing to reconstruct from.
            JSONObject firstLabels = results.getJSONObject(0).optJSONObject("metric");
            Metric metric = (firstLabels != null && !firstLabels.isEmpty())
                    ? labelObjectToMetric(firstLabels)
                    : fallbackMetric;

            java.util.TreeMap<Long, Double> byTimestamp = new java.util.TreeMap<>();
            for (int r = 0; r < results.length(); r++) {
                JSONArray values = results.getJSONObject(r).optJSONArray("values");
                if (values == null) continue;
                for (int i = 0; i < values.length(); i++) {
                    JSONArray pair = values.getJSONArray(i);
                    double unixTs = pair.getDouble(0);
                    double value  = parsePromValue(pair.getString(1));
                    long millis   = Math.round(unixTs * 1_000.0);
                    byTimestamp.put(millis, value);
                }
            }

            List<DataPoint> points = new ArrayList<>(byTimestamp.size());
            for (java.util.Map.Entry<Long, Double> e : byTimestamp.entrySet()) {
                points.add(new ImmutableDataPoint(Instant.ofEpochMilli(e.getKey()), e.getValue()));
            }
            return new RangeResult(metric, points);
        } catch (JSONException | IllegalStateException | NumberFormatException e) {
            throw new StorageException(
                "failed to parse Prometheus /query_range response: " + e.getMessage(), e);
        }
    }

    /** Translate Prometheus's textual form of non-finite doubles into Java's. */
    static double parsePromValue(String raw) {
        if (raw == null) throw new NumberFormatException("null value");
        switch (raw) {
            case "NaN":    return Double.NaN;
            case "+Inf":   return Double.POSITIVE_INFINITY;
            case "-Inf":   return Double.NEGATIVE_INFINITY;
            case "Inf":    return Double.POSITIVE_INFINITY;
            default:       return Double.parseDouble(raw);
        }
    }

    private static Metric labelObjectToMetric(JSONObject labels) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder();
        for (String key : labels.keySet()) {
            // Use optString so an unexpectedly null / nested-object value
            // becomes an empty string rather than throwing JSONException.
            String value = labels.optString(key, "");
            if ("__name__".equals(key)) {
                mb.intrinsicTag(IntrinsicTagNames.name, value);
            } else if (IntrinsicTagNames.resourceId.equals(key)) {
                mb.intrinsicTag(IntrinsicTagNames.resourceId, value);
            } else if (key.length() > LabelMapper.EXTATTR_PREFIX.length()
                    && key.startsWith(LabelMapper.EXTATTR_PREFIX)) {
                // Strip the onms_extattr_ prefix and deposit on the EXTERNAL
                // partition. This is the partition OpenNMS-core's
                // TimeseriesResourceStorageDao.getStringAttributes() reads
                // for resource-graph placeholder substitution (${name},
                // ${datname}, ${spcname}). Bare `onms_extattr_` (empty
                // suffix) falls through to the catch-all below — emitting an
                // external tag with an empty key would be worse than
                // preserving the anomalous label name verbatim.
                //
                // Branch order against the onms_attr_ branch below is
                // functionally irrelevant today: the two prefixes are
                // stem-distinct (they share `onms_` but diverge at the next
                // character — `e` vs `a`), so neither subsumes the other.
                // The else-if chain is written defensively for the
                // hypothetical case where a future reserved prefix would
                // subsume a shorter one; in that scenario the longer prefix
                // must be tested first.
                mb.externalTag(key.substring(LabelMapper.EXTATTR_PREFIX.length()), value);
            } else if (key.length() > LabelMapper.ATTR_PREFIX.length()
                    && key.startsWith(LabelMapper.ATTR_PREFIX)) {
                // Strip the onms_attr_ prefix and deposit on the META
                // partition. Same bare-prefix fall-through behavior as
                // onms_extattr_ above.
                mb.metaTag(key.substring(LabelMapper.ATTR_PREFIX.length()), value);
            } else {
                mb.metaTag(key, value);
            }
        }
        return mb.build();
    }

    private static JSONObject parseJson(String json) {
        if (json == null || json.isEmpty()) {
            throw new IllegalStateException("empty response body");
        }
        return new JSONObject(json);
    }

    private static void requireSuccess(JSONObject root) {
        String status = root.optString("status", "");
        if (!"success".equals(status)) {
            String err = root.optString("error", "(no error field)");
            throw new IllegalStateException(
                "Prometheus API returned status=" + status + ", error=" + err);
        }
    }
}
