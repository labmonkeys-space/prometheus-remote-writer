/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metadata.MetadataEmitter;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restores a resource's string attributes and categories from the metadata
 * rows ({@code onms_resource_attr}, {@code onms_resource_category}) onto the
 * {@link Metric}s that {@code findMetrics} returns, so OpenNMS graph
 * placeholders ({@code ${name}}, {@code ${hrStorageDescr}}, …) resolve from
 * what is on the wire and nothing has to be curated for correctness.
 *
 * <p>One instant query per batch of {@code read.discovery-batch-size}
 * resources, never one per metric, capped like two-phase discovery and sent
 * as a POST so the batch is not bounded by a request-line limit. The
 * selector always carries the resource list, so it never selects the whole
 * attribute space, and it is scoped to this instance when {@code instance.id}
 * is set. The window is the registry's expiry, so a resource that has been
 * idle, or unseen since a restart, keeps its attributes for as long as the
 * write path would remember it. That is shorter than the discovery lookback:
 * a resource the write path has not seen for longer than the expiry is still
 * listed, but comes back without attributes.
 *
 * <p>A rename leaves the old row inside the window, so rows are taken from
 * the resource's newest emission. The rows of one emission can land moments
 * apart (they shard independently, and a refused batch is retried), so a row
 * within half a cadence of the newest stamp counts as current, and the newest
 * row per key wins. A removed attribute or category therefore lingers until
 * the next cadence emission.
 *
 * <p>Enrichment is best effort. A failed metadata query is logged and its
 * resources go back unenriched: a resource listing must never fail because
 * its aliases could not be read. No cache: OpenNMS already caches in front
 * of {@code findMetrics}, and a second cache would serve a stale alias for
 * its TTL after exactly the operator action the metadata series exist to
 * survive.
 */
final class ResourceMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceMetadataReader.class);

    /** A form POST returning the response body; the read client's own HTTP path. */
    @FunctionalInterface
    interface Http {
        String post(String url, String form) throws StorageException;
    }

    private static final String ROW_METRICS =
            MetadataEmitter.ATTR_METRIC + "|" + MetadataEmitter.CATEGORY_METRIC;
    private static final long WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    /** The subquery step: the default lookback delta of Prometheus, Mimir
     *  and Cortex, so every sample falls inside the lookback of some
     *  evaluation point. A backend with a lookback delta below this misses
     *  emissions between the points. */
    static final String SUBQUERY_STEP = "5m";

    private final PrometheusRemoteWriterConfig config;
    private final Http http;
    private final PluginMetrics metrics;
    private volatile long lastWarnNanos = System.nanoTime() - WARN_INTERVAL_NANOS;

    ResourceMetadataReader(PrometheusRemoteWriterConfig config, Http http) {
        this(config, http, null);
    }

    ResourceMetadataReader(PrometheusRemoteWriterConfig config, Http http, PluginMetrics metrics) {
        this.config = Objects.requireNonNull(config, "config");
        this.http = Objects.requireNonNull(http, "http");
        this.metrics = metrics;
    }

    /**
     * The metrics, each with its resource's attributes and categories added,
     * or unchanged when the metadata could not be read.
     */
    List<Metric> enrich(List<Metric> metrics) {
        if (metrics.isEmpty() || config.getMetadataCadenceMs() <= 0) return metrics;
        try {
            return doEnrich(metrics);
        } catch (StorageException | RuntimeException e) {
            warn("metadata rows could not be read; returning {} metric(s) without attributes: {}",
                    metrics.size(), e.getMessage());
            return metrics;
        }
    }

    private List<Metric> doEnrich(List<Metric> metrics) throws StorageException {
        List<String> resourceIds = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Metric m : metrics) {
            String rid = resourceIdOf(m);
            if (rid != null && seen.add(rid)) resourceIds.add(rid);
        }
        if (resourceIds.isEmpty()) return metrics;

        int batchSize = Math.max(1, config.getDiscoveryBatchSize());
        int batches = (resourceIds.size() + batchSize - 1) / batchSize;
        if (batches > PrometheusReadClient.MAX_PHASE2_CALLS) {
            warn("metadata enrichment would issue {} queries for {} resource(s) at "
                    + "read.discovery-batch-size={} (cap {}); returning them without attributes",
                    batches, resourceIds.size(), batchSize, PrometheusReadClient.MAX_PHASE2_CALLS);
            return metrics;
        }

        // Per resource: the rows of its newest emission. Every row is valued
        // with its newest sample's stamp; the max per resource identifies the
        // emission, and an older row (the value before a rename, a category
        // since removed) inside the window is left out. A batch that fails
        // leaves only its own resources unenriched.
        Map<String, Long> newest = new HashMap<>();
        List<Row> rows = new ArrayList<>();
        String url = config.getReadUrl() + "/api/v1/query";
        for (int from = 0; from < resourceIds.size(); from += batchSize) {
            List<String> chunk = resourceIds.subList(from, Math.min(from + batchSize, resourceIds.size()));
            if (this.metrics != null) this.metrics.findMetricsEnrichmentBatches(1);
            String body;
            try {
                body = http.post(url, "query=" + PrometheusReadClient.urlEncode(query(chunk)));
            } catch (StorageException | RuntimeException e) {
                warn("metadata rows could not be read for {} resource(s); returning them without attributes: {}",
                        chunk.size(), e.getMessage());
                continue;
            }
            for (PromResponseParser.InstantSample s : PromResponseParser.parseInstantVector(body)) {
                String rid = s.labels().get(IntrinsicTagNames.resourceId);
                if (rid == null || !Double.isFinite(s.value())) continue;
                long stampMs = Math.round(s.value() * 1000.0);
                rows.add(new Row(rid, s.labels(), stampMs));
                newest.merge(rid, stampMs, Math::max);
            }
        }
        if (rows.isEmpty()) return metrics;

        // The rows of one emission can land moments apart, so anything within
        // half a cadence of the newest stamp is current; in ascending stamp
        // order the newest row per key wins. A function result carries no
        // __name__, so a row is told apart by its own labels: key/value is an
        // attribute, category a category.
        long tolerance = config.getMetadataCadenceMs() / 2;
        rows.sort(Comparator.comparingLong(Row::stampMs));
        Map<String, Map<String, String>> attributes = new HashMap<>();
        Map<String, Set<String>> categories = new HashMap<>();
        for (Row r : rows) {
            if (r.stampMs() < newest.get(r.resourceId()) - tolerance) continue;
            String key = r.labels().get("key");
            String value = r.labels().get("value");
            String category = r.labels().get("category");
            if (key != null && value != null) {
                attributes.computeIfAbsent(r.resourceId(), k -> new TreeMap<>()).put(key, value);
            } else if (category != null) {
                categories.computeIfAbsent(r.resourceId(), k -> new TreeSet<>()).add(category);
            }
        }

        List<Metric> out = new ArrayList<>(metrics.size());
        for (Metric m : metrics) {
            String rid = resourceIdOf(m);
            Map<String, String> attrs = rid == null ? null : attributes.get(rid);
            Set<String> cats = rid == null ? null : categories.get(rid);
            out.add(attrs == null && cats == null ? m : withMetadata(m, attrs, cats));
        }
        return out;
    }

    private record Row(String resourceId, Map<String, String> labels, long stampMs) {}

    /**
     * {@code timestamp(sel) or max_over_time(timestamp(sel)[window:5m])} with
     * {@code sel = {__name__=~"attr|category"[, onms_instance_id="…"],
     * resourceId=~"^(…)$"}}: every row series inside the window, valued with
     * the timestamp of its newest sample, so the reader can tell a resource's
     * latest emission from an older one. {@code timestamp()} yields the
     * sample's own time only on a raw selector, and a raw selector sees only
     * the lookback delta, so the subquery evaluates it at the delta's default
     * step across the window. Subquery steps are aligned, so the last one can
     * sit up to a step before now; the plain {@code timestamp(sel)} on the
     * left covers that head exactly, and {@code or} keeps it for a series
     * present in both. The selector comes from the same builder two-phase
     * discovery uses, escaping included.
     */
    private String query(List<String> resourceIds) {
        List<TagMatcher> matchers = new ArrayList<>(2);
        matchers.add(ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS_REGEX).key(IntrinsicTagNames.name).value(ROW_METRICS).build());
        String instanceId = config.getInstanceId();
        if (instanceId != null && !instanceId.isEmpty()) {
            matchers.add(ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS).key(MetadataEmitter.INSTANCE_ID_LABEL).value(instanceId).build());
        }
        String sel = PromQLBuilder.fromMatchersWithResourceIdAlternation(matchers, resourceIds);
        return "timestamp(" + sel + ") or max_over_time(timestamp(" + sel + ")["
                + windowSeconds() + "s:" + SUBQUERY_STEP + "])";
    }

    /** The registry's expiry, and never less than two cadences. */
    private long windowSeconds() {
        long ms = Math.max(2 * config.getMetadataCadenceMs(), MetadataEmitter.EXPIRE_AFTER_MS);
        return Math.max(1L, TimeUnit.MILLISECONDS.toSeconds(ms));
    }

    private static String resourceIdOf(Metric m) {
        for (Tag t : m.getIntrinsicTags()) {
            if (IntrinsicTagNames.resourceId.equals(t.getKey())) return t.getValue();
        }
        return null;
    }

    /**
     * The metric with the rows deposited. The rows win over an external tag
     * or a {@code categories} meta tag the series already carries: those
     * came from labels on the data series, which can be as old as the series
     * lookback, while the rows are the resource's newest emission.
     */
    private static Metric withMetadata(Metric m, Map<String, String> attrs, Set<String> cats) {
        ImmutableMetric.MetricBuilder b = ImmutableMetric.builder();
        for (Tag t : m.getIntrinsicTags()) b.intrinsicTag(t.getKey(), t.getValue());
        boolean replaceCategories = cats != null && !cats.isEmpty();
        for (Tag t : m.getMetaTags()) {
            if (replaceCategories && "categories".equals(t.getKey())) continue;
            b.metaTag(t.getKey(), t.getValue());
        }
        for (Tag t : m.getExternalTags()) {
            if (attrs != null && attrs.containsKey(t.getKey())) continue;
            b.externalTag(t.getKey(), t.getValue());
        }
        if (attrs != null) {
            for (Map.Entry<String, String> a : attrs.entrySet()) b.externalTag(a.getKey(), a.getValue());
        }
        if (replaceCategories) b.metaTag("categories", String.join(",", cats));
        return b.build();
    }

    private void warn(String message, Object... args) {
        long now = System.nanoTime();
        if (now - lastWarnNanos < WARN_INTERVAL_NANOS) return;
        lastWarnNanos = now;
        LOG.warn(message, args);
    }
}
