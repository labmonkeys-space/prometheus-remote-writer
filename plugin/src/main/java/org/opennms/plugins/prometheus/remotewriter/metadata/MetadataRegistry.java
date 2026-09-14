/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.plugins.prometheus.remotewriter.mapper.IfSpeedNormalizer;
import org.opennms.plugins.prometheus.remotewriter.mapper.MetadataProcessor;

/**
 * One entry per OpenNMS resource the write path has seen, holding what its
 * metadata series should say. The label mapper feeds it on every sample; the
 * {@link MetadataEmitter} drains it on its cadence and on change.
 *
 * <p>Every sample of every metric of a resource carries that resource's full
 * tag set, many times a minute, so {@link #observe} has to be cheap: it hashes
 * the eligible tags without allocating and only builds a snapshot when the
 * hash is new. The hash is 64 bits and order-independent; a collision would
 * hide one change until the next one, which is a risk taken deliberately
 * against comparing maps on the hot path.
 *
 * <p>What counts as an attribute: every meta or external tag except the
 * intrinsics (not walked), {@code mtype}, {@code categories} (rows of its
 * own), {@code ifSpeed} and {@code ifHighSpeed} (a gauge of their own),
 * the keys the data series carry as labels ({@link #LABEL_KEYS}, while the
 * label is on the wire), OpenNMS's {@code cat_<Name>=Name} mirrors of
 * {@code categories}, context
 * keys containing {@code :} (owned by the metadata processor) and the secret
 * denylist; a tag with an empty value is no attribute either. Keys keep
 * their OpenNMS spelling, because a row is a fact about OpenNMS and a graph
 * placeholder is named after it.
 */
public final class MetadataRegistry {

    private static final class Entry {
        volatile ResourceMetadata snapshot;
        volatile long hash;
        volatile long lastSeenMs;
        /** {@link Long#MIN_VALUE} until first emitted. */
        volatile long lastEmittedMs = Long.MIN_VALUE;
        volatile boolean dirty = true;
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final LongSupplier clockMillis;
    /** The {@link #LABEL_KEYS} whose label is on the wire, so no row is needed. */
    private final Set<String> rowlessKeys;

    /**
     * @param rowlessKeys the source keys to skip as rows because their label
     *                    is emitted: {@link #LABEL_KEYS} minus what
     *                    {@code labels.exclude} removes, see
     *                    {@code PrometheusRemoteWriterConfig#metadataRowlessKeys()}
     */
    public MetadataRegistry(LongSupplier clockMillis, Set<String> rowlessKeys) {
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
        this.rowlessKeys = Set.copyOf(Objects.requireNonNull(rowlessKeys, "rowlessKeys"));
    }

    public MetadataRegistry(LongSupplier clockMillis) {
        this(clockMillis, LABEL_KEYS.keySet());
    }

    public MetadataRegistry() {
        this(System::currentTimeMillis);
    }

    /**
     * Record what {@code metric} says about {@code resourceId}.
     *
     * @return true when the resource is new or its metadata changed, so its
     *         series are due at once
     */
    public boolean observe(String resourceId, Metric metric) {
        Objects.requireNonNull(resourceId, "resourceId");
        Objects.requireNonNull(metric, "metric");
        long now = clockMillis.getAsLong();
        long hash = hashOf(metric);
        Entry e = entries.get(resourceId);
        if (e != null && e.hash == hash) {
            e.lastSeenMs = now;
            return false;
        }
        ResourceMetadata snapshot = snapshotOf(resourceId, metric);
        entries.compute(resourceId, (k, old) -> {
            Entry entry = old == null ? new Entry() : old;
            if (entry.hash != hash || entry.snapshot == null) {
                entry.snapshot = snapshot;
                entry.hash = hash;
                entry.dirty = true;
            }
            entry.lastSeenMs = now;
            return entry;
        });
        return true;
    }

    /**
     * The resources whose series are due: new or changed since the last
     * emission, or last emitted at least {@code cadenceMs} ago. Does not mark
     * anything; the emitter calls {@link #markEmitted} once the pipeline has
     * taken the rows, so a refused or failed emission is retried next tick
     * rather than waiting out a cadence.
     */
    public List<ResourceMetadata> dueForEmission(long cadenceMs) {
        long now = clockMillis.getAsLong();
        List<ResourceMetadata> due = new ArrayList<>();
        for (Entry e : entries.values()) {
            synchronized (e) {
                boolean cadenceElapsed = e.lastEmittedMs != Long.MIN_VALUE && now - e.lastEmittedMs >= cadenceMs;
                if (e.dirty || cadenceElapsed) due.add(e.snapshot);
            }
        }
        return due;
    }

    /**
     * The rows for these snapshots reached the pipeline. A snapshot that has
     * been replaced since it was taken stays dirty, so the newer metadata is
     * still emitted.
     */
    public void markEmitted(List<ResourceMetadata> emitted) {
        long now = clockMillis.getAsLong();
        for (ResourceMetadata r : emitted) {
            Entry e = entries.get(r.resourceId());
            if (e == null) continue;
            synchronized (e) {
                if (e.snapshot == r) e.dirty = false;
                e.lastEmittedMs = now;
            }
        }
    }

    /** Forget resources not observed within {@code maxIdleMs}. Returns how many. */
    public int expire(long maxIdleMs) {
        long now = clockMillis.getAsLong();
        int removed = 0;
        for (Map.Entry<String, Entry> me : entries.entrySet()) {
            if (now - me.getValue().lastSeenMs > maxIdleMs && entries.remove(me.getKey(), me.getValue())) {
                removed++;
            }
        }
        return removed;
    }

    /** Resources currently held. */
    public int size() {
        return entries.size();
    }

    // -- eligibility and snapshot --------------------------------------------

    /**
     * Source keys whose value every data series of the resource carries as
     * a label (the mapper's default set), mapped to that label. They are not
     * rows while the label is on the wire: no shipped OpenNMS report
     * dereferences them as a {@code {placeholder}} (Horizon 36's
     * {@code snmp-graph.properties.d} reads {@code ifName} in the flow
     * reports, {@code ifSpeed}, {@code ifHighSpeed} and the info-column keys,
     * and none of these), and the read path hands them back from the label.
     * {@code ifName} stays a row although it is a label too, and
     * {@code nodeId} is a row because its label {@code node} carries
     * {@code foreignSource:foreignId} when both are set, not the id.
     */
    public static final Map<String, String> LABEL_KEYS = Map.of(
            "nodeLabel",     "node_label",
            "foreignSource", "foreign_source",
            "foreignId",     "foreign_id",
            "location",      "location");

    /** OpenNMS emits one {@code cat_<Name>=Name} tag per category next to
     *  {@code categories}; the category rows are their wire form. */
    static final String CATEGORY_TAG_PREFIX = "cat_";

    /** Whether a meta or external tag key can be a resource attribute at all. */
    static boolean isAttributeKey(String key) {
        if (key == null || key.isEmpty()) return false;
        if (key.indexOf(':') >= 0) return false;
        if (MetaTagNames.mtype.equals(key)) return false;
        if ("categories".equals(key) || "ifSpeed".equals(key) || "ifHighSpeed".equals(key)) return false;
        return !MetadataProcessor.isPlainKeyDenied(key);
    }

    /** OpenNMS's mirror of a category: {@code cat_<Name>} with the name as value. */
    static boolean isCategoryMirror(Tag t) {
        String key = t.getKey();
        return key != null && key.startsWith(CATEGORY_TAG_PREFIX)
                && key.substring(CATEGORY_TAG_PREFIX.length()).equals(t.getValue());
    }

    /** Whether a tag becomes a row for this registry. */
    private boolean isRow(Tag t) {
        return isAttributeKey(t.getKey()) && !rowlessKeys.contains(t.getKey()) && !isCategoryMirror(t);
    }

    /**
     * Why a key can never be a row for a registry that skips
     * {@code rowlessKeys}, for a validation message; null when it can be
     * one, or is excluded for another reason. A {@code cat_*} key is
     * judged by its prefix here, since the value that tells OpenNMS's
     * mirror from a custom key is not known at configuration time.
     */
    public static String whyNotARow(String key, Set<String> rowlessKeys) {
        if (key == null) return null;
        if (rowlessKeys.contains(key)) {
            return "every data series carries it as the label '" + LABEL_KEYS.get(key) + "'";
        }
        if (key.startsWith(CATEGORY_TAG_PREFIX)) return "categories are the onms_resource_category rows";
        return null;
    }

    /** Whether a tag contributes to the snapshot at all. */
    private boolean isUsedTag(Tag t) {
        String key = t.getKey();
        return "categories".equals(key) || "ifSpeed".equals(key) || "ifHighSpeed".equals(key) || isRow(t);
    }

    /**
     * Hash of exactly the tags the snapshot is built from. {@code mtype}
     * differs between a resource's counter and gauge metrics, so hashing it
     * would flip the hash on every other sample of the same resource.
     */
    private long hashOf(Metric metric) {
        long sum = 0L;
        int count = 0;
        for (Tag t : metric.getMetaTags())     { if (isUsedTag(t)) { sum += tagHash(t); count++; } }
        for (Tag t : metric.getExternalTags()) { if (isUsedTag(t)) { sum += tagHash(t); count++; } }
        return mix(sum) ^ count;
    }

    private static long tagHash(Tag t) {
        long k = t.getKey() == null ? 0L : t.getKey().hashCode();
        long v = t.getValue() == null ? 0L : t.getValue().hashCode();
        return mix((k << 32) ^ (v & 0xffffffffL));
    }

    /** SplitMix64 finaliser: spreads the bits of a sum so order-independence
     *  does not cost distribution. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    private ResourceMetadata snapshotOf(String resourceId, Metric metric) {
        TreeMap<String, String> attributes = new TreeMap<>();
        TreeSet<String> categories = new TreeSet<>();
        String ifSpeed = null, ifHighSpeed = null;
        // Meta first, then external, first one wins: the same precedence the
        // label mapper's merged tag view uses.
        for (java.util.Collection<Tag> partition : List.of(metric.getMetaTags(), metric.getExternalTags())) {
            for (Tag t : partition) {
                String key = t.getKey();
                String value = t.getValue();
                // An empty value is no attribute: an unaliased interface has
                // ifAlias="", and Prometheus treats an empty label as absent,
                // so emitting it would make a series the plugin misdescribes.
                if (key == null || value == null || value.isEmpty()) continue;
                if ("categories".equals(key)) {
                    for (String c : value.split(",")) {
                        String trimmed = c.trim();
                        if (!trimmed.isEmpty()) categories.add(trimmed);
                    }
                } else if ("ifSpeed".equals(key)) {
                    if (ifSpeed == null) ifSpeed = value;
                } else if ("ifHighSpeed".equals(key)) {
                    if (ifHighSpeed == null) ifHighSpeed = value;
                } else if (isRow(t)) {
                    attributes.putIfAbsent(key, value);
                }
            }
        }
        Long ifSpeedBps = IfSpeedNormalizer.normalize(ifHighSpeed, ifSpeed);
        return new ResourceMetadata(resourceId,
                Collections.unmodifiableSortedMap(attributes),
                Collections.unmodifiableSortedSet(categories),
                ifSpeedBps);
    }
}
