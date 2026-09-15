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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.plugins.prometheus.remotewriter.mapper.IfSpeedNormalizer;
import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;
import org.opennms.plugins.prometheus.remotewriter.mapper.MetadataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>What counts as an attribute: a meta or external tag whose key is shaped
 * like an OpenNMS collector alias ({@link #isIdentifierShaped}), except the
 * intrinsics (not walked), {@code mtype}, {@code categories} (rows of its
 * own), {@code ifSpeed} and {@code ifHighSpeed} (a gauge of their own),
 * the keys the data series carry as labels ({@link #LABEL_KEYS}, while the
 * label is on the wire), OpenNMS's {@code cat_<Name>=Name} mirrors of
 * {@code categories}, context
 * keys containing {@code :} (owned by the metadata processor) and the secret
 * denylist; a tag with an empty value is no attribute either. Keys keep
 * their OpenNMS spelling, because a row is a fact about OpenNMS and a graph
 * placeholder is named after it.
 *
 * <p>The shape rule is what keeps a resource's metadata a property of the
 * resource. OpenNMS attaches per-metric meta tags whose key is the metric's
 * identity, and treating those as attributes made the snapshot change with
 * whichever metric was observed: the hash flapped, so the cadence never
 * bound, and the key space grew without limit (#223). {@code metadata
 * .attr-exclude} and {@code metadata.attr-include} let an operator correct
 * the rule in either direction without a release.
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

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRegistry.class);

    /** At most one churn WARN in this span, however many resources flap. */
    static final long CHURN_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(10);
    /** Keys named in one churn WARN before it says "and more". */
    private static final int CHURN_LOG_KEYS = 8;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final LongSupplier clockMillis;
    /** The {@link #LABEL_KEYS} whose label is on the wire, so no row is needed. */
    private final Set<String> rowlessKeys;
    /** {@code metadata.attr-exclude}: keys the shape rule admitted, dropped. */
    private final List<Pattern> attrExcludeGlobs;
    /** {@code metadata.attr-include}: keys the shape rule rejected, admitted. */
    private final List<Pattern> attrIncludeGlobs;
    /** {@link Long#MIN_VALUE} until the first churn WARN. */
    private final AtomicLong lastChurnLogMs = new AtomicLong(Long.MIN_VALUE);

    /**
     * @param rowlessKeys the source keys to skip as rows because their label
     *                    is emitted: {@link #LABEL_KEYS} minus what
     *                    {@code labels.exclude} removes, see
     *                    {@code PrometheusRemoteWriterConfig#metadataRowlessKeys()}
     * @param attrIncludeGlobs {@code metadata.attr-include}, keys to admit
     *                    although they are not shaped like an attribute key
     * @param attrExcludeGlobs {@code metadata.attr-exclude}, keys to drop
     *                    although they are
     */
    public MetadataRegistry(LongSupplier clockMillis, Set<String> rowlessKeys,
                            List<String> attrIncludeGlobs, List<String> attrExcludeGlobs) {
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
        this.rowlessKeys = Set.copyOf(Objects.requireNonNull(rowlessKeys, "rowlessKeys"));
        this.attrIncludeGlobs = compile(attrIncludeGlobs);
        this.attrExcludeGlobs = compile(attrExcludeGlobs);
    }

    public MetadataRegistry(LongSupplier clockMillis, Set<String> rowlessKeys) {
        this(clockMillis, rowlessKeys, List.of(), List.of());
    }

    public MetadataRegistry(LongSupplier clockMillis) {
        this(clockMillis, LABEL_KEYS.keySet());
    }

    public MetadataRegistry() {
        this(System::currentTimeMillis);
    }

    private static List<Pattern> compile(List<String> globs) {
        Objects.requireNonNull(globs, "globs");
        if (globs.isEmpty()) return List.of();
        List<Pattern> out = new ArrayList<>(globs.size());
        for (String g : globs) out.add(LabelMapper.globToPattern(g));
        return List.copyOf(out);
    }

    private static boolean matchesAny(String s, List<Pattern> patterns) {
        for (Pattern p : patterns) {
            if (p.matcher(s).matches()) return true;
        }
        return false;
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
        // Changed again before the last change was even emitted: this
        // resource is flapping, which costs a series set per observation and
        // makes the cadence meaningless. Read before the compute, so the
        // pair described is the one this call replaces.
        boolean churning = e != null && e.dirty && e.snapshot != null;
        ResourceMetadata previous = churning ? e.snapshot : null;
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
        if (previous != null) reportChurn(resourceId, previous, snapshot, now);
        return true;
    }

    /**
     * One WARN per {@link #CHURN_LOG_INTERVAL_MS}, naming a resource whose
     * metadata changed again before the previous change was emitted and the
     * keys that differ. The counter {@code metadata_resource_changes_total}
     * says how much of this there is; this line says which keys to look at,
     * which is the difference between reading a graph and reading a profile.
     * The key sets are compared only when the line is due.
     */
    private void reportChurn(String resourceId, ResourceMetadata previous,
                             ResourceMetadata current, long now) {
        long last = lastChurnLogMs.get();
        if (last != Long.MIN_VALUE && now - last < CHURN_LOG_INTERVAL_MS) return;
        if (!lastChurnLogMs.compareAndSet(last, now)) return;
        TreeSet<String> differing = new TreeSet<>(previous.attributes().keySet());
        differing.addAll(current.attributes().keySet());
        differing.removeIf(k -> Objects.equals(previous.attributes().get(k),
                                               current.attributes().get(k)));
        String named;
        if (differing.isEmpty()) {
            // The attributes are identical, so what moved was a category or
            // the speed. Say so rather than printing an empty list.
            named = previous.categories().equals(current.categories())
                    ? "none; the interface speed changed"
                    : "none; the categories changed";
        } else {
            named = differing.stream().limit(CHURN_LOG_KEYS)
                    .collect(java.util.stream.Collectors.joining(", "));
            if (differing.size() > CHURN_LOG_KEYS) {
                named = named + ", and " + (differing.size() - CHURN_LOG_KEYS) + " more";
            }
        }
        LOG.warn("resource {} changed its metadata again before the last change was emitted; "
                + "attribute keys that differ: [{}]. A key that belongs to one metric rather than "
                + "to the resource makes every metadata series of the resource be re-emitted on "
                + "every sample: exclude it with metadata.attr-exclude. "
                + "metadata_resource_changes_total counts these; this warning is logged at most "
                + "once every {} minutes.",
                resourceId, named, TimeUnit.MILLISECONDS.toMinutes(CHURN_LOG_INTERVAL_MS));
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

    /** Longest key still read as an attribute; every OpenNMS collector alias
     *  is far shorter, the RRD data-source grammar they inherit having capped
     *  them at 19 characters. */
    static final int MAX_ATTRIBUTE_KEY_LENGTH = 64;

    /**
     * Whether a key is shaped like an OpenNMS collector alias: letters,
     * digits, {@code _} and {@code -}, at most
     * {@link #MAX_ATTRIBUTE_KEY_LENGTH} characters.
     *
     * <p>This is what tells a resource's attribute from one metric's
     * identity. OpenNMS names a string attribute with the alias its
     * datacollection gives it ({@code ifName}, {@code ifAlias},
     * {@code hrStorageDescr}), and names a metric with a path:
     * {@code ICMP/10.42.0.1} for latency, {@code SNMP_<oid>.<ifIndex>} for a
     * collected OID, a dotted mbean path for JMX. A path is a property of
     * one metric of the resource, so a snapshot built from it changes with
     * whichever metric was observed.
     *
     * <p>The rule fails closed: an attribute OpenNMS does not name like an
     * attribute is dropped, which costs a graph placeholder and is
     * correctable with {@code metadata.attr-include}, rather than admitted,
     * which costs the backend a series per resource per key. A character
     * loop, not a regular expression: this runs per tag per sample.
     */
    static boolean isIdentifierShaped(String key) {
        int n = key.length();
        if (n == 0 || n > MAX_ATTRIBUTE_KEY_LENGTH) return false;
        for (int i = 0; i < n; i++) {
            char c = key.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                      || (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (!ok) return false;
        }
        return true;
    }

    /** Whether a key is barred from being an attribute whatever its shape and
     *  whatever an operator's globs say: it has a series of its own, it
     *  belongs to the metadata passthrough, or it is credential-shaped. The
     *  keys the data series carry as labels are not barred here; that is
     *  {@link #rowlessKeys}, which {@link #isRow} applies, because it depends
     *  on which labels {@code labels.exclude} left on the wire. */
    private static boolean isStructurallyBlocked(String key) {
        if (key == null || key.isEmpty()) return true;
        if (key.indexOf(':') >= 0) return true;
        if (MetaTagNames.mtype.equals(key)) return true;
        if ("categories".equals(key) || "ifSpeed".equals(key) || "ifHighSpeed".equals(key)) return true;
        return MetadataProcessor.isPlainKeyDenied(key);
    }

    /**
     * Whether a meta or external tag key can be a resource attribute: the
     * structural bars, the operator's globs, then the shape rule. Exclude is
     * tested before include, so a key both admit is dropped; the narrower
     * intent wins, and a key can always be kept out.
     *
     * @param includeGlobs {@code metadata.attr-include}, keys to admit
     *                     although they are not shaped like an attribute key
     * @param excludeGlobs {@code metadata.attr-exclude}, keys to drop
     *                     although they are
     */
    static boolean isAttributeKey(String key, List<Pattern> includeGlobs, List<Pattern> excludeGlobs) {
        if (key == null || key.isEmpty()) return false;
        if (isStructurallyBlocked(key)) return false;
        if (matchesAny(key, excludeGlobs)) return false;
        if (matchesAny(key, includeGlobs)) return true;
        return isIdentifierShaped(key);
    }

    /** {@link #isAttributeKey(String, List, List)} with no operator globs. */
    static boolean isAttributeKey(String key) {
        return isAttributeKey(key, List.of(), List.of());
    }

    /** Compile {@code metadata.attr-include} or {@code metadata.attr-exclude}
     *  for the two-list overloads here and in {@link InfoColumns}. */
    public static List<Pattern> compileGlobs(List<String> globs) {
        return compile(globs);
    }

    /** OpenNMS's mirror of a category: {@code cat_<Name>} with the name as value. */
    static boolean isCategoryMirror(Tag t) {
        String key = t.getKey();
        return key != null && key.startsWith(CATEGORY_TAG_PREFIX)
                && key.substring(CATEGORY_TAG_PREFIX.length()).equals(t.getValue());
    }

    /** Whether a tag becomes a row for this registry. */
    private boolean isRow(Tag t) {
        return isAttributeKey(t.getKey(), attrIncludeGlobs, attrExcludeGlobs)
                && !rowlessKeys.contains(t.getKey()) && !isCategoryMirror(t);
    }

    /**
     * Why a key can never be a row for a registry that skips
     * {@code rowlessKeys}, for a validation message; null when it can be
     * one, or is excluded for another reason. A {@code cat_*} key is
     * judged by its prefix here, since the value that tells OpenNMS's
     * mirror from a custom key is not known at configuration time.
     */
    public static String whyNotARow(String key, Set<String> rowlessKeys) {
        return whyNotARow(key, rowlessKeys, List.of(), List.of());
    }

    /** {@link #whyNotARow(String, Set)} judged with the operator's globs, so
     *  a key {@code metadata.attr-include} admits is a row and one
     *  {@code metadata.attr-exclude} drops is not. */
    public static String whyNotARow(String key, Set<String> rowlessKeys,
                                    List<Pattern> includeGlobs, List<Pattern> excludeGlobs) {
        if (key == null) return null;
        if (rowlessKeys.contains(key)) {
            return "every data series carries it as the label '" + LABEL_KEYS.get(key) + "'";
        }
        if (key.startsWith(CATEGORY_TAG_PREFIX)) return "categories are the onms_resource_category rows";
        if (matchesAny(key, excludeGlobs)) return "metadata.attr-exclude drops it";
        if (matchesAny(key, includeGlobs)) return null;
        if (!isIdentifierShaped(key)) {
            return "it is not shaped like an attribute key (letters, digits, '_' and '-', at most "
                    + MAX_ATTRIBUTE_KEY_LENGTH + " characters), so it reads as one metric's identity "
                    + "rather than a property of the resource; metadata.attr-include admits it";
        }
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
