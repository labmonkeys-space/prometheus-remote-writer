/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns the {@link MetadataRegistry} into series on the wire, per resource,
 * on first sight, on change, and otherwise every cadence:
 * <ul>
 *   <li>{@code onms_resource_attr{resourceId,key,value} 1}, one row per
 *       attribute up to the budget;</li>
 *   <li>{@code onms_resource_category{resourceId,category} 1}, one row per
 *       surveillance category;</li>
 *   <li>{@code onms_resource_info{resourceId,<column>…} 1}, the configured
 *       columns ({@link InfoColumns}), for query ergonomics;</li>
 *   <li>{@code onms_resource_ifspeed{resourceId}}, the interface speed in
 *       bits per second as a gauge.</li>
 * </ul>
 *
 * <p>The rows have fixed label names whatever OpenNMS supplies, so an
 * attribute key nobody has seen before cannot grow the backend's label-name
 * index; cardinality moves to series count, which the per-resource budget
 * bounds and {@code metadata_attrs_dropped_total} makes visible. The info
 * series is the one place operator-chosen label names reach the wire, which
 * is why {@link InfoColumns#parse} validates them.
 *
 * <p>Metadata samples go through the write pipeline like any other, in
 * batches of the emitter's own, never folded into a data request.
 * {@link #emitDue()} is the whole unit of work and is synchronous, so tests
 * drive it with a clock and no thread; {@link #start()} runs it every second.
 */
public final class MetadataEmitter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataEmitter.class);

    public static final String ATTR_METRIC     = "onms_resource_attr";
    public static final String CATEGORY_METRIC = "onms_resource_category";
    /** Interface speed in bits per second, a gauge so it multiplies as-is. */
    public static final String IFSPEED_METRIC  = "onms_resource_ifspeed";
    /** The configured columns, one series per resource that has any of them. */
    public static final String INFO_METRIC     = "onms_resource_info";
    /** Labels every metadata series carries; {@link InfoColumns} reserves them. */
    public static final String RESOURCE_ID_LABEL = "resourceId";
    public static final String INSTANCE_ID_LABEL = "onms_instance_id";

    /** A resource not seen for this long is dropped from the registry. */
    static final long EXPIRE_AFTER_MS = TimeUnit.HOURS.toMillis(24);
    /** How often the thread looks for due resources. */
    static final long TICK_MS = 1_000L;
    /** Samples per call to the sink. */
    static final int BATCH = 1_000;

    /** @param instanceId  {@code instance.id}, stamped on every metadata series
     *                     as {@code onms_instance_id} when set, as on the data series
     *  @param infoColumns column → attribute key, from {@link InfoColumns#parse} */
    public record Settings(long cadenceMs, int attrBudget, String instanceId,
                           Map<String, String> infoColumns) {
        public Settings {
            if (cadenceMs < 1) throw new IllegalArgumentException("cadenceMs must be >= 1");
            if (attrBudget < 1) throw new IllegalArgumentException("attrBudget must be >= 1");
            Objects.requireNonNull(infoColumns, "infoColumns");
        }
    }

    private final MetadataRegistry registry;
    private final Settings settings;
    /** Offers a batch to the pipeline and returns how many samples were refused. */
    private final ToIntFunction<List<MappedSample>> sink;
    private final PluginMetrics metrics;
    private final LongSupplier clockMillis;

    private volatile boolean running;
    private Thread thread;
    // Zero, not Long.MIN_VALUE: `now - MIN_VALUE` overflows negative for any
    // real clock, which would make both "at least a minute ago" checks false
    // for the life of the process.
    private long lastExpiryMs;
    private long lastSinkErrorMs;

    /** @param sink offers a batch to the write pipeline and returns how many
     *              of its samples were refused; a refused or thrown batch is
     *              retried next tick */
    public MetadataEmitter(MetadataRegistry registry, Settings settings,
                           ToIntFunction<List<MappedSample>> sink, PluginMetrics metrics,
                           LongSupplier clockMillis) {
        this.registry    = Objects.requireNonNull(registry, "registry");
        this.settings    = Objects.requireNonNull(settings, "settings");
        this.sink        = Objects.requireNonNull(sink, "sink");
        this.metrics     = Objects.requireNonNull(metrics, "metrics");
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
    }

    /**
     * Emit every resource that is due. Returns the number of samples handed
     * to the sink.
     */
    public int emitDue() {
        long now = clockMillis.getAsLong();
        List<MappedSample> batch = new ArrayList<>(BATCH);
        List<ResourceMetadata> inBatch = new ArrayList<>();
        List<ResourceMetadata> empty = new ArrayList<>();
        int emitted = 0;
        for (ResourceMetadata r : registry.dueForEmission(settings.cadenceMs())) {
            if (r.isEmpty()) { empty.add(r); continue; }
            samplesFor(r, now, batch);
            inBatch.add(r);
            if (batch.size() >= BATCH) {
                emitted += flush(batch, inBatch);
            }
        }
        emitted += flush(batch, inBatch);
        // Nothing to say is still "emitted": otherwise it stays due forever.
        if (!empty.isEmpty()) registry.markEmitted(empty);
        return emitted;
    }

    /** Append the rows for one resource to {@code out}; returns how many. */
    int samplesFor(ResourceMetadata r, long now, List<MappedSample> out) {
        String resourceId = Sanitizer.labelValue(r.resourceId());
        int before = out.size();
        int kept = 0;
        for (Map.Entry<String, String> a : r.attributes().entrySet()) {
            if (kept == settings.attrBudget()) break;
            Map<String, String> labels = base(ATTR_METRIC, resourceId);
            labels.put("key", Sanitizer.labelValue(a.getKey()));
            labels.put("value", Sanitizer.labelValue(a.getValue()));
            out.add(new MappedSample(labels, now, 1.0, now));
            kept++;
        }
        int dropped = r.attributes().size() - kept;
        if (dropped > 0) metrics.metadataAttrsDropped(dropped);
        for (String category : r.categories()) {
            Map<String, String> labels = base(CATEGORY_METRIC, resourceId);
            labels.put("category", Sanitizer.labelValue(category));
            out.add(new MappedSample(labels, now, 1.0, now));
        }
        // Columns read the whole attribute set, not the budgeted rows: the
        // budget bounds series count, and one info series per resource is
        // fixed whatever it carries.
        Map<String, String> info = null;
        for (Map.Entry<String, String> column : settings.infoColumns().entrySet()) {
            String value = r.attributes().get(column.getValue());
            if (value == null) continue;
            if (info == null) info = base(INFO_METRIC, resourceId);
            info.put(column.getKey(), Sanitizer.labelValue(value));
        }
        if (info != null) out.add(new MappedSample(info, now, 1.0, now));
        if (r.ifSpeedBps() != null) {
            out.add(new MappedSample(base(IFSPEED_METRIC, resourceId), now, (double) r.ifSpeedBps(), now));
        }
        return out.size() - before;
    }

    private Map<String, String> base(String metricName, String resourceId) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put(MappedSample.METRIC_NAME_LABEL, metricName);
        labels.put(RESOURCE_ID_LABEL, resourceId);
        if (settings.instanceId() != null && !settings.instanceId().isEmpty()) {
            labels.put(INSTANCE_ID_LABEL, Sanitizer.labelValue(settings.instanceId()));
        }
        return labels;
    }

    /**
     * Offer the batch; on full acceptance mark its resources emitted. A
     * refusal or an exception leaves them due, so the next tick retries; the
     * rows the pipeline did take are re-sent then, which a backend takes as
     * identical samples.
     */
    private int flush(List<MappedSample> batch, List<ResourceMetadata> inBatch) {
        if (batch.isEmpty()) return 0;
        List<MappedSample> copy = new ArrayList<>(batch);
        List<ResourceMetadata> resources = new ArrayList<>(inBatch);
        batch.clear();
        inBatch.clear();
        int refused = sink.applyAsInt(copy);
        int taken = copy.size() - refused;
        metrics.metadataSeriesEmitted(taken);
        if (refused == 0) registry.markEmitted(resources);
        return taken;
    }

    // -- thread ---------------------------------------------------------------

    public synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::run, "prometheus-remote-writer-metadata");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stop the thread. It is joined first and interrupted only if it does
     * not return: an interrupt landing inside the sink would close the shard's
     * WAL segment under a write, and the thread is never more than a tick
     * plus one batch away from noticing {@code running}.
     */
    public synchronized void stop() {
        running = false;
        Thread t = thread;
        thread = null;
        if (t == null) return;
        try {
            t.join(TICK_MS + 2_000);
            if (t.isAlive()) {
                LOG.warn("metadata emitter did not stop within {}ms, interrupting", TICK_MS + 2_000);
                t.interrupt();
                t.join(1_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        LOG.info("metadata emitter started (cadence-ms={}, attr-budget={}, info-columns={})",
                settings.cadenceMs(), settings.attrBudget(), settings.infoColumns());
        while (running) {
            try {
                Thread.sleep(TICK_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!running) break;
            tick();
        }
        LOG.info("metadata emitter stopped");
    }

    /** One tick: emit what is due, and expire once a minute. */
    void tick() {
        long now = clockMillis.getAsLong();
        try {
            emitDue();
        } catch (RuntimeException e) {
            // A disk-tier failure in the sink is already a StorageException
            // for OpenNMS's writers; here it would repeat every second.
            if (now - lastSinkErrorMs >= TimeUnit.MINUTES.toMillis(1)) {
                lastSinkErrorMs = now;
                LOG.warn("metadata emission failed; retrying every tick: {}", e.getMessage(), e);
            }
        }
        if (now - lastExpiryMs >= TimeUnit.MINUTES.toMillis(1)) {
            lastExpiryMs = now;
            int expired = registry.expire(EXPIRE_AFTER_MS);
            if (expired > 0) LOG.debug("forgot {} resource(s) not seen for 24 h", expired);
        }
    }
}
