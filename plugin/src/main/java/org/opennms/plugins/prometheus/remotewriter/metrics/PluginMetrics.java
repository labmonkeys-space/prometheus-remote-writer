/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Plugin-internal metrics registry. Counters are owned by this class and
 * updated by the flush pipeline; gauges are registered with callbacks to the
 * components that already hold authoritative state (SampleQueue, HttpClient,
 * LabelMapper, Storage), so there is a single source of truth per metric.
 */
public final class PluginMetrics {

    public static final String SAMPLES_WRITTEN                 = "samples_written_total";
    public static final String SAMPLES_DROPPED_4XX             = "samples_dropped_4xx_total";
    public static final String SAMPLES_DROPPED_5XX             = "samples_dropped_5xx_total";
    public static final String SAMPLES_DROPPED_TRANSPORT       = "samples_dropped_transport_total";
    public static final String SAMPLES_DROPPED_QUEUE_FULL      = "samples_dropped_queue_full_total";
    public static final String SAMPLES_DROPPED_NONFINITE       = "samples_dropped_nonfinite_total";
    public static final String SAMPLES_DROPPED_DUPLICATE       = "samples_dropped_duplicate_total";
    public static final String SAMPLES_UNPARSEABLE_RESOURCE_ID = "samples_unparseable_resource_id_total";
    public static final String SAMPLES_SYNTHESIZED_MTYPE       = "samples_synthesized_mtype_total";
    public static final String DELETE_NOOP                     = "delete_noop_total";
    public static final String METADATA_DENYLIST_BLOCKED       = "metadata_denylist_blocked_total";
    public static final String QUEUE_DEPTH                     = "queue_depth";
    public static final String HTTP_BYTES_WRITTEN              = "http_bytes_written_total";
    public static final String HTTP_WRITES_SUCCESSFUL          = "http_writes_successful_total";
    public static final String HTTP_WRITES_FAILED              = "http_writes_failed_total";
    public static final String HTTP_IN_FLIGHT                  = "http_in_flight";

    // --- WAL metrics (wal.enabled=true only; gauges registered on start) ---
    public static final String WAL_BYTES_WRITTEN               = "wal_bytes_written_total";
    public static final String WAL_BYTES_CHECKPOINTED          = "wal_bytes_checkpointed_total";
    public static final String WAL_REPLAY_SAMPLES              = "wal_replay_samples_total";
    public static final String WAL_BATCHES_DROPPED_4XX         = "wal_batches_dropped_4xx_total";
    public static final String SAMPLES_DROPPED_WAL_FULL        = "samples_dropped_wal_full_total";
    public static final String WAL_FRAMES_DROPPED_CORRUPTED    = "wal_frames_dropped_corrupted_total";
    public static final String WAL_DISK_USAGE_BYTES            = "wal_disk_usage_bytes";
    public static final String WAL_SEGMENTS_ACTIVE             = "wal_segments_active";

    // --- Read-path discovery metrics ---------------------------------------
    public static final String FIND_METRICS_SINGLE_PASS_TOTAL  = "find_metrics_single_pass_total";
    public static final String FIND_METRICS_TWO_PHASE_TOTAL    = "find_metrics_two_phase_total";
    public static final String FIND_METRICS_PHASE2_BATCHES_TOTAL = "find_metrics_phase2_batches_total";

    private final MetricRegistry registry = new MetricRegistry();
    private final Counter samplesWritten;
    private final Counter samplesDropped4xx;
    private final Counter samplesDropped5xx;
    private final Counter samplesDroppedTransport;
    private final Counter samplesDroppedNonfinite;
    private final Counter samplesDroppedDuplicate;
    private final Counter samplesUnparseableResourceId;
    private final Counter samplesSynthesizedMtype;

    private final Counter walBytesWritten;
    private final Counter walBytesCheckpointed;
    private final Counter walReplaySamples;
    private final Counter walBatchesDropped4xx;
    private final Counter samplesDroppedWalFull;
    private final Counter walFramesDroppedCorrupted;

    private final Counter findMetricsSinglePass;
    private final Counter findMetricsTwoPhase;
    private final Counter findMetricsPhase2Batches;

    public PluginMetrics() {
        this.samplesWritten               = registry.counter(SAMPLES_WRITTEN);
        this.samplesDropped4xx            = registry.counter(SAMPLES_DROPPED_4XX);
        this.samplesDropped5xx            = registry.counter(SAMPLES_DROPPED_5XX);
        this.samplesDroppedTransport      = registry.counter(SAMPLES_DROPPED_TRANSPORT);
        this.samplesDroppedNonfinite      = registry.counter(SAMPLES_DROPPED_NONFINITE);
        this.samplesDroppedDuplicate      = registry.counter(SAMPLES_DROPPED_DUPLICATE);
        this.samplesUnparseableResourceId = registry.counter(SAMPLES_UNPARSEABLE_RESOURCE_ID);
        this.samplesSynthesizedMtype      = registry.counter(SAMPLES_SYNTHESIZED_MTYPE);
        this.walBytesWritten              = registry.counter(WAL_BYTES_WRITTEN);
        this.walBytesCheckpointed         = registry.counter(WAL_BYTES_CHECKPOINTED);
        this.walReplaySamples             = registry.counter(WAL_REPLAY_SAMPLES);
        this.walBatchesDropped4xx         = registry.counter(WAL_BATCHES_DROPPED_4XX);
        this.samplesDroppedWalFull        = registry.counter(SAMPLES_DROPPED_WAL_FULL);
        this.walFramesDroppedCorrupted    = registry.counter(WAL_FRAMES_DROPPED_CORRUPTED);
        this.findMetricsSinglePass        = registry.counter(FIND_METRICS_SINGLE_PASS_TOTAL);
        this.findMetricsTwoPhase          = registry.counter(FIND_METRICS_TWO_PHASE_TOTAL);
        this.findMetricsPhase2Batches     = registry.counter(FIND_METRICS_PHASE2_BATCHES_TOTAL);
    }

    public MetricRegistry registry() { return registry; }

    // ---- counter mutators (called by Flusher per flush) -------------------

    public void samplesWritten(long n)                 { if (n > 0) samplesWritten.inc(n); }
    public void samplesDropped4xx(long n)              { if (n > 0) samplesDropped4xx.inc(n); }
    public void samplesDropped5xx(long n)              { if (n > 0) samplesDropped5xx.inc(n); }
    public void samplesDroppedTransport(long n)        { if (n > 0) samplesDroppedTransport.inc(n); }
    public void samplesDroppedNonfinite(long n)        { if (n > 0) samplesDroppedNonfinite.inc(n); }
    public void samplesDroppedDuplicate(long n)        { if (n > 0) samplesDroppedDuplicate.inc(n); }
    public void samplesUnparseableResourceId(long n)   { if (n > 0) samplesUnparseableResourceId.inc(n); }
    public void samplesSynthesizedMtype(long n)        { if (n > 0) samplesSynthesizedMtype.inc(n); }

    public void walBytesWritten(long n)                { if (n > 0) walBytesWritten.inc(n); }
    public void walBytesCheckpointed(long n)           { if (n > 0) walBytesCheckpointed.inc(n); }
    public void walReplaySamples(long n)               { if (n > 0) walReplaySamples.inc(n); }
    public void walBatchesDropped4xx(long n)           { if (n > 0) walBatchesDropped4xx.inc(n); }
    public void samplesDroppedWalFull(long n)          { if (n > 0) samplesDroppedWalFull.inc(n); }
    public void walFramesDroppedCorrupted(long n)      { if (n > 0) walFramesDroppedCorrupted.inc(n); }

    public void findMetricsSinglePass()                { findMetricsSinglePass.inc(); }
    public void findMetricsTwoPhase()                  { findMetricsTwoPhase.inc(); }
    public void findMetricsPhase2Batches(long n)       { if (n > 0) findMetricsPhase2Batches.inc(n); }

    // ---- gauge registration (called by Storage on start) ------------------

    public void registerLongGauge(String name, LongSupplier supplier) {
        // Replace an existing gauge with the same name (idempotent on hot-reload).
        if (registry.getGauges().containsKey(name)) {
            registry.remove(name);
        }
        Gauge<Long> gauge = supplier::getAsLong;
        registry.register(name, gauge);
    }

    // ---- read side (for StatsCommand) -------------------------------------

    /** Snapshot of all registered metrics as a name → Number map, sorted by name. */
    public Map<String, Number> snapshot() {
        Map<String, Number> out = new LinkedHashMap<>();
        registry.getCounters().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> out.put(e.getKey(), e.getValue().getCount()));
        registry.getGauges().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    Object v = e.getValue().getValue();
                    out.put(e.getKey(), v instanceof Number n ? n : 0);
                });
        return out;
    }
}
