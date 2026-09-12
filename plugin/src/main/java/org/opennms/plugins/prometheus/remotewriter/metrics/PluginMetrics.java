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
import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** Registered only when writer.shards > 1 — see registerGauges. */
    public static final String SHARD_SKEW_PCT                  = "shard_skew_pct";
    public static final String HTTP_BYTES_WRITTEN              = "http_bytes_written_total";
    public static final String HTTP_WRITES_SUCCESSFUL          = "http_writes_successful_total";
    public static final String HTTP_WRITES_FAILED              = "http_writes_failed_total";
    /** Failed writes by cause, as request counts (the samples_dropped_* counters carry the sample counts). */
    public static final String HTTP_WRITES_4XX                 = "http_writes_4xx_total";
    public static final String HTTP_WRITES_5XX                 = "http_writes_5xx_total";
    public static final String HTTP_WRITES_TRANSPORT           = "http_writes_transport_total";
    /** Upper bounds in ms of the cumulative write-duration buckets; the last is +Inf. */
    public static final int[] HTTP_WRITE_DURATION_BUCKETS_MS   = {5, 10, 25, 50, 100, 250, 1000, Integer.MAX_VALUE};
    /** Gauge name for one cumulative bucket: {@code http_write_duration_bucket_le_<ms|inf>}. */
    public static String httpWriteDurationBucketName(int leMs) {
        return "http_write_duration_bucket_le_" + (leMs == Integer.MAX_VALUE ? "inf" : Integer.toString(leMs));
    }
    /** Milliseconds queue-mode flushers spent building requests (protobuf, snappy). */
    public static final String FLUSHER_BUILD_MS                = "flusher_build_ms_total";
    public static final String HTTP_IN_FLIGHT                  = "http_in_flight";
    /** Wall milliseconds spent inside remote-write HTTP calls, retries and
     *  backoff included. Over http_writes_successful_total + http_writes_failed_total
     *  it is the mean round-trip; over wall time it is flusher utilisation. */
    public static final String HTTP_WRITE_DURATION_MS          = "http_write_duration_ms_total";
    /** Milliseconds the queue-mode flushers spent waiting in pollBatch with
     *  nothing to send. Under writer.shards > 1 every flusher adds to it. */
    public static final String FLUSHER_IDLE_MS                 = "flusher_idle_ms_total";
    /** Milliseconds queue-mode flushers spent waiting for a batch to fill
     *  after a head sample arrived (batch.linger-ms). Not idle: there was
     *  something to send. */
    public static final String FLUSHER_LINGER_MS               = "flusher_linger_ms_total";
    /** Wall milliseconds OpenNMS's writer threads spent inside store().
     *  Over store_calls_total it is the mean call time. */
    public static final String STORE_CALL_DURATION_MS          = "store_call_duration_ms_total";
    public static final String STORE_CALLS                     = "store_calls_total";
    public static final String STORE_CALLS_FAILED              = "store_calls_failed_total";
    /** Samples handed to store() before mapping: the caller's view of offered load. */
    public static final String STORE_SAMPLES_OFFERED           = "store_samples_offered_total";
    /** Samples the label mapper could not map (no metric name); never offered to a queue or the WAL. */
    public static final String SAMPLES_DROPPED_UNMAPPED        = "samples_dropped_unmapped_total";
    /** Samples that had left the queue (built or in the builder's hand) when a
     *  forced shutdown interrupted the flusher before they could be sent. */
    public static final String SAMPLES_DROPPED_SHUTDOWN        = "samples_dropped_shutdown_total";
    /** Maximum total queue depth observed since activation. */
    public static final String QUEUE_DEPTH_HIGH_WATER          = "queue_depth_high_water";

    /** MBean domain every counter and gauge is published under while the
     *  plugin is active; the metric name is the MBean's {@code name} key. */
    public static final String JMX_DOMAIN = "org.opennms.plugins.prometheus.remotewriter";

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

    /** Nanoseconds, summed exactly; exposed as a millisecond gauge so sub-ms
     *  polls do not round to zero and vanish. */
    private final java.util.concurrent.atomic.AtomicLong flusherIdleNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong flusherLingerNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong flusherBuildNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong storeCallNanos = new java.util.concurrent.atomic.AtomicLong();
    private final Counter storeCalls;
    private final Counter storeCallsFailed;
    private final Counter storeSamplesOffered;
    private final Counter samplesDroppedUnmapped;
    private final Counter samplesDroppedShutdown;

    private static final Logger LOG = LoggerFactory.getLogger(PluginMetrics.class);
    private JmxReporter jmxReporter;

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
        registerLongGauge(FLUSHER_IDLE_MS, () -> flusherIdleNanos.get() / 1_000_000L);
        registerLongGauge(FLUSHER_LINGER_MS, () -> flusherLingerNanos.get() / 1_000_000L);
        registerLongGauge(FLUSHER_BUILD_MS, () -> flusherBuildNanos.get() / 1_000_000L);
        registerLongGauge(STORE_CALL_DURATION_MS, () -> storeCallNanos.get() / 1_000_000L);
        this.storeCalls                   = registry.counter(STORE_CALLS);
        this.storeCallsFailed             = registry.counter(STORE_CALLS_FAILED);
        this.storeSamplesOffered          = registry.counter(STORE_SAMPLES_OFFERED);
        this.samplesDroppedUnmapped       = registry.counter(SAMPLES_DROPPED_UNMAPPED);
        this.samplesDroppedShutdown       = registry.counter(SAMPLES_DROPPED_SHUTDOWN);
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

    public void flusherIdleNanos(long n)               { if (n > 0) flusherIdleNanos.addAndGet(n); }
    public void flusherLingerNanos(long n)             { if (n > 0) flusherLingerNanos.addAndGet(n); }
    public void flusherBuildNanos(long n)              { if (n > 0) flusherBuildNanos.addAndGet(n); }
    public void storeCallNanos(long n)                 { if (n > 0) storeCallNanos.addAndGet(n); }
    public void storeCall()                            { storeCalls.inc(); }
    public void storeCallFailed()                      { storeCallsFailed.inc(); }
    public void storeSamplesOffered(long n)            { if (n > 0) storeSamplesOffered.inc(n); }
    public void samplesDroppedUnmapped(long n)         { if (n > 0) samplesDroppedUnmapped.inc(n); }
    public void samplesDroppedShutdown(long n)         { if (n > 0) samplesDroppedShutdown.inc(n); }

    // ---- JMX exposure ------------------------------------------------------

    /**
     * Publish every metric of this registry as an MBean under
     * {@link #JMX_DOMAIN}. Metrics registered later (the gauges Storage adds
     * on start) are picked up too, since the reporter listens to the
     * registry. Idempotent. A registration failure is logged and swallowed:
     * self-metrics are not worth a write outage.
     */
    public synchronized void startJmxReporter() {
        if (jmxReporter != null) return;
        try {
            // Assign before start(): if start() throws part-way through,
            // stopJmxReporter() still unregisters whatever got registered.
            jmxReporter = JmxReporter.forRegistry(registry).inDomain(JMX_DOMAIN).build();
            jmxReporter.start();
        } catch (RuntimeException e) {
            LOG.warn("could not publish plugin metrics over JMX: {}", e.getMessage(), e);
            stopJmxReporter();
        }
    }

    /** Unregister the MBeans published by {@link #startJmxReporter()}. Idempotent. */
    public synchronized void stopJmxReporter() {
        if (jmxReporter == null) return;
        try {
            jmxReporter.stop();
        } catch (RuntimeException e) {
            LOG.warn("error unpublishing plugin metrics from JMX: {}", e.getMessage(), e);
        } finally {
            jmxReporter = null;
        }
    }

    // ---- gauge registration (called by Storage on start) ------------------

    /** Gauge name for one shard's queue depth ({@code shard_<i>_queue_depth}).
     *  Registered only when writer.shards > 1. */
    public static String shardQueueDepthName(int shard) {
        return "shard_" + shard + "_queue_depth";
    }

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
