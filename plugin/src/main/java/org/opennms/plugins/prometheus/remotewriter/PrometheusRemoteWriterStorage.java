/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.HttpHeadersConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.queue.Shards;
import org.opennms.plugins.prometheus.remotewriter.queue.WalFlusher;
import org.opennms.plugins.prometheus.remotewriter.read.PrometheusReadClient;
import org.opennms.plugins.prometheus.remotewriter.wal.Checkpoint;
import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wal.WalFullException;
import org.opennms.plugins.prometheus.remotewriter.wal.WalRecovery;
import org.opennms.plugins.prometheus.remotewriter.wal.WalRecovery.RecoveredWal;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top-level TimeSeriesStorage bean — composes the full pipeline.
 *
 * <p>The active pipeline lives in an immutable {@link Active} record published
 * via a single {@code volatile} reference. {@link #start()} atomically
 * publishes a new Active after constructing every collaborator; {@link #stop()}
 * atomically clears the reference before tearing the collaborators down.
 * SPI methods snapshot the reference once at entry and operate on that local
 * snapshot — this eliminates the torn-read race between a reader (e.g.
 * {@code store()}) and {@code stop()}.
 */
public class PrometheusRemoteWriterStorage implements TimeSeriesStorage {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriterStorage.class);
    private static final long DELETE_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    /** Previous active config, used only for hot-reload diff logging. */
    private static final AtomicReference<PrometheusRemoteWriterConfig> LAST_ACTIVE =
            new AtomicReference<>();

    /** Previous active {@code http.headers.*} snapshot, used only for hot-reload
     *  diff logging. Stored as the immutable map returned by
     *  {@link HttpHeadersConfig#headers()} so we compare value-shapes, not
     *  the (single, stateful) bean instance. */
    private static final AtomicReference<java.util.Map<String, String>> LAST_HEADERS =
            new AtomicReference<>();

    /**
     * One-shot gate for the instance.id-unset WARN. Static so the warning
     * fires once per bundle lifecycle regardless of how many times the
     * blueprint container reloads the plugin on config changes.
     */
    private static final AtomicBoolean INSTANCE_ID_UNSET_WARNED = new AtomicBoolean(false);

    /**
     * Emission count for the instance.id-unset WARN — independent of the
     * boolean gate so tests can pin "WARN fires exactly once" rather than
     * only "gate flipped to true". Incremented inside the CAS-success branch
     * of {@link #warnIfInstanceIdUnset()}, so a refactor that moves the
     * {@code LOG.warn} call outside that branch would be caught by the
     * count-based assertions in {@code PrometheusRemoteWriterStorageTest}.
     */
    private static final AtomicInteger INSTANCE_ID_UNSET_WARN_COUNT = new AtomicInteger(0);

    /** One-shot gate for the wire.protocol-version=2 startup WARN. */
    private static final AtomicBoolean WIRE_V2_WARNED = new AtomicBoolean(false);
    private static final AtomicInteger WIRE_V2_WARN_COUNT = new AtomicInteger(0);

    /**
     * Everything constructed at {@link #start()} time. Published as a unit via
     * the {@link #active} volatile so SPI callers can snapshot the whole
     * pipeline without worrying about partial views.
     *
     * <p>Two modes share this record:
     * <ul>
     *   <li><b>Queue mode</b> (wal.enabled=false, v0.4 default): {@code queue}
     *       and {@code flusher} are non-null; all WAL fields are null.</li>
     *   <li><b>WAL mode</b> (wal.enabled=true): {@code walWriter},
     *       {@code walFlusher}, {@code checkpoint}, {@code walDir} are
     *       non-null; {@code queue} and {@code flusher} are null.</li>
     * </ul>
     * The {@code walEnabled()} convenience tells SPI methods which branch
     * to take without re-reading config (immune to hot-reload mid-call).
     */
    private record Active(
            LabelMapper           labelMapper,
            Shards                shards,         // queue mode only
            RemoteWriteHttpClient writeClient,
            PrometheusReadClient  readClient,
            WalWriter             walWriter,      // WAL mode only
            WalFlusher            walFlusher,     // WAL mode only
            Checkpoint            checkpoint,     // WAL mode only
            Path                  walDir,         // WAL mode only
            PluginMetrics         metrics) {

        boolean walEnabled() { return walWriter != null; }
    }

    private final PrometheusRemoteWriterConfig config;
    private final HttpHeadersConfig httpHeadersConfig;
    private volatile Active active;

    private final AtomicLong deleteNoopTotal        = new AtomicLong();
    // nanoTime-based so NTP backsteps or container resume can't freeze the
    // throttle in the "just logged" state forever.
    private final AtomicLong deleteWarnLastNanos    = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong deleteWarnSinceLastLog = new AtomicLong();

    /** Test-friendly constructor — no operator-supplied custom HTTP headers.
     *  Production code wires the two-arg form via Blueprint. */
    public PrometheusRemoteWriterStorage(PrometheusRemoteWriterConfig config) {
        this(config, HttpHeadersConfig.empty());
    }

    public PrometheusRemoteWriterStorage(PrometheusRemoteWriterConfig config,
                                         HttpHeadersConfig httpHeadersConfig) {
        this.config = Objects.requireNonNull(config, "config");
        this.httpHeadersConfig = Objects.requireNonNull(httpHeadersConfig, "httpHeadersConfig");
    }

    // --- Blueprint lifecycle -----------------------------------------------

    /** Idempotent: if already active, does nothing. On construction failure,
     *  rolls back any collaborators that were already built.
     *  <p>
     *  Missing / invalid config does NOT throw from here. When this bundle
     *  is deployed as a KAR, Karaf starts the blueprint container before
     *  Felix fileinstall has pushed the etc/ cfg file into ConfigAdmin, so
     *  the cm:property-placeholder falls back to its empty defaults and
     *  validate() would fail. Throwing would mark the container failed
     *  permanently — the update-strategy="reload" on the placeholder
     *  cannot revive it. Instead we log a warning and leave {@code active}
     *  null; the SPI methods already reject calls in that state. Once
     *  ConfigAdmin delivers real properties the placeholder reload tears
     *  down and re-creates the container, giving us another shot.
     */
    public synchronized void start() {
        if (active != null) {
            LOG.debug("start() called while already active; ignoring");
            return;
        }
        try {
            config.validate();
        } catch (IllegalStateException bad) {
            LOG.warn("prometheus-remote-writer not yet configured ({}); "
                   + "waiting for ConfigAdmin to deliver real properties. "
                   + "If this persists, check etc/org.opennms.plugins.tss.prometheusremotewriter.cfg.",
                    bad.getMessage());
            return;
        }

        warnIfInstanceIdUnset();
        warnIfWireV2();

        if (config.isWalEnabled()) {
            startWalMode();
        } else {
            startQueueMode();
        }
    }

    private void startQueueMode() {
        PluginMetrics         m  = null;
        LabelMapper           lm = null;
        RemoteWriteHttpClient wc = null;
        PrometheusReadClient  rc = null;
        Shards                sh = null;
        try {
            m  = new PluginMetrics();
            lm = new LabelMapper(config, m);
            wc = new RemoteWriteHttpClient(config, httpHeadersConfig);
            rc = new PrometheusReadClient(config, m, httpHeadersConfig);
            sh = new Shards(config.getWriterShards(), config.getQueueCapacity(), wc,
                    config.getBatchSize(), config.getFlushIntervalMs(), m,
                    org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders
                            .forVersion(config.getWireProtocolVersion()));

            Active built = new Active(lm, sh, wc, rc, null, null, null, null, m);
            registerGauges(built);
            logActivationOrDiff();
            sh.start();
            active = built;
        } catch (RuntimeException e) {
            rollbackStart(sh, null, wc, rc, null);
            throw e;
        }
    }

    private void startWalMode() {
        // Operator-visible one-shot WARN if queue.capacity was explicitly
        // set — under wal.enabled=true, it is ignored. We log rather than
        // fail because the default (10_000) is harmless; operators who
        // did set it may otherwise wonder where their config went.
        if (config.getQueueCapacity() != 10_000) {
            LOG.warn("queue.capacity={} is ignored when wal.enabled=true; "
                    + "the WAL replaces the in-memory queue as source of truth. "
                    + "Size the WAL via wal.max-size-bytes instead.",
                    config.getQueueCapacity());
        }

        PluginMetrics         m  = null;
        LabelMapper           lm = null;
        RemoteWriteHttpClient wc = null;
        PrometheusReadClient  rc = null;
        WalWriter             ww = null;
        WalFlusher            wf = null;
        RecoveredWal          recovered = null;
        try {
            m  = new PluginMetrics();
            lm = new LabelMapper(config, m);

            String walPathStr = config.resolveWalPath();
            Path walDir = Paths.get(walPathStr);
            recovered = WalRecovery.recover(walDir, config.getWalFsync(), effectiveMaxPayload());
            m.walReplaySamples(recovered.pendingSampleCount());

            ww = WalWriter.resume(walDir, recovered.activeSegment(),
                    config.getWalSegmentSizeBytes(),
                    config.getWalMaxSizeBytes(),
                    config.getWalOverflow(),
                    config.getWalFsync(),
                    effectiveMaxPayload());

            wc = new RemoteWriteHttpClient(config, httpHeadersConfig);
            rc = new PrometheusReadClient(config, m, httpHeadersConfig);
            wf = new WalFlusher(walDir, ww, recovered.checkpoint(), effectiveMaxPayload(),
                    wc, config.getBatchSize(), config.getFlushIntervalMs(), m,
                    org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders
                            .forVersion(config.getWireProtocolVersion()));

            Active built = new Active(lm, null, wc, rc, ww, wf,
                    recovered.checkpoint(), walDir, m);
            registerGauges(built);
            logActivationOrDiff();
            LOG.info("prometheus-remote-writer WAL active (path={}, pending={} samples, "
                    + "disk={} bytes, checkpoint={})",
                    walDir, recovered.pendingSampleCount(), recovered.totalBytesOnDisk(),
                    recovered.checkpoint().lastSentOffset());
            wf.start();
            active = built;
        } catch (IOException | RuntimeException e) {
            rollbackStart(null, wf, wc, rc, ww);
            if (e instanceof RuntimeException re) throw re;
            throw new IllegalStateException("WAL startup failed", e);
        }
    }

    /**
     * Effective max payload in bytes for WAL frame decode — currently a
     * static 1 MiB; chosen to comfortably exceed the largest plausible
     * single-sample encoding (a few hundred bytes with a full label set)
     * while rejecting obviously-absurd length prefixes from a corrupted
     * segment.
     */
    private static int effectiveMaxPayload() {
        return 1 << 20; // 1 MiB
    }

    public synchronized void stop() {
        Active a = active;
        if (a == null) {
            LOG.debug("stop() called while not active; ignoring");
            return;
        }
        // Clear first so concurrent SPI callers see the state change
        // immediately and fail cleanly with StorageException instead of
        // touching collaborators being torn down.
        active = null;

        LOG.info("prometheus-remote-writer stopping");
        if (a.walEnabled()) {
            stopWalMode(a);
        } else {
            stopQueueMode(a);
        }
        try { a.writeClient().shutdown(); } catch (RuntimeException e) { LOG.warn("write client shutdown: {}", e.getMessage(), e); }
        try { a.readClient().shutdown();  } catch (RuntimeException e) { LOG.warn("read client shutdown: {}",  e.getMessage(), e); }

        // Clear the static hot-reload diff anchors so a fresh start() after
        // stop() logs "activated" rather than a spurious "reloaded". Both
        // anchors must be cleared in lockstep — if only LAST_ACTIVE is
        // reset, the next reload would diff headers against a stale
        // snapshot from before the stop, producing spurious (set) ->
        // (unset) lines (R2-P1).
        LAST_ACTIVE.set(null);
        LAST_HEADERS.set(null);
    }

    private void stopQueueMode(Active a) {
        try {
            a.shards().stop(config.getShutdownGracePeriodMs());
            int residual = a.shards().totalDepth();
            if (residual > 0) {
                LOG.warn("shutdown completed with {} sample(s) still queued; dropping", residual);
            }
        } catch (RuntimeException e) {
            LOG.warn("error stopping flusher: {}", e.getMessage(), e);
        }
    }

    private void stopWalMode(Active a) {
        try {
            // The grace period bounds the stop-thread's wait for the
            // flusher loop to exit. WAL durability means unflushed
            // samples are NOT lost — they replay on next start.
            //
            // Note: Thread.interrupt() does NOT cancel an in-flight
            // OkHttp call. A POST stuck on a dead TCP connection will
            // continue running until http.read-timeout-ms even after
            // the grace window elapses; the writeClient.shutdown()
            // below cancels the dispatcher to break out faster.
            int pending = a.walFlusher().pendingSampleCount();
            long checkpointAtStop = a.checkpoint().lastSentOffset();
            a.walFlusher().stop(config.getShutdownGracePeriodMs());
            if (pending > 0) {
                LOG.info("WAL shutdown: {} sample(s) drained past checkpoint not yet "
                        + "acknowledged; will replay from checkpoint offset {} on "
                        + "next start", pending, checkpointAtStop);
            }
        } catch (RuntimeException e) {
            LOG.warn("error stopping wal-flusher: {}", e.getMessage(), e);
        }
        try {
            a.walWriter().close();
        } catch (IOException | RuntimeException e) {
            LOG.warn("error closing wal writer: {}", e.getMessage(), e);
        }
    }

    // --- TimeSeriesStorage -------------------------------------------------

    @Override
    public void store(List<Sample> samples) throws StorageException {
        Active a = active;
        if (a == null) {
            throw new StorageException("prometheus-remote-writer is not accepting writes "
                    + "(plugin is stopped or not yet started)");
        }
        if (samples == null || samples.isEmpty()) return;

        if (a.walEnabled()) {
            storeToWal(a, samples);
        } else {
            storeToQueue(a, samples);
        }
    }

    private void storeToQueue(Active a, List<Sample> samples) throws StorageException {
        for (Sample s : samples) {
            MappedSample mapped = a.labelMapper().map(s);
            if (mapped == null) continue;
            a.shards().enqueue(mapped);
        }
    }

    private void storeToWal(Active a, List<Sample> samples) throws StorageException {
        for (int i = 0; i < samples.size(); i++) {
            Sample s = samples.get(i);
            MappedSample mapped = a.labelMapper().map(s);
            if (mapped == null) continue;
            byte[] encoded = WalEntryCodec.encode(mapped);
            try {
                WalWriter.AppendResult r = a.walWriter().appendWithStats(encoded);
                if (r.evictedFrames() > 0) {
                    a.metrics().samplesDroppedWalFull(r.evictedFrames());
                }
                // Count what actually went to disk: 4-byte length + payload
                // + 4-byte CRC = Frame.HEADER_BYTES + encoded.length.
                a.metrics().walBytesWritten(org.opennms.plugins.prometheus.remotewriter.wal.Frame.HEADER_BYTES + encoded.length);
            } catch (WalFullException full) {
                // Bump the counter by the number of samples actually
                // refused — this sample plus any later ones the caller
                // had queued. Using `samples.size()` would double-count
                // samples that already landed earlier in this batch and
                // could over-count by an entire batch on a typical
                // multi-sample store() call. The eviction count carried
                // on the exception covers any frames already evicted
                // BEFORE the giving-up throw (rare; only when a single
                // frame exceeds the entire cap).
                int refused = samples.size() - i;
                a.metrics().samplesDroppedWalFull(
                    (long) refused + full.evictedFramesBeforeFailure());
                throw new StorageException(
                    "WAL is full under backpressure policy (" + full.getMessage() + "); "
                    + "increase wal.max-size-bytes, switch to wal.overflow=drop-oldest, "
                    + "or resolve the downstream outage that is preventing drain",
                    full);
            } catch (IllegalStateException stateEx) {
                // Triggered when start() partially rolled back, or when
                // stop() snapped the writer between SPI snapshot of
                // Active and the appendWithStats call. Wrap as
                // StorageException so OpenNMS sees a typed error rather
                // than an unchecked exception.
                throw new StorageException(
                    "WAL writer is not in an appendable state ("
                    + stateEx.getMessage() + ") — plugin may be stopping or "
                    + "in a recovered-but-failed state", stateEx);
            } catch (IOException io) {
                throw new StorageException("WAL append failed: " + io.getMessage(), io);
            }
        }
    }

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> tagMatchers) throws StorageException {
        Active a = active;
        if (a == null) throw new StorageException("findMetrics called before start()");
        return a.readClient().findMetrics(tagMatchers);
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) throws StorageException {
        // Adapter for the deprecated SPI method; delegates to getTimeSeriesData.
        TimeSeriesData data = getTimeSeriesData(request);
        Metric metric = data.getMetric();
        List<Sample> out = new ArrayList<>(data.getDataPoints().size());
        for (DataPoint dp : data.getDataPoints()) {
            out.add(ImmutableSample.builder()
                    .metric(metric)
                    .time(dp.getTime())
                    .value(dp.getValue())
                    .build());
        }
        return out;
    }

    @Override
    public TimeSeriesData getTimeSeriesData(TimeSeriesFetchRequest request) throws StorageException {
        Active a = active;
        if (a == null) throw new StorageException("getTimeSeriesData called before start()");
        return a.readClient().getTimeSeriesData(request);
    }

    @Override
    public boolean supportsAggregation(Aggregation aggregation) {
        return aggregation == Aggregation.NONE;
    }

    @Override
    public void delete(Metric metric) {
        deleteNoopTotal.incrementAndGet();
        deleteWarnSinceLastLog.incrementAndGet();

        long now = System.nanoTime();
        long prev = deleteWarnLastNanos.get();
        // First call: prev == Long.MIN_VALUE; subtraction would overflow, so
        // special-case to "log the first one".
        boolean due = prev == Long.MIN_VALUE || (now - prev) >= DELETE_WARN_INTERVAL_NANOS;
        if (due && deleteWarnLastNanos.compareAndSet(prev, now)) {
            long count = deleteWarnSinceLastLog.getAndSet(0);
            LOG.warn("delete(Metric) called {} time(s) in the last {}s — the plugin "
                   + "does not propagate deletes to Prometheus (no remote-write delete "
                   + "semantic exists). Configure retention at the backend tier.",
                    count, TimeUnit.NANOSECONDS.toSeconds(DELETE_WARN_INTERVAL_NANOS));
        }
    }

    // --- Accessors for the Karaf shell command -----------------------------

    public PluginMetrics getMetrics() {
        Active a = active;
        return a == null ? null : a.metrics();
    }

    public long getDeleteNoopTotal() { return deleteNoopTotal.get(); }

    // --- internals ---------------------------------------------------------

    private void warnIfInstanceIdUnset() {
        String iid = config.getInstanceId();
        if ((iid == null || iid.isEmpty())
                && INSTANCE_ID_UNSET_WARNED.compareAndSet(false, true)) {
            INSTANCE_ID_UNSET_WARN_COUNT.incrementAndGet();
            LOG.warn("PrometheusRemoteWriter: instance.id is not set. This is fine "
                   + "for a single OpenNMS instance writing to a dedicated backend. "
                   + "If you run multiple OpenNMS instances against a shared "
                   + "Prometheus-compatible backend, set instance.id to a stable "
                   + "per-instance identifier so samples can be distinguished by "
                   + "the onms_instance_id label.");
        }
    }

    /**
     * One-shot WARN naming the backend version requirements for v2.
     * Fires once per JVM lifetime when {@code wire.protocol-version=2}
     * is configured, so operators on older backends see a heads-up
     * before their data starts hitting 4xx drops.
     */
    private void warnIfWireV2() {
        if (config.getWireProtocolVersion() == 2
                && WIRE_V2_WARNED.compareAndSet(false, true)) {
            WIRE_V2_WARN_COUNT.incrementAndGet();
            LOG.warn("PrometheusRemoteWriter: wire.protocol-version=2 is set. "
                   + "Requires a v2-capable backend: Prometheus 2.50+, Mimir 2.10+, "
                   + "VictoriaMetrics with v2 ingest enabled, Grafana Cloud, or "
                   + "equivalent. Older backends will return 4xx and the batch is "
                   + "dropped (see samples_dropped_4xx_total). Verify backend "
                   + "compatibility before relying on this setting.");
        }
    }

    /** Lock guarding the two test-visible WARN-state fields so
     *  {@link #resetInstanceIdWarnedForTesting()} writes them atomically
     *  from a reader's perspective. */
    private static final Object WARN_STATE_LOCK = new Object();

    /** Visible for tests — resets BOTH the one-shot WARN gate and the
     *  emission-count counter so a sequence of start()/stop() cycles within
     *  a single test run can exercise the WARN deterministically. The reset
     *  is guarded so a reader between the two underlying writes cannot
     *  observe a drifted (gate=true, count=0) state. */
    static void resetInstanceIdWarnedForTesting() {
        synchronized (WARN_STATE_LOCK) {
            INSTANCE_ID_UNSET_WARNED.set(false);
            INSTANCE_ID_UNSET_WARN_COUNT.set(0);
        }
    }

    /** Visible for tests — true once the WARN gate has flipped (i.e. the
     *  {@code LOG.warn} in {@link #warnIfInstanceIdUnset()} has been
     *  emitted at least once already within this JVM). Lets tests assert
     *  the one-shot semantic without taking a dependency on a log-capture
     *  framework. */
    static boolean isInstanceIdWarnedForTesting() {
        return INSTANCE_ID_UNSET_WARNED.get();
    }

    /** Visible for tests — number of times the
     *  {@link #INSTANCE_ID_UNSET_WARN_COUNT} counter has been incremented.
     *  Pairs with {@link #isInstanceIdWarnedForTesting} to distinguish "the
     *  gate flipped" from "the counter moved". The counter lives inside the
     *  CAS-success branch alongside {@code LOG.warn}; a refactor that moves
     *  the {@code incrementAndGet} call out would be caught by assertions
     *  here. (A refactor that moves only the {@code LOG.warn} line and
     *  leaves the counter intact would not be caught — this counter is a
     *  proxy for the WARN, not a substitute for log capture.) */
    static int getInstanceIdWarnCountForTesting() {
        return INSTANCE_ID_UNSET_WARN_COUNT.get();
    }

    /** Visible for tests — resets the wire.protocol-version=2 startup
     *  WARN gate and counter atomically. Mirrors the instance-id-warn
     *  reset pattern. */
    static void resetWireV2WarnedForTesting() {
        synchronized (WARN_STATE_LOCK) {
            WIRE_V2_WARNED.set(false);
            WIRE_V2_WARN_COUNT.set(0);
        }
    }

    /** Visible for tests — count of v2-WARN emissions in this JVM. */
    static int getWireV2WarnCountForTesting() {
        return WIRE_V2_WARN_COUNT.get();
    }

    private void logActivationOrDiff() {
        PrometheusRemoteWriterConfig previous = LAST_ACTIVE.getAndSet(config);
        // Capture the headers snapshot ONCE so the anchor we store and the
        // "after" passed to diff are the same map reference. The bean's
        // headers field is volatile and a concurrent Aries-driven
        // updated() between two reads could otherwise produce a diff that
        // doesn't agree with what we stored as the new anchor (R2-P2).
        java.util.Map<String, String> currentHeaders = httpHeadersConfig.headers();
        java.util.Map<String, String> previousHeaders = LAST_HEADERS.getAndSet(currentHeaders);
        if (previous == null) {
            LOG.info("prometheus-remote-writer activated (write.url={}, read.url={})",
                     config.getWriteUrl(), config.getReadUrl());
            return;
        }
        // Two diff sources: scalar config (PrometheusRemoteWriterConfig.diff)
        // plus prefix-scanned headers (HttpHeadersConfig.diff). Both emit
        // the same line format; we concatenate for a single per-reload log.
        List<String> changes = new java.util.ArrayList<>(config.diff(previous));
        changes.addAll(HttpHeadersConfig.diff(previousHeaders, currentHeaders));
        if (changes.isEmpty()) {
            LOG.info("prometheus-remote-writer reloaded; configuration unchanged");
        } else {
            LOG.info("prometheus-remote-writer reloaded; {} change(s):", changes.size());
            for (String line : changes) {
                LOG.info("  {}", line);
            }
        }
    }

    private void registerGauges(Active a) {
        PluginMetrics m = a.metrics();
        // Every lambda below captures the Active parameter, so gauges continue
        // reading the correct collaborators even if stop() has cleared the
        // volatile `active`. The Active object itself stays alive as long as
        // the registry holds these gauges.
        m.registerLongGauge(PluginMetrics.HTTP_BYTES_WRITTEN,       a.writeClient()::getBytesWritten);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_SUCCESSFUL,   a.writeClient()::getWritesSuccessful);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_FAILED,
                () -> a.writeClient().getWrites4xx()
                    + a.writeClient().getWrites5xxExhausted()
                    + a.writeClient().getWritesTransportError());
        m.registerLongGauge(PluginMetrics.HTTP_IN_FLIGHT,           () -> (long) a.writeClient().getInFlightCalls());
        m.registerLongGauge(PluginMetrics.DELETE_NOOP,              this::getDeleteNoopTotal);
        m.registerLongGauge(PluginMetrics.METADATA_DENYLIST_BLOCKED, a.labelMapper()::getMetadataDenylistBlockedCount);

        if (a.walEnabled()) {
            registerWalGauges(a);
        } else {
            m.registerLongGauge(PluginMetrics.QUEUE_DEPTH,              () -> (long) a.shards().totalDepth());
            m.registerLongGauge(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL, a.shards()::totalSamplesDroppedQueueFull);
            // Per-shard gauges only when actually sharded — keeps the N=1
            // metric surface byte-identical to the classic pipeline.
            int shards = a.shards().shardCount();
            if (shards > 1) {
                m.registerLongGauge(PluginMetrics.SHARD_SKEW_PCT, a.shards()::skewPct);
                for (int i = 0; i < shards; i++) {
                    final int shard = i;
                    m.registerLongGauge(PluginMetrics.shardQueueDepthName(shard),
                            () -> (long) a.shards().depth(shard));
                }
            }
        }
    }

    private void registerWalGauges(Active a) {
        PluginMetrics m = a.metrics();
        Path walDir = a.walDir();
        // wal_disk_usage_bytes: sum of .seg file sizes in the WAL
        // directory. A dir scan per gauge-read; acceptable because the
        // Karaf shell and Dropwizard registry only snapshot on demand.
        m.registerLongGauge(PluginMetrics.WAL_DISK_USAGE_BYTES, () -> {
            try { return a.walWriter().currentTotalBytes(); }
            catch (IOException e) { return -1L; }
        });
        m.registerLongGauge(PluginMetrics.WAL_SEGMENTS_ACTIVE, () -> {
            try (var stream = java.nio.file.Files.newDirectoryStream(
                    walDir, "*" + WalSegment.SEG_EXT)) {
                long count = 0;
                for (@SuppressWarnings("unused") Path p : stream) count++;
                return count;
            } catch (IOException e) { return -1L; }
        });
    }

    private static void rollbackStart(Shards sh, WalFlusher wf, RemoteWriteHttpClient wc,
                                      PrometheusReadClient rc, WalWriter ww) {
        if (sh != null) {
            try { sh.stop(0); } catch (RuntimeException ignored) {}
        }
        if (wf != null) {
            try { wf.stop(0); } catch (RuntimeException ignored) {}
        }
        if (ww != null) {
            try { ww.close(); } catch (IOException | RuntimeException ignored) {}
        }
        if (wc != null) {
            try { wc.shutdown(); } catch (RuntimeException ignored) {}
        }
        if (rc != null) {
            try { rc.shutdown(); } catch (RuntimeException ignored) {}
        }
    }
}
