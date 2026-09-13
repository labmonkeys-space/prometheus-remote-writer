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
import java.util.Map;
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
import org.opennms.plugins.prometheus.remotewriter.queue.OverflowBucket;
import org.opennms.plugins.prometheus.remotewriter.queue.Shards;
import org.opennms.plugins.prometheus.remotewriter.read.PrometheusReadClient;
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
    private static final AtomicReference<Map<String, String>> LAST_HEADERS =
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
     * <p>One pipeline, one shape. Before 0.8.0 this record carried a queue-mode
     * half and a WAL-mode half with a discriminator, because the two were
     * alternatives; the disk tier now sits behind the queue inside
     * {@link Shards} rather than replacing it, so there is nothing to branch
     * on. A deployment with no disk tier is the same pipeline with the buckets
     * left out.
     */
    private record Active(
            LabelMapper           labelMapper,
            Shards                shards,
            RemoteWriteHttpClient writeClient,
            PrometheusReadClient  readClient,
            PluginMetrics         metrics,
            PrometheusRemoteWriterConfig.StorePolicy storePolicy) {
    }

    private final PrometheusRemoteWriterConfig config;
    private final HttpHeadersConfig httpHeadersConfig;
    private volatile Active active;

    /** Serialises ALL_OR_NOTHING calls so the room check and the offers are
     *  one step; without it two writer threads can both pass the check for
     *  the same free slots and one of them ends up partially accepted, which
     *  is the outcome that policy exists to prevent. Offers never block, so
     *  the hold is short. PARTIAL does not take it. */
    private final Object allOrNothingLock = new Object();

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

        // Two separate header-config failures, both of which must stop the
        // plugin serving rather than let it write without the operator's
        // headers.
        //
        // First: was the configuration delivered at all? "Never delivered"
        // and "operator configured no headers" both leave the header map
        // empty, so without this check they are indistinguishable — and
        // "never delivered" is precisely the state this plugin shipped in
        // twice, under two different Blueprint wirings, with the whole unit
        // suite green. A false here is a wiring regression, not operator
        // error, so the message says so.
        if (!httpHeadersConfig.isDelivered()) {
            LOG.error("prometheus-remote-writer not started — the http.headers.* "
                    + "configuration was never delivered to the plugin. This is a "
                    + "wiring fault rather than a configuration mistake: the "
                    + "Blueprint <cm:cm-properties> element for the plugin PID did "
                    + "not reach HttpHeadersConfig.init(). Custom headers would be "
                    + "silently absent from every request, so the plugin declines "
                    + "to serve.");
            return;
        }

        // Second: was what arrived valid? Same posture as the core-config
        // check above, for the same reason. We log and leave active == null
        // (SPI calls are rejected in that state) rather than throwing,
        // because a throw marks the Blueprint container permanently failed
        // and the placeholder reload could not revive it from a corrected
        // .cfg.
        String headersError = httpHeadersConfig.validationError();
        if (headersError != null) {
            LOG.error("prometheus-remote-writer not started — invalid "
                    + "http.headers.* configuration: {}. The plugin will "
                    + "activate on the next save of "
                    + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg "
                    + "once the entry is corrected.", headersError);
            return;
        }

        warnIfInstanceIdUnset();
        warnIfWireV2();
        logEffectiveAuth();

        try {
            startPipeline();
        } catch (IllegalStateException e) {
            // Same contract as the config and header failures above: leave the
            // plugin inactive and let the next cfg save revive it, rather than
            // throwing and marking the Blueprint container permanently failed.
            // This became reachable in 0.8.0 — the disk tier is on by default,
            // so an unresolvable or unwritable overflow.dir now fails a start
            // that used to succeed with wal.enabled=false.
            LOG.error("prometheus-remote-writer not started — {}. The plugin will activate on "
                    + "the next save of etc/org.opennms.plugins.tss.prometheusremotewriter.cfg "
                    + "once the problem is resolved.", e.getMessage());
        }
    }

    /**
     * State the authentication the plugin will actually use, once, at startup.
     *
     * <p>This exists because the plugin cannot detect the failure it guards
     * against. Karaf resolves an unset {@code ${env:NAME}} to an empty string
     * before Configuration Admin sees it, so by the time any of this code
     * runs, {@code auth.bearer.token = ${env:TYPO}} and a deliberately blank
     * {@code auth.bearer.token =} are byte-identical. Blank also means
     * "disabled" in the shipped reference configuration, so neither a
     * validation failure nor a warning could tell the two apart without
     * firing on every deployment that simply does not use authentication.
     *
     * <p>What is left is to make the outcome visible: an operator who
     * expected bearer auth and reads "authentication: none" has the answer in
     * front of them. Logged at INFO because no-auth is a legitimate
     * configuration, not a fault.
     */
    private void logEffectiveAuth() {
        AUTH_LINE_COUNT.incrementAndGet();
        LOG.info("prometheus-remote-writer authentication: {}", effectiveAuthLine());
    }

    /** The full line body, tenant suffix included. Package-private so a test
     *  can assert the suffix, which is otherwise unreachable. */
    String effectiveAuthLine() {
        return effectiveAuthDescription()
             + (config.hasTenant() ? "; tenant.org-id set" : "");
    }

    /** Counts emissions of the startup authentication line. Sits inside
     *  {@link #logEffectiveAuth()} alongside the {@code LOG.info} so a test can
     *  assert the line was actually emitted from a real {@code start()} —
     *  asserting only {@link #effectiveAuthDescription()} would let a refactor
     *  that drops the call from {@code start()} ship with a green suite. Same
     *  proxy-for-the-log pattern, and the same limitation, as
     *  {@link #INSTANCE_ID_UNSET_WARN_COUNT}. */
    private static final AtomicInteger AUTH_LINE_COUNT = new AtomicInteger(0);

    /** Visible for tests — number of startup authentication lines emitted in
     *  this JVM. */
    static int getAuthLineCountForTesting() {
        return AUTH_LINE_COUNT.get();
    }

    /** Visible for tests — resets the authentication-line emission counter. */
    static void resetAuthLineCountForTesting() {
        AUTH_LINE_COUNT.set(0);
    }

    /**
     * Describe the authentication mode the plugin will actually use. Split out
     * of {@link #logEffectiveAuth()} and package-private so unit tests can
     * assert it as a pure function, without the log-capture appender this repo
     * has declined to add as a test dependency. Same precedent as
     * {@code HttpHeadersConfig.formatActivationMessage}.
     *
     * <p>Names keys, never values. The scheme keyword is operator-supplied text
     * sitting one line above the credentials in the same file, so a paste into
     * the wrong key is an easy slip; echoing it here would copy the secret into
     * the Karaf log. Nothing else in this codebase echoes a configured value —
     * {@code HttpHeadersConfig} logs names only, {@code diff()} masks.
     */
    String effectiveAuthDescription() {
        // Branches must mirror the emitters' exactly, including their order:
        // a line that reports a mode the request builder would not produce is
        // worse than no line. Both use the complete-block predicates.
        if (config.canEmitBasicAuthHeader()) {
            return "basic (auth.basic.username + auth.basic.password)";
        }
        if (config.hasBearerAuth()) {
            return "bearer (auth.bearer.token)";
        }
        if (config.canEmitAuthorizationHeader()) {
            return "custom scheme (auth.authorization.type + "
                 + "auth.authorization.credentials; the keyword is not echoed here)";
        }
        return "none — no Authorization header will be sent. If you "
             + "configured auth.bearer.token, auth.basic.* or "
             + "auth.authorization.* with an "
             + "${env:NAME} reference, check that the variable is set: "
             + "Karaf resolves an unset reference to an empty value, "
             + "which is indistinguishable from leaving the key blank";
    }

    private void startPipeline() {
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
                    config.getBatchSize(), config.getFlushIntervalMs(), config.getBatchLingerMs(), m,
                    org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders
                            .forVersion(config.getWireProtocolVersion()),
                    bucketFactory(m), config.getOverflowDrain());

            PrometheusRemoteWriterConfig.StorePolicy policy = config.resolvedStorePolicy();
            Active built = new Active(lm, sh, wc, rc, m, policy);
            registerGauges(built);
            m.startJmxReporter();
            logActivationOrDiff();
            logEffectiveWritePath(policy);
            sh.start();
            active = built;
        } catch (RuntimeException e) {
            if (m != null) m.stopJmxReporter();
            rollbackStart(sh, wc, rc);
            throw e;
        }
    }

    /**
     * One line naming the write path as it actually resolved, so an operator
     * upgrading into changed defaults can grep for what they got rather than
     * reading seven keys and inferring. 0.8.0 moves three of them, so this is
     * the first thing worth having in the log.
     */
    private void logEffectiveWritePath(PrometheusRemoteWriterConfig.StorePolicy policy) {
        String storePolicy = policy.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-')
                + (config.getStorePolicy() == PrometheusRemoteWriterConfig.StorePolicy.AUTO
                        ? " (auto: " + PrometheusRemoteWriterConfig.OPENNMS_BUFFER_TYPE_PROPERTY + "="
                          + System.getProperty(PrometheusRemoteWriterConfig.OPENNMS_BUFFER_TYPE_PROPERTY) + ")"
                        : " (configured)");
        LOG.info("write path: writer.shards={}, queue.capacity={} ({} per shard), "
                + "batch.size={}, batch.linger-ms={}, overflow={}, overflow.drain={}, "
                + "overflow.full={}, queue.store-policy={}",
                config.getWriterShards(), config.getQueueCapacity(),
                config.getQueueCapacity() / config.getWriterShards(),
                config.getBatchSize(), config.getBatchLingerMs(),
                config.isOverflowEnabled()
                        ? config.getOverflowMaxSizeBytes() + " bytes ("
                          + config.overflowBytesPerShard() + " per shard)"
                        : "disabled",
                config.getOverflowDrain().name().toLowerCase(java.util.Locale.ROOT),
                config.getOverflowFull().name().toLowerCase(java.util.Locale.ROOT).replace('_', '-'),
                storePolicy);
    }

    /**
     * Build the per-shard bucket opener, or null when no disk tier is
     * configured. Each shard gets its own subdirectory of
     * {@code overflow.dir}, which is what lets the buckets be independent
     * ordered logs rather than one log with parallel readers.
     */
    private java.util.function.IntFunction<OverflowBucket> bucketFactory(PluginMetrics m) {
        if (!config.isOverflowEnabled()) {
            LOG.info("overflow.max-size-bytes=0 — no disk tier; a full queue refuses, "
                    + "as before 0.8.0");
            return null;
        }
        Path root = Paths.get(config.resolveOverflowDir());
        long perShard   = config.overflowBytesPerShard();
        long segment    = config.overflowSegmentSizeBytes();
        LOG.info("overflow tier at {} ({} bytes over {} shard(s), {} per shard, full={}, fsync={})",
                root, config.getOverflowMaxSizeBytes(), config.getWriterShards(), perShard,
                config.getOverflowFull(), config.getOverflowFsync());
        return shard -> {
            Path dir = root.resolve("shard-" + shard);
            try {
                OverflowBucket bucket = OverflowBucket.open(dir, perShard, segment,
                        config.getOverflowFull(), config.getOverflowFsync(),
                        effectiveMaxPayload(), "shard-" + shard);
                int recovered = bucket.pendingSamples();
                if (recovered > 0) {
                    m.walReplaySamples(recovered);
                    LOG.info("shard {}: recovered {} pending sample(s) from {}; they ship "
                            + "before anything offered after this start",
                            shard, recovered, dir);
                }
                return bucket;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "could not open the overflow bucket for shard " + shard + " at " + dir
                        + " — check that overflow.dir exists and is writable, or set "
                        + "overflow.max-size-bytes=0 to run without a disk tier", e);
            }
        };
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
        stopPipeline(a);
        // After the drain so a scrape during the grace period still sees the
        // final totals; before the clients go so no gauge reads a closed one.
        a.metrics().stopJmxReporter();
        try { a.writeClient().shutdown(); } catch (RuntimeException e) { LOG.warn("write client shutdown: {}", e.getMessage(), e); }
        try { a.readClient().shutdown();  } catch (RuntimeException e) { LOG.warn("read client shutdown: {}",  e.getMessage(), e); }

        // Clear the static hot-reload diff anchors so a fresh start() after
        // stop() logs "activated" rather than a spurious "reloaded". Both
        // anchors must be cleared in lockstep — if only LAST_ACTIVE is
        // reset, the next reload would diff headers against a stale
        // snapshot from before the stop, producing spurious (set) ->
        // (unset) lines.
        LAST_ACTIVE.set(null);
        LAST_HEADERS.set(null);
    }

    private void stopPipeline(Active a) {
        try {
            a.shards().stop(config.getShutdownGracePeriodMs());
            int residual = a.shards().totalDepth();
            if (residual > 0) {
                LOG.warn("shutdown completed with {} sample(s) still queued; dropping", residual);
            }
            // Anything on disk is durable and replays on next start, so it is
            // reported rather than mourned.
            int pending = a.shards().totalOverflowPending();
            if (pending > 0) {
                LOG.info("shutdown with {} sample(s) pending in the overflow tier; "
                        + "they ship on next start", pending);
            }
        } catch (RuntimeException e) {
            LOG.warn("error stopping flusher: {}", e.getMessage(), e);
        }
        try {
            a.shards().close();
        } catch (RuntimeException e) {
            LOG.warn("error closing overflow buckets: {}", e.getMessage(), e);
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
        // Caller-side accounting before anything else, so offered, written
        // and dropped reconcile from the plugin's own counters (#155). The
        // duration is the RED signal for this request type: it is what
        // OpenNMS's writer threads pay per call (#162).
        long started = System.nanoTime();
        a.metrics().storeCall();
        try {
            if (samples == null || samples.isEmpty()) return;
            a.metrics().storeSamplesOffered(samples.size());
            try {
                storeMapped(a, samples);
            } catch (StorageException | RuntimeException e) {
                a.metrics().storeCallFailed();
                throw e;
            }
        } finally {
            a.metrics().storeCallNanos(System.nanoTime() - started);
        }
    }

    private void storeMapped(Active a, List<Sample> samples) throws StorageException {
        // Map first: the policy decisions below need the routed shard, and a
        // sample the mapper skips is not offered to any shard.
        List<MappedSample> mapped = new ArrayList<>(samples.size());
        for (Sample s : samples) {
            MappedSample m = a.labelMapper().map(s);
            if (m == null) { a.metrics().samplesDroppedUnmapped(1); continue; }
            mapped.add(m);
        }
        if (mapped.isEmpty()) return;

        if (a.storePolicy() == PrometheusRemoteWriterConfig.StorePolicy.ALL_OR_NOTHING) {
            // Refuse the whole call without placing anything, so a caller that
            // retries the whole call on exception (Horizon's offheap writer)
            // never re-sends samples we already took. The lock makes
            // check-then-place atomic against other store() threads; the
            // flusher only ever removes, which can only make room.
            synchronized (allOrNothingLock) {
                if (!everyShardHasRoomFor(a.shards(), mapped)) {
                    countRefused(a, mapped.size());
                    throw noRoom(a, mapped.size(), mapped.size());
                }
                acceptAll(a, mapped);
            }
            return;
        }
        acceptAll(a, mapped);
    }

    /** PARTIAL: attempt every sample so a shard that is out of room does not
     *  discard samples bound for shards with room (#156). Shards fill
     *  independently, and with a disk tier configured a shard is out of room
     *  only when its bucket is at its bound too. One exception per call, not
     *  per refused sample: under overload that is tens of thousands of stack
     *  traces a second saved. */
    private static void acceptAll(Active a, List<MappedSample> mapped) throws StorageException {
        int refused;
        try {
            // One accept lock per shard per call, not one per sample (#182).
            refused = a.shards().acceptAll(mapped);
        } catch (java.io.UncheckedIOException io) {
            // The disk tier could not be written. Surface it as a typed error
            // rather than an unchecked one, so OpenNMS backs off instead of
            // seeing the plugin throw something it does not model.
            throw new StorageException(
                "prometheus-remote-writer could not write to the overflow tier: "
                + io.getMessage() + " — check that overflow.dir is present and writable",
                io.getCause());
        }
        if (refused > 0) {
            countRefused(a, refused);
            throw noRoom(a, refused, mapped.size());
        }
    }

    /** Book refusals against whichever tier was the one that ran out. */
    private static void countRefused(Active a, int refused) {
        if (a.shards().overflowEnabled()) {
            a.shards().countDroppedOverflowFull(refused);
        } else {
            a.shards().countDroppedQueueFull(refused);
        }
    }

    private static boolean everyShardHasRoomFor(Shards shards, List<MappedSample> mapped) {
        int[] needed = new int[shards.shardCount()];
        for (MappedSample m : mapped) needed[shards.shardOf(m)]++;
        for (int shard = 0; shard < needed.length; shard++) {
            if (needed[shard] > 0 && !shards.hasRoomFor(shard, needed[shard])) return false;
        }
        return true;
    }

    private static StorageException noRoom(Active a, int refused, int offered) {
        return a.shards().overflowEnabled()
                ? new StorageException("prometheus-remote-writer overflow tier full: refused "
                        + refused + " of " + offered + " sample(s) (writer.shards="
                        + a.shards().shardCount() + "); raise overflow.max-size-bytes, switch to "
                        + "overflow.full=drop-oldest, or resolve the downstream outage that is "
                        + "preventing drain; see samples_dropped_overflow_full_total")
                : new StorageException("prometheus-remote-writer queue full: refused " + refused
                        + " of " + offered + " sample(s) (writer.shards=" + a.shards().shardCount()
                        + "); see samples_dropped_queue_full_total");
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
        // doesn't agree with what we stored as the new anchor.
        Map<String, String> currentHeaders = httpHeadersConfig.headers();
        Map<String, String> previousHeaders = LAST_HEADERS.getAndSet(currentHeaders);
        if (previous == null) {
            LOG.info("prometheus-remote-writer activated (write.url={}, read.url={})",
                     config.getWriteUrl(), config.getReadUrl());
            return;
        }
        // Two diff sources: scalar config (PrometheusRemoteWriterConfig.diff)
        // plus prefix-scanned headers (HttpHeadersConfig.diff). Both emit
        // the same line format; we concatenate for a single per-reload log.
        List<String> changes = new ArrayList<>(config.diff(previous));
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
        m.registerLongGauge(PluginMetrics.HTTP_WRITE_DURATION_MS,   a.writeClient()::getWriteDurationMs);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_4XX,          a.writeClient()::getWrites4xx);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_5XX,          a.writeClient()::getWrites5xxExhausted);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_TRANSPORT,    a.writeClient()::getWritesTransportError);
        for (int le : PluginMetrics.HTTP_WRITE_DURATION_BUCKETS_MS) {
            m.registerLongGauge(PluginMetrics.httpWriteDurationBucketName(le),
                    () -> a.writeClient().getWriteDurationBucketCount(le));
        }
        m.registerLongGauge(PluginMetrics.DELETE_NOOP,              this::getDeleteNoopTotal);
        m.registerLongGauge(PluginMetrics.METADATA_DENYLIST_BLOCKED, a.labelMapper()::getMetadataDenylistBlockedCount);

        m.registerLongGauge(PluginMetrics.QUEUE_DEPTH,              () -> (long) a.shards().totalDepth());
        m.registerLongGauge(PluginMetrics.QUEUE_DEPTH_HIGH_WATER,   a.shards()::depthHighWater);
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
        if (a.shards().overflowEnabled()) {
            registerOverflowGauges(a);
        }
    }

    private void registerOverflowGauges(Active a) {
        PluginMetrics m = a.metrics();
        m.registerLongGauge(PluginMetrics.OVERFLOW_PENDING_SAMPLES,
                () -> (long) a.shards().totalOverflowPending());
        m.registerLongGauge(PluginMetrics.OVERFLOW_BYTES,       a.shards()::totalOverflowBytes);
        m.registerLongGauge(PluginMetrics.SAMPLES_SPILLED,      a.shards()::totalSamplesSpilled);
        m.registerLongGauge(PluginMetrics.SAMPLES_DROPPED_OVERFLOW_FULL,
                a.shards()::totalSamplesDroppedOverflowFull);
        m.registerLongGauge(PluginMetrics.SAMPLES_EVICTED_OVERFLOW,
                a.shards()::totalSamplesEvictedOverflow);
        m.registerLongGauge(PluginMetrics.OVERFLOW_RECOVERING_SHARDS,
                () -> (long) a.shards().recoveringShards());
        m.registerLongGauge(PluginMetrics.OVERFLOW_OLDEST_PENDING_AGE_MS,
                a.shards()::oldestPendingAgeMs);
        // Per-shard depth is what shows hash skew filling one bucket while
        // its siblings idle — the failure arm G ran into on the memory tier.
        int shards = a.shards().shardCount();
        for (int i = 0; i < shards; i++) {
            final int shard = i;
            m.registerLongGauge(PluginMetrics.OVERFLOW_PENDING_SAMPLES_SHARD + "_" + shard,
                    () -> (long) a.shards().overflowPending(shard));
        }
    }

    private static void rollbackStart(Shards sh, RemoteWriteHttpClient wc,
                                      PrometheusReadClient rc) {
        if (sh != null) {
            try { sh.stop(0); } catch (RuntimeException ignored) {}
            try { sh.close(); } catch (RuntimeException ignored) {}
        }
        if (wc != null) {
            try { wc.shutdown(); } catch (RuntimeException ignored) {}
        }
        if (rc != null) {
            try { rc.shutdown(); } catch (RuntimeException ignored) {}
        }
    }
}
