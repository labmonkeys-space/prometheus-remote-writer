/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.metadata.InfoColumns;
import org.opennms.plugins.prometheus.remotewriter.metadata.MetadataRegistry;
import org.opennms.plugins.prometheus.remotewriter.queue.OverflowBucket;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment;

/**
 * Plugin configuration. Blueprint sets each property individually through the
 * ConfigAdmin-backed property placeholder; {@link #validate()} runs once on
 * bean init and throws {@link IllegalStateException} with an actionable
 * message if the configuration is not usable. Validation failure causes the
 * downstream {@code TimeSeriesStorage} service bean to fail activation, which
 * leaves the OSGi service unregistered until the operator corrects the file.
 */
public class PrometheusRemoteWriterConfig {

    public enum MetadataCase { PRESERVE, SNAKE_CASE }

    /**
     * Read-side resource discovery strategy. {@code SINGLE_PASS} (default)
     * issues one {@code GET /api/v1/series} per {@code findMetrics} call,
     * followed under either strategy by the metadata enrichment queries,
     * one per {@code read.discovery-batch-size} resources. {@code LABEL_VALUES_FIRST} opts into the two-phase path:
     * enumerate {@code resourceId} values via
     * {@code /api/v1/label/resourceId/values}, then batch exact-match
     * alternations through {@code /api/v1/series} per
     * {@link #discoveryBatchSize}. The two-phase path only fires when the
     * matcher collection actually carries a regex on {@code resourceId} —
     * see {@link org.opennms.plugins.prometheus.remotewriter.read.PrometheusReadClient}.
     */
    public enum DiscoveryStrategy { SINGLE_PASS, LABEL_VALUES_FIRST }

    /**
     * What {@code store()} does when a shard's queue is full (queue mode
     * only). {@code PARTIAL}: attempt every sample, count the refused ones,
     * throw once. Right for a caller that drops the call on exception, such
     * as Horizon's default ring-buffer writer. {@code ALL_OR_NOTHING}:
     * enqueue nothing and throw if any shard lacks room for its share of the
     * call. Right for a caller that retries the whole call until accepted,
     * such as Horizon's offheap writer, which would otherwise re-send the
     * accepted samples on every retry. {@code AUTO}: pick from Horizon's
     * {@code org.opennms.timeseries.config.buffer_type} system property at
     * activation (see {@link #resolvedStorePolicy()}).
     */
    public enum StorePolicy { PARTIAL, ALL_OR_NOTHING, AUTO }

    /** System property Horizon's timeseries layer reads its buffer type from
     *  ({@code opennms.properties}); {@code RINGBUFFER} by default, {@code OFFHEAP}
     *  for the retrying writer. Not set on Sentinel, where the value lives in
     *  ConfigAdmin, so {@code AUTO} resolves to {@code PARTIAL} there. */
    public static final String OPENNMS_BUFFER_TYPE_PROPERTY = "org.opennms.timeseries.config.buffer_type";

    // --- Endpoint ---
    private String writeUrl;
    private String readUrl;

    // --- Source identity ---
    /** Operator-supplied identifier for this OpenNMS instance. When non-empty,
     *  every outbound sample carries an {@code onms_instance_id} label with
     *  this value so operators running multiple OpenNMS instances against a
     *  shared Prometheus-compatible backend can disambiguate samples in
     *  PromQL. Orthogonal to {@link #tenantOrgId}; see README. */
    private String instanceId;

    /** Operator-supplied override for the {@code job} default label. When
     *  non-empty, every outbound sample emits {@code job=<this value>}
     *  regardless of the resourceId-derived value (see
     *  {@link org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper}
     *  for the derivation table). When unset, the plugin derives per-sample
     *  from the resourceId shape. */
    private String jobName;

    // --- Auth ---
    private String basicUsername;
    private String basicPassword;
    private String bearerToken;

    /** Scheme keyword of a custom {@code Authorization} header, e.g.
     *  {@code Token} or {@code ApiKey}. Emitted
     *  verbatim — trimmed, never lowercased — as
     *  {@code Authorization: <type> <credentials>}. REQUIRED whenever
     *  {@link #authorizationCredentials} is set: there is deliberately no
     *  default, so a typo'd {@code ${env:}} reference cannot silently become
     *  {@code Bearer} against a backend expecting something else. Named after
     *  Prometheus's own {@code authorization: {type, credentials}} block. */
    private String authorizationType;

    /** Credentials half of a custom {@code Authorization} header. Required
     *  whenever the {@code auth.authorization.*} block is used at all. */
    private String authorizationCredentials;

    private String tenantOrgId;

    // --- TLS ---
    private String tlsCaFile;
    private boolean tlsInsecureSkipVerify;

    // --- Queue & batching ---
    /** Total in-memory buffer, split evenly across {@link #writerShards}.
     *  Default 40,000 since 0.8.0 — 10,000 a shard at the default four,
     *  which is what the benchmark's accepting configuration used. */
    private int  queueCapacity          = 40_000;
    private int  batchSize              = 1_000;
    private long flushIntervalMs        = 1_000L;
    /** {@code batch.linger-ms}: after a head sample arrives, how long a
     *  flusher waits for the batch to reach batch.size before sending.
     *  Default 100 since 0.8.0: at four shards on an 11,000-device fleet a
     *  linger of 0 produced 3,300 requests a second of 19 samples each, so
     *  this trades a tenth of a second of latency for full batches. */
    private long batchLingerMs          = 100L;
    private int  retryMaxAttempts       = 5;
    private long retryInitialBackoffMs  = 250L;
    private long retryMaxBackoffMs      = 10_000L;

    // --- HTTP ---
    private long httpConnectTimeoutMs = 5_000L;
    private long httpReadTimeoutMs    = 30_000L;
    private long httpWriteTimeoutMs   = 30_000L;
    private int  httpMaxConnections   = 16;

    // --- Shutdown ---
    private long shutdownGracePeriodMs = 10_000L;

    // --- Write parallelism ---
    /** Number of write shards. 1 (default) is the classic single-flusher
     *  pipeline. N>1 splits the queue-mode pipeline into N shards, each
     *  owning a disjoint set of series (hash of the canonical label set),
     *  a queue segment of {@code queue.capacity / N}, and a flusher thread
     *  with at most one request in flight — so the Remote Write
     *  in-order-per-series rule holds structurally while shards flush in
     *  parallel. Each shard also owns its own overflow bucket, so sharding
     *  and durability combine.
     *  <p>Default 4 since 0.8.0, from the benchmark: one shard at the old
     *  defaults lost 7.5% of samples over eight hours with the backend idle.
     *  Note that this is four concurrent streams to the backend where there
     *  was one — mind per-tenant ingest limits. */
    private int writerShards = 4;

    /** Segments per shard bucket. Drop-oldest evicts a whole segment, so a
     *  bucket needs several for eviction to be a partial loss rather than a
     *  total one; GC releases at the same granularity. Fixed rather than
     *  configurable so the segment count per shard does not grow with
     *  writer.shards. */
    static final int SEGMENTS_PER_SHARD = 8;

    /** Smallest per-shard slice the segment arithmetic still works at:
     *  SEGMENTS_PER_SHARD segments of 1 KiB. This is a correctness floor, not
     *  a sizing recommendation — the docs carry the outage-window arithmetic
     *  an operator should actually size against. */
    static final long MIN_OVERFLOW_BYTES_PER_SHARD = SEGMENTS_PER_SHARD * 1024L;

    // --- Read path ---
    /** How far back findMetrics() looks when no explicit start is provided. */
    private long maxSeriesLookbackSeconds = 7_776_000L; // 90 days

    /** Strategy for {@code findMetrics()} resource enumeration. Default
     *  {@link DiscoveryStrategy#SINGLE_PASS} preserves v0.5.0 behavior
     *  exactly. Operators on Mimir / Thanos can flip to
     *  {@link DiscoveryStrategy#LABEL_VALUES_FIRST} to dodge the
     *  broad-regex {@code series}-scan ceiling. See
     *  {@code openspec/specs/tss-plugin/spec.md} requirement
     *  "Optional two-phase resource discovery" for the trigger heuristic. */
    private DiscoveryStrategy discoveryStrategy = DiscoveryStrategy.SINGLE_PASS;

    /** Maximum number of {@code resourceId} values exact-match-alternated
     *  into one phase-2 selector when {@link #discoveryStrategy} is
     *  {@link DiscoveryStrategy#LABEL_VALUES_FIRST}. Above this cap the
     *  enumeration is split into multiple phase-2 calls. Bounds: {@code
     *  [1, 200]}. Default 50, picked to stay comfortably below typical
     *  HTTP request-line caps (~8 KiB at typical OpenNMS resourceId
     *  sizes); upper bound 200 holds at ~30 KiB worst case, which fits
     *  cloud LB defaults but is the safe ceiling above default
     *  nginx / Mimir 8 KiB limits. Also sizes the metadata enrichment
     *  queries, under either strategy. */
    private int discoveryBatchSize = 50;

    // --- Label policy ---
    private String labelsInclude;
    private String labelsExclude;
    private String labelsRename;
    private String labelsCopy;
    private String metricPrefix;

    /** {@code queue.store-policy}; see {@link StorePolicy}. */
    private StorePolicy storePolicy = StorePolicy.AUTO;
    /** The v0.x attribute-label keys a {@code .cfg} still sets. Blueprint
     *  binds them so they reach {@link #validate()}, which rejects any that
     *  is set; see {@link #validateRemovedLabelKeys()}. */
    private final Set<String> removedLabelKeys = new LinkedHashSet<>();

    /** Label names the data series carried before 1.0.0. */
    private static final Set<String> REMOVED_LABELS =
            Set.of("if_descr", "if_speed", "ifSpeed", "ifHighSpeed", "categories");
    private static final List<String> REMOVED_LABEL_PREFIXES =
            List.of("onms_cat_", "onms_attr_", "onms_extattr_");
    /** Source tags that were labels before 1.0.0 and are consumed without
     *  being emitted now, so a literal {@code labels.include} of one would
     *  be a silent no-op. */
    private static final Set<String> REMOVED_SOURCE_KEYS =
            Set.of("ifDescr", "ifSpeed", "ifHighSpeed", "categories");

    // --- Parsed-map caches ---
    // labelsRenameMap() / labelsCopyMap() are called multiple times per
    // validate() (once per sub-validator) and again from LabelMapper's
    // constructor. Cache the parsed result once per underlying-string
    // lifecycle; setters invalidate. Parse errors stay uncached so each
    // call re-throws deterministically (see parse-error-idempotence tests).
    //
    // `volatile` guards against reorderings if a future caller reads these
    // concurrently with a setter (Blueprint hot-reload creates a new config
    // bean rather than mutating an existing one, so today's access is
    // effectively single-threaded — but the spec's SHALL language on "same
    // instance" deserves an explicit memory-model barrier).
    private transient volatile Map<String, String> cachedLabelsRenameMap;
    private transient volatile Map<String, List<String>> cachedLabelsCopyMap;

    // --- Metadata passthrough ---
    private boolean      metadataEnabled;
    private String       metadataInclude;
    private String       metadataExclude;
    private String       metadataLabelPrefix = "onms_meta_";
    private MetadataCase metadataCase        = MetadataCase.PRESERVE;

    // --- Wire format ---
    /** Prometheus Remote Write protocol version on the wire. {@code 1}
     *  emits the existing v1 format (no behavior change for existing
     *  deployments). {@code 2} emits the Prometheus 2.50+ v2 format with
     *  string interning — requires a v2-capable backend (Prometheus
     *  ≥2.50, Mimir ≥2.10, VictoriaMetrics with v2, Grafana Cloud, or
     *  equivalent). The overflow tier is wire-version-agnostic, so flipping
     *  this knob with samples pending on disk is safe — the next flush
     *  emits according to the new value. */
    private int wireProtocolVersion = 1;

    // --- Disk overflow tier ---
    /** Directory holding the per-shard overflow buckets, one subdirectory
     *  per shard. Empty (default) resolves to
     *  {@code ${karaf.data}/prometheus-remote-writer/overflow} at startup.
     *  Operators running containerised Karaf with an ephemeral
     *  {@code ${karaf.data}} MUST set this to a mounted volume or the tier
     *  evaporates across restarts, undermining its whole point. */
    private String overflowDir = "";

    /** Total on-disk footprint cap across every shard's bucket, divided
     *  equally at activation. Default 4 GiB. Zero disables the disk tier
     *  entirely: no directory is created and a full memory queue refuses,
     *  which is the pre-0.8.0 behaviour. Divided by the offered rate, this
     *  is how long a backend outage can last before the plugin refuses. */
    private long overflowMaxSizeBytes = 4L * 1024 * 1024 * 1024;

    /** What a shard does when its bucket is at its size bound:
     *  {@code refuse} — the append is refused, counted in
     *  {@code samples_dropped_overflow_full_total}, and store() throws
     *  (default, so silent loss is always a deliberate choice);
     *  {@code drop-oldest} — evict the oldest whole segment and accept. */
    private OverflowBucket.FullPolicy overflowFull = OverflowBucket.FullPolicy.REFUSE;

    /** How a shard divides its drain between the two tiers:
     *  {@code ordered} — the bucket is drained to empty before any memory
     *  sample, and while it holds anything the shard's memory queue stays
     *  empty, so per-series order survives the tier boundary (default, correct
     *  on every backend); {@code concurrent} — memory keeps being used while
     *  the bucket drains and the two tiers alternate, so fresh samples do not
     *  queue behind the backlog. Concurrent gives up per-series order across
     *  the boundary and needs a backend that accepts out-of-order writes. */
    private OverflowBucket.DrainPolicy overflowDrain = OverflowBucket.DrainPolicy.ORDERED;

    /** {@code metadata.cadence-ms}: how often a resource's metadata series
     *  (onms_resource_attr, onms_resource_category, …) are re-emitted when
     *  nothing changed. New or changed metadata is emitted at once. A PromQL
     *  join needs a lookback of at least two cadences, e.g.
     *  {@code last_over_time(onms_resource_attr[30m])} at the default.
     *  0 switches the metadata series off. */
    private long metadataCadenceMs = 15L * 60L * 1000L;
    /** {@code metadata.attr-budget}: attributes per resource emitted as rows,
     *  lowest keys first; the rest are counted in metadata_attrs_dropped_total. */
    private int metadataAttrBudget = 16;
    /** {@code metadata.info-columns}: {@code column=key} entries for the
     *  onms_resource_info series. See {@link InfoColumns}. */
    private String metadataInfoColumns = InfoColumns.DEFAULT_SPEC;

    /** Fsync policy for bucket segments: {@code always} (fsync every
     *  append; tightest RPO, lowest throughput), {@code batch} (fsync at
     *  the flush-interval boundary; loses at most ~flush-interval-ms of
     *  samples on kill -9), {@code none} (OS page cache only; suitable for
     *  ephemeral deployments). Default {@code batch}. */
    private WalSegment.FsyncPolicy overflowFsync = WalSegment.FsyncPolicy.BATCH;

    /**
     * Validate a fully populated config. Called by Blueprint's {@code init-method}
     * through {@link org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage}.
     * @throws IllegalStateException if the configuration is rejected
     */
    public void validate() {
        if (isBlank(writeUrl)) {
            throw new IllegalStateException(
                "write.url is required — configure it in "
                + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg");
        }
        if (isBlank(readUrl)) {
            throw new IllegalStateException(
                "read.url is required — configure it in "
                + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg");
        }
        validateRenameTargets();
        validateCopyTargets();
        validateCrossPrimitiveTargets();
        // Header-safety, applied to EVERY operator-supplied value that lands
        // verbatim in a request header. OkHttp throws IllegalArgumentException
        // on an illegal byte when the request is built — at which point the
        // write path's retry loop, which catches IOException only, lets it
        // escape into Flusher's catch-all: an ERROR per batch, every batch
        // dropped, and a plugin that still reports healthy. Reject at startup
        // instead, so a config typo cannot become a permanent silent stall.
        //
        // auth.basic.* is deliberately absent from this list: both halves are
        // base64-encoded before they reach the header, so no byte of them can
        // reach addHeader raw, and RFC 7617 explicitly permits UTF-8 in Basic
        // credentials. Guarding them here would reject working configurations
        // to prevent a failure that cannot occur.
        //
        // Messages name the key and never echo the value — an operator who
        // pasted a token into the wrong one of two adjacent keys must not have
        // it copied into the Karaf log.
        if (!isBlank(bearerToken)) {
            requireNoCrLf("auth.bearer.token", bearerToken);
            requirePrintableAscii("auth.bearer.token", bearerToken);
        }
        if (!isBlank(tenantOrgId)) {
            requireNoCrLf("tenant.org-id", tenantOrgId);
            requirePrintableAscii("tenant.org-id", tenantOrgId);
        }
        if (hasAnyAuthorizationField()) {
            if (!isBlank(authorizationType)) {
                requireNoCrLf("auth.authorization.type", authorizationType);
                requireHttpToken("auth.authorization.type", authorizationType);
            }
            if (!isBlank(authorizationCredentials)) {
                requireNoCrLf("auth.authorization.credentials", authorizationCredentials);
                requirePrintableAscii("auth.authorization.credentials", authorizationCredentials);
            }
            // Rejected here rather than in the setter on purpose: a setter throw
            // fails the Blueprint container permanently, while a validate()
            // throw is caught by PrometheusRemoteWriterStorage.start() and
            // leaves the plugin inert but recoverable on the next .cfg save.
            // The message is the whole operator UX — it only ever surfaces as a
            // Karaf log line. Echoing the value is safe in this one branch: it
            // can only ever be the five letters of "basic", in some casing.
            if ("basic".equalsIgnoreCase(authorizationType)) {
                throw new IllegalStateException(
                    "auth.authorization.type = '" + authorizationType + "' is not supported — "
                    + "Basic credentials must be built from the username and the password. "
                    + "Use auth.basic.username and auth.basic.password instead; the plugin "
                    + "base64-encodes them for you.");
            }
            // No default scheme. A defaulted 'Bearer' would turn
            // 'type = ${env:TYPO}' into a wrong-but-plausible header: the
            // plugin would send 'Bearer <token>' to a backend expecting
            // 'Token <token>', 401 forever, with a clean startup and a log line
            // claiming authentication was configured. Prometheus defaults here
            // because its authorization block is the only way to spell a bearer
            // token; this plugin ships auth.bearer.token, so the default bought
            // nothing and cost a silent failure mode.
            if (isBlank(authorizationType)) {
                throw new IllegalStateException(
                    "auth.authorization.type is required whenever "
                    + "auth.authorization.credentials is set — there is no default scheme. "
                    + "Set it to the keyword your backend expects, e.g. Token or "
                    + "ApiKey. For a plain bearer token use "
                    + "auth.bearer.token instead. If this key uses ${env:NAME}, check the "
                    + "variable is set: Karaf resolves an unset reference to an empty value.");
            }
            if (isBlank(authorizationCredentials)) {
                throw new IllegalStateException(
                    "auth.authorization.credentials is required whenever the "
                    + "auth.authorization.* block is used — set it in "
                    + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg, or remove "
                    + "auth.authorization.type to disable the block");
            }
        }
        if (hasBasicAuth() && (isBlank(basicUsername) || isBlank(basicPassword))) {
            throw new IllegalStateException(
                "Basic auth requires both auth.basic.username and auth.basic.password");
        }
        // Exclusivity LAST, so an incomplete block reports its own missing key
        // rather than being masked by a collision it only appears to have.
        // Name the blocks the operator actually set — with three of them, a
        // generic "these three are exclusive" line leaves them diffing the
        // file to find which two collided.
        List<String> configuredAuthBlocks = new ArrayList<>();
        if (hasBasicAuth())             configuredAuthBlocks.add("auth.basic.*");
        if (hasBearerAuth())            configuredAuthBlocks.add("auth.bearer.token");
        if (hasAnyAuthorizationField()) configuredAuthBlocks.add("auth.authorization.*");
        if (configuredAuthBlocks.size() > 1) {
            throw new IllegalStateException(
                "auth.basic.*, auth.bearer.token and auth.authorization.* are mutually "
                + "exclusive — each one produces the Authorization header. Configured: "
                + String.join(" and ", configuredAuthBlocks)
                + ". Keep exactly one of the three blocks, or none.");
        }
        if (queueCapacity < 1) {
            throw new IllegalStateException("queue.capacity must be >= 1");
        }
        if (batchSize < 1) {
            throw new IllegalStateException("batch.size must be >= 1");
        }
        if (batchSize > queueCapacity) {
            throw new IllegalStateException(
                "batch.size (" + batchSize + ") must not exceed queue.capacity ("
                + queueCapacity + ")");
        }
        if (flushIntervalMs < 1) {
            throw new IllegalStateException("flush.interval-ms must be >= 1");
        }
        if (batchLingerMs < 0) {
            throw new IllegalStateException("batch.linger-ms must be >= 0 (got " + batchLingerMs + ")");
        }
        if (metadataCadenceMs < 0) {
            throw new IllegalStateException("metadata.cadence-ms must be >= 0, 0 switching the metadata series off (got " + metadataCadenceMs + ")");
        }
        validateResourceIdKept();
        if (metadataAttrBudget < 1) {
            throw new IllegalStateException("metadata.attr-budget must be >= 1 (got " + metadataAttrBudget + ")");
        }
        try {
            InfoColumns.parse(metadataInfoColumns, metadataRowlessKeys());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("metadata.info-columns: " + e.getMessage(), e);
        }
        if (retryMaxAttempts < 0) {
            throw new IllegalStateException("retry.max-attempts must be >= 0");
        }
        if (retryInitialBackoffMs < 0 || retryMaxBackoffMs < retryInitialBackoffMs) {
            throw new IllegalStateException(
                "retry.initial-backoff-ms must be >= 0 and <= retry.max-backoff-ms");
        }
        if (httpMaxConnections < 1) {
            throw new IllegalStateException("http.max-connections must be >= 1");
        }
        if (shutdownGracePeriodMs < 0) {
            throw new IllegalStateException("shutdown.grace-period-ms must be >= 0");
        }
        if (instanceId != null) {
            // instance.id is emitted as a series-identity label on every sample;
            // silent truncation / malformed text-format output would be
            // worst-case (series collisions across instances, federation
            // breakage). Reject at validate() with an actionable message.
            for (int i = 0; i < instanceId.length(); i++) {
                if (Character.isISOControl(instanceId.charAt(i))) {
                    throw new IllegalStateException(
                        "instance.id must not contain control characters "
                        + "(\\n, \\t, \\0, etc.) — got one at offset " + i);
                }
            }
            int bytes = instanceId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (bytes > Sanitizer.MAX_LABEL_VALUE_BYTES) {
                throw new IllegalStateException(
                    "instance.id is " + bytes + " UTF-8 bytes; must not exceed "
                    + Sanitizer.MAX_LABEL_VALUE_BYTES
                    + " (Prometheus label-value cap). Pick a shorter identifier.");
            }
        }
        if (jobName != null) {
            // Same series-identity concerns apply to job.name — it overrides
            // the per-sample job label value on every sample when set.
            for (int i = 0; i < jobName.length(); i++) {
                if (Character.isISOControl(jobName.charAt(i))) {
                    throw new IllegalStateException(
                        "job.name must not contain control characters "
                        + "(\\n, \\t, \\0, etc.) — got one at offset " + i);
                }
            }
            int bytes = jobName.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (bytes > Sanitizer.MAX_LABEL_VALUE_BYTES) {
                throw new IllegalStateException(
                    "job.name is " + bytes + " UTF-8 bytes; must not exceed "
                    + Sanitizer.MAX_LABEL_VALUE_BYTES
                    + " (Prometheus label-value cap). Pick a shorter value.");
            }
        }

        validateDiscovery();
        validateRemovedLabelKeys();
        // Shards first: the overflow budget is divided by writer.shards, so an
        // out-of-range shard count has to be rejected before that arithmetic.
        validateWriterShards();
        validateOverflow();
    }

    /**
     * Cross-key rules for {@code writer.shards}. Sharding multiplies
     * concurrent outbound requests (one per shard), so it is capped by the
     * connection pool; it splits {@code queue.capacity} evenly, so each
     * shard must still be able to fill a batch. Each shard owns its own
     * ordered overflow bucket, so sharding and durability combine.
     */
    private void validateWriterShards() {
        if (writerShards < 1 || writerShards > 64) {
            throw new IllegalStateException(
                "writer.shards must be in [1, 64] (got " + writerShards + ")");
        }
        if (writerShards == 1) return; // classic pipeline — nothing else to check
        if (writerShards > httpMaxConnections) {
            throw new IllegalStateException(
                "writer.shards (" + writerShards + ") must not exceed http.max-connections ("
                + httpMaxConnections + ") — each shard can hold one request in flight.");
        }
        if (queueCapacity / writerShards < batchSize) {
            throw new IllegalStateException(
                "queue.capacity / writer.shards (" + queueCapacity + " / " + writerShards + " = "
                + (queueCapacity / writerShards) + ") must be >= batch.size (" + batchSize
                + ") — otherwise a shard can never fill a batch. Raise queue.capacity or "
                + "lower writer.shards / batch.size.");
        }
    }

    /**
     * The v0.x attribute labels were removed in 1.0.0: resource attributes,
     * categories and the interface speed travel as metadata series, not as
     * labels on the data series. A {@code .cfg} that still sets one of the
     * keys that shaped them, renames or copies from one of the labels, or
     * includes one of their source tags by name, or excludes one of the
     * labels, must not start and quietly emit a different schema than the
     * operator expects, so each is a validation error that names the
     * replacement.
     */
    private void validateRemovedLabelKeys() {
        List<String> found = new ArrayList<>(removedLabelKeys);
        for (String from : labelsRenameMap().keySet()) {
            if (isRemovedLabel(from)) found.add("labels.rename source '" + from + "'");
        }
        for (String from : labelsCopyMap().keySet()) {
            if (isRemovedLabel(from)) found.add("labels.copy source '" + from + "'");
        }
        for (String entry : labelsIncludeGlobs()) {
            if (REMOVED_SOURCE_KEYS.contains(entry)) found.add("labels.include entry '" + entry + "'");
        }
        for (String entry : labelsExcludeGlobs()) {
            if (isRemovedLabel(entry)) found.add("labels.exclude entry '" + entry + "'");
        }
        if (found.isEmpty()) return;
        throw new IllegalStateException(
            String.join(", ", found)
            + ": removed in 1.0.0. Resource string attributes are the onms_resource_attr rows "
            + "and the onms_resource_info columns (metadata.info-columns), categories are "
            + "onms_resource_category rows and the interface speed is the onms_resource_ifspeed "
            + "gauge; none of them are labels on the data series any more. Remove the "
            + "entries from the .cfg.");
    }

    private static boolean isRemovedLabel(String name) {
        if (REMOVED_LABELS.contains(name)) return true;
        for (String p : REMOVED_LABEL_PREFIXES) if (name.startsWith(p)) return true;
        return false;
    }

    /**
     * Validate the two-phase discovery knobs. Strategy parsing already
     * happened in {@link #setDiscoveryStrategy(String)} (which throws on
     * bad input); this method enforces the batch-size bounds.
     */
    private void validateDiscovery() {
        if (discoveryBatchSize < 1 || discoveryBatchSize > 200) {
            throw new IllegalStateException(
                "read.discovery-batch-size must be in [1, 200] (got "
                + discoveryBatchSize + ")");
        }
    }

    private void validateOverflow() {
        if (overflowMaxSizeBytes == 0) return; // tier disabled — the rest is moot
        if (overflowMaxSizeBytes < 0) {
            throw new IllegalStateException(
                "overflow.max-size-bytes must be >= 0 (got " + overflowMaxSizeBytes
                + "); 0 disables the disk tier");
        }
        // Each shard gets an equal slice and needs room for the SEGMENTS_PER_SHARD
        // segments that make drop-oldest and GC meaningful — evicting the oldest
        // of one segment would evict the whole bucket.
        long perShard = overflowMaxSizeBytes / writerShards;
        if (perShard < MIN_OVERFLOW_BYTES_PER_SHARD) {
            throw new IllegalStateException(
                "overflow.max-size-bytes / writer.shards (" + overflowMaxSizeBytes + " / "
                + writerShards + " = " + perShard + ") must be >= " + MIN_OVERFLOW_BYTES_PER_SHARD
                + " — each shard's bucket needs room for " + SEGMENTS_PER_SHARD
                + " segments of at least 1 KiB. Raise overflow.max-size-bytes, lower "
                + "writer.shards, or set "
                + "overflow.max-size-bytes=0 to run without a disk tier.");
        }
        // overflow.dir resolution is deferred to resolveOverflowDir() since
        // Blueprint may read karaf.data lazily; validate() only enforces the
        // syntactically-required fields here. Physical-path writability is
        // checked when the buckets are opened at startup.
    }

    /**
     * Reject {@code labels.rename} entries whose {@code to} target would
     * silently clobber a default-allowlist label at flush time, or duplicate
     * another rename's target. Invoked from {@link #validate()}.
     *
     * <p>Runtime-level collisions that arise from {@code labels.include}
     * surfacing a source tag whose snake-cased name happens to match a rename
     * target are NOT caught here — they depend on per-sample tag presence and
     * can only be observed at flush. Static-known collisions are caught.
     */
    /**
     * {@code resourceId} is the join key of every metadata series and the
     * read path's resource contract; a config that excludes or renames it on
     * the data series breaks both with no error at query time.
     */
    private void validateResourceIdKept() {
        for (String glob : labelsExcludeGlobs()) {
            if (globMatches(glob, "resourceId")) {
                throw new IllegalStateException("labels.exclude = " + glob + " would remove resourceId, "
                        + "the join key of the onms_resource_* metadata series and the read path's resource contract");
            }
        }
        if (labelsRenameMap().containsKey("resourceId")) {
            throw new IllegalStateException("labels.rename must not rename resourceId: it is the join key of the "
                    + "onms_resource_* metadata series and the read path's resource contract");
        }
    }

    /** Glob with {@code *} and {@code ?}, the grammar labels.exclude uses. */
    private static boolean globMatches(String glob, String name) {
        StringBuilder rx = new StringBuilder();
        for (char c : glob.toCharArray()) {
            if (c == '*') rx.append(".*");
            else if (c == '?') rx.append('.');
            else rx.append(java.util.regex.Pattern.quote(String.valueOf(c)));
        }
        return name.matches(rx.toString());
    }

    private void validateRenameTargets() {
        Map<String, String> renameMap = labelsRenameMap();   // may throw on bad syntax
        if (renameMap.isEmpty()) return;
        // Accumulate all errors so operators see every bad entry in one log
        // line instead of fix-restart-fix-restart. Skip secondary checks on
        // entries that already hit a reserved-name or reserved-prefix
        // violation; re-reporting the same target for "also duplicate" is
        // noise.
        List<String> errors = new ArrayList<>();
        Set<String> seenTargets = new HashSet<>();
        for (Map.Entry<String, String> e : renameMap.entrySet()) {
            String to = e.getValue();
            String reservedError = checkReservedCollision("labels.rename", "renaming", to);
            if (reservedError != null) {
                errors.add(reservedError);
                continue;
            }
            if (!seenTargets.add(to)) {
                errors.add(
                    "labels.rename has two entries with the same target '" + to
                    + "'. At most one rename can target a given label name.");
            }
        }
        if (!errors.isEmpty()) {
            String suffix = errors.size() == 1 ? "" : "s";
            throw new IllegalStateException(
                "labels.rename has " + errors.size() + " error" + suffix + ":\n  - "
                + String.join("\n  - ", errors));
        }
    }

    /**
     * Shared collision check for {@code labels.rename} and {@code labels.copy}
     * target validation. Returns a formatted error message if {@code to} is
     * not a valid Prometheus label name, collides with a default-allowlist
     * label name, or collides with a reserved prefix; returns {@code null}
     * when clean. A new collision rule added here applies uniformly to both
     * primitives — which is the point.
     *
     * <p>The sanitary check runs first because the downstream reserved-name
     * and reserved-prefix checks compare literal strings: without the guard,
     * a target like {@code foreign-source} would pass those checks (the raw
     * string isn't in the reserved set) but at emit time
     * {@link org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer#labelName}
     * would rewrite it to {@code foreign_source}, silently clobbering the
     * default label.
     */
    private static String checkReservedCollision(String primitiveKey, String verbIng, String to) {
        String sanitized = Sanitizer.labelName(to);
        if (!sanitized.equals(to)) {
            return primitiveKey + " target '" + to + "' is not a valid Prometheus label name "
                + "(would be sanitized to '" + sanitized + "' at emit time). Rewrite it to match "
                + "[a-zA-Z_][a-zA-Z0-9_]*.";
        }
        if (LabelMapper.RESERVED_LABEL_NAMES.contains(to)) {
            return primitiveKey + " target '" + to + "' collides with the default label '" + to
                + "'. The plugin already emits this label; " + verbIng + " onto it would silently "
                + "clobber the default value. Pick a different 'to' name.";
        }
        for (String prefix : LabelMapper.RESERVED_LABEL_PREFIXES) {
            if (to.startsWith(prefix)) {
                return primitiveKey + " target '" + to + "' collides with the reserved prefix '"
                    + prefix + "*' (metadata passthrough). Pick a different 'to' name.";
            }
        }
        return null;
    }

    public boolean hasBasicAuth()  { return !isBlank(basicUsername) || !isBlank(basicPassword); }
    public boolean hasBearerAuth() { return !isBlank(bearerToken); }

    /**
     * True when EITHER half of the {@code auth.authorization.*} block is set —
     * i.e. the operator touched the block at all, however incompletely. Only
     * {@link #validate()} wants this reading: a half-configured block must
     * reach a named error rather than silently sending nothing. Emitters must
     * NOT branch on it; they want {@link #canEmitAuthorizationHeader()}.
     * Package-private to keep that distinction from being picked up by accident
     * outside this class.
     */
    boolean hasAnyAuthorizationField() {
        return !isBlank(authorizationType) || !isBlank(authorizationCredentials);
    }

    /**
     * True when the {@code auth.authorization.*} block is COMPLETE and can
     * therefore produce a well-formed header — both the scheme keyword and the
     * credentials are present. This is what the two emitters branch on.
     *
     * <p>{@link #validate()} rejects a half-configured block, but
     * {@code PrometheusReadClient}'s constructor accepts a config it never
     * validates, so an emitter trusting validation would send the literal
     * {@code "Token null"} or {@code "null abc123"}. The guard belongs in the
     * class that owns the invariant, not in a caller's assumption about another
     * class.
     */
    public boolean canEmitAuthorizationHeader() {
        return !isBlank(authorizationType) && !isBlank(authorizationCredentials);
    }

    /**
     * True when {@code auth.basic.*} is COMPLETE. The sibling of
     * {@link #canEmitAuthorizationHeader()}, and needed for the same reason:
     * {@link #hasBasicAuth()} is either-half, so a username-only config that
     * never went through {@link #validate()} would otherwise emit
     * {@code Basic } + base64 of {@code "u:null"} — a garbage credential on the
     * wire rather than no credential.
     */
    public boolean canEmitBasicAuthHeader() {
        return !isBlank(basicUsername) && !isBlank(basicPassword);
    }

    public boolean hasTenant()     { return !isBlank(tenantOrgId); }

    public List<String> labelsIncludeGlobs() { return parseCsv(labelsInclude); }
    public List<String> labelsExcludeGlobs() { return parseCsv(labelsExclude); }
    public List<String> metadataIncludeGlobs() { return parseCsv(metadataInclude); }
    public List<String> metadataExcludeGlobs() { return parseCsv(metadataExclude); }

    /** Renames parsed as a { from -> to } map; order preserved. Cached — the
     *  same map instance is returned on every call until {@link #setLabelsRename}
     *  is invoked with a new value. Parse errors are NOT cached: a subsequent
     *  call with the same malformed string re-parses and re-throws. */
    public Map<String, String> labelsRenameMap() {
        if (cachedLabelsRenameMap != null) {
            return cachedLabelsRenameMap;
        }
        cachedLabelsRenameMap = parseLabelsRenameMap();
        return cachedLabelsRenameMap;
    }

    private Map<String, String> parseLabelsRenameMap() {
        if (isBlank(labelsRename)) {
            return Collections.emptyMap();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String pair : labelsRename.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                // Tolerate trailing/internal empty CSV segments (e.g. "a->b,,c->d"
                // or "a->b, ").
                continue;
            }
            // Use -1 limit so trailing '->' is preserved as a third empty
            // part and rejected as malformed; default split() drops trailing
            // empties and would silently parse "a->b->" as "a -> b".
            String[] parts = trimmed.split("->", -1);
            if (parts.length != 2) {
                throw new IllegalStateException(
                    "labels.rename entry must be 'from->to', got: " + pair);
            }
            String from = parts[0].trim();
            String to   = parts[1].trim();
            if (from.isEmpty() || to.isEmpty()) {
                throw new IllegalStateException(
                    "labels.rename entry has empty side: " + pair);
            }
            // Reject duplicate 'from' — LinkedHashMap.put would silently drop
            // the earlier entry, producing a config that parses cleanly but
            // keeps only the last rename. That's almost always a copy-paste
            // error; refuse to start.
            if (out.containsKey(from)) {
                throw new IllegalStateException(
                    "labels.rename has two entries with the same 'from' key '" + from
                    + "'. At most one rename can originate from a given label.");
            }
            out.put(from, to);
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * Copies parsed as a {@code { from -> [to1, to2, ...] }} multimap; order
     * preserved. Unlike {@code labels.rename}, multiple copy directives with
     * the same {@code from} key are permitted — {@code labels.copy = node ->
     * instance, node -> host} emits both {@code instance} and {@code host}
     * with {@code node}'s value.
     *
     * <p>Cached — the same map instance is returned on every call until
     * {@link #setLabelsCopy} is invoked with a new value. Parse errors are NOT
     * cached: a subsequent call with the same malformed string re-parses and
     * re-throws.
     */
    public Map<String, List<String>> labelsCopyMap() {
        if (cachedLabelsCopyMap != null) {
            return cachedLabelsCopyMap;
        }
        cachedLabelsCopyMap = parseLabelsCopyMap();
        return cachedLabelsCopyMap;
    }

    private Map<String, List<String>> parseLabelsCopyMap() {
        if (isBlank(labelsCopy)) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (String pair : labelsCopy.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) continue;
            String[] parts = trimmed.split("->", -1);
            if (parts.length != 2) {
                throw new IllegalStateException(
                    "labels.copy entry must be 'from->to', got: " + pair);
            }
            String from = parts[0].trim();
            String to   = parts[1].trim();
            if (from.isEmpty() || to.isEmpty()) {
                throw new IllegalStateException(
                    "labels.copy entry has empty side: " + pair);
            }
            out.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
        }
        Map<String, List<String>> immutable = new LinkedHashMap<>(out.size());
        for (Map.Entry<String, List<String>> e : out.entrySet()) {
            immutable.put(e.getKey(), Collections.unmodifiableList(e.getValue()));
        }
        return Collections.unmodifiableMap(immutable);
    }

    /**
     * Reject {@code labels.copy} entries whose {@code to} target would silently
     * clobber a default-allowlist label at flush time, duplicate another
     * copy's target, or collide with a {@code labels.rename} target.
     *
     * <p>Mirrors the structure of {@link #validateRenameTargets()} — same
     * reserved exact set, same reserved prefix list — with one additional
     * cross-primitive check against rename targets so operators can't have two
     * writers contending for the same label name.
     */
    private void validateCopyTargets() {
        Map<String, List<String>> copyMap = labelsCopyMap();   // may throw on bad syntax
        if (copyMap.isEmpty()) return;
        List<String> errors = new ArrayList<>();
        Set<String> seenTargets = new HashSet<>();
        for (Map.Entry<String, List<String>> e : copyMap.entrySet()) {
            for (String to : e.getValue()) {
                String reservedError = checkReservedCollision("labels.copy", "copying", to);
                if (reservedError != null) {
                    errors.add(reservedError);
                    continue;
                }
                if (!seenTargets.add(to)) {
                    errors.add(
                        "labels.copy has two entries with the same target '" + to
                        + "'. At most one copy can target a given label name.");
                }
            }
        }
        if (!errors.isEmpty()) {
            String suffix = errors.size() == 1 ? "" : "s";
            throw new IllegalStateException(
                "labels.copy has " + errors.size() + " error" + suffix + ":\n  - "
                + String.join("\n  - ", errors));
        }
    }

    /**
     * Reject any label name that appears as a target in BOTH {@code labels.rename}
     * and {@code labels.copy}. Runs after the individual rename/copy validators —
     * each primitive's own rules have already passed at this point — so the only
     * remaining class of error is cross-primitive collision. The error wording is
     * symmetric: it names the label, not a primitive, so operators can't
     * accidentally think one primitive "owns" the collision.
     */
    private void validateCrossPrimitiveTargets() {
        Map<String, String> renameMap = labelsRenameMap();
        Map<String, List<String>> copyMap = labelsCopyMap();
        if (renameMap.isEmpty() || copyMap.isEmpty()) return;
        Set<String> renameTargets = new HashSet<>(renameMap.values());
        List<String> errors = new ArrayList<>();
        for (List<String> copyTargets : copyMap.values()) {
            for (String to : copyTargets) {
                if (renameTargets.contains(to)) {
                    errors.add(
                        "label '" + to + "' is the target of both a labels.rename "
                        + "and a labels.copy. At most one primitive can write a given label name.");
                }
            }
        }
        if (!errors.isEmpty()) {
            String suffix = errors.size() == 1 ? "" : "s";
            throw new IllegalStateException(
                "label pipeline has " + errors.size() + " cross-primitive error" + suffix + ":\n  - "
                + String.join("\n  - ", errors));
        }
    }

    /**
     * Human-readable diff against another config, suitable for logging on
     * hot-reload. Returns an empty list when the two configs are equal.
     * Secrets (passwords, bearer token) are masked.
     */
    public List<String> diff(PrometheusRemoteWriterConfig other) {
        if (other == null) {
            return Collections.singletonList("(no prior config)");
        }
        List<String> out = new ArrayList<>();
        diffStr(out, "write.url",                 other.writeUrl,              writeUrl);
        diffStr(out, "read.url",                  other.readUrl,               readUrl);
        diffStr(out, "instance.id",               other.instanceId,            instanceId);
        diffStr(out, "job.name",                  other.jobName,               jobName);
        diffStr(out, "auth.basic.username",       other.basicUsername,         basicUsername);
        diffMasked(out, "auth.basic.password",    other.basicPassword,         basicPassword);
        diffMasked(out, "auth.bearer.token",      other.bearerToken,           bearerToken);
        diffStr(out, "auth.authorization.type",   other.authorizationType,     authorizationType);
        diffMasked(out, "auth.authorization.credentials",
                                                  other.authorizationCredentials, authorizationCredentials);
        diffStr(out, "tenant.org-id",             other.tenantOrgId,           tenantOrgId);
        diffStr(out, "tls.ca-file",               other.tlsCaFile,             tlsCaFile);
        diffBool(out, "tls.insecure-skip-verify", other.tlsInsecureSkipVerify, tlsInsecureSkipVerify);
        diffInt(out, "queue.capacity",            other.queueCapacity,         queueCapacity);
        diffInt(out, "batch.size",                other.batchSize,             batchSize);
        diffLong(out, "flush.interval-ms",        other.flushIntervalMs,       flushIntervalMs);
        diffLong(out, "batch.linger-ms",          other.batchLingerMs,         batchLingerMs);
        diffLong(out, "metadata.cadence-ms",      other.metadataCadenceMs,     metadataCadenceMs);
        diffLong(out, "metadata.attr-budget",     other.metadataAttrBudget,    metadataAttrBudget);
        diffStr(out, "metadata.info-columns",     other.metadataInfoColumns,   metadataInfoColumns);
        diffInt(out, "retry.max-attempts",        other.retryMaxAttempts,      retryMaxAttempts);
        diffLong(out, "retry.initial-backoff-ms", other.retryInitialBackoffMs, retryInitialBackoffMs);
        diffLong(out, "retry.max-backoff-ms",     other.retryMaxBackoffMs,     retryMaxBackoffMs);
        diffLong(out, "http.connect-timeout-ms",  other.httpConnectTimeoutMs,  httpConnectTimeoutMs);
        diffLong(out, "http.read-timeout-ms",     other.httpReadTimeoutMs,     httpReadTimeoutMs);
        diffLong(out, "http.write-timeout-ms",    other.httpWriteTimeoutMs,    httpWriteTimeoutMs);
        diffInt(out, "http.max-connections",      other.httpMaxConnections,    httpMaxConnections);
        diffLong(out, "shutdown.grace-period-ms", other.shutdownGracePeriodMs, shutdownGracePeriodMs);
        diffInt(out, "writer.shards",             other.writerShards,          writerShards);
        diffStr(out, "queue.store-policy",        other.storePolicy.name(),    storePolicy.name());
        diffLong(out, "max-series-lookback-seconds", other.maxSeriesLookbackSeconds, maxSeriesLookbackSeconds);
        diffStr(out, "read.discovery-strategy",   other.discoveryStrategy.name(),   discoveryStrategy.name());
        diffInt(out, "read.discovery-batch-size", other.discoveryBatchSize,         discoveryBatchSize);
        diffStr(out, "labels.include",            other.labelsInclude,         labelsInclude);
        diffStr(out, "labels.exclude",            other.labelsExclude,         labelsExclude);
        diffStr(out, "labels.rename",             other.labelsRename,          labelsRename);
        diffStr(out, "labels.copy",               other.labelsCopy,            labelsCopy);
        diffStr(out, "metric.prefix",             other.metricPrefix,          metricPrefix);
        diffBool(out, "metadata.enabled",         other.metadataEnabled,       metadataEnabled);
        diffStr(out, "metadata.include",          other.metadataInclude,       metadataInclude);
        diffStr(out, "metadata.exclude",          other.metadataExclude,       metadataExclude);
        diffStr(out, "metadata.label-prefix",     other.metadataLabelPrefix,   metadataLabelPrefix);
        diffStr(out, "metadata.case",             other.metadataCase.name(),   metadataCase.name());
        diffInt(out, "wire.protocol-version",     other.wireProtocolVersion,   wireProtocolVersion);
        return out;
    }

    // ---------- Setters (Blueprint property binding) --------------------------

    public void setWriteUrl(String v)              { writeUrl = blankToNull(v); }
    public void setReadUrl(String v)               { readUrl = blankToNull(v); }
    public void setInstanceId(String v)            { instanceId = blankToNull(v); }
    public void setJobName(String v)               { jobName = blankToNull(v); }
    public void setBasicUsername(String v)         { basicUsername = blankToNull(v); }
    public void setBasicPassword(String v)         { basicPassword = blankToNull(v); }
    public void setBearerToken(String v)           { bearerToken = blankToNull(v); }
    // blankToNull trims and maps empty to null; casing is left alone, which is
    // the contract for the scheme keyword. Validation of the value lives in
    // validate(), never here — see the comment on the 'basic' rejection.
    public void setAuthorizationType(String v)         { authorizationType = blankToNull(v); }
    public void setAuthorizationCredentials(String v)  { authorizationCredentials = blankToNull(v); }
    public void setTenantOrgId(String v)           { tenantOrgId = blankToNull(v); }
    public void setTlsCaFile(String v)             { tlsCaFile = blankToNull(v); }
    public void setTlsInsecureSkipVerify(boolean v){ tlsInsecureSkipVerify = v; }
    public void setQueueCapacity(int v)            { queueCapacity = v; }
    public void setBatchSize(int v)                { batchSize = v; }
    public void setFlushIntervalMs(long v)         { flushIntervalMs = v; }
    public void setBatchLingerMs(long v)           { batchLingerMs = v; }
    public void setMetadataCadenceMs(long v)       { metadataCadenceMs = v; }
    public void setMetadataAttrBudget(int v)       { metadataAttrBudget = v; }
    public void setMetadataInfoColumns(String v)   { metadataInfoColumns = v == null ? "" : v; }
    public void setRetryMaxAttempts(int v)         { retryMaxAttempts = v; }
    public void setRetryInitialBackoffMs(long v)   { retryInitialBackoffMs = v; }
    public void setRetryMaxBackoffMs(long v)       { retryMaxBackoffMs = v; }
    public void setHttpConnectTimeoutMs(long v)    { httpConnectTimeoutMs = v; }
    public void setHttpReadTimeoutMs(long v)       { httpReadTimeoutMs = v; }
    public void setHttpWriteTimeoutMs(long v)      { httpWriteTimeoutMs = v; }
    public void setHttpMaxConnections(int v)       { httpMaxConnections = v; }
    public void setShutdownGracePeriodMs(long v)      { shutdownGracePeriodMs = v; }
    public void setWriterShards(int v)                { writerShards = v; }
    public void setMaxSeriesLookbackSeconds(long v)   { maxSeriesLookbackSeconds = v; }

    /**
     * Parse {@code read.discovery-strategy}. Accepts {@code single-pass} and
     * {@code label-values-first} (case-insensitive, whitespace-trimmed). Empty
     * → default ({@code SINGLE_PASS}). Anything else throws.
     */
    public void setDiscoveryStrategy(String v) {
        String normalized = blankToNull(v);
        if (normalized == null) {
            discoveryStrategy = DiscoveryStrategy.SINGLE_PASS;
            return;
        }
        String lc = normalized.toLowerCase(java.util.Locale.ROOT);
        switch (lc) {
            case "single-pass" -> discoveryStrategy = DiscoveryStrategy.SINGLE_PASS;
            case "label-values-first" -> discoveryStrategy = DiscoveryStrategy.LABEL_VALUES_FIRST;
            default -> throw new IllegalStateException(
                "read.discovery-strategy must be 'single-pass' or 'label-values-first' (got: '"
                + v + "')");
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type; otherwise blueprint rejects the bean. Same pattern as
    // setMetadataCase / setOverflowFsync / setOverflowFull.
    public void setDiscoveryStrategy(DiscoveryStrategy v) {
        discoveryStrategy = v == null ? DiscoveryStrategy.SINGLE_PASS : v;
    }

    public void setDiscoveryBatchSize(int v)       { discoveryBatchSize = v; }
    public void setLabelsInclude(String v)         { labelsInclude = blankToNull(v); }
    public void setLabelsExclude(String v)         { labelsExclude = blankToNull(v); }
    public void setLabelsRename(String v) {
        labelsRename = blankToNull(v);
        cachedLabelsRenameMap = null;
    }
    public void setLabelsCopy(String v) {
        labelsCopy = blankToNull(v);
        cachedLabelsCopyMap = null;
    }
    public void setMetricPrefix(String v)          { metricPrefix = blankToNull(v); }
    public void setMetadataEnabled(boolean v)      { metadataEnabled = v; }
    public void setMetadataInclude(String v)       { metadataInclude = blankToNull(v); }
    public void setMetadataExclude(String v)       { metadataExclude = blankToNull(v); }
    public void setMetadataLabelPrefix(String v) {
        String trimmed = blankToNull(v);
        if (trimmed == null) {
            metadataLabelPrefix = "onms_meta_";
            return;
        }
        // Sanitize the prefix to the Prometheus label-name grammar so an
        // operator-supplied "my-prefix." can't produce an invalid label name
        // downstream.
        String sanitized = Sanitizer.labelName(trimmed);
        metadataLabelPrefix = sanitized;
    }
    public void setMetadataCase(String v) {
        if (isBlank(v)) {
            metadataCase = MetadataCase.PRESERVE;
            return;
        }
        String normalized = v.trim().toUpperCase().replace('-', '_');
        try {
            metadataCase = MetadataCase.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "metadata.case must be 'preserve' or 'snake_case', got: " + v);
        }
    }

    public void setStorePolicy(String v) {
        if (isBlank(v)) {
            storePolicy = StorePolicy.AUTO;
            return;
        }
        String normalized = v.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        try {
            storePolicy = StorePolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "queue.store-policy must be 'partial', 'all-or-nothing' or 'auto', got: " + v);
        }
    }

    public void setStorePolicy(StorePolicy v) {
        storePolicy = v == null ? StorePolicy.AUTO : v;
    }

    /**
     * The policy {@code store()} runs under: the configured value, or for
     * {@code AUTO} the one matching OpenNMS's writer as published in
     * {@link #OPENNMS_BUFFER_TYPE_PROPERTY}. {@code OFFHEAP} (any case) means
     * the retrying writer and selects {@code ALL_OR_NOTHING}; anything else,
     * including unset, selects {@code PARTIAL}.
     */
    public StorePolicy resolvedStorePolicy() {
        if (storePolicy != StorePolicy.AUTO) return storePolicy;
        String bufferType = System.getProperty(OPENNMS_BUFFER_TYPE_PROPERTY);
        return bufferType != null && bufferType.trim().equalsIgnoreCase("OFFHEAP")
                ? StorePolicy.ALL_OR_NOTHING
                : StorePolicy.PARTIAL;
    }

    // --- Removed in 1.0.0 ----------------------------------------------------
    // Blueprint still binds these keys so a v0.x .cfg that sets one reaches
    // validateRemovedLabelKeys() instead of being silently ignored.

    public void setIfSpeedMode(String v)        { recordRemoved("labels.if-speed-mode", v); }
    public void setCategoriesMode(String v)     { recordRemoved("labels.categories-mode", v); }
    public void setLabelProfile(String v)       { recordRemoved("labels.profile", v); }
    public void setAttrMode(String v)           { recordRemoved("labels.attr-mode", v); }
    public void setLabelsAttrInclude(String v)  { recordRemoved("labels.attr-include", v); }

    private void recordRemoved(String key, String v) {
        if (blankToNull(v) == null) removedLabelKeys.remove(key); else removedLabelKeys.add(key);
    }

    // --- Overflow tier setters -----------------------------------------------

    public void setOverflowDir(String v)          { overflowDir = v == null ? "" : v.trim(); }
    public void setOverflowMaxSizeBytes(long v)   { overflowMaxSizeBytes = v; }

    /** {@code none} is the config grammar for the segment layer's {@code NEVER}. */
    public void setOverflowFsync(String v) {
        if (isBlank(v)) {
            overflowFsync = WalSegment.FsyncPolicy.BATCH;
            return;
        }
        String normalized = v.trim().toUpperCase();
        if ("NONE".equals(normalized)) normalized = "NEVER";
        try {
            overflowFsync = WalSegment.FsyncPolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "overflow.fsync must be 'always', 'batch', or 'none', got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type. Without this overload the String setter above is the
    // only pairing candidate and Sentinel's blueprint rejects the bean.
    // Same pattern as setMetadataCase / setWireProtocolVersion(int).
    public void setOverflowFsync(WalSegment.FsyncPolicy v) {
        overflowFsync = v == null ? WalSegment.FsyncPolicy.BATCH : v;
    }

    public void setOverflowFull(String v) {
        if (isBlank(v)) {
            overflowFull = OverflowBucket.FullPolicy.REFUSE;
            return;
        }
        // Normalise to the enum grammar: "drop-oldest" → "DROP_OLDEST".
        String normalized = v.trim().toUpperCase().replace('-', '_');
        try {
            overflowFull = OverflowBucket.FullPolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "overflow.full must be 'refuse' or 'drop-oldest', got: " + v);
        }
    }

    public void setOverflowFull(OverflowBucket.FullPolicy v) {
        overflowFull = v == null ? OverflowBucket.FullPolicy.REFUSE : v;
    }

    public void setOverflowDrain(String v) {
        if (isBlank(v)) {
            overflowDrain = OverflowBucket.DrainPolicy.ORDERED;
            return;
        }
        String normalized = v.trim().toUpperCase().replace('-', '_');
        try {
            overflowDrain = OverflowBucket.DrainPolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "overflow.drain must be 'ordered' or 'concurrent', got: " + v);
        }
    }

    public void setOverflowDrain(OverflowBucket.DrainPolicy v) {
        overflowDrain = v == null ? OverflowBucket.DrainPolicy.ORDERED : v;
    }

    public void setWireProtocolVersion(String v) {
        // Treat null, empty, AND whitespace-only as "use default" so an
        // operator config of `wire.protocol-version =   ` doesn't throw.
        // Mirrors the overflow.fsync / overflow.full handling above.
        String normalized = blankToNull(v);
        if (normalized == null) {
            wireProtocolVersion = 1;
            return;
        }
        switch (normalized) {
            case "1" -> wireProtocolVersion = 1;
            case "2" -> wireProtocolVersion = 2;
            default -> throw new IllegalStateException(
                "wire.protocol-version must be '1' or '2', got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type. Without this overload the String setter above is the
    // only pairing candidate and Sentinel's blueprint rejects the bean
    // with "At least one Setter method has to match the type of the
    // Getter method for property wireProtocolVersion". Same pattern as
    // setMetadataCase below.
    public void setWireProtocolVersion(int v) {
        switch (v) {
            case 1, 2 -> wireProtocolVersion = v;
            default -> throw new IllegalStateException(
                "wire.protocol-version must be 1 or 2, got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type (JavaBean property contract). Without this overload the
    // String setter above is the only pairing candidate and blueprint
    // rejects the bean with "At least one Setter method has to match the
    // type of the Getter method for property metadataCase".
    public void setMetadataCase(MetadataCase v) {
        metadataCase = v == null ? MetadataCase.PRESERVE : v;
    }

    // ---------- Getters -------------------------------------------------------

    public String  getWriteUrl()              { return writeUrl; }
    public String  getReadUrl()               { return readUrl; }
    public String  getInstanceId()            { return instanceId; }
    public String  getJobName()               { return jobName; }
    public String  getBasicUsername()         { return basicUsername; }
    public String  getBasicPassword()         { return basicPassword; }
    public String  getBearerToken()           { return bearerToken; }
    public String  getAuthorizationType()        { return authorizationType; }
    public String  getAuthorizationCredentials() { return authorizationCredentials; }
    public String  getTenantOrgId()           { return tenantOrgId; }
    public String  getTlsCaFile()             { return tlsCaFile; }
    public boolean isTlsInsecureSkipVerify()  { return tlsInsecureSkipVerify; }
    public int     getQueueCapacity()         { return queueCapacity; }
    public int     getBatchSize()             { return batchSize; }
    public long    getFlushIntervalMs()       { return flushIntervalMs; }
    public long    getBatchLingerMs()         { return batchLingerMs; }
    public long    getMetadataCadenceMs()     { return metadataCadenceMs; }
    public int     getMetadataAttrBudget()    { return metadataAttrBudget; }
    public String  getMetadataInfoColumns()   { return metadataInfoColumns; }
    /** The parsed {@code metadata.info-columns}, column → attribute key. */
    public Map<String, String> metadataInfoColumns() { return InfoColumns.parse(metadataInfoColumns, metadataRowlessKeys()); }

    /**
     * The source keys the metadata rows skip because every data series
     * carries them as a label: {@link MetadataRegistry#LABEL_KEYS} minus the
     * ones whose label {@code labels.exclude} removes from the wire, since a
     * value on neither series would be lost. A rename keeps the value on the
     * wire and changes nothing here.
     */
    public Set<String> metadataRowlessKeys() {
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<String, String> e : MetadataRegistry.LABEL_KEYS.entrySet()) {
            boolean excluded = false;
            for (String glob : labelsExcludeGlobs()) {
                if (globMatches(glob, e.getValue())) { excluded = true; break; }
            }
            if (!excluded) out.add(e.getKey());
        }
        return out;
    }
    public int     getRetryMaxAttempts()      { return retryMaxAttempts; }
    public long    getRetryInitialBackoffMs() { return retryInitialBackoffMs; }
    public long    getRetryMaxBackoffMs()     { return retryMaxBackoffMs; }
    public long    getHttpConnectTimeoutMs()  { return httpConnectTimeoutMs; }
    public long    getHttpReadTimeoutMs()     { return httpReadTimeoutMs; }
    public long    getHttpWriteTimeoutMs()    { return httpWriteTimeoutMs; }
    public int     getHttpMaxConnections()    { return httpMaxConnections; }
    public long    getShutdownGracePeriodMs()    { return shutdownGracePeriodMs; }
    public int     getWriterShards()             { return writerShards; }
    public long    getMaxSeriesLookbackSeconds() { return maxSeriesLookbackSeconds; }
    public DiscoveryStrategy getDiscoveryStrategy() { return discoveryStrategy; }
    public int     getDiscoveryBatchSize()     { return discoveryBatchSize; }
    public String  getLabelsInclude()         { return labelsInclude; }
    public String  getLabelsExclude()         { return labelsExclude; }
    public String  getLabelsRename()          { return labelsRename; }
    public String  getLabelsCopy()            { return labelsCopy; }
    public String  getMetricPrefix()          { return metricPrefix; }
    public StorePolicy getStorePolicy()       { return storePolicy; }
    public boolean isMetadataEnabled()        { return metadataEnabled; }
    public String  getMetadataInclude()       { return metadataInclude; }
    public String  getMetadataExclude()       { return metadataExclude; }
    public String  getMetadataLabelPrefix()   { return metadataLabelPrefix; }
    public MetadataCase getMetadataCase()     { return metadataCase; }

    // --- Overflow tier getters -----------------------------------------------

    public String getOverflowDir()                       { return overflowDir; }
    public long   getOverflowMaxSizeBytes()              { return overflowMaxSizeBytes; }
    public OverflowBucket.FullPolicy getOverflowFull()   { return overflowFull; }
    public OverflowBucket.DrainPolicy getOverflowDrain() { return overflowDrain; }
    public WalSegment.FsyncPolicy    getOverflowFsync()  { return overflowFsync; }

    /** True when a disk tier is configured; false disables spilling entirely. */
    public boolean isOverflowEnabled() { return overflowMaxSizeBytes > 0; }

    /** Byte budget for one shard's bucket: the total split evenly. */
    public long overflowBytesPerShard() { return overflowMaxSizeBytes / writerShards; }

    /** Segment size for one shard's bucket, derived so a bucket always holds
     *  {@link #SEGMENTS_PER_SHARD} of them. */
    public long overflowSegmentSizeBytes() { return overflowBytesPerShard() / SEGMENTS_PER_SHARD; }

    public int getWireProtocolVersion() { return wireProtocolVersion; }

    /**
     * Resolve {@link #overflowDir} to an absolute directory path. If the
     * operator set a non-blank value, it's returned verbatim. If blank
     * (the default), resolves to
     * {@code ${karaf.data}/prometheus-remote-writer/overflow} using the
     * {@code karaf.data} system property.
     *
     * <p>Intended to be called only when {@link #isOverflowEnabled()} is true.
     *
     * @throws IllegalStateException if the path is unresolvable (blank
     *   overflow.dir AND no karaf.data system property) — the operator must
     *   set an explicit overflow.dir outside Karaf, rather than have
     *   segments land somewhere unintended.
     */
    public String resolveOverflowDir() {
        if (!isBlank(overflowDir)) return overflowDir;
        String karafData = System.getProperty("karaf.data");
        if (karafData == null || karafData.isEmpty()) {
            throw new IllegalStateException(
                "a disk tier is configured (overflow.max-size-bytes=" + overflowMaxSizeBytes
                + ") but overflow.dir is empty and the karaf.data system property is not "
                + "set — either set overflow.dir explicitly, run inside a Karaf instance "
                + "where karaf.data is available, or set overflow.max-size-bytes=0 to run "
                + "without a disk tier");
        }
        return karafData + "/prometheus-remote-writer/overflow";
    }

    // ---------- helpers -------------------------------------------------------

    private static boolean isBlank(String s) { return s == null || s.isEmpty(); }

    // ---------- Authorization header-safety checks ----------------------------
    //
    // These mirror HttpHeadersConfig.validateValue's rules for the same reason
    // it has them: the value ends up in an HTTP header. Every message names the
    // key and never echoes the value.

    /** Header-splitting / response-splitting guard. */
    private static void requireNoCrLf(String key, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\r' || c == '\n') {
                throw new IllegalStateException(
                    key + " contains a CR or LF character — rejected to prevent header "
                    + "injection. (The offending value is not echoed here.)");
            }
        }
    }

    /** Printable ASCII, with HT and SP allowed — the rule http.headers.* values
     *  already follow. Some schemes carry parameter lists with spaces, so SP
     *  stays legal in the credentials half. */
    private static void requirePrintableAscii(String key, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\t' || c == ' ') continue;
            if (c < 0x20 || c > 0x7E) {
                // No offset: in a message whose whole point is not echoing the
                // value, an index would still tell a log reader where the first
                // illegal byte of a secret sits. The sibling in
                // HttpHeadersConfig omits it for the same reason.
                throw new IllegalStateException(
                    key + " contains a non-printable or non-ASCII byte — header content "
                    + "must be printable ASCII (HT and SP allowed). Base64-encode a binary "
                    + "credential at the source. (The offending value is not echoed here.)");
            }
        }
    }

    /**
     * The scheme keyword must be a single RFC 7230 token: {@code tchar+}, i.e.
     * {@code [!#$%&'*+.^_`|~0-9A-Za-z-]+}. This is NOT an allowlist — it rejects
     * no legitimate scheme keyword, registered or otherwise. What it catches is
     * the plausible misreading of the key as "the whole header value":
     * {@code auth.authorization.type = Token abc123} would otherwise emit
     * {@code Authorization: Token abc123 <credentials>}, silently wrong.
     */
    private static void requireHttpToken(String key, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean tchar = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                         || (c >= '0' && c <= '9')
                         || "!#$%&'*+-.^_`|~".indexOf(c) >= 0;
            if (tchar) continue;
            if (c == ' ' || c == '\t') {
                throw new IllegalStateException(
                    key + " contains whitespace — it takes only the scheme keyword (a "
                    + "single word such as Token or ApiKey), not the whole header value. "
                    + "Put the rest in auth.authorization.credentials. (The offending "
                    + "value is not echoed here.)");
            }
            throw new IllegalStateException(
                key + " contains a character that is not legal in an HTTP scheme keyword "
                + "— it must match [!#$%&'*+-.^_`|~0-9A-Za-z]+. "
                + "(The offending value is not echoed here.)");
        }
    }

    private static String blankToNull(String s) {
        if (s == null) return null;
        // Strip all Unicode whitespace — including NBSP (U+00A0), narrow NBSP
        // (U+202F), figure space (U+2007), and ideographic space (U+3000).
        // trim() strips ≤ U+0020 only; strip() uses Character.isWhitespace()
        // which excludes non-breaking spaces. (?U)\s matches the full Unicode
        // White_Space property, catching all of them. Without this,
        // invisible-character-only config values pass through as valid.
        String stripped = s.replaceAll("(?U)^\\s+|\\s+$", "");
        return stripped.isEmpty() ? null : stripped;
    }

    private static List<String> parseCsv(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(
            Arrays.stream(csv.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .toList());
    }

    private static void diffStr(List<String> out, String key, String before, String after) {
        if (!Objects.equals(before, after)) {
            out.add(key + ": " + show(before) + " -> " + show(after));
        }
    }

    private static void diffMasked(List<String> out, String key, String before, String after) {
        if (!Objects.equals(before, after)) {
            out.add(key + ": " + mask(before) + " -> " + mask(after));
        }
    }

    private static void diffBool(List<String> out, String key, boolean before, boolean after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static void diffInt(List<String> out, String key, int before, int after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static void diffLong(List<String> out, String key, long before, long after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static String show(String s)  { return s == null ? "(unset)" : "\"" + s + "\""; }
    private static String mask(String s)  { return s == null ? "(unset)" : "(set)"; }
}
