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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;

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
     * issues exactly one {@code GET /api/v1/series} per {@code findMetrics}
     * call. {@code LABEL_VALUES_FIRST} opts into the two-phase path:
     * enumerate {@code resourceId} values via
     * {@code /api/v1/label/resourceId/values}, then batch exact-match
     * alternations through {@code /api/v1/series} per
     * {@link #discoveryBatchSize}. The two-phase path only fires when the
     * matcher collection actually carries a regex on {@code resourceId} —
     * see {@link org.opennms.plugins.prometheus.remotewriter.read.PrometheusReadClient}.
     */
    public enum DiscoveryStrategy { SINGLE_PASS, LABEL_VALUES_FIRST }

    /**
     * Interface-speed label emission mode. {@code NORMALIZED} (default)
     * emits a single {@code if_speed} label in bits per second, computed as
     * {@code ifHighSpeed × 1_000_000} when non-zero, else {@code ifSpeed} —
     * the v0.4.x default and the right shape for greenfield Prometheus
     * dashboards. {@code RAW} emits the source tags verbatim as two labels
     * ({@code ifSpeed} and {@code ifHighSpeed}) and skips {@code if_speed}
     * entirely; matches the AGPL {@code opennms-cortex-tss-plugin}'s wire
     * shape so cortex-fielded dashboards and alert rules keep working
     * without a value-semantics rewrite. The two camelCase label names
     * are reserved against {@code labels.rename}/{@code labels.copy}
     * targets in both modes.
     */
    public enum IfSpeedMode { NORMALIZED, RAW }

    /**
     * Surveillance-categories label emission mode. {@code PER_CATEGORY}
     * (default) splits the OpenNMS-supplied {@code categories} source tag
     * on {@code ,} and emits one {@code onms_cat_<sanitized-name>="true"}
     * label per category — the v0.4.x default and the right shape for
     * set-membership PromQL ({@code {onms_cat_Server="true"}}). {@code RAW}
     * emits a single {@code categories="<verbatim-comma-joined>"} label
     * matching the AGPL {@code opennms-cortex-tss-plugin}'s wire shape so
     * cortex-fielded dashboards keep working without rewrite. {@code BOTH}
     * emits BOTH encodings on every sample carrying the {@code categories}
     * source tag — a migration scaffold for operators porting dashboards
     * from {@code onms_cat_X="true"} to {@code categories=~".*X.*"}.
     *
     * <p>The plugin delegates ordering to OpenNMS-core's
     * {@code MetaTagDataLoader.mapCategories()} which alphabetically sorts
     * the category list before joining with {@code ","} (no space) — see
     * {@code openspec/changes/add-cortex-categories-compat/design.md} §Spike
     * for the upstream-contract reference.
     *
     * <p>The {@code categories} (singular) label name and the
     * {@code onms_cat_*} label-name prefix are reserved against
     * {@code labels.rename} / {@code labels.copy} targets in all three
     * modes; flipping the mode does not unmask a previously-accepted
     * rename collision.
     */
    public enum CategoriesMode { PER_CATEGORY, RAW, BOTH }

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
    private String tenantOrgId;

    // --- TLS ---
    private String tlsCaFile;
    private boolean tlsInsecureSkipVerify;

    // --- Queue & batching ---
    private int  queueCapacity          = 10_000;
    private int  batchSize              = 1_000;
    private long flushIntervalMs        = 1_000L;
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
     *  nginx / Mimir 8 KiB limits. Ignored — with a startup WARN — when
     *  the strategy is {@code SINGLE_PASS}. */
    private int discoveryBatchSize = 50;

    /** Did the operator set {@code read.discovery-batch-size} explicitly to
     *  a non-default value? The validator uses this to emit a one-shot
     *  WARN when the value is set but the strategy is {@code SINGLE_PASS}
     *  (the knob would otherwise silently no-op). Set in
     *  {@link #setDiscoveryBatchSize(int)}, queried from
     *  {@link #validate()}. */
    private transient boolean discoveryBatchSizeExplicitlySet;

    // --- Label policy ---
    private String labelsInclude;
    private String labelsExclude;
    private String labelsRename;
    private String labelsCopy;
    private String metricPrefix;
    private IfSpeedMode ifSpeedMode = IfSpeedMode.NORMALIZED;
    private CategoriesMode categoriesMode = CategoriesMode.PER_CATEGORY;

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
     *  equivalent). The WAL is wire-version-agnostic, so flipping this
     *  knob with pending samples in the WAL is safe — the next flush
     *  emits according to the new value. */
    private int wireProtocolVersion = 1;

    // --- Write-Ahead Log (WAL) ---
    /** Whether to durably persist samples to disk before ack'ing store().
     *  When false (default), samples buffer in memory via queue.capacity and
     *  are lost on process restart — matching v0.4 behavior. When true, the
     *  plugin maintains a Write-Ahead Log at {@link #walPath} and replaces
     *  the in-memory queue as source of truth; queue.capacity is ignored
     *  with a startup WARN. See the "Write-Ahead Log" section of the
     *  README for disk-space planning. */
    private boolean walEnabled;

    /** Directory for WAL segments + checkpoint. Empty (default) resolves
     *  to {@code ${karaf.data}/prometheus-remote-writer/wal} at startup.
     *  Operators running containerised Karaf with an ephemeral
     *  {@code ${karaf.data}} MUST set this to a mounted volume or the WAL
     *  will evaporate across restarts (undermining its whole point). */
    private String walPath = "";

    /** Total on-disk footprint cap. Reached → {@link #walOverflow} policy
     *  kicks in. Default 512 MB. */
    private long walMaxSizeBytes = 536_870_912L;

    /** Per-segment rotation threshold. A new segment is opened when the
     *  active one crosses this size. Smaller = more segments = more
     *  syscalls for listing / GC; larger = fewer but coarser GC unit.
     *  Default 64 MB. */
    private long walSegmentSizeBytes = 67_108_864L;

    /** Fsync policy: {@code always} (fsync every append; tightest RPO,
     *  lowest throughput), {@code batch} (fsync at flush-interval
     *  boundary; loses at most ~flush-interval-ms of samples on kill -9),
     *  {@code never} (OS page cache only; suitable for ephemeral
     *  deployments). Default {@code batch}. */
    private WalSegment.FsyncPolicy walFsync = WalSegment.FsyncPolicy.BATCH;

    /** Overflow policy when {@link #walMaxSizeBytes} is reached:
     *  {@code backpressure} — new samples refused with StorageException
     *  (matches v0.4 queue-full semantics, default); {@code drop-oldest}
     *  — evict the oldest segment to make room. */
    private WalWriter.OverflowPolicy walOverflow = WalWriter.OverflowPolicy.BACKPRESSURE;

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
        if (hasBasicAuth() && hasBearerAuth()) {
            throw new IllegalStateException(
                "auth.basic.* and auth.bearer.token are mutually exclusive — "
                + "configure one or neither, not both");
        }
        if (hasBasicAuth() && (isBlank(basicUsername) || isBlank(basicPassword))) {
            throw new IllegalStateException(
                "Basic auth requires both auth.basic.username and auth.basic.password");
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

        validateWal();
        validateDiscovery();
        validateIfSpeedMode();
    }

    /**
     * Validate the {@code labels.if-speed-mode} knob. Mode parsing already
     * happened in {@link #setIfSpeedMode(String)} (which throws on bad
     * input); this method emits a one-shot WARN when the operator set a
     * {@code labels.rename = if_speed -> X} entry while the mode is
     * {@code raw} (the rename is a no-op there since {@code if_speed} is
     * not emitted in raw mode). Don't reject — operators flipping the mode
     * mid-deployment may keep the rename briefly during transition.
     */
    private void validateIfSpeedMode() {
        if (ifSpeedMode == IfSpeedMode.RAW
                && labelsRenameMap().containsKey("if_speed")) {
            org.slf4j.LoggerFactory.getLogger(PrometheusRemoteWriterConfig.class).warn(
                "labels.rename = if_speed -> {} is a no-op when labels.if-speed-mode=raw, "
                + "since 'if_speed' is not emitted in raw mode. Remove the rename or set "
                + "labels.if-speed-mode=normalized.",
                labelsRenameMap().get("if_speed"));
        }
    }

    /**
     * Validate the two-phase discovery knobs. Strategy parsing already
     * happened in {@link #setDiscoveryStrategy(String)} (which throws on
     * bad input); this method enforces the batch-size bounds and the
     * "set non-default with single-pass strategy" warn.
     */
    private void validateDiscovery() {
        if (discoveryBatchSize < 1 || discoveryBatchSize > 200) {
            throw new IllegalStateException(
                "read.discovery-batch-size must be in [1, 200] (got "
                + discoveryBatchSize + ")");
        }
        if (discoveryStrategy == DiscoveryStrategy.SINGLE_PASS
                && discoveryBatchSizeExplicitlySet
                && discoveryBatchSize != 50) {
            // One-shot WARN — set non-default while strategy is single-pass
            // is almost always a configuration mistake (operator copied the
            // knob without flipping the strategy). Don't reject — operators
            // experimenting with the toggle should be able to flip it back
            // without triggering startup errors.
            org.slf4j.LoggerFactory.getLogger(PrometheusRemoteWriterConfig.class).warn(
                "read.discovery-batch-size={} is set but read.discovery-strategy=single-pass; "
                + "the batch-size knob has no effect on the single-pass path. "
                + "Either flip read.discovery-strategy to label-values-first or remove the batch-size override.",
                discoveryBatchSize);
        }
    }

    private void validateWal() {
        if (!walEnabled) return; // all other wal.* fields are ignored when disabled
        if (walMaxSizeBytes <= 0) {
            throw new IllegalStateException(
                "wal.max-size-bytes must be > 0 when wal.enabled=true (got " + walMaxSizeBytes + ")");
        }
        if (walSegmentSizeBytes <= 0) {
            throw new IllegalStateException(
                "wal.segment-size-bytes must be > 0 when wal.enabled=true (got " + walSegmentSizeBytes + ")");
        }
        if (walSegmentSizeBytes > walMaxSizeBytes) {
            throw new IllegalStateException(
                "wal.segment-size-bytes (" + walSegmentSizeBytes + ") must not exceed "
                + "wal.max-size-bytes (" + walMaxSizeBytes + ") — otherwise a single "
                + "segment alone could fill the cap and leave nothing to evict under "
                + "drop-oldest");
        }
        // wal.path resolution is deferred to resolveWalPath() since Blueprint
        // may read karaf.data lazily; validate() only enforces the
        // syntactically-required fields here. Physical-path writability is
        // checked in WalRecovery at startup.
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
            String base = primitiveKey + " target '" + to + "' collides with the default label '" + to
                + "'. The plugin already emits this label; " + verbIng + " onto it would silently "
                + "clobber the default value. Pick a different 'to' name.";
            if ("ifSpeed".equals(to) || "ifHighSpeed".equals(to)) {
                base += " To recover cortex-compatible interface-speed labels,"
                      + " set 'labels.if-speed-mode = raw' instead of a rename.";
            } else if ("categories".equals(to)) {
                base += " To recover cortex-compatible categories labels,"
                      + " set 'labels.categories-mode = raw' (or 'both' during"
                      + " migration) instead of a rename.";
            }
            return base;
        }
        for (String prefix : LabelMapper.RESERVED_LABEL_PREFIXES) {
            if (to.startsWith(prefix)) {
                String reason;
                switch (prefix) {
                    case "onms_cat_":     reason = "surveillance categories";                       break;
                    case "onms_attr_":    reason = "resource string attributes (meta partition)";   break;
                    case "onms_extattr_": reason = "resource string attributes (external partition)"; break;
                    default:              reason = "metadata passthrough";                          break;
                }
                return primitiveKey + " target '" + to + "' collides with the reserved prefix '"
                    + prefix + "*' (" + reason + "). Pick a different 'to' name.";
            }
        }
        return null;
    }

    public boolean hasBasicAuth()  { return !isBlank(basicUsername) || !isBlank(basicPassword); }
    public boolean hasBearerAuth() { return !isBlank(bearerToken); }
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
        diffStr(out, "tenant.org-id",             other.tenantOrgId,           tenantOrgId);
        diffStr(out, "tls.ca-file",               other.tlsCaFile,             tlsCaFile);
        diffBool(out, "tls.insecure-skip-verify", other.tlsInsecureSkipVerify, tlsInsecureSkipVerify);
        diffInt(out, "queue.capacity",            other.queueCapacity,         queueCapacity);
        diffInt(out, "batch.size",                other.batchSize,             batchSize);
        diffLong(out, "flush.interval-ms",        other.flushIntervalMs,       flushIntervalMs);
        diffInt(out, "retry.max-attempts",        other.retryMaxAttempts,      retryMaxAttempts);
        diffLong(out, "retry.initial-backoff-ms", other.retryInitialBackoffMs, retryInitialBackoffMs);
        diffLong(out, "retry.max-backoff-ms",     other.retryMaxBackoffMs,     retryMaxBackoffMs);
        diffLong(out, "http.connect-timeout-ms",  other.httpConnectTimeoutMs,  httpConnectTimeoutMs);
        diffLong(out, "http.read-timeout-ms",     other.httpReadTimeoutMs,     httpReadTimeoutMs);
        diffLong(out, "http.write-timeout-ms",    other.httpWriteTimeoutMs,    httpWriteTimeoutMs);
        diffInt(out, "http.max-connections",      other.httpMaxConnections,    httpMaxConnections);
        diffLong(out, "shutdown.grace-period-ms", other.shutdownGracePeriodMs, shutdownGracePeriodMs);
        diffLong(out, "max-series-lookback-seconds", other.maxSeriesLookbackSeconds, maxSeriesLookbackSeconds);
        diffStr(out, "read.discovery-strategy",   other.discoveryStrategy.name(),   discoveryStrategy.name());
        diffInt(out, "read.discovery-batch-size", other.discoveryBatchSize,         discoveryBatchSize);
        diffStr(out, "labels.include",            other.labelsInclude,         labelsInclude);
        diffStr(out, "labels.exclude",            other.labelsExclude,         labelsExclude);
        diffStr(out, "labels.rename",             other.labelsRename,          labelsRename);
        diffStr(out, "labels.copy",               other.labelsCopy,            labelsCopy);
        diffStr(out, "labels.if-speed-mode",      other.ifSpeedMode.name(),    ifSpeedMode.name());
        diffStr(out, "labels.categories-mode",    other.categoriesMode.name(), categoriesMode.name());
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
    public void setTenantOrgId(String v)           { tenantOrgId = blankToNull(v); }
    public void setTlsCaFile(String v)             { tlsCaFile = blankToNull(v); }
    public void setTlsInsecureSkipVerify(boolean v){ tlsInsecureSkipVerify = v; }
    public void setQueueCapacity(int v)            { queueCapacity = v; }
    public void setBatchSize(int v)                { batchSize = v; }
    public void setFlushIntervalMs(long v)         { flushIntervalMs = v; }
    public void setRetryMaxAttempts(int v)         { retryMaxAttempts = v; }
    public void setRetryInitialBackoffMs(long v)   { retryInitialBackoffMs = v; }
    public void setRetryMaxBackoffMs(long v)       { retryMaxBackoffMs = v; }
    public void setHttpConnectTimeoutMs(long v)    { httpConnectTimeoutMs = v; }
    public void setHttpReadTimeoutMs(long v)       { httpReadTimeoutMs = v; }
    public void setHttpWriteTimeoutMs(long v)      { httpWriteTimeoutMs = v; }
    public void setHttpMaxConnections(int v)       { httpMaxConnections = v; }
    public void setShutdownGracePeriodMs(long v)      { shutdownGracePeriodMs = v; }
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
    // setMetadataCase / setWalFsync / setWalOverflow.
    public void setDiscoveryStrategy(DiscoveryStrategy v) {
        discoveryStrategy = v == null ? DiscoveryStrategy.SINGLE_PASS : v;
    }

    public void setDiscoveryBatchSize(int v) {
        discoveryBatchSize = v;
        // Track whether the operator changed it; the validator uses this to
        // distinguish "default 50 with single-pass" (silent) from "operator
        // set 200 with single-pass" (warn — likely a configuration mistake).
        discoveryBatchSizeExplicitlySet = (v != 50);
    }
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
        // Reject prefixes that collide with another emitter's reserved
        // namespace (e.g. onms_cat_, onms_attr_). onms_meta_ itself is the
        // default and is intentionally exempt — that's this emitter's own
        // canonical home.
        //
        // Comparison is lowercased so an operator value like ONMS_ATTR_ is
        // caught — the rejection's intent is "namespace ownership", not
        // "byte equality on the wire". And the subsumption check goes both
        // ways so a SHORTER operator prefix that the reserved namespace
        // starts with (e.g. metadata.label-prefix = onms_) is also rejected,
        // since it would emit metadata into the same wire shape as the
        // reserved emitter.
        String lc = sanitized.toLowerCase(java.util.Locale.ROOT);
        for (String reserved : LabelMapper.RESERVED_LABEL_PREFIXES) {
            if ("onms_meta_".equals(reserved)) continue;
            if (lc.equals(reserved) || lc.startsWith(reserved) || reserved.startsWith(lc)) {
                throw new IllegalStateException(
                    "metadata.label-prefix '" + sanitized + "' collides with the reserved '"
                    + reserved + "*' namespace owned by another emitter. Pick a different prefix.");
            }
        }
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

    public void setIfSpeedMode(String v) {
        if (isBlank(v)) {
            ifSpeedMode = IfSpeedMode.NORMALIZED;
            return;
        }
        // Locale.ROOT — under tr_TR, default-locale toUpperCase() turns lowercase
        // 'i' into dotted-İ which IfSpeedMode.valueOf would reject. Same precedent
        // as setDiscoveryStrategy's toLowerCase(Locale.ROOT).
        String normalized = v.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        try {
            ifSpeedMode = IfSpeedMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "labels.if-speed-mode must be 'normalized' or 'raw', got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type. Same pattern as setMetadataCase / setDiscoveryStrategy /
    // setWireProtocolVersion(int).
    public void setIfSpeedMode(IfSpeedMode v) {
        ifSpeedMode = v == null ? IfSpeedMode.NORMALIZED : v;
    }

    public void setCategoriesMode(String v) {
        if (isBlank(v)) {
            categoriesMode = CategoriesMode.PER_CATEGORY;
            return;
        }
        // Locale.ROOT — same Turkish-locale-bug avoidance as setIfSpeedMode.
        String normalized = v.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        try {
            categoriesMode = CategoriesMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "labels.categories-mode must be 'per-category', 'raw', or 'both', got: " + v);
        }
    }

    // Aries Blueprint setter overload — same pattern as setIfSpeedMode(IfSpeedMode).
    public void setCategoriesMode(CategoriesMode v) {
        categoriesMode = v == null ? CategoriesMode.PER_CATEGORY : v;
    }

    // --- WAL setters ---------------------------------------------------------

    public void setWalEnabled(boolean v)        { walEnabled = v; }
    public void setWalPath(String v)            { walPath = v == null ? "" : v.trim(); }
    public void setWalMaxSizeBytes(long v)      { walMaxSizeBytes = v; }
    public void setWalSegmentSizeBytes(long v)  { walSegmentSizeBytes = v; }

    public void setWalFsync(String v) {
        if (isBlank(v)) {
            walFsync = WalSegment.FsyncPolicy.BATCH;
            return;
        }
        String normalized = v.trim().toUpperCase();
        try {
            walFsync = WalSegment.FsyncPolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "wal.fsync must be 'always', 'batch', or 'never', got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type. Without this overload the String setter above is the
    // only pairing candidate and Sentinel's blueprint rejects the bean.
    // Same pattern as setMetadataCase / setWireProtocolVersion(int).
    public void setWalFsync(WalSegment.FsyncPolicy v) {
        walFsync = v == null ? WalSegment.FsyncPolicy.BATCH : v;
    }

    public void setWireProtocolVersion(String v) {
        // Treat null, empty, AND whitespace-only as "use default" so an
        // operator config of `wire.protocol-version =   ` doesn't throw.
        // Mirrors the wal.fsync / wal.overflow handling above.
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

    public void setWalOverflow(String v) {
        if (isBlank(v)) {
            walOverflow = WalWriter.OverflowPolicy.BACKPRESSURE;
            return;
        }
        // Normalise to the enum grammar: "drop-oldest" → "DROP_OLDEST".
        String normalized = v.trim().toUpperCase().replace('-', '_');
        try {
            walOverflow = WalWriter.OverflowPolicy.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "wal.overflow must be 'backpressure' or 'drop-oldest', got: " + v);
        }
    }

    // Aries Blueprint requires at least one setter to match the getter's
    // return type. Without this overload the String setter above is the
    // only pairing candidate and Sentinel's blueprint rejects the bean.
    // Same pattern as setMetadataCase / setWireProtocolVersion(int) /
    // setWalFsync(FsyncPolicy).
    public void setWalOverflow(WalWriter.OverflowPolicy v) {
        walOverflow = v == null ? WalWriter.OverflowPolicy.BACKPRESSURE : v;
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
    public String  getTenantOrgId()           { return tenantOrgId; }
    public String  getTlsCaFile()             { return tlsCaFile; }
    public boolean isTlsInsecureSkipVerify()  { return tlsInsecureSkipVerify; }
    public int     getQueueCapacity()         { return queueCapacity; }
    public int     getBatchSize()             { return batchSize; }
    public long    getFlushIntervalMs()       { return flushIntervalMs; }
    public int     getRetryMaxAttempts()      { return retryMaxAttempts; }
    public long    getRetryInitialBackoffMs() { return retryInitialBackoffMs; }
    public long    getRetryMaxBackoffMs()     { return retryMaxBackoffMs; }
    public long    getHttpConnectTimeoutMs()  { return httpConnectTimeoutMs; }
    public long    getHttpReadTimeoutMs()     { return httpReadTimeoutMs; }
    public long    getHttpWriteTimeoutMs()    { return httpWriteTimeoutMs; }
    public int     getHttpMaxConnections()    { return httpMaxConnections; }
    public long    getShutdownGracePeriodMs()    { return shutdownGracePeriodMs; }
    public long    getMaxSeriesLookbackSeconds() { return maxSeriesLookbackSeconds; }
    public DiscoveryStrategy getDiscoveryStrategy() { return discoveryStrategy; }
    public int     getDiscoveryBatchSize()     { return discoveryBatchSize; }
    public String  getLabelsInclude()         { return labelsInclude; }
    public String  getLabelsExclude()         { return labelsExclude; }
    public String  getLabelsRename()          { return labelsRename; }
    public String  getLabelsCopy()            { return labelsCopy; }
    public String  getMetricPrefix()          { return metricPrefix; }
    public IfSpeedMode getIfSpeedMode()       { return ifSpeedMode; }
    public CategoriesMode getCategoriesMode() { return categoriesMode; }
    public boolean isMetadataEnabled()        { return metadataEnabled; }
    public String  getMetadataInclude()       { return metadataInclude; }
    public String  getMetadataExclude()       { return metadataExclude; }
    public String  getMetadataLabelPrefix()   { return metadataLabelPrefix; }
    public MetadataCase getMetadataCase()     { return metadataCase; }

    // --- WAL getters ---------------------------------------------------------

    public boolean isWalEnabled()                       { return walEnabled; }
    public String  getWalPath()                         { return walPath; }
    public long    getWalMaxSizeBytes()                 { return walMaxSizeBytes; }
    public long    getWalSegmentSizeBytes()             { return walSegmentSizeBytes; }
    public WalSegment.FsyncPolicy    getWalFsync()      { return walFsync; }
    public WalWriter.OverflowPolicy  getWalOverflow()   { return walOverflow; }

    public int getWireProtocolVersion() { return wireProtocolVersion; }

    /**
     * Resolve {@link #walPath} to an absolute directory path. If the
     * operator set a non-blank value, it's returned verbatim. If blank
     * (the default), resolves to {@code ${karaf.data}/prometheus-remote-writer/wal}
     * using the {@code karaf.data} system property.
     *
     * <p>Intended to be called only when {@link #isWalEnabled()} is true.
     *
     * @throws IllegalStateException if the path is unresolvable (blank
     *   wal.path AND no karaf.data system property) — operator must set
     *   an explicit wal.path outside Karaf.
     */
    public String resolveWalPath() {
        if (!isBlank(walPath)) return walPath;
        String karafData = System.getProperty("karaf.data");
        if (karafData == null || karafData.isEmpty()) {
            throw new IllegalStateException(
                "wal.enabled=true but wal.path is empty and the karaf.data "
                + "system property is not set — either set wal.path explicitly "
                + "or run inside a Karaf instance where karaf.data is available");
        }
        return karafData + "/prometheus-remote-writer/wal";
    }

    // ---------- helpers -------------------------------------------------------

    private static boolean isBlank(String s) { return s == null || s.isEmpty(); }

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
