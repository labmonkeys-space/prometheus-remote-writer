# Changelog

All notable changes to this project are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`labels.if-speed-mode` configuration knob (cortex-compat for SNMP
  interface-speed labels).** Operators migrating dashboards and alert
  rules from the AGPL `opennms-cortex-tss-plugin` can now flip the wire
  shape of the SNMP interface-speed labels with a single config
  directive instead of rewriting PromQL. Two enum values:
    - `normalized` (default, v0.4.x bit-for-bit): one `if_speed` label
      in bps, computed as `ifHighSpeed × 1_000_000` when non-zero, else
      `ifSpeed`. Prometheus-idiomatic, immediately rateable, no PromQL
      branching.
    - `raw`: two camelCase labels `ifSpeed` and `ifHighSpeed` emitted
      with the SNMP source values verbatim (cortex parity); no
      normalization, no synthesis. The single `if_speed` label is NOT
      emitted in raw mode. A row with only `ifHighSpeed` source-present
      emits ONLY `ifHighSpeed` — there is no `ifSpeed = ifHighSpeed × 1_000_000`
      synthesis (cortex parity, pinned by a `documented-behavior` test).
  The two camelCase names `ifSpeed` and `ifHighSpeed` are added to the
  reserved-rename / reserved-copy target set **unconditionally**
  (regardless of the active mode) so a hot-reload from `normalized` to
  `raw` cannot unmask a previously-accepted `labels.rename = X -> ifSpeed`
  collision. Operators upgrading from v0.4.2 with the cortex-migration
  recipe verbatim will see a startup error on the `if_speed -> ifSpeed`
  rename line — drop that line and add `labels.if-speed-mode = raw`
  instead. The new rejection error message names both the collision and
  the supported migration path. The reserved-target enforcement is the
  only operator-facing tightening; the default emission shape is
  unchanged from v0.4.x. The implementation is clean-room — designed
  from the SNMP MIB-II `ifTable` / `ifXTable` definitions and direct
  observation of the cortex emission shape via its public
  `toPrometheusTimeSeries` label-emission contract. No code or pattern
  is taken from the AGPL cortex source.

  *WAL flip semantics.* The WAL stores already-mapped `MappedSample`
  records, so flipping `labels.if-speed-mode` with pending WAL contents
  produces a brief mixed-emission window (typically seconds at default
  drain cadence): pre-flip samples flush under the old mode, new
  samples emit under the new mode. Operators see briefly mixed-mode
  series identity at the backend during the flip; old series age out of
  retention naturally.

- **`labels.categories-mode` configuration knob (cortex-compat for
  surveillance-categories labels).** Sibling to `labels.if-speed-mode` —
  resolves the second of three structural-difference rows in the v0.4.2
  *Migration from `opennms-cortex-tss-plugin`* doc table. Operators
  migrating dashboards from the AGPL cortex plugin can now flip the
  wire shape of the categories surface with a single config directive
  instead of rewriting per-category-boolean queries
  (`{onms_cat_Server="true"}`) into substring/regex form
  (`categories=~".*Server.*"`). Three enum values:
    - `per-category` (default, v0.4.x bit-for-bit): one
      `onms_cat_<sanitized-name>="true"` label per category.
      Prometheus-idiomatic for set-membership PromQL.
    - `raw`: a single `categories="<comma-joined-names>"` label,
      emitted with the OpenNMS-supplied source value verbatim. Cortex
      parity. The per-category `onms_cat_*` labels are NOT emitted.
    - `both`: emit both encodings on every sample carrying the
      `categories` source tag — a migration scaffold for porting
      dashboards one panel at a time, then dropping to `raw`. Wire
      overhead during `both`: ~30-50 bytes per sample for nodes with at
      least one category.
  The `categories` (singular) label name is added to the reserved
  rename / copy target set **unconditionally**; the existing
  `onms_cat_*` prefix reservation stays unconditional. Hot-reload
  footgun-closure pattern matches the v0.4.3 `ifSpeed` / `ifHighSpeed`
  reservation.

  *Spike finding (load-bearing).* OpenNMS-core's
  `MetaTagDataLoader.mapCategories()` (Apache 2.0) pre-sorts the
  category list alphabetically with `Collections.sort(catList)` before
  joining via `String.join(",", catList)` (no space). Raw mode in this
  plugin delegates ordering to that upstream contract — no
  canonicalization on the plugin's side, no cardinality bug from
  unstable order. The "byte-for-byte cortex parity" claim is achievable
  for free. A test (`categories_raw_mode_does_not_resort_source_value`)
  pins the delegation so a future contributor "helpfully" adding a sort
  would break cortex parity at CI time.

  *Cortex-parity edge case.* Category names containing literal commas
  (e.g. a single category named `"Foo,Bar"`) collide with the
  comma-delimited representation in any mode — `categories="Foo,Bar"`
  is indistinguishable from two-category `"Foo"` + `"Bar"`. Cortex
  broke on this too. Documented in the *Categories labels in raw and
  both modes* doc subsection; pinned by the
  `categories_with_comma_in_name_breaks_raw_mode_round_trip` test.

  *WAL flip semantics.* Same as `labels.if-speed-mode`. WAL-flip note
  in the migration doc extended to cover both knobs.

  Implementation is clean-room — designed from OpenNMS-core's
  `MetaTagDataLoader.mapCategories()` (Apache 2.0) and direct
  observation of the cortex emission shape via its public
  `toPrometheusTimeSeries` label-emission contract. No code or pattern
  is taken from the AGPL cortex source.

### Changed

- **Reserved exact rename / copy target set tightened to include
  `ifSpeed` and `ifHighSpeed`.** *Affects only operators using the v0.4.2
  cortex-migration recipe verbatim.* The recipe published in v0.4.2's
  `Migration from opennms-cortex-tss-plugin` doc section included
  `labels.rename = if_speed -> ifSpeed` to recover the cortex spelling
  for the speed label. With `labels.if-speed-mode` landing, `ifSpeed` is
  now reserved (always — see above) and the rename rejects at startup
  with an actionable error pointing at the new knob. Migration: drop the
  `if_speed -> ifSpeed` line from your `labels.rename` and add
  `labels.if-speed-mode = raw` to your cfg; the rest of the recipe is
  unchanged. The doc section is updated in lockstep.

- **Reserved exact rename / copy target set tightened to include
  `categories`.** Same shape as the `ifSpeed` / `ifHighSpeed`
  tightening above. *No v0.4.x cortex-migration recipe published a
  `labels.rename = X -> categories` directive* — the v0.4.2 doc
  explicitly described the categories surface as "structurally
  different — dashboard rewrite required" — so the actual
  operator-impact surface is small. Migration: drop any
  `labels.rename = X -> categories` directives and use
  `labels.categories-mode = raw` (or `both` during migration) instead.
  The error message names both the collision and the new knob.

- **In-tree test fixture format normalized.** `LabelMapperTest`
  fixture builders previously used `"Routers, ProductionSites"` (with
  a stray space) as the `categories` source value. Real OpenNMS output
  is `"ProductionSites,Routers"` (alphabetically sorted, no space).
  Fixture updated to match. Existing per-category tests pass unchanged
  (the split-and-trim logic is tolerant of either format).

## [0.4.2] — 2026-05-08

### Documentation

- **New section "Migration from `opennms-cortex-tss-plugin`"** in the
  *Labels and enrichment* docs. Operators moving from the AGPL cortex
  plugin can recover the camelCase label spellings cortex emitted
  (`nodeLabel`, `foreignSource`, `foreignId`, `ifName`, `ifDescr`,
  `ifSpeed`) with a copy-paste `labels.rename` recipe. Honest call-outs
  cover three caveats the rename can't bridge: `ifSpeed` value
  semantics differ for high-speed interfaces (this plugin normalises
  `ifHighSpeed × 1_000_000` when non-zero, cortex emitted both raw),
  `nodeId` is not directly recoverable (this plugin emits FS-qualified
  `node` instead of cortex's raw numeric dbId), and `categories` is a
  structural difference (per-category `onms_cat_<name>="true"` booleans
  vs cortex's single comma-separated label) that requires a
  dashboard-level rewrite. A unit test in `PrometheusRemoteWriterConfigTest`
  runs the published recipe through `validate()` so a future
  reserved-target change that would silently break the recipe fails CI
  instead.

## [0.4.1] — 2026-05-07

### Added

- **Optional two-phase resource discovery for `findMetrics`.**
  Operators on Grafana Mimir, Thanos, or other large multi-tenant
  backends can opt into a two-phase path that enumerates `resourceId`
  values via `GET /api/v1/label/resourceId/values` first and then issues
  batched `GET /api/v1/series` calls with anchored exact-match
  alternation (`resourceId=~"^(v1|v2|…)$"`) per
  `read.discovery-batch-size` chunk. The motivation is the broad-regex
  scan ceiling: when OpenNMS calls `findMetrics` for resource-graph
  rendering with a regex on `resourceId` (the typical wildcard
  discovery shape), the single-pass `series` endpoint forces the
  backend to scan every matching series across the lookback window —
  Prometheus standalone tolerates it; Mimir's series API is
  block-store-bound and Thanos's StoreAPI fan-out amplifies the cost.
  The `label/values` endpoint is index-only on those systems and
  returns just the distinct `resourceId` strings, which the phase-2
  alternation then resolves through label-postings rather than a full
  series scan. Two new operator-facing knobs:
    - `read.discovery-strategy` (enum, default `single-pass`):
      `single-pass` preserves v0.5.0 behavior bit-for-bit; flip to
      `label-values-first` to opt into the two-phase path.
    - `read.discovery-batch-size` (int, default `50`, range
      `[1, 200]`): maximum `resourceId` values per phase-2
      alternation; chunks above this cap split into multiple phase-2
      calls. Setting this with `single-pass` strategy logs a one-shot
      WARN — the knob has no effect on the single-pass path.
  Fires only when the matcher collection contains an `EQUALS_REGEX`
  matcher on `resourceId` and no `NOT_EQUALS_REGEX` on the same key.
  Exact-match callers, matchers on other labels, NOT_EQUALS_REGEX-only
  on `resourceId`, and mixed EQUALS_REGEX + NOT_EQUALS_REGEX on
  `resourceId` all stay on the single-pass path even with the toggle
  on (the alternation form silently inverts the operator's exclusion
  intent for negative regex; falling through preserves correctness at
  the cost of the optimization for those rarer call shapes). The plugin does not silently fall back from
  two-phase to single-pass on phase-1 failure — a non-2xx response
  surfaces as `StorageException`, identical to single-pass error
  semantics. New observability counters
  `find_metrics_two_phase_total`, `find_metrics_single_pass_total`,
  and `find_metrics_phase2_batches_total` make the path mix and
  per-call batch counts visible. Migration: leave the default for
  vanilla Prometheus; flip to `label-values-first` on Mimir / Thanos
  and tune `read.discovery-batch-size` against your workload. The
  implementation is clean-room — designed from the
  [Prometheus HTTP query API spec](https://prometheus.io/docs/prometheus/latest/querying/api/)
  (specifically `/api/v1/label/<name>/values` with `match[]` filtering,
  available since Prometheus 2.24, 2021-01) and direct observation of
  the use case. No code or pattern is taken from the AGPL-3.0
  `opennms-cortex-tss-plugin`'s `feature/label-values-discovery`
  branch.

### Changed

- **Apache Karaf bumped from 4.4.10 to 4.4.11** (`org.apache.karaf.shell:org.apache.karaf.shell.core` and `org.apache.karaf.tooling:karaf-maven-plugin`, via Dependabot PRs [#48] and [#49]). Patch-level upstream bump; no behavior change for the plugin.

## [0.4.0] — 2026-05-05

### Fixed

- **Resource-graph placeholder substitution rendered the literal
  `${name}` / `${datname}` / `${spcname}` in OpenNMS Horizon resource
  graphs.** OpenNMS resource-graph templates substitute these shell-
  style placeholders against *string attributes* attached to a
  resource. On the integration-API write path, OpenNMS-core's
  `TimeseriesPersistOperationBuilder` attaches them to
  `Sample.metric.externalTags()`; the motivating case is the attribute
  named `name` (the *Eventd Processing Stats* row, JDBC datasource
  labels, similar) whose key collides with the intrinsic `name`
  (metric-name) tag the plugin emits as `__name__`. Two independent
  gates in `LabelMapper` dropped the value: the `putIfAbsent` shadow
  merge in `collectTags()` discarded it before any allowlist ran, and
  the `consumedSourceKeys` skip in `applyInclude` blocked the obvious
  operator workaround (`labels.include = name`). On the read side,
  OpenNMS-core's `TimeseriesResourceStorageDao.getStringAttributes()`
  consults `Metric.getExternalTags()` only, so partition fidelity is
  required end-to-end.

  The fix uses two reserved label prefixes — one per partition — to
  round-trip plain-key tags through Prometheus while preserving which
  partition each tag came from:

  - `onms_attr_<sanitized_key>` carries the **meta** partition (MATE-
    scope tags, `mtype` aside).
  - `onms_extattr_<sanitized_key>` carries the **external** partition
    (collector-emitted resource string attributes — the values OpenNMS
    placeholder substitution actually reads).

  Mechanism per partition:

  - **Write side** — a new `emitAttrLabels` step walks each partition's
    tag list directly off the source `Metric` (bypassing the shadow
    merge in `collectTags`) and emits each tag as `<prefix><sanitized_
    key>=<sanitized_value>`. Same skip-filters apply to both
    partitions: empty key, colon-keyed (context tags continue to flow
    through `MetadataProcessor`'s `onms_meta_*`), `mtype` (handled by
    the dedicated default emission), and the built-in plain-key secret
    denylist (`*password*`, `*secret*`, `*token*`, `snmp-community`,
    all case-insensitive — narrower than the context-tag form's
    `*:*key*` so legitimate resource attributes shaped like
    `primary_key` / `partition_key` / `foreign_key` flow through). The
    external-tag pass additionally skips keys the default emitter
    already represents under canonical names (`nodeLabel`,
    `foreignSource`, `foreignId`, `nodeId`, `categories`, `ifName`,
    `ifDescr`, `ifSpeed`, `ifHighSpeed`, `location`) so they aren't
    double-emitted under the new prefix.
  - **Read side** — `PromResponseParser.labelObjectToMetric` strips
    `onms_attr_` to `metric.getMetaTags()` and `onms_extattr_` to
    `metric.getExternalTags()`. Bare prefixes fall through to the
    catch-all (preserving the verbatim label name as a generic meta
    tag). Prefixed forms are not also emitted under their raw names —
    single source of truth, per partition.

  Identifier-shaped attribute names (`name`, `datname`, `spcname`, …)
  round-trip identically. Non-identifier source keys (e.g. `rack-unit`)
  arrive on the read side in their sanitized form (`rack_unit`) —
  adjust placeholder references accordingly. There is no read-side
  synthesis for samples written before this fix; pre-fix data
  continues to render literal placeholders until it ages out of
  backend retention.

### Added

- **`onms_attr_*` reserved label-name prefix** carrying the meta
  partition through the round-trip.
- **`onms_extattr_*` reserved label-name prefix** carrying the
  external partition through the round-trip — the partition OpenNMS-
  core's `TimeseriesResourceStorageDao.getStringAttributes()` reads
  for resource-graph placeholder substitution. Same reserved-prefix
  discipline as `onms_attr_*`: `labels.rename`, `labels.copy`, and
  `metadata.label-prefix` directives whose target lands in the
  namespace are rejected at startup. Per-prefix opt-out via
  `labels.exclude = onms_attr_*` / `labels.exclude = onms_extattr_*`.
- **Partition-tagged collision messages.** Validation rejection text
  for `labels.rename` / `labels.copy` / `metadata.label-prefix`
  collisions on the resource-attribute namespaces reads `"resource
  string attributes (meta partition)"` for `onms_attr_*` and `"resource
  string attributes (external partition)"` for `onms_extattr_*`. Helps
  operators distinguish the two partitions when debugging a config
  rejection.
- **`metadata.label-prefix` collision rejection.** Setting
  `metadata.label-prefix` to a value that collides with another
  emitter's reserved namespace (`onms_cat_`, `onms_attr_`,
  `onms_extattr_`, or any shorter prefix that subsumes them like
  `onms_`) is now rejected at startup with an actionable message.
  Comparison is case-insensitive (`ONMS_ATTR_` is caught) and
  bidirectional (a shorter operator value subsuming a reserved prefix
  is caught too). The default `onms_meta_` and unrelated values like
  `custom_` remain accepted.
- **`LabelMapper.ATTR_PREFIX` and `LabelMapper.EXTATTR_PREFIX` are
  public constants** so `PromResponseParser` can reference them
  cross-package and write/read stay in lockstep on the round-trip wire
  shape. ABI-relevant for any caller depending on the literals.

## [0.3.3] — 2026-05-05

A patch release whose headline is the **graph-rendering hotfix**: every
time-series graph fetch in OpenNMS Horizon 35 returned HTTP 500 against
a Prometheus stack served by v0.3.2, because OpenNMS-core
unconditionally dereferences an `mtype` meta tag the plugin was dropping
on write and could not reconstruct on read. v0.3.3 restores the
round-trip and adds a defensive read-side fallback for data already on
disk from before the upgrade.

**Upgrade if you use time-series graphs.** Every v0.3.2 deployment that
renders graphs through this plugin's read path is hitting the NPE and
serving 500s on `/measurements`. The fix is automatic on bundle
activation — no `.cfg` changes, no migration, no operator action beyond
installing the new KAR. Deployments that don't yet render graphs can
safely defer to the next minor release.

**No other behaviour change** from v0.3.2 — same Remote Write v1 wire
format, same authentication surface, same WAL semantics, same KAR
contents (modulo the bug fix). One additional Prometheus label per
series (`mtype`); cardinality is unchanged because the value is
constant per series.

### Fixed

- **Time-series graph rendering returned HTTP 500.** OpenNMS Horizon's
  `NewtsConverterUtils.dataPointToRow` unconditionally dereferences
  `MetaTagNames.mtype` on every `Metric` the plugin returns from
  `findMetrics` and `getTimeSeriesData`; v0.3.2 dropped the tag on
  write and could not reconstruct it on read. The web-tier surfaced
  the error as `NullPointerException: Cannot invoke
  "org.opennms.integration.api.v1.timeseries.Tag.getValue()" because
  the return value of "Metric.getFirstTagByKey(String)" is null` in
  `web.log`, with HTTP 500 returned to Grafana / the OpenNMS UI.
  The fix is two-sided:
  - **Write side** — `mtype` is now a default Prometheus label,
    sourced from each `Sample`'s `MetaTagNames.mtype` meta tag (set
    by the OpenNMS writer on every sample). Reserved against
    `labels.rename` collisions like the rest of the default
    allowlist; `labels.exclude = mtype` is honored but breaks graph
    rendering for new writes (the read-side fallback below recovers
    the data, with counter graphs degrading to gauges — supported
    only for non-OpenNMS consumers of the same Prometheus stack).
  - **Read side** — when a Prometheus response lacks the `mtype`
    label (typical of data already on disk from before this fix),
    the plugin synthesizes `mtype="gauge"` on the reconstructed
    Metric so OpenNMS's late-aggregation can complete. Counter
    metrics in legacy data render as cumulative values rather than
    rates — visibly less informative but never wrong; new writes
    preserve the original mtype, so post-upgrade counter rendering
    is correct.

### Added

- **`samples_synthesized_mtype_total`** plugin metric. Counts every
  read-time `mtype="gauge"` synthesis (one tick per `Metric`
  reconstruction — per matched series in `findMetrics`, once per
  fetch in `getTimeSeriesData`). The expected operator signal:
  the counter rises while Prometheus retention still holds pre-fix
  data, then drops to flat once the retention window has cycled
  past. A counter that rises indefinitely instead of plateauing
  almost always means `labels.exclude = mtype` is configured.
  Visible via `opennms:prometheus-writer-stats` alongside the
  existing `samples_*_total` family.
- **One-shot WARN per metric name** when synthesis fires, scoped
  to one bundle activation, gated by an insertion-ordered LRU
  bounded at 256 distinct names. Beyond the cap, the counter
  continues to increment but no further WARNs fire for evicted
  names — bounding log spam under pathological metric-name
  cardinality without losing the observability signal.

### Verifying this release

This release is GPG-signed by the project key (`0x1FC793D7F2E3FDDD`,
fingerprint `53BC D4E3 C0CC 9ACF 40F4  6669 1FC7 93D7 F2E3 FDDD`).
The KAR, the SHA-512 checksum, and the SBOM each ship with a
detached `.asc` signature. See
[`RELEASING.md` § Verifying a release](https://github.com/opennms-forge/prometheus-remote-writer/blob/main/RELEASING.md#verifying-a-release)
for the full verification recipe and the canonical-fingerprint
cross-check.

## [0.3.2] — 2026-04-27

A maintenance release whose headline is **the project's new home**: the
repository has migrated from `labmonkeys-space/prometheus-remote-writer`
to `opennms-forge/prometheus-remote-writer`, and this is the first release
shipped from the new owner. The same release closes the **supply-chain
v1** loop with a CycloneDX 1.6 SBOM and GPG-signed artifacts attached
to every GitHub Release.

**No runtime behaviour change.** The plugin code, wire format, configuration
surface, and KAR contents are unchanged from v0.3.1. Existing v0.3.1
deployments do not need to upgrade for any fix. The upgrade is appropriate
if you want signed artifacts for verification, the SBOM for vulnerability
scanning, or the canonical opennms-forge URLs in your bookmarks and
documentation links.

### Added

- **CycloneDX 1.6 SBOM attached to every GitHub Release.** Aggregate across
  the full Maven reactor (compile + runtime scopes; test scope excluded),
  shipped as `prometheus-remote-writer-<version>.cdx.json` alongside the
  KAR. Generate locally with `make sbom` (opt-in via the `sbom` Maven
  profile, so default `make build` and `make kar` are unchanged). Consumed
  by Trivy / Grype / Dependency-Track / FOSSA-style supply-chain tooling.
- **GPG-signed releases.** Every GitHub Release now ships detached GPG
  signatures (`.asc`) for the KAR, the SHA-512 checksum file, and the SBOM.
  The release tag itself is GPG-signed by the project key — the release
  workflow refuses to publish from an unsigned tag. The public key ships
  as a `KEYS` release asset; the canonical fingerprint is published in
  [`RELEASING.md`](RELEASING.md) "Release signing key" for out-of-band
  cross-check. See "Verifying a release" in the same document for the
  consumer-facing verification steps.
- **`AGENTS.md` at the repository root**, declaring
  `opennms-forge/prometheus-remote-writer` as the canonical repository and
  prohibiting AI agents from pushing to non-canonical copies.

### Changed

- **Repository home migrated** from `labmonkeys-space/prometheus-remote-writer`
  to `opennms-forge/prometheus-remote-writer`. All in-tree URL references
  (README, RELEASING, docs `_attributes.adoc`, CI badges, issue tracker,
  comparison links) updated to the new owner. The legacy GitHub redirect
  remains active for in-flight clones, but new bookmarks should target the
  canonical home.
- **Docs site republished** at <https://opennms-forge.github.io/prometheus-remote-writer/>.
  The previous `labmonkeys-space.github.io/prometheus-remote-writer/` URL no
  longer serves content (cross-org GitHub Pages does not redirect).
- **Author attribution recorded across the source tree.** All 62 `.java`
  files now use the SPDX-Identifier short-form copyright header with a
  `Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>` line.
  The parent `pom.xml` gained a `<developers>` block, an `<scm>` block,
  and an `<issueManagement>` block all pointing at the new owner.
- **Dependency modernization** via Dependabot cascade. All updates picked
  up green CI:
  - `org.junit:junit-bom` 5.11.3 → 6.0.3 (major)
  - `com.google.protobuf:protobuf-java` 3.25.5 → 4.34.1 (major)
  - `org.assertj:assertj-core` 3.26.3 → 3.27.7
  - `org.json:json` 20240303 → 20251224
  - `org.slf4j:slf4j-api` 2.0.13 → 2.0.17
  - `maven-surefire-plugin` and `maven-failsafe-plugin` 3.5.2 → 3.5.5
  - `actions/configure-pages` v5 → v6
  - `actions/deploy-pages` v4 → v5
  - `actions/upload-pages-artifact` v3 → v5

### Fixed

- **GitHub Pages self-heal.** The `publish-docs.yml` workflow now verifies
  Pages is in `build_type=workflow` mode before deploying. During the
  Dependabot cascade, an interaction between `actions/configure-pages@v6`
  and the existing Pages config flipped the build type to `legacy`,
  silently letting classic Jekyll auto-build the README and shadow the
  AsciiDoc docs. The new verify step fails the workflow loudly if the flip
  recurs, surfacing the regression rather than silently publishing the
  wrong content. (Auto-correction would require admin scope, which the
  workflow's `GITHUB_TOKEN` cannot have; the verify-and-fail-loudly
  pattern keeps the workflow self-defended without elevated permissions.)

## [0.3.1] — 2026-04-27

Patch release whose headline is the **Aries Blueprint setter-overload fix**:
without it, the plugin bundle in v0.3.0 fails to start on Karaf containers
with strict Blueprint validation (notably OpenNMS Sentinel 35.0.5). Existing
Horizon Core deployments work fine on v0.3.0, so this is a low-urgency upgrade
unless you intend to install the plugin on a strict-Blueprint container.

Also bundled: spike-driven docs sharpening that nails down the plugin's
*Horizon Core only* deployment scope, an inlined Makefile smoke harness, and
an opt-in Sentinel deployment proof-of-concept that lives outside the public
docs (developer-iteration surface only).

### Fixed

- **Aries Blueprint property type-match** — added `int` / enum setter
  overloads for `wireProtocolVersion`, `walFsync`, and `walOverflow` on
  `PrometheusRemoteWriterConfig` (#25, #26). Aries Blueprint's
  `PropertyDescriptor` validation rejects beans whose getter returns a
  non-`String` while the only setter takes `String`. Without the
  overloads, the plugin bundle fails to start on strict Blueprint
  containers like OpenNMS Sentinel 35.0.5 with errors of the form
  `At least one Setter method has to match the type of the Getter method
  for property <name>`. Same pattern was previously fixed for
  `metadataCase`; the v2 wire-format work missed it.
- **README Compatibility table** — markup was data rows only with no
  header / separator, so GitHub squashed the four lines into one
  unreadable block. Now renders as a proper table.

### Changed

- **Install heading narrowed to "Install on OpenNMS Horizon Core"**
  (was "Install on a running Horizon instance"). Matches the new
  *What this plugin does NOT* overview admonition. The follow-up
  "Restart OpenNMS for the strategy switch to take effect" line is
  also tightened to "Restart Core …".
- **Smoke harness lives in the Makefile.** `e2e/smoke.sh` removed;
  `make smoke` (and per-backend `make smoke-prometheus` /
  `smoke-mimir` / `smoke-victoriametrics`) carry the same shape.
  New tunables: `BACKENDS`, `SMOKE_TIMEOUT`, `SMOKE_POLL`. Per
  project convention, CI invokes Makefile targets directly.
- **Metatag heads-up moved earlier in the install flow.** A new TIP
  in *Activate the plugin as the active TSS* points at the
  *Minimal metatag config* subsection so operators set up node /
  foreign-source / location labels alongside the strategy switch
  rather than discovering bare-`resourceId` series after restart.
- **Compatibility entry** — *OpenNMS Horizon* renamed to
  *OpenNMS Horizon Core* in the README.

### Added

- **"What this plugin does NOT" section** in the docs overview —
  explicitly states the plugin runs on Horizon Core only and is not
  yet supported on Sentinel, with the architectural reason
  (no Core → Sentinel sample-dispatch path in OpenNMS upstream for
  OIA TSS plugins).
- **End-to-end sandbox docs section** — content from `e2e/README.md`
  migrated into the AsciiDoc docs site. `e2e/README.md` slimmed to
  a quick-reference card pointing at the docs.
- **`sentinel-deployment` spec capability** — captures the spike's
  empirically-verified plugin behaviour on Sentinel (installability,
  OIA TSS service binding, Sentinel-side gotchas to work around,
  reference Compose stack). Lives in `openspec/specs/`.
- **Sentinel deployment proof-of-concept stack** under `e2e/sentinel/`
  — runnable via `make sentinel-poc` / `make sentinel-poc-down` for
  ongoing iteration. **Developer-iteration surface only**: deliberately
  excluded from the public docs and from `make smoke`'s default
  `BACKENDS` list because no sample driver is wired in today and
  the upstream Core → Sentinel sample-dispatch gap blocks end-to-end
  sample flow for OIA TSS plugins generally.

### Removed

- **`e2e/smoke.sh`** — replaced by the Makefile-based smoke harness.
- **"Path B: feature install via Maven coordinates (deferred)"**
  install subsection — the plugin isn't published to a Maven repo
  today, so advertising the option as deferred only added noise.
  Single linear install flow now: drop the KAR, verify.

### Compatibility

- **OpenNMS Horizon Core** 35+ (Java 17, Karaf 4.4.x) — same as v0.3.0.
  No breaking config changes.
- **OpenNMS Sentinel** — installable but **not supported as a
  deployment surface** for sample flow yet. The plugin's bundle
  reaches `Active` and the OIA `TimeSeriesStorage` service registers,
  but no documented path exists today by which Core-collected samples
  reach a Sentinel-side persister via OIA TSS.

## [0.3.0] — 2026-04-26

### Added

- **`wire.protocol-version` configuration key** — operator-selectable
  Prometheus Remote Write protocol version. Accepts `1` (default,
  unchanged behavior) or `2` (Prometheus 3.0+ wire format with string
  interning). Default `1` means existing deployments observe no
  difference after upgrade. v2 reduces pre-snappy wire bytes by
  interning each label name and value once per request rather than
  per series; the savings depend on how heavily series share labels.
- **Remote Write v2 wire format** — when `wire.protocol-version=2`, the
  plugin emits `io.prometheus.write.v2.Request` payloads with
  `Content-Type: application/x-protobuf;proto=io.prometheus.write.v2.Request`
  and `X-Prometheus-Remote-Write-Version: 2.0.0`. The WAL is
  wire-version-agnostic — flipping the knob with pending samples in
  the WAL is safe; the next flush emits in the new version.
- **One-shot startup WARN** when `wire.protocol-version=2` is set,
  naming the required backend versions (Prometheus 3.0+ recommended,
  Mimir 2.10+, VictoriaMetrics with v2, Grafana Cloud, or equivalent).
  Older backends will return 4xx — or, on Prometheus 2.50–2.54,
  *silently drop* the payload while ack'ing 2xx — so the WARN gives
  operators a heads-up before the data starts hitting
  `samples_dropped_4xx_total` (or worse, vanishing without a counter).

### Backend compatibility

- **v1 (default)**: any Prometheus / Mimir / VictoriaMetrics that
  accepts Remote Write v1 — same as v0.2.0 and earlier.
- **v2**: Prometheus 3.0+ recommended (default-enabled, stable
  receiver). Prometheus 2.55+ works with
  `--web.enable-remote-write-receiver` set explicitly. **Prometheus
  2.50–2.54 ship an experimental v2 receiver that silently drops v2
  payloads** under documented edge cases — do NOT rely on it; pin to
  3.0+ instead. Mimir 2.10+, VictoriaMetrics with v2 ingest enabled,
  Grafana Cloud, or equivalent are also supported. The plugin does
  NOT auto-detect or fall back; an operator on an older backend who
  flips to v2 will see 4xx drops (or silent loss on 2.50–2.54) until
  they revert. Integration tests pin `prom/prometheus:v3.0.1`.

### Out of scope

- Native histograms — the v2 schema supports them but OpenNMS doesn't
  produce them; the v2 builder leaves the field empty.
- Exemplars — same; no trace-ID source in OpenNMS today.
- Per-series metadata — same; no `help`/`unit` source on the OpenNMS
  side.
- Created-timestamp counter-reset signal — not populated.

These are not breaking decisions; future changes can populate any of
the v2 fields without modifying the wire layer added here.

### Tests

- **WAL + Remote Write v2 integration tests** — new
  `PrometheusRemoteWriteWalV2IT` covers two scenarios that close the
  bottom-right corner of the IT matrix: WAL-buffered samples replayed
  under v2 after a restart, and samples written under
  `wire.protocol-version=1` then replayed under
  `wire.protocol-version=2` (pinning the wire-version-agnostic-WAL
  invariant end-to-end against `prom/prometheus:v3.0.1`).
- **v1 vs v2 dedup-parity unit test** — `RemoteWriteV1V2DedupParityTest`
  pins identical drop counts and surviving-sample shapes across the two
  builders for duplicate-timestamp dedup, non-finite filter, and
  per-series isolation. Catches future divergence loudly.

### Fixed

- **`Metadata` proto upstream parity** — added `reserved 2;` to the v2
  `Metadata` message to match the upstream `io.prometheus.write.v2.Request`
  schema. Operationally low-impact (the field number was already
  unused) but prevents a future schema bump from silently re-using it
  for a different concept.

## [0.2.0] — 2026-04-25

### Added

- **Write-Ahead Log (WAL)** — optional, opt-in via `wal.enabled=false`
  (default). When enabled, the plugin durably persists every mapped
  sample to disk before ack'ing `store()`; the WAL replaces the
  in-memory `ArrayBlockingQueue` as source of truth, so samples survive
  process restart AND extended endpoint outages. Six new config keys:
  - `wal.enabled` (bool, default `false`)
  - `wal.path` (default `${karaf.data}/prometheus-remote-writer/wal`)
  - `wal.max-size-bytes` (default 512 MB — total footprint cap)
  - `wal.segment-size-bytes` (default 64 MB — per-segment rotation)
  - `wal.fsync` (`always | batch | never`, default `batch` — fsync at
    flush-interval boundary)
  - `wal.overflow` (`backpressure | drop-oldest`, default `backpressure`
    — matches v0.4 queue-full semantics)

  On-disk format: length-prefixed protobuf `WalEntry` frames with
  CRC32C; rotating segment files (`00000000000000000000.seg`, ...);
  companion `.idx` jsonl per segment; `checkpoint.json` tracks the
  last offset confirmed shipped. Crash recovery scans the newest
  segment, truncates any torn tail, and replays from the checkpoint.
  See README "Write-Ahead Log" for operator guidance.

- **Eight new metrics** (wal.enabled=true only), surfaced via
  `opennms:prometheus-writer-stats`:
  - Counters: `wal_bytes_written_total`,
    `wal_bytes_checkpointed_total` (bytes that moved past the
    durable checkpoint), `wal_replay_samples_total` (one-shot
    at startup), `wal_batches_dropped_4xx_total`,
    `samples_dropped_wal_full_total`,
    `wal_frames_dropped_corrupted_total` (sealed-segment
    skips due to bit rot / torn frames)
  - Gauges: `wal_disk_usage_bytes`, `wal_segments_active`

- **`samples_unparseable_resource_id_total` counter** — increments once per
  sample whose `resourceId` tag failed all parser grammars (bracketed,
  slash-FS, slash-DB) or was absent. Surfaced via
  `opennms:prometheus-writer-stats` alongside the existing `samples_*_total`
  counters. Lets operators see the v0.4 "catch-all `job=opennms`" bucket
  size without grepping logs — a non-zero rate signals config drift, a new
  resourceId shape, or unexpected upstream input.

### Changed

- **`shutdown.grace-period-ms` semantic shift when `wal.enabled=true`** —
  the knob now bounds the wait for the flusher loop to exit cleanly,
  rather than a data-drain window. (Note: `Thread.interrupt()` does
  not cancel an in-flight OkHttp call, so a worker blocked on a dead
  TCP connection may continue past the grace window until
  `http.read-timeout-ms`; the HTTP client is shut down right after
  the grace, which is what actually breaks the call.) WAL durability
  means no sample is lost at shutdown regardless of grace value;
  anything unshipped replays on next start. Operators who set a large
  grace value (e.g., 60_000) for drain-safety under v0.4 can safely
  reduce it. The `wal.enabled=false` path (default) retains the v0.4
  drain-or-lose semantics exactly.
- **`queue.capacity` ignored when `wal.enabled=true`** — WARN logged
  at startup if the operator explicitly sets it. The WAL replaces the
  in-memory queue; size via `wal.max-size-bytes` instead.

### Tests

- **Blueprint-wiring regression test** — reflectively cross-checks every
  `<property name="X">` in `OSGI-INF/blueprint/blueprint.xml` against the
  corresponding `set*` method on `PrometheusRemoteWriterConfig`, and
  both-directions against every `<cm:property>` default. Catches the
  v0.3-style silent-no-op where `labels.copy` had a Java setter but no
  Blueprint binding — the kind of bug no existing test would see.
- **IT storage-rebuild isolation** — five `PrometheusRemoteWriteIT` tests
  that recreate `PrometheusRemoteWriterStorage` with a custom config now
  use a local `override` variable with try/finally stop. Earlier pattern
  (field reassignment between stop/start) would leak a started storage on
  an assertion failure and leak a stopped reference on a constructor or
  `start()` throw.

## Internal milestone: default `job` + `instance` labels (folded into 0.2.0)

### Added

- **`job` default label** — derived from the sample's `resourceId` shape:
  bracketed or slash-DB SNMP patterns → `"snmp"`, slash-FS groups named
  `jmx-*` or `opennms-jvm` → `"jmx"`, unparseable or absent resourceIds →
  `"opennms"` (catch-all). Emitted unconditionally so `{job=~".+"}` covers
  every sample.
- **`instance` default label** — mirror of `node` with the same derivation
  precedence (FS-qualified external tags > parsed slash-FS > parsed slash-DB
  > external `nodeId`). Emitted iff `node` is emitted. `node` is kept
  alongside — dashboards filtering on `{node="X"}` continue working
  unchanged.
- **`job.name` configuration key** — when set to a non-empty value,
  overrides the per-sample `job` derivation with a fleet-wide constant.
  Default: unset (use derivation).

  Together, these enable the Prom-idiomatic `{job="X", instance="Y"}`
  scoping idiom for cross-source dashboards that combine OpenNMS data
  with node-exporter / OTel / other Prometheus data sources in a shared
  backend. See README "Cross-source filtering" for the semantics and the
  deliberate `instance = subject` stance.

### Changed (BREAKING)

- **`instance` and `job` added to the reserved-target list.**
  `labels.rename = X -> instance` / `X -> job` and `labels.copy = Y -> instance`
  / `Y -> job` are now rejected at startup with the existing "collides with
  the default label" error. Operators who previously wrote
  `labels.copy = node -> instance` as a workaround for the missing default
  can remove the directive (the default emission covers it) — the config
  parser would reject it anyway.

### Changed

- **Internal**: `PrometheusRemoteWriterConfig.labelsRenameMap()` and
  `labelsCopyMap()` now cache their parsed result, returning the same `Map`
  instance across repeated calls within one config-string lifecycle. Setters
  invalidate. `validate()` no longer re-parses the same string three times
  per invocation. The returned maps continue to be unmodifiable (as before);
  the behavior change is strictly tighter — same content, stable identity.

### Fixed

- `labels.copy` is now actually wired up from ConfigAdmin — v0.3 introduced
  the Java setter and the parser + validator, but the Blueprint property
  binding and the default placeholder entry were both missing. Operators
  who tried `labels.copy = ...` in their `.cfg` on v0.3 saw a silent no-op.
  v0.4 adds the Blueprint `<cm:property>` entry and the `<property name="labelsCopy">`
  binding alongside `instance.id` and the new `job.name` knob. Also picks up
  an earlier docs oversight: the README Configuration block was missing
  `instance.id` entirely (has been since v0.1); now includes both `instance.id`
  and `job.name` under a "Source identity" subsection.

### Tests

- Pinned the `instance.id` WARN-suppression emission count — a refactor that
  kept the one-shot gate correct but moved `LOG.warn` outside the CAS-success
  branch would have passed the existing boolean-gate assertions and silently
  re-fired on every bundle activation. New counter +
  `getInstanceIdWarnCountForTesting()` accessor + three scenarios (first
  start, silent re-start, hot-reload with instance.id flipped on).
- Pinned the harder one-pass invariant of `labels.copy`: the stage reads
  source values from its input map, not from the accumulating output, so a
  chained directive whose source was just clobbered by an earlier directive
  still sees the ORIGINAL value.

### Upgrade notes

- **Dashboards using `{node="X"}` are unaffected.** `node` is still a default
  label with the same value semantics; `instance` is an additional emission.
- **Dashboards built around `{job=..., instance=...}` work out of the box.**
  No need for per-deployment `labels.copy = node -> instance` — it's a
  default emit now.
- **Operators with `labels.copy = node -> instance` in `.cfg`** must remove
  it: v0.4 rejects the directive at startup because `instance` is reserved.
  Once removed, the default emission carries the same value.
- **Wire cost**: +30-50 bytes per sample for the two new labels (`job` is
  short low-cardinality; `instance` shares cardinality and values with
  `node`). For deployments pushing 1000 samples/sec, typical <1% wire
  traffic increase. Opt out with `labels.exclude = instance, job` if
  neither label is useful for your backend.
- **Mixed backends**: operators combining OpenNMS data with node-exporter
  will see `instance` carry different value shapes across sources
  (node-exporter: `host:port`; OpenNMS: `<foreignSource>:<foreignId>` or
  numeric dbId). This is by design — `instance` means "the measured
  device" from each source's perspective; `job` is the primary cross-source
  scoping filter. Cross-source value-correlation bridges (for queries that
  need the SAME value to identify "the same box" across sources) remain
  the operator's responsibility via backend `relabel_config`.
- **Operators who configured `labels.copy` via Karaf `config:edit` on v0.3**
  (rather than via the `.cfg` file, where the binding was missing on v0.3):
  verify your `labels.copy` value is still set after the v0.4 bundle
  activates. v0.4 adds the previously-missing `<cm:property name="labels.copy">`
  default to the Blueprint descriptor; Aries Blueprint's merge behavior
  between an empty default and an existing ConfigAdmin dictionary may reset
  the value in edge cases. Re-apply via `config:edit` or the `.cfg` file if
  so.

## Internal milestone: `labels.copy` primitive (folded into 0.2.0)

### Added

- **`labels.copy` primitive** — a new `labels.*` configuration key that emits
  the value of an existing label under an additional name, leaving both
  present on the wire. Solves dashboard-portability scenarios
  (`labels.copy = node -> instance` to bridge OpenNMS-native and
  Prometheus-idiomatic vocabulary), migration-period dual emission
  (emit old + new name for a release cycle, then drop), and integration with
  systems that hardcode label names (multi-tenant Mimir's `tenant` label,
  vendor alert bundles, Grafana folder conventions).

  Config syntax mirrors `labels.rename` — a comma-separated list of
  `from -> to` pairs — with one difference: the same `from` key may appear
  with multiple targets (`labels.copy = node -> instance, node -> host`)
  to emit all named copies.

  Pipeline position: between `labels.include` and `labels.rename`
  (`defaults → exclude → include → copy → rename → metadata`). Copy is
  one-pass (snapshots source values at stage entry; `copy = a -> b, b -> c`
  does NOT produce `c`) and operates on pre-rename names.

  Validation: copy targets must not collide with default-allowlist labels,
  reserved prefixes (`onms_cat_*`, `onms_meta_*`), another copy target, or a
  `labels.rename` target. Unknown copy sources are logged once at startup
  and treated as per-sample no-ops thereafter.

- **`rename`/`copy` target-validation sharing** — the reserved-name and
  reserved-prefix checks are now centralised in a single helper. A new
  collision rule added to one primitive applies uniformly to the other.

### Known limitations

- **`labels.copy` is the last mapping primitive.** Further transformations —
  regex extraction, case conversion, conditional emission, value rewriting —
  belong on the backend via Prometheus / Mimir `relabel_config`, not in this
  plugin. This is a deliberate scope commitment: the plugin is a simple
  remote-write sender, and the ecosystem has better tools for the heavier
  transformations downstream.
- **Glob support on the copy source** (e.g., `onms_cat_* -> cat_*`) is not
  supported in v0.3. Worth reconsidering in a future release if real demand
  materialises; adds validation complexity that v0.3 does not need.

### Upgrade notes

- **Purely additive.** Deployments that do not set `labels.copy` see no
  behavior change. The internal refactor that shares validation code between
  `labels.rename` and `labels.copy` is operator-invisible.
- **Wire cost.** Each copy directive adds ~20-50 bytes per sample. Typical
  deployments configuring one or two copies see <1% write-traffic increase.
- **Cardinality shift.** Turning a copy on once changes series identity
  (Prometheus identifies series by the full label-value set), replacing the
  pre-copy series with post-copy series of the same count. No ongoing
  cardinality growth.

## Internal milestone: label enrichment v0.2 (folded into 0.2.0)

### Breaking changes

#### `labels.rename` targets validated at startup

Entries whose `to` value collides with a default-allowlist label name or a
reserved prefix cause the plugin to refuse to start with an actionable error
message. Duplicate rename targets across entries (`a -> cluster, b -> cluster`)
and duplicate `from` keys (`a -> cluster, a -> tenant` — the second silently
overwrote the first before) are likewise rejected. Multiple rename errors are
accumulated into one startup error so operators fix-once, restart-once.
Previously these configs were accepted and silently clobbered the colliding
default at flush time — a data-quality incident with no visibility surface.

Reserved exact targets: `__name__`, `resourceId`, `node`, `foreign_source`,
`foreign_id`, `node_label`, `location`, `resource_type`, `resource_instance`,
`if_name`, `if_descr`, `if_speed`, `onms_instance_id`.
Reserved prefixes: `onms_cat_*`, `onms_meta_*`.

The `onms_instance_id` name is reserved unconditionally, even when
`instance.id` is unset, so a subsequent hot-reload enabling the knob cannot
unmask an already-broken rename.

Error message shape:

```
labels.rename target 'foreign_source' collides with the default label
'foreign_source'. The plugin already emits this label; renaming onto it
would silently clobber the default value. Pick a different 'to' name.
```

Migration: pick a non-reserved `to` name. Pre-upgrade, scan your cfg
(swap the etc path for your install's actual location):

```bash
grep -E '^[[:space:]]*labels\.rename' /opt/opennms/etc/org.opennms.plugins.tss.prometheusremotewriter.cfg
```

#### `labels.include = *` no longer re-emits consumed default source keys

Operators running `labels.include = *` on v0.1 received five duplicate labels
whose snake-cased source-key forms did not collide with a default label name,
and therefore slipped past the v0.1 `putIfAbsent`-based dedup. These
duplicates are no longer emitted in v0.2.

Migration table:

| Deprecated label | Use instead                | Why                                                                                                                   |
|---               |---                         |---                                                                                                                    |
| `name`           | `__name__`                 | Prometheus-native metric name.                                                                                        |
| `resource_id`    | `resourceId`               | Raw OpenNMS resource identifier.                                                                                      |
| `if_high_speed`  | `if_speed`                 | Normalized bits-per-second: `ifHighSpeed × 10⁶` when non-zero, else `ifSpeed`.                                        |
| `node_id`        | `node`                     | FS-qualified identity when available, else parsed from the `resourceId`, else the numeric dbId. To keep the `node_id` label name for existing dashboards, use `labels.rename = node -> node_id` — but note the *value* is now FS-qualified (`<fs>:<fid>`) when available, not the raw numeric dbId v0.1 emitted under `node_id`. Dashboards that assumed a purely numeric value must be updated. |
| `categories`     | `onms_cat_<name>` per cat  | Per-category expansion, not a single comma-separated label.                                                           |

The six other snake-cased source-key forms (`foreign_source`, `foreign_id`,
`node_label`, `location`, `if_name`, `if_descr`) collided on label name with a
default and were already single-valued in v0.1 via `putIfAbsent` — they are
unchanged in v0.2.

This also supersedes an undocumented v0.1 quirk where
`labels.rename = foreign_source -> foreign_source_raw` combined with
`labels.include = *` produced both `foreign_source_raw` (renamed default) and
`foreign_source` (re-surfaced from the source tag). In v0.2 only
`foreign_source_raw` is emitted — the source key is consumed and the include
pass skips it. Operators who want the original value preserved under a
different label name use `labels.rename` alone.

Deployments running narrow `labels.include` patterns (e.g. `sys*, asset*`)
are unaffected. Pre-upgrade, scan your dashboards and alert rules for the
five deprecated label names.

### Added

- **Slash-path `resourceId` grammars** — `ResourceIdParser` now recognises
  `snmp/fs/<foreignSource>/<foreignId>/<group>/<instance…>` and
  `snmp/<dbNodeId>/<group>/<instance…>` in addition to the existing bracketed
  form. Self-monitor, JMX, and legacy-path samples now acquire `node`,
  `resource_type`, and `resource_instance` labels automatically. The instance
  segment is greedy, so MBean object names with embedded separators stay
  intact. Bracketed-form matching is unchanged — no previously parseable
  resourceId changes shape.
- **`instance.id` config and `onms_instance_id` label** — stamps every
  outbound sample with an operator-supplied identifier so multiple OpenNMS
  instances writing into the same Prometheus-compatible backend can be
  distinguished and aggregated in PromQL. Works against every
  Prometheus-compatible backend (Prometheus, Mimir, Cortex, VictoriaMetrics,
  Thanos). Orthogonal to `tenant.org-id` (backend tenant isolation); use
  either, both, or neither depending on deployment shape. See the README
  section "Identifying samples from multiple OpenNMS instances" for the
  decision table.
- **Startup WARN when `instance.id` is unset** — one-shot, informational.
  Fires once per bundle lifecycle; not repeated by hot-reload cycles.
  Single-instance deployments can ignore or silence by setting the knob.
- **README: "Label enrichment is two-sided"** — documents the OpenNMS-side
  `org.opennms.timeseries.tin.metatags.tag.*` prerequisite with a minimal
  four-property example, a pointer at the sandbox comprehensive example, the
  `exposeCategories` opt-in flag, and the node-record-must-exist caveat.

### Known limitations

These are deliberate omissions for v0.2, surfaced so operators can plan around
them:

- **Remote Write v2** — stable since Prometheus 2.50 but not yet universally
  deployed across Mimir / VictoriaMetrics / Cortex / Thanos Receive. Adopting
  v2 before universal backend support would fork the compatibility matrix.
  Workaround: v1 (what this plugin ships) is universally supported. Tracked
  for a future release.
- **Durable on-disk write buffer (WAL)** — v0.2 still uses a bounded
  in-memory queue and drops samples on overflow, counted via
  `samples_dropped_queue_full_total`. Workaround: alert on that counter and
  size `queue.capacity` for your peak ingest rate. Tracked for a future
  release.
- **Per-tenant routing / multi-destination fan-out** — one `write.url` and
  one `tenant.org-id` per plugin instance. Workaround: run one OpenNMS
  instance per destination. Tracked for a future release.

### Upgrade notes

- **`instance.id` path is purely additive.** Deployments that do not set
  `instance.id` emit identical labels to v0.1 and gain exactly one new `WARN`
  line on startup. Deployments using `tenant.org-id` alone continue unchanged;
  setting `instance.id` is an additive migration that unlocks cross-instance
  PromQL without disturbing tenant isolation.
- **Slash-path enrichment is additive.** Self-monitor, JMX, and legacy-path
  samples that previously emitted only `{resourceId="snmp/…"}` now also emit
  `node`, `resource_type`, and `resource_instance`. No previously parseable
  resourceId changes shape.
- **`labels.include = *` deployments**: audit dashboards and alert rules for
  the five deprecated label names above; migrate before upgrade.

## [0.1.0] — 2026-04-22

First release. Implements the OpenNMS `TimeSeriesStorage` SPI against any
Prometheus-compatible Remote Write v1 endpoint.

### Highlights

- **Backend-agnostic Remote Write v1** — ships against Prometheus, Grafana
  Mimir, VictoriaMetrics, Cortex, and Thanos Receive. Snappy + protobuf on
  write, the Prometheus HTTP query API on read.
- **OpenNMS-native label model** — resource context surfaced as first-class
  Prometheus labels (`node`, `node_label`, `location`, `resource_type`,
  `if_name`, `if_speed`, `onms_cat_*`, …) with operator-configurable
  include/exclude/rename policy.
- **Production-grade operation** — Basic/Bearer/`X-Scope-OrgID` auth, custom
  TLS CA bundles, bounded in-memory queue with batched flush and 5xx
  exponential backoff, backpressure-aware enqueue, Dropwizard self-metrics,
  and graceful drain on shutdown.

### Compatibility

- OpenNMS Horizon 35
- Java 17
- OpenNMS Integration API v2.0 (Apache 2.0)
- Prometheus Remote Write v1 (`prom/prometheus`, Grafana Mimir,
  VictoriaMetrics, Cortex, Thanos Receive)

### Known deviations

- **Partition-lossy read path.** Prometheus does not model the OpenNMS
  intrinsic/meta/external tag partition. On read, every non-intrinsic label
  is attached as a meta tag; the original partition is not preserved.
- **`shouldDeleteMetrics` and partition-equality tests** from the OpenNMS
  TSS compliance suite are skipped — see `PrometheusComplianceIT` for
  per-test rationale.

### License

Apache License 2.0. Clean-room implementation against public specifications;
no code lifted from the AGPL `opennms-cortex-tss-plugin`. Reference sources
are the Prometheus Remote Write spec, the Prometheus HTTP query API docs,
the upstream Prometheus protobuf definitions, and the Apache-2.0 Prometheus
Go sanitization rules.

### Full changes

- Time Series Storage plugin that writes samples via Prometheus Remote Write v1
  (snappy + protobuf) and reads via the Prometheus HTTP query API.
- Opinionated default label allowlist surfacing OpenNMS resource context as
  native Prometheus labels: `__name__`, `resourceId`, `node`, `node_label`,
  `foreign_source`, `foreign_id`, `location`, `resource_type`,
  `resource_instance`, `if_name`, `if_descr`, `if_speed`, and one
  `onms_cat_<name>` label per surveillance category.
- `resourceId` parsing: kept raw and parsed into structured
  `node` / `resource_type` / `resource_instance` labels; parse failure falls
  back to raw-only emission.
- `if_speed` normalization: `ifHighSpeed × 1_000_000` preferred, falling back
  to `ifSpeed` when zero or absent. Produces a single bits-per-second label.
- Operator-configurable label policy: `labels.include` (add non-default
  source tags), `labels.exclude` (remove defaults), `labels.rename`
  (`from -> to`).
- OpenNMS metadata passthrough — **off by default**. Opt in with
  `metadata.enabled = true` and `metadata.include` globs. Built-in credential
  denylist (`*password*`, `*secret*`, `*token*`, `*key*`, `snmp-community`)
  is always applied even when the operator's globs would match. Labels
  namespaced under `onms_meta_<context>_<key>`.
- Authentication: Basic, Bearer (mutually exclusive), and `X-Scope-OrgID`
  multi-tenant header.
- TLS: JDK truststore by default; custom PEM CA bundle via `tls.ca-file`;
  insecure skip-verify available behind an explicit opt-in with hourly
  WARN log.
- Write pipeline: bounded in-memory queue, batch flush on size or interval,
  5xx exponential-backoff retry, 4xx drop with body captured to WARN,
  transport errors treated as 5xx for accounting.
- Backpressure: queue overflow throws `StorageException` so OpenNMS sees the
  failure and increments `samples_dropped_queue_full_total`.
- Read path: `findMetrics` → `GET /api/v1/series`, `getTimeSeriesData` →
  `GET /api/v1/query_range`. Step derivation from request range when
  `getStep()` is unset; clamped to Prometheus's 11 000 points-per-query
  ceiling.
- Delete path: `delete(Metric)` is a no-op (Remote Write has no delete
  semantic) that counts via `delete_noop_total` and logs a rate-limited
  WARN once per minute.
- Self-metrics via a Dropwizard `MetricRegistry`; `opennms:prometheus-writer-stats`
  Karaf shell command prints a stable name/value table.
- Graceful shutdown: stops accepting new enqueues, drains the queue within
  `shutdown.grace-period-ms`, then terminates in-flight HTTP calls. Residual
  queue depth is logged at WARN.
- Karaf feature `prometheus-remote-writer` shipping a pre-populated
  `etc/org.opennms.plugins.tss.prometheusremotewriter.cfg` on install.

[Unreleased]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/opennms-forge/prometheus-remote-writer/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/opennms-forge/prometheus-remote-writer/releases/tag/v0.1.0

[#48]: https://github.com/opennms-forge/prometheus-remote-writer/pull/48
[#49]: https://github.com/opennms-forge/prometheus-remote-writer/pull/49
