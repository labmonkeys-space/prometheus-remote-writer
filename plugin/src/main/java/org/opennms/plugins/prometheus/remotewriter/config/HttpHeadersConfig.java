/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the operator-supplied set of arbitrary HTTP headers attached to
 * outbound write and read requests. Blueprint injects the full property set
 * at the plugin's ConfigAdmin PID through {@code <cm:cm-properties>}; the
 * bean filters keys by the {@code http.headers.} prefix, validates names and
 * values, and exposes the accepted set via {@link #headers()}.
 *
 * <p>The injection mechanism is load-bearing. Neither
 * {@code <cm:managed-properties update-strategy="component-managed">} nor a
 * {@code ManagedService} registration works here: the first fires only on a
 * configuration change, and the second is delivered after Blueprint has
 * already run the storage bean's {@code init-method}, which builds the HTTP
 * clients. Both left the plugin sending requests with no operator headers.
 * {@code <cm:cm-properties>} is resolved as a dependency before bean
 * instantiation, which is the ordering this bean needs.
 *
 * <p>This bean is intentionally a separate Blueprint bean from
 * {@link PrometheusRemoteWriterConfig}: that class uses the
 * {@code <cm:property-placeholder>} scalar-injection pattern and is policed
 * by {@code BlueprintWiringTest} for 1:1 setter-binding parity. Prefix-
 * scanned properties don't fit that contract — they have variable
 * cardinality — so they live here.
 *
 * <p>Wire-format invariants ({@code Content-Type}, {@code Content-Encoding},
 * {@code X-Prometheus-Remote-Write-Version}, etc.) and headers already
 * managed by other configuration keys ({@code Authorization},
 * {@code X-Scope-OrgID}) are hard-rejected. {@code User-Agent} is allowed
 * and overrides the plugin default.
 *
 * <p>Header values are masked in any human-readable representation
 * (toString, startup log) using the same {@code (value redacted)} pattern
 * the existing config bean uses for {@code auth.basic.password} /
 * {@code auth.bearer.token}.
 */
public final class HttpHeadersConfig {

    private static final Logger LOG = LoggerFactory.getLogger(HttpHeadersConfig.class);

    /** Property-key prefix that marks an operator-supplied HTTP header. */
    static final String PREFIX = "http.headers.";

    /**
     * Reserved header names — comparison is case-insensitive, so the set is
     * stored as lowercase. Split into two conceptual groups (wire-protocol
     * invariants and "already managed by another config key") so the
     * rejection message can carry a targeted hint.
     */
    private static final Set<String> RESERVED_PROTOCOL = Set.of(
            "content-type",
            "content-encoding",
            "content-length",
            "x-prometheus-remote-write-version",
            "transfer-encoding",
            "host",
            "connection",
            "upgrade",
            // Transport headers OkHttp manages implicitly. Accept-Encoding is
            // the load-bearing one: OkHttp only performs transparent gzip
            // decompression when it added the header itself, so an operator
            // value silently disables it and PrometheusReadClient then parses
            // gzip bytes as JSON (and the response cap counts compressed
            // bytes). TE and Expect are hop-by-hop transport negotiation;
            // Www-Authenticate is a response header with no meaning on a
            // request. Prometheus reserves all of these too.
            //
            // Accept is deliberately NOT reserved: PrometheusReadClient sets
            // none of its own, so an operator value is the only one on the
            // request, and a backend-specific Accept is a legitimate need.
            "accept-encoding",
            "te",
            "expect",
            "www-authenticate"
    );
    private static final Set<String> RESERVED_MANAGED = Set.of(
            "authorization",
            "proxy-authorization",
            "x-scope-orgid"
    );

    /**
     * RFC 7230 token grammar: {@code 1*tchar} where
     * {@code tchar = "!" | "#" | "$" | "%" | "&" | "'" | "*" | "+" | "-" |
     * "." | "^" | "_" | "`" | "|" | "~" | DIGIT | ALPHA}. No whitespace,
     * no control characters, no separator characters
     * ({@code : ( ) , / < = > ? @ [ \ ] { }}).
     */
    private static final Pattern HEADER_NAME = Pattern.compile(
            "^[!#$%&'*+\\-.^_`|~0-9a-zA-Z]+$");

    /**
     * Snapshot of validated headers, preserving operator casing for the on-
     * the-wire header name. Volatile because Aries can call
     * {@link #applyProperties(Map)} from a ConfigAdmin thread concurrently with
     * clients reading via {@link #headers()}.
     */
    private volatile Map<String, String> headers = Collections.emptyMap();

    /**
     * Message from the most recent failed validation, or {@code null} when the
     * current configuration is valid. Read by
     * {@code PrometheusRemoteWriterStorage.start()} so an invalid header set
     * makes the plugin inert instead of silently unauthenticated. Volatile for
     * the same reason as {@link #headers}.
     */
    private volatile String validationError;

    /**
     * Properties injected by Blueprint's {@code <cm:cm-properties>}, or
     * {@code null} for the no-arg (test / empty) construction path. Held as
     * the raw injected reference: with {@code update="true"} Aries mutates
     * this object in place on a configuration change.
     */
    private final Map<?, ?> injectedProperties;

    /**
     * True once the configuration has actually been handed to this bean.
     *
     * <p>Exists because "nothing was delivered" and "the operator configured
     * no headers" are otherwise indistinguishable — both leave
     * {@link #headers} empty and {@link #validationError} null. That is the
     * exact state this plugin shipped in twice under two different Blueprint
     * wirings, sending every request without the operator's headers while the
     * unit suite stayed green. {@code PrometheusRemoteWriterStorage.start()}
     * refuses to activate when this is false, so a delivery regression is
     * loud instead of silent.
     */
    private volatile boolean delivered;

    /** No-arg construction — no operator properties; used by tests and
     *  {@link #empty()}. Born delivered: there is nothing to wait for. */
    public HttpHeadersConfig() {
        this.injectedProperties = null;
        this.delivered = true;
    }

    /**
     * Blueprint construction. Takes the full property set at the plugin PID
     * from {@code <cm:cm-properties>}; {@link #init()} parses it.
     *
     * @param injectedProperties the PID dictionary (a {@link java.util.Properties}
     *                           in practice, hence the wildcard key type)
     */
    public HttpHeadersConfig(Map<?, ?> injectedProperties) {
        this.injectedProperties = injectedProperties;
    }

    /**
     * Blueprint {@code init-method}. Runs during bean creation, which
     * Blueprint performs before the storage bean's own {@code init-method}
     * builds the HTTP clients — so the header set is populated by the time
     * anything reads it. A validation failure is recorded in
     * {@link #validationError()} rather than thrown: throwing here would
     * fail the Blueprint container permanently, and the
     * {@code update-strategy="reload"} placeholder could not revive it from
     * a corrected .cfg.
     */
    public void init() {
        if (injectedProperties == null) {
            delivered = true;
            return;
        }
        // Snapshot under the injected object's own lock before iterating:
        // update="true" means Aries mutates this instance in place, and a
        // concurrent update mid-iteration would throw
        // ConcurrentModificationException out of init() — the permanent
        // container failure this method exists to avoid.
        Map<String, Object> copy = new LinkedHashMap<>();
        synchronized (injectedProperties) {
            for (Map.Entry<?, ?> e : injectedProperties.entrySet()) {
                if (e.getKey() != null) copy.put(e.getKey().toString(), e.getValue());
            }
        }
        try {
            applyProperties(copy);
        } catch (RuntimeException bad) {
            // RuntimeException, not just IllegalStateException: anything that
            // escapes here aborts bean creation and fails the container
            // permanently, which no .cfg correction can revive.
            // applyProperties already recorded and logged the validation case;
            // record anything else so start() still refuses to serve.
            if (validationError == null) {
                validationError = String.valueOf(bad.getMessage());
                LOG.error("http.headers.* could not be applied — the plugin will "
                        + "not serve until it is corrected", bad);
            } else {
                LOG.debug("http.headers.* rejected at activation", bad);
            }
        } finally {
            // Delivery happened either way. The distinction start() cares
            // about is "did anything reach this bean", not "was it valid".
            delivered = true;
        }
    }

    /**
     * Whether the configuration was ever handed to this bean. False means
     * Blueprint never ran {@link #init()} — a wiring regression, not an
     * operator mistake. See {@link #delivered}.
     */
    public boolean isDelivered() {
        return delivered;
    }

    /**
     * Applies a property dictionary. Called from {@link #init()} in
     * production and directly from tests. Receives the full property set at
     * the plugin's PID; we filter, parse, and validate the
     * {@code http.headers.*} subset.
     *
     * <p>Two failure shapes:
     * <ul>
     *   <li>{@code properties == null} — "no configuration set". The
     *       {@link #headers} snapshot is RESET to empty and the non-empty to
     *       empty transition is logged, because it silently moves a
     *       gateway-authenticated deployment to unauthenticated. NOTE: this
     *       branch is currently reachable only from tests — {@link #init()}
     *       returns early on a null injection and otherwise always passes a
     *       non-null copy. It is kept because any future re-delivery path
     *       must not drop headers silently.</li>
     *   <li>Validation throws {@link IllegalStateException} — the message is
     *       first recorded in {@link #validationError()} and logged at ERROR
     *       (name-only diagnostic; values are never echoed), then rethrown to
     *       {@link #init()}, which records it and returns.
     *       {@code PrometheusRemoteWriterStorage.start()}
     *       consults {@link #validationError()} and refuses to activate, so a
     *       bad header set makes the plugin inert rather than silently
     *       unauthenticated.</li>
     * </ul>
     *
     * <p>The {@link #headers} field is left untouched on a validation failure,
     * but do not read that as production-visible retention: the sibling
     * {@code <cm:property-placeholder update-strategy="reload">} on the same
     * PID tears down and re-creates the whole container on every {@code .cfg}
     * change, so a rebuilt instance always starts from the empty default. The
     * retention is observable only within one bean lifetime.
     *
     * <p>The method is {@code synchronized} as a defense in depth — the
     * OSGi ConfigAdmin specification serialises {@code updated()} calls per
     * service, but explicit serialisation removes any dependence on
     * implementation-specific dispatch ordering.
     *
     * @param properties full ConfigAdmin dictionary at the plugin PID, or
     *                   {@code null} if no configuration has been set yet
     */
    public synchronized void applyProperties(Map<String, ?> properties) {
        if (properties == null) {
            if (!this.headers.isEmpty()) {
                LOG.info("http.headers.* configuration removed — the {} custom "
                       + "header(s) previously attached to outbound write and "
                       + "read requests will no longer be sent",
                        this.headers.size());
            }
            this.headers = Collections.emptyMap();
            this.validationError = null;
            return;
        }
        Map<String, String> parsed;
        try {
            parsed = extractAndValidate(properties);
        } catch (IllegalStateException bad) {
            // Record before rethrowing. init() catches the throw so bean
            // creation still succeeds; PrometheusRemoteWriterStorage reads
            // validationError() and declines to activate instead, which
            // mirrors how start() already handles an invalid core config —
            // inert but revivable by a corrected .cfg, rather than a
            // permanently failed container.
            this.validationError = bad.getMessage();
            LOG.error("Invalid http.headers.* configuration — the plugin will "
                    + "not serve until it is corrected: {}", bad.getMessage());
            throw bad;
        }
        this.validationError = null;
        Map<String, String> immutable = Collections.unmodifiableMap(parsed);
        // Suppress the activation log on no-op reloads — any .cfg change
        // re-fires updated() on this bean (we share the PID with the scalar
        // placeholder), so without this guard operators see the same
        // "Custom HTTP headers attached: ..." line on every reload even
        // when none of the http.headers.* keys changed.
        boolean changed = !this.headers.equals(immutable);
        this.headers = immutable;
        if (changed) {
            logActivation(parsed);
        }
    }

    /**
     * Returns the validated header set, preserving operator casing on names.
     * Map is immutable; safe to share across threads.
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Message from the most recent failed {@code http.headers.*} validation,
     * or {@code null} when the current configuration is valid.
     *
     * <p>Exists because {@link #init()} deliberately swallows the validation
     * throw — letting it escape would fail the Blueprint container
     * permanently, and no {@code .cfg} correction could revive it. The storage
     * bean reads this at {@code start()} and declines to activate when it is
     * non-null, so a rejected header set never degrades into "started, but
     * sending unauthenticated requests".
     */
    public String validationError() {
        return validationError;
    }

    /**
     * Human-readable diff between a prior headers snapshot and the current
     * one, suitable for logging on hot-reload. Mirrors the shape of
     * {@code PrometheusRemoteWriterConfig.diff}: one line per header added,
     * removed, or changed; the line names the header but never echoes a
     * value — secrets stay masked even at value change. Line format:
     * <ul>
     *   <li>{@code http.headers.<name>: (set) -> (set)} for value change</li>
     *   <li>{@code http.headers.<name>: (unset) -> (set)} for added</li>
     *   <li>{@code http.headers.<name>: (set) -> (unset)} for removed</li>
     * </ul>
     *
     * <p>Argument is a {@link Map} (not another bean instance) because this
     * bean is stateful — one instance, mutated by Aries — so comparing two
     * beans is meaningless. The storage layer snapshots the previous
     * headers map via {@link #headers()} and passes it here on reload.
     *
     * @param previous the prior snapshot, or {@code null} for the first
     *                 activation (when nothing was previously set)
     * @return diff lines; empty when the snapshots are equal or both empty
     */
    public List<String> diff(Map<String, String> previous) {
        return diff(previous, this.headers);
    }

    /**
     * Variant of {@link #diff(Map)} that takes both snapshots explicitly.
     * Used by callers that need to snapshot the {@code volatile} {@link #headers}
     * field exactly once and pass the same reference into both the prior-
     * anchor capture and the diff call — eliminates a sub-microsecond
     * window where a concurrent {@link #applyProperties(Map)} between the two
     * reads would compute the diff against the wrong "after" snapshot.
     *
     * @param previous the prior snapshot, or {@code null}
     * @param current  the "after" snapshot (typically a captured
     *                 {@link #headers()} result)
     * @return diff lines
     */
    public static List<String> diff(Map<String, String> previous,
                                    Map<String, String> current) {
        Map<String, String> after = (current == null)
                ? Collections.emptyMap() : current;
        Map<String, String> before = (previous == null)
                ? Collections.emptyMap() : previous;
        if (before.equals(after)) return Collections.emptyList();

        List<String> out = new ArrayList<>();
        // Sort by header name for deterministic output regardless of input
        // map iteration order (LinkedHashMap preserves insertion which comes
        // from ConfigAdmin's unordered dictionary).
        Set<String> allNames = new TreeSet<>();
        allNames.addAll(before.keySet());
        allNames.addAll(after.keySet());
        for (String name : allNames) {
            boolean wasSet = before.containsKey(name);
            boolean isSet  = after.containsKey(name);
            if (wasSet && isSet) {
                // Objects.equals — defends against a caller-supplied map
                // with a null value, which would NPE on .get(name).equals(...).
                if (!Objects.equals(before.get(name), after.get(name))) {
                    out.add("http.headers." + name + ": (set) -> (set)");
                }
            } else if (isSet) {
                out.add("http.headers." + name + ": (unset) -> (set)");
            } else {
                out.add("http.headers." + name + ": (set) -> (unset)");
            }
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Factory for a no-headers instance — used by tests and by callers that
     * need a non-null placeholder when no Blueprint wiring is present
     * (e.g., the single-arg test constructor on the HTTP clients).
     */
    public static HttpHeadersConfig empty() {
        return new HttpHeadersConfig();
    }

    @Override
    public String toString() {
        return "HttpHeadersConfig[count=" + headers.size() + " (values redacted)]";
    }

    // -- internals -----------------------------------------------------------

    private static Map<String, String> extractAndValidate(Map<String, ?> properties) {
        Map<String, String> out = new LinkedHashMap<>();
        // Parallel lowercase-key set so we can reject case-insensitive
        // duplicates (`http.headers.X-Foo` AND `http.headers.x-foo`) — both
        // would otherwise pass the case-sensitive map.put and reach the wire
        // as two distinct lines, with backend behavior undefined.
        Set<String> seenLower = new HashSet<>();
        for (Map.Entry<String, ?> e : properties.entrySet()) {
            String key = e.getKey();
            if (key == null || !key.startsWith(PREFIX)) continue;

            String headerName = key.substring(PREFIX.length());
            validateName(headerName);
            checkReserved(headerName);

            String lower = headerName.toLowerCase(Locale.ROOT);
            if (!seenLower.add(lower)) {
                throw new IllegalStateException(
                    "http.headers entry '" + headerName + "' duplicates "
                    + "another entry differing only in case — HTTP header "
                    + "names are case-insensitive; configure one casing");
            }

            Object rawValue = e.getValue();
            // ConfigAdmin values are typed Object. A .cfg edit always yields a
            // String, but a programmatically-populated configuration can carry
            // an array or a Collection, whose toString() would be attached as
            // a header value verbatim (e.g. "[Ljava.lang.String;@1a2b").
            // Reject those rather than putting a JVM identity hash on the wire.
            if (rawValue != null
                    && (rawValue.getClass().isArray() || rawValue instanceof Collection)) {
                throw new IllegalStateException(
                    "http.headers entry '" + headerName + "' has a multi-valued "
                    + "configuration value — header values must be a single "
                    + "string; configure one value per header name");
            }
            String value = rawValue == null ? "" : rawValue.toString();
            String trimmed = trimAscii(value);
            validateValue(headerName, trimmed);

            out.put(headerName, trimmed);
        }
        return out;
    }

    private static void validateName(String name) {
        if (name.isEmpty()) {
            throw new IllegalStateException(
                "http.headers.* property has an empty header name component "
                + "after the 'http.headers.' prefix — check for a stray "
                + "trailing dot in the property key");
        }
        // Defense-in-depth log-injection guard. Java Properties syntax can't
        // produce a property key with a real CR/LF (those are line breaks
        // in the file format), but ConfigAdmin can also be programmatically
        // populated. Reject CR/LF before the name lands in any error
        // message that gets logged.
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '\r' || c == '\n') {
                throw new IllegalStateException(
                    "http.headers.* property key contains a CR or LF "
                    + "character — rejected to prevent log injection");
            }
        }
        if (!HEADER_NAME.matcher(name).matches()) {
            throw new IllegalStateException(
                "http.headers entry '" + name + "' has an invalid header "
                + "name — must match the RFC 7230 token grammar "
                + "(letters, digits, and !#$%&'*+-.^_`|~ only; no whitespace "
                + "or separators)");
        }
    }

    private static void checkReserved(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (RESERVED_PROTOCOL.contains(lower)) {
            throw new IllegalStateException(
                "http.headers entry '" + name + "' is reserved — the plugin "
                + "sets this header per the wire-protocol invariants and it "
                + "cannot be overridden via http.headers.*");
        }
        if (RESERVED_MANAGED.contains(lower)) {
            String pointer = switch (lower) {
                case "authorization" ->
                    "use auth.basic.username + auth.basic.password, or "
                    + "auth.bearer.token, instead";
                case "proxy-authorization" ->
                    "the plugin does not proxy-authenticate; configure the "
                    + "proxy credentials on the proxy itself";
                case "x-scope-orgid" ->
                    "use tenant.org-id instead";
                default ->
                    // Defensive — RESERVED_MANAGED only contains the two
                    // names above today, but if someone extends the set
                    // without updating this switch, fail loudly rather than
                    // silently emit a vague hint.
                    throw new IllegalStateException(
                        "internal error: reserved-managed header '" + lower
                        + "' has no rejection hint configured");
            };
            throw new IllegalStateException(
                "http.headers entry '" + name + "' is reserved — "
                + pointer);
        }
    }

    private static void validateValue(String name, String trimmed) {
        if (trimmed.isEmpty()) {
            throw new IllegalStateException(
                "http.headers entry '" + name + "' has an empty value — "
                + "if you intended to inject an environment variable, note "
                + "that the plugin does not currently expand ${env:NAME} in "
                + "configuration values; the value must be literal cleartext");
        }
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '\r' || c == '\n') {
                // Header-splitting / response-splitting guard. Echo the
                // header name only — never the value — to avoid leaking a
                // partially-formed secret to the Karaf log.
                throw new IllegalStateException(
                    "http.headers entry '" + name + "' value contains a CR "
                    + "or LF character — rejected to prevent header injection");
            }
            if (c == '\t' || c == ' ') continue;
            if (c < 0x20 || c > 0x7E) {
                throw new IllegalStateException(
                    "http.headers entry '" + name + "' value contains a "
                    + "non-printable or non-ASCII byte — header values must "
                    + "be printable ASCII (HT and SP allowed)");
            }
        }
    }

    /**
     * Trim leading and trailing space/tab only. Deliberately narrower than
     * {@link String#strip()} — header values must stay within the printable
     * ASCII + HT range, and stripping arbitrary Unicode whitespace would
     * accept inputs that the subsequent value validator rejects, producing
     * confusing error messages.
     */
    private static String trimAscii(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && (s.charAt(start) == ' ' || s.charAt(start) == '\t')) start++;
        while (end > start && (s.charAt(end - 1) == ' ' || s.charAt(end - 1) == '\t')) end--;
        return (start == 0 && end == s.length()) ? s : s.substring(start, end);
    }

    /**
     * Emit one INFO log line listing the detected header names (alphabetical,
     * values masked) so operators can spot a property-name typo at startup
     * without round-tripping through the backend. Skipped when the map is
     * empty — the v0.4.x default state — to avoid log noise.
     */
    private static void logActivation(Map<String, String> parsed) {
        if (parsed.isEmpty()) return;
        LOG.info(formatActivationMessage(parsed));
    }

    /**
     * Build the activation-message string. Package-private and static so unit
     * tests can assert the redaction shape without configuring a log-capture
     * appender — production code reaches this only via {@link #logActivation}.
     */
    static String formatActivationMessage(Map<String, String> parsed) {
        StringBuilder sb = new StringBuilder("Custom HTTP headers attached: ");
        boolean first = true;
        for (String name : new TreeSet<>(parsed.keySet())) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(name).append(" (value redacted)");
        }
        return sb.toString();
    }

    // -- equals/hashCode left as default identity — instances are wired by
    // -- Blueprint and never compared as values. Tests that need to compare
    // -- expected vs actual header sets call .headers() directly.
}
