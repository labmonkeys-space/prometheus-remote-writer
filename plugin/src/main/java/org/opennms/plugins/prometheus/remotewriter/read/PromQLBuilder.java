/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;

/**
 * Builds PromQL selectors ({@code {name="value",other=~"re"}}) from OpenNMS
 * tag matchers and intrinsic tags. Matches the label schema the plugin uses
 * on the write side: {@code name → __name__}, everything else passes through
 * with label-name sanitization.
 */
public final class PromQLBuilder {

    private PromQLBuilder() {}

    /** Build a {@code {…}} selector from a collection of TagMatchers.
     *  Rejects null or empty input: Prometheus rejects an empty selector
     *  with a 400, and silently sending one would mask the real cause. */
    public static String fromMatchers(Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers");
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("matchers must not be empty");
        }
        String body = matchers.stream()
                .map(PromQLBuilder::renderMatcher)
                .collect(Collectors.joining(","));
        return "{" + body + "}";
    }

    /**
     * Build a {@code {…}} selector from a metric's intrinsic tags — all
     * matchers are EQUALS. Labels are emitted in lexicographic order by name
     * so the produced selector is stable across runs (matters for log
     * readability and any upstream URL caching).
     */
    public static String fromIntrinsicTags(Set<Tag> intrinsicTags) {
        Objects.requireNonNull(intrinsicTags, "intrinsicTags");
        if (intrinsicTags.isEmpty()) {
            throw new IllegalArgumentException("intrinsic tags must not be empty");
        }
        String body = intrinsicTags.stream()
                .map(t -> new String[] { renderLabel(t.getKey()), escape(t.getValue()) })
                .sorted(Comparator.comparing(pair -> pair[0]))
                .map(pair -> pair[0] + "=\"" + pair[1] + "\"")
                .collect(Collectors.joining(","));
        return "{" + body + "}";
    }

    private static String renderMatcher(TagMatcher m) {
        String label = renderLabel(m.getKey());
        String op = switch (m.getType()) {
            case EQUALS            -> "=";
            case NOT_EQUALS        -> "!=";
            case EQUALS_REGEX      -> "=~";
            case NOT_EQUALS_REGEX  -> "!~";
        };
        return label + op + "\"" + escape(m.getValue()) + "\"";
    }

    private static String renderLabel(String tagKey) {
        if (IntrinsicTagNames.name.equals(tagKey)) {
            return "__name__";
        }
        // IntrinsicTagNames.resourceId is already a valid label name.
        return Sanitizer.labelName(tagKey);
    }

    /**
     * Build a phase-1 selector for the two-phase discovery path: the original
     * matcher collection minus any matchers keyed on {@code resourceId}.
     * Phase 1 calls {@code /api/v1/label/resourceId/values} to enumerate
     * resourceIds; the regex-on-resourceId would over-constrain that
     * enumeration (we want it to enumerate every resourceId consistent with
     * the *non-resourceId* constraints, then phase 2 narrows by alternation).
     *
     * <p>Returns {@code null} when stripping leaves no matchers behind —
     * the caller signals this by omitting the {@code match[]} parameter from
     * the phase-1 URL entirely. Prometheus tolerates an absent
     * {@code match[]} and enumerates every value of the named label in the
     * lookback window.
     */
    public static String fromMatchersExcludingResourceId(Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers");
        // Sort by key for URL stability across calls — same input collection
        // (in any order) produces the same selector bytes. Round-2 fix
        // (P-R2-2) brings phase 1 in line with the round-1 phase-2 sort.
        // `Comparator.nullsFirst` tolerates a null `getKey()` (round-2 P-R2-3)
        // — a malformed matcher would NPE the bare `Comparator.comparing`.
        String body = matchers.stream()
                .filter(m -> !IntrinsicTagNames.resourceId.equals(m.getKey()))
                .sorted(Comparator.comparing(TagMatcher::getKey, Comparator.nullsFirst(Comparator.naturalOrder())))
                .map(PromQLBuilder::renderMatcher)
                .collect(Collectors.joining(","));
        if (body.isEmpty()) return null;
        return "{" + body + "}";
    }

    /**
     * Build a phase-2 selector for the two-phase discovery path: the original
     * matcher collection with any {@code resourceId}-keyed matchers replaced
     * by an anchored exact-match alternation over the supplied resourceIds.
     * Non-resourceId matchers are preserved verbatim.
     *
     * <p>The alternation is {@code resourceId=~"^(v1|v2|…|vN)$"}. Anchoring
     * with {@code ^…$} prevents accidental over-match (a hypothetical
     * {@code resourceId="v1X"} would match an unanchored {@code v1|v2}).
     * Each value is regex-meta-escaped via {@link #escapeRegexLiteral} so
     * the alternation matches the value byte-for-byte — important because
     * OpenNMS resourceIds use the bracketed grammar
     * ({@code node[1].interfaceSnmp[en0]}), which is full of regex
     * meta-characters.
     *
     * @throws IllegalArgumentException if {@code resourceIds} is empty —
     *   the caller is responsible for chunking before calling, and an empty
     *   chunk indicates a logic error rather than an empty result.
     */
    public static String fromMatchersWithResourceIdAlternation(
            Collection<TagMatcher> matchers, List<String> resourceIds) {
        Objects.requireNonNull(matchers, "matchers");
        Objects.requireNonNull(resourceIds, "resourceIds");
        if (resourceIds.isEmpty()) {
            throw new IllegalArgumentException("resourceIds must not be empty");
        }
        // Sort non-resourceId matchers by key for URL stability across calls
        // — same input collection (in any order) produces the same selector
        // string. Matches the order-stability contract of fromIntrinsicTags.
        // Code-review round 1 (P5): without sorting, a Set-typed input could
        // produce different selector bytes on each call, defeating any
        // upstream caching and breaking test determinism.
        // Round 2 (P-R2-3): `Comparator.nullsFirst` tolerates a null
        // `getKey()` — a malformed matcher would NPE the bare
        // `Comparator.comparing` introduced in round 1.
        List<TagMatcher> sortedNonRid = matchers.stream()
                .filter(m -> !IntrinsicTagNames.resourceId.equals(m.getKey()))
                .sorted(Comparator.comparing(TagMatcher::getKey, Comparator.nullsFirst(Comparator.naturalOrder())))
                .toList();
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (TagMatcher m : sortedNonRid) {
            if (!first) sb.append(',');
            sb.append(renderMatcher(m));
            first = false;
        }
        if (!first) sb.append(',');
        sb.append(IntrinsicTagNames.resourceId).append("=~\"^(");
        boolean firstValue = true;
        for (String v : resourceIds) {
            if (!firstValue) sb.append('|');
            // Regex-meta-escape first, then PromQL-value-escape. The two
            // layers compose: the PromQL-value-escape only adds backslash-
            // escapes for backslash and quote; those are already neutralised
            // by the regex-meta-escape, so double-escaping is safe.
            sb.append(escape(escapeRegexLiteral(v)));
            firstValue = false;
        }
        sb.append(")$\"}");
        return sb.toString();
    }

    /**
     * Escape a string so its byte sequence becomes a regex literal —
     * a regex that matches the input byte-for-byte. Distinct from
     * {@link #escape(String)}, which handles the PromQL label-value grammar
     * (backslash + quote + C0 controls). Regex meta-escape is needed before
     * PromQL-value-escape: the alternation values land inside a {@code =~}
     * matcher, so the regex compiler interprets them.
     *
     * <p>Meta-characters escaped: {@code . * + ? ^ $ ( ) [ ] { } | \}. Other
     * characters pass through. {@code Pattern.quote}'s {@code \Q...\E}
     * sentinels would also work but produce uglier wire selectors and break
     * if the input itself contains {@code \E}; the per-character backslash
     * escape is robust and self-contained.
     */
    static String escapeRegexLiteral(String v) {
        if (v == null || v.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(v.length() + 8);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '.', '*', '+', '?', '^', '$',
                     '(', ')', '[', ']', '{', '}',
                     '|', '\\' -> sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Escape PromQL label-value grammar: backslash, double-quote, and all
     *  C0 control characters (newline, carriage return, tab, etc.) so values
     *  cannot produce log-line injection or break selector parsing on strict
     *  Prometheus forks. */
    static String escape(String v) {
        if (v == null || v.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(v.length() + 4);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        // Escape remaining C0 controls using the backslash-u
                        // four-hex-digit form.
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
