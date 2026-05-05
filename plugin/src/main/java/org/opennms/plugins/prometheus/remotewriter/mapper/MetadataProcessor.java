/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig.MetadataCase;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;

/**
 * Emits OpenNMS metadata attributes as Prometheus labels, with built-in
 * credential denylist and operator-configurable include/exclude globs.
 *
 * <p><b>Disabled by default.</b> When enabled, source tags whose key contains a
 * colon (the OpenNMS {@code context:key} convention) are treated as metadata
 * candidates. Each candidate is:
 * <ol>
 *   <li>Filtered through the built-in denylist — applied unconditionally so
 *       operator globs can never leak credentials. On a match, the value is
 *       dropped and {@link #getDenylistBlockedCount()} is incremented.</li>
 *   <li>Filtered through the user's {@code metadata.exclude} globs.</li>
 *   <li>Kept only if it matches one of the user's {@code metadata.include}
 *       globs.</li>
 *   <li>Emitted as a label named
 *       {@code <metadata.label-prefix><sanitized-key>} with the original
 *       value, optionally snake-cased.</li>
 * </ol>
 *
 * <p>Metadata labels never overwrite default-allowlist labels; on a collision
 * the default wins.
 */
public final class MetadataProcessor {

    /** Built-in denylist. Always applied. Patterns match the full source tag
     *  key (including {@code context:} prefix). */
    static final List<String> BUILTIN_DENYLIST = List.of(
            "*:*password*",
            "*:*secret*",
            "*:*token*",
            "*:*key*",
            "*:snmp-community");

    /** Plain-key (no {@code context:} prefix) denylist, applied by
     *  {@link LabelMapper#emitAttrLabels} when round-tripping arbitrary
     *  Sample meta tags through the {@code onms_attr_} namespace. Same
     *  intent as {@link #BUILTIN_DENYLIST} but matches keys that don't
     *  carry the OpenNMS context prefix (e.g. a raw {@code password}
     *  meta tag).
     *
     *  <p>Deliberately narrower than {@link #BUILTIN_DENYLIST}: the
     *  context-tag form has {@code *:*key*} to catch credential
     *  attachments to context tags, but in the plain-key path that
     *  pattern would also block legitimate OpenNMS resource string
     *  attributes shaped like {@code primary_key}, {@code partition_key},
     *  {@code foreign_key} — the exact attributes resource graphs
     *  substitute via {@code ${...}} placeholders. The remaining
     *  patterns ({@code *password*}, {@code *secret*}, {@code *token*},
     *  {@code snmp-community}) cover credential-shaped names without
     *  blocking schema descriptors. */
    static final List<String> BUILTIN_PLAIN_DENYLIST = List.of(
            "*password*",
            "*secret*",
            "*token*",
            "snmp-community");

    private static final List<Pattern> BUILTIN_PLAIN_DENYLIST_GLOBS =
            compileCaseInsensitive(BUILTIN_PLAIN_DENYLIST);

    /** Package-private predicate used by {@link LabelMapper} to filter
     *  plain-key meta tags before emitting them under the {@code onms_attr_}
     *  prefix. Case-insensitive so the safety net can't be bypassed by an
     *  unusual casing. */
    static boolean isPlainKeyDenied(String key) {
        return matchesAny(key, BUILTIN_PLAIN_DENYLIST_GLOBS);
    }

    private final boolean enabled;
    private final String prefix;
    private final MetadataCase caseMode;
    private final List<Pattern> includeGlobs;
    private final List<Pattern> excludeGlobs;
    private final List<Pattern> builtinDenylistGlobs;

    // Atomic because the gauge in PluginMetrics reads this from a different
    // thread than the write/flush pipeline increments it on.
    private final AtomicLong denylistBlockedCount = new AtomicLong();

    public MetadataProcessor(PrometheusRemoteWriterConfig config) {
        Objects.requireNonNull(config, "config");
        this.enabled              = config.isMetadataEnabled();
        this.prefix               = config.getMetadataLabelPrefix();
        this.caseMode             = config.getMetadataCase();
        this.includeGlobs         = compile(config.metadataIncludeGlobs());
        this.excludeGlobs         = compile(config.metadataExcludeGlobs());
        this.builtinDenylistGlobs = compileCaseInsensitive(BUILTIN_DENYLIST);
    }

    /**
     * Emit metadata labels into the supplied labels map (in place).
     * @return number of metadata candidates blocked by the built-in denylist
     *         during this call
     */
    public int emitInto(Map<String, String> labels, Map<String, String> sourceTags) {
        if (!enabled) return 0;
        int blocked = 0;
        for (Map.Entry<String, String> e : sourceTags.entrySet()) {
            String key = e.getKey();
            if (!looksLikeMetadata(key)) continue;

            if (matchesAny(key, builtinDenylistGlobs)) {
                blocked++;
                continue;
            }
            if (matchesAny(key, excludeGlobs)) continue;
            if (!matchesAny(key, includeGlobs)) continue;

            String labelName = prefix + Sanitizer.labelName(applyCase(key));
            // Never overwrite a default-allowlist label with metadata.
            labels.putIfAbsent(labelName, Sanitizer.labelValue(e.getValue()));
        }
        if (blocked > 0) denylistBlockedCount.addAndGet(blocked);
        return blocked;
    }

    public long getDenylistBlockedCount() { return denylistBlockedCount.get(); }

    private static boolean looksLikeMetadata(String tagKey) {
        // OpenNMS metadata arrives as context:key; anything without a colon is
        // not a metadata candidate by this plugin's convention.
        return tagKey.indexOf(':') >= 0;
    }

    private String applyCase(String key) {
        return caseMode == MetadataCase.SNAKE_CASE ? toSnakeCase(key) : key;
    }

    /** camelCase / PascalCase → snake_case. Correctly separates uppercase
     *  runs from the following word (e.g. {@code HTTPServer → http_server},
     *  {@code APIKey → api_key}, {@code HTTPProxyURL → http_proxy_url}).
     *  Non-alphanumerics pass through; the sanitizer handles them later. */
    static String toSnakeCase(String in) {
        StringBuilder sb = new StringBuilder(in.length() + 4);
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            if (i > 0 && Character.isUpperCase(c)) {
                char prev = in.charAt(i - 1);
                boolean prevIsLowerOrDigit = Character.isLowerCase(prev) || Character.isDigit(prev);
                // Run-of-uppercase boundary: the last uppercase in a run
                // belongs to the next word when followed by lowercase.
                boolean lastCapInRun = Character.isUpperCase(prev)
                        && i + 1 < in.length()
                        && Character.isLowerCase(in.charAt(i + 1));
                if (prevIsLowerOrDigit || lastCapInRun) {
                    sb.append('_');
                }
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    private static List<Pattern> compile(List<String> globs) {
        if (globs.isEmpty()) return List.of();
        List<Pattern> out = new ArrayList<>(globs.size());
        for (String g : globs) out.add(LabelMapper.globToPattern(g));
        return List.copyOf(out);
    }

    /** Built-in denylist patterns are case-insensitive so {@code *:*PASSWORD*},
     *  {@code *:APIKey}, etc. all match — the safety net can't be bypassed by
     *  an operator using a different casing on their keys. */
    private static List<Pattern> compileCaseInsensitive(List<String> globs) {
        if (globs.isEmpty()) return List.of();
        List<Pattern> out = new ArrayList<>(globs.size());
        for (String g : globs) out.add(LabelMapper.globToPattern(g, Pattern.CASE_INSENSITIVE));
        return List.copyOf(out);
    }

    private static boolean matchesAny(String s, List<Pattern> patterns) {
        for (Pattern p : patterns) {
            if (p.matcher(s).matches()) return true;
        }
        return false;
    }
}
