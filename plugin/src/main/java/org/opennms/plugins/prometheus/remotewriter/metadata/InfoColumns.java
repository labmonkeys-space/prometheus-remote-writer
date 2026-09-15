/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * The {@code metadata.info-columns} grammar: comma-separated {@code column=key}
 * entries, where {@code column} is a label name on the
 * {@code onms_resource_info} series and {@code key} is the OpenNMS attribute
 * it is read from.
 *
 * <p>The default covers the string attributes OpenNMS's shipped
 * datacollection emits for its standard resource types, one column per key,
 * so enabling host-resources storage or the PostgreSQL collector needs no
 * edit. A custom datacollection names its own column. The collector
 * attribute {@code name} becomes {@code resource_name}: a label called
 * {@code name} next to {@code __name__} is a trap in every legend and query.
 */
public final class InfoColumns {

    public static final String DEFAULT_SPEC =
            "if_alias=ifAlias, if_descr=ifDescr, resource_name=name, "
            + "hr_storage_descr=hrStorageDescr, dsk_path=dskPath, datname=datname, spcname=spcname";

    /**
     * Label names a column cannot take: the ones {@link MetadataEmitter} stamps
     * on every metadata series, and {@code name}, which next to
     * {@code __name__} is the trap {@code resource_name} exists to avoid.
     */
    public static final Set<String> RESERVED = Set.of(
            MappedSample.METRIC_NAME_LABEL,
            MetadataEmitter.RESOURCE_ID_LABEL,
            MetadataEmitter.INSTANCE_ID_LABEL,
            "name");

    private InfoColumns() {}

    /**
     * Parse a spec into column → key, in the order written.
     *
     * @throws IllegalArgumentException naming the offending entry, when an
     *         entry lacks {@code =}, a side is empty, a column is not a label
     *         name, is reserved, or repeats
     */
    public static Map<String, String> parse(String spec) {
        return parse(spec, MetadataRegistry.LABEL_KEYS.keySet());
    }

    /**
     * @param rowlessKeys the keys the registry skips as rows (their label is
     *                    on the data series), which a column cannot read
     */
    public static Map<String, String> parse(String spec, Set<String> rowlessKeys) {
        return parse(spec, rowlessKeys, java.util.List.of(), java.util.List.of());
    }

    /**
     * {@link #parse(String, Set)} judged with the operator's attribute globs,
     * so a column may read a key {@code metadata.attr-include} admits and may
     * not read one {@code metadata.attr-exclude} drops. Without them the
     * validation would refuse a configuration the registry accepts, and
     * accept one whose column can only ever be empty.
     *
     * @param attrIncludeGlobs {@code metadata.attr-include} globs
     * @param attrExcludeGlobs {@code metadata.attr-exclude} globs
     */
    public static Map<String, String> parse(String spec, Set<String> rowlessKeys,
                                            java.util.List<String> attrIncludeGlobs,
                                            java.util.List<String> attrExcludeGlobs) {
        java.util.List<java.util.regex.Pattern> include = MetadataRegistry.compileGlobs(attrIncludeGlobs);
        java.util.List<java.util.regex.Pattern> exclude = MetadataRegistry.compileGlobs(attrExcludeGlobs);
        Map<String, String> out = new LinkedHashMap<>();
        if (spec == null || spec.isBlank()) return Collections.emptyMap();   // no columns, no info series
        for (String raw : spec.split(",")) {
            String entry = raw.trim();
            if (entry.isEmpty()) continue;
            int eq = entry.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("'" + entry + "' is not of the form column=key");
            }
            String column = entry.substring(0, eq).trim();
            String key = entry.substring(eq + 1).trim();
            if (column.isEmpty() || key.isEmpty()) {
                throw new IllegalArgumentException("'" + entry + "' needs both a column and a key (column=key)");
            }
            // The same rule the wire applies to every other label name.
            if (!Sanitizer.labelName(column).equals(column)) {
                throw new IllegalArgumentException("column '" + column + "' is not a valid label name");
            }
            if ("name".equals(column)) {
                throw new IllegalArgumentException("column 'name' would sit next to __name__ in every legend "
                        + "and query; use resource_name=" + key);
            }
            if (RESERVED.contains(column)) {
                throw new IllegalArgumentException("column '" + column + "' is reserved on the info series");
            }
            // A key the registry never records would make a column that is
            // always empty, with nothing to say why.
            String why = MetadataRegistry.whyNotARow(key, rowlessKeys, include, exclude);
            if (why != null) {
                throw new IllegalArgumentException("key '" + key + "' is not a resource attribute: " + why);
            }
            if (!MetadataRegistry.isAttributeKey(key, include, exclude)) {
                throw new IllegalArgumentException("key '" + key + "' is not a resource attribute "
                        + "(the metric type, categories, the interface speed pair, context keys with ':' "
                        + "and secret keys never become attributes)");
            }
            if (out.put(column, key) != null) {
                throw new IllegalArgumentException("column '" + column + "' is listed twice");
            }
        }
        return Collections.unmodifiableMap(out);
    }
}
