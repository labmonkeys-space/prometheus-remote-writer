/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.mapper;

/**
 * Normalises the SNMP {@code (ifSpeed, ifHighSpeed)} pair into a single
 * bits-per-second value. Mirrors the behaviour of the Prometheus SNMP exporter:
 * {@code ifHighSpeed} is expressed in megabits-per-second and is preferred
 * whenever it carries a non-zero value, because {@code ifSpeed} saturates at
 * 4.29 Gb/s.
 *
 * <p>Returns {@code null} when neither input is parseable — the caller must
 * then omit the {@code if_speed} label rather than emit a misleading value.
 */
public final class IfSpeedNormalizer {

    private IfSpeedNormalizer() {}

    /**
     * @param highSpeedRaw  value of the SNMP {@code ifHighSpeed} attribute (megabits/sec), may be {@code null}
     * @param speedRaw      value of the SNMP {@code ifSpeed} attribute (bits/sec), may be {@code null}
     * @return normalised bits-per-second, or {@code null} if neither is usable
     */
    public static Long normalize(String highSpeedRaw, String speedRaw) {
        Long hs = parseNonNegative(highSpeedRaw);
        if (hs != null && hs > 0) {
            try {
                return Math.multiplyExact(hs, 1_000_000L);
            } catch (ArithmeticException overflow) {
                // ifHighSpeed × 1e6 overflows Long — nothing sensible to report;
                // fall through to ifSpeed, and if that's absent too return null.
            }
        }
        Long s = parseNonNegative(speedRaw);
        if (s != null) {
            return s;
        }
        return null;
    }

    /**
     * Predicate over the same input grammar {@link #parseNonNegative} accepts:
     * non-null, non-empty, parseable as a non-negative {@code long} (after
     * trimming). Used by {@link LabelMapper}'s raw-mode emission to apply the
     * same source-presence filter the normalized path applies — so whitespace-
     * only, non-numeric, or negative source values are dropped instead of being
     * emitted verbatim into a series-identity label.
     */
    static boolean isParseableNonNegative(String raw) {
        return parseNonNegative(raw) != null;
    }

    private static Long parseNonNegative(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            long v = Long.parseLong(raw.trim());
            return v >= 0 ? v : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
