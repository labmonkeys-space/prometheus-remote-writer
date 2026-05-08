/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IfSpeedNormalizerTest {

    @Test
    void prefers_high_speed_when_non_zero() {
        // ifHighSpeed=10000 (megabits) wins over ifSpeed=4294967295 (saturated 32-bit).
        assertThat(IfSpeedNormalizer.normalize("10000", "4294967295"))
                .isEqualTo(10_000L * 1_000_000L);
    }

    @Test
    void falls_back_to_ifspeed_when_high_speed_is_zero() {
        assertThat(IfSpeedNormalizer.normalize("0", "100000000"))
                .isEqualTo(100_000_000L);
    }

    @Test
    void falls_back_to_ifspeed_when_high_speed_is_absent() {
        assertThat(IfSpeedNormalizer.normalize(null, "1000000000"))
                .isEqualTo(1_000_000_000L);
    }

    @Test
    void accepts_ifspeed_zero_without_discarding() {
        // Some interfaces report ifSpeed=0 (loopback, tunnel with unknown speed).
        // Still informative; emit it.
        assertThat(IfSpeedNormalizer.normalize(null, "0")).isEqualTo(0L);
    }

    @Test
    void returns_null_when_both_inputs_are_absent() {
        assertThat(IfSpeedNormalizer.normalize(null, null)).isNull();
    }

    @Test
    void returns_null_when_inputs_are_non_numeric() {
        assertThat(IfSpeedNormalizer.normalize("fast", "quick")).isNull();
    }

    @Test
    void trims_whitespace() {
        assertThat(IfSpeedNormalizer.normalize(" 1000 ", null)).isEqualTo(1_000L * 1_000_000L);
    }

    @Test
    void rejects_negative_values() {
        assertThat(IfSpeedNormalizer.normalize("-1", "-1")).isNull();
    }

    @Test
    void high_speed_overflow_falls_back_to_ifspeed() {
        // ifHighSpeed × 1e6 overflows Long — must not wrap to a negative.
        assertThat(IfSpeedNormalizer.normalize("9223372036855", "100")).isEqualTo(100L);
    }

    @Test
    void high_speed_overflow_without_ifspeed_fallback_returns_null() {
        assertThat(IfSpeedNormalizer.normalize("9223372036855", null)).isNull();
    }

    // ---------- isParseableNonNegative predicate ---------------------------

    @Test
    void is_parseable_non_negative_accepts_zero() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative("0")).isTrue();
    }

    @Test
    void is_parseable_non_negative_accepts_positive() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative("100000000")).isTrue();
    }

    @Test
    void is_parseable_non_negative_accepts_whitespace_padded_numeric() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative(" 100 ")).isTrue();
    }

    @Test
    void is_parseable_non_negative_rejects_null_and_empty() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative(null)).isFalse();
        assertThat(IfSpeedNormalizer.isParseableNonNegative("")).isFalse();
    }

    @Test
    void is_parseable_non_negative_rejects_whitespace_only() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative("   ")).isFalse();
    }

    @Test
    void is_parseable_non_negative_rejects_non_numeric() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative("abc")).isFalse();
        assertThat(IfSpeedNormalizer.isParseableNonNegative("fast")).isFalse();
    }

    @Test
    void is_parseable_non_negative_rejects_negative() {
        assertThat(IfSpeedNormalizer.isParseableNonNegative("-1")).isFalse();
        assertThat(IfSpeedNormalizer.isParseableNonNegative("-100")).isFalse();
    }
}
