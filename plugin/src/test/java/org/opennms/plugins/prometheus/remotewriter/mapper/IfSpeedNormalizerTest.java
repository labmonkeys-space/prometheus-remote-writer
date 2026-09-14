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
    void prefers_high_speed_once_ifspeed_saturates() {
        // ifHighSpeed=10000 (megabits) wins over ifSpeed=4294967295 (saturated 32-bit).
        assertThat(IfSpeedNormalizer.normalize("10000", "4294967295"))
                .isEqualTo(10_000L * 1_000_000L);
    }

    @Test
    void prefers_the_exact_ifspeed_below_the_cap() {
        // A T1: ifHighSpeed is whole megabits (2), ifSpeed is the truth (1,544,000).
        assertThat(IfSpeedNormalizer.normalize("2", "1544000")).isEqualTo(1_544_000L);
        assertThat(IfSpeedNormalizer.normalize("1000", "1000000000")).isEqualTo(1_000_000_000L);
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

}
