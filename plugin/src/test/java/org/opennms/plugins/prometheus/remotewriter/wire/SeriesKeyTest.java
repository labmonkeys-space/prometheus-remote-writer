/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SeriesKeyTest {

    @Test
    void equal_label_maps_in_different_insertion_orders_give_equal_keys() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("__name__", "m"); a.put("node", "n1"); a.put("job", "snmp");
        Map<String, String> b = new LinkedHashMap<>();
        b.put("job", "snmp"); b.put("node", "n1"); b.put("__name__", "m");

        SeriesKey ka = SeriesKey.of(a), kb = SeriesKey.of(b);
        assertThat(ka).isEqualTo(kb);
        assertThat(ka.hashCode()).isEqualTo(kb.hashCode());
    }

    @Test
    void names_are_sorted_and_values_aligned() {
        SeriesKey k = SeriesKey.of(Map.of("node", "n1", "__name__", "m", "job", "snmp"));
        assertThat(k.names()).containsExactly("__name__", "job", "node");
        assertThat(k.values()).containsExactly("m", "snmp", "n1");
        assertThat(k.size()).isEqualTo(3);
    }

    @Test
    void different_values_give_different_keys() {
        assertThat(SeriesKey.of(Map.of("__name__", "m", "node", "n1")))
                .isNotEqualTo(SeriesKey.of(Map.of("__name__", "m", "node", "n2")));
    }

    @Test
    void mapped_sample_carries_its_key_and_an_enqueue_stamp() {
        long before = System.currentTimeMillis();
        MappedSample s = new MappedSample(Map.of("__name__", "m", "node", "n1"), 1L, 1.0);
        assertThat(s.key()).isEqualTo(SeriesKey.of(Map.of("node", "n1", "__name__", "m")));
        assertThat(s.enqueuedEpochMs()).isBetween(before, System.currentTimeMillis());
    }
}
