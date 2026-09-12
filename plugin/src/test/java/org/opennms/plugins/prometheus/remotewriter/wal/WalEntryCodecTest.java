/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class WalEntryCodecTest {

    @Test
    void minimal_sample_round_trips() {
        MappedSample original = sample(Map.of("__name__", "up"), 1_000L, 1.0);
        byte[] bytes = WalEntryCodec.encode(original);
        MappedSample decoded = WalEntryCodec.decode(bytes);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void sample_with_many_labels_round_trips() {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "ifHCInOctets");
        labels.put("node", "NOC:router-42");
        labels.put("job", "snmp");
        labels.put("instance", "NOC:router-42");
        labels.put("foreign_source", "NOC");
        labels.put("foreign_id", "router-42");
        labels.put("location", "colo-west");
        labels.put("if_name", "GigabitEthernet0/0");
        labels.put("if_descr", "WAN uplink");
        labels.put("onms_cat_Routers", "");
        labels.put("onms_cat_Production", "");
        MappedSample original = sample(labels, 1_748_400_000_000L, 12345.0);

        byte[] bytes = WalEntryCodec.encode(original);
        MappedSample decoded = WalEntryCodec.decode(bytes);

        assertThat(decoded.labels()).containsExactlyInAnyOrderEntriesOf(labels);
        assertThat(decoded.timestampMs()).isEqualTo(original.timestampMs());
        assertThat(decoded.value()).isEqualTo(original.value());
    }

    @Test
    void unicode_label_values_survive_round_trip() {
        Map<String, String> labels = Map.of(
                "__name__", "disk_free_bytes",
                "path", "/usr/local/файлы/📁",
                "node_label", "tøkyö-server-01.øverlöck.example");
        MappedSample original = sample(labels, 0L, Double.MAX_VALUE);
        MappedSample decoded = WalEntryCodec.decode(WalEntryCodec.encode(original));
        assertThat(decoded.labels()).isEqualTo(labels);
    }

    @Test
    void empty_label_value_round_trips() {
        // onms_cat_* labels deliberately carry empty string values
        // (the category name is encoded in the label NAME).
        Map<String, String> labels = Map.of(
                "__name__", "up",
                "onms_cat_Routers", "");
        MappedSample original = sample(labels, 0L, 1.0);
        MappedSample decoded = WalEntryCodec.decode(WalEntryCodec.encode(original));
        assertThat(decoded.labels()).isEqualTo(labels);
    }

    @Test
    void decode_preserves_metric_name_as_first_label_key() {
        // Hardens the deterministic-iteration-order comment in WalEntryCodec:
        // __name__ is always the first entry in the reconstructed labels map
        // regardless of what order it was in the source.
        Map<String, String> scrambled = new LinkedHashMap<>();
        scrambled.put("z_last_alphabetically", "value");
        scrambled.put("__name__", "metric");
        scrambled.put("a_first_alphabetically", "value");
        MappedSample original = sample(scrambled, 0L, 0.0);

        MappedSample decoded = WalEntryCodec.decode(WalEntryCodec.encode(original));
        assertThat(decoded.labels().keySet().iterator().next())
                .isEqualTo(MappedSample.METRIC_NAME_LABEL);
    }

    @Test
    void decode_of_garbage_bytes_throws_with_actionable_message() {
        byte[] garbage = "this is not a protobuf".getBytes();
        assertThatThrownBy(() -> WalEntryCodec.decode(garbage))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parseable WalEntry protobuf");
    }

    @Test
    void decode_of_empty_bytes_rejects_due_to_empty_metric_name() {
        // proto3 parses empty bytes as a default-valued WalEntry (empty
        // strings, zero numbers, empty maps). The codec defends against
        // this: a WalEntry with empty metric_name is malformed — would
        // become a MappedSample with {"__name__": ""}, which Prometheus
        // rejects at the wire. Better to fail loud at decode time than
        // ship an invalid series.
        assertThatThrownBy(() -> WalEntryCodec.decode(new byte[0]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty metric_name");
    }

    @Test
    void encode_then_decode_round_trips_consistently() {
        // proto3 map<string,string> iteration order is unspecified, so
        // two encodes of the same sample MAY produce different byte
        // sequences across protobuf-java versions even for identical
        // input. What matters is that round-trip equality holds —
        // encoded bytes decode back to a MappedSample that equals the
        // original (by value, not by byte-pattern). This keeps the
        // test resilient to runtime implementation changes.
        MappedSample sample = sample(Map.of(
                "__name__", "metric",
                "a", "1",
                "b", "2",
                "c", "3"), 42L, 3.14);
        MappedSample first = WalEntryCodec.decode(WalEntryCodec.encode(sample));
        MappedSample second = WalEntryCodec.decode(WalEntryCodec.encode(sample));
        assertThat(first).isEqualTo(sample);
        assertThat(second).isEqualTo(sample);
        assertThat(first).isEqualTo(second);
    }

    // Helpers -----------------------------------------------------------------

    private static MappedSample sample(Map<String, String> labels, long ts, double value) {
        return new MappedSample(new LinkedHashMap<>(labels), ts, value);
    }

    @Test
    void round_trip_preserves_series_key_and_enqueue_stamp() {
        MappedSample in = new MappedSample(java.util.Map.of("__name__", "m", "node", "n1"), 1_000L, 1.5, 123_456L);
        MappedSample out = WalEntryCodec.decode(WalEntryCodec.encode(in));
        assertThat(out.key()).isEqualTo(in.key());
        assertThat(out.enqueuedEpochMs()).isEqualTo(123_456L);
    }

    @Test
    void legacy_frame_without_stamp_decodes_with_replay_time() {
        // A frame written before the stamp existed carries 0 in the field.
        byte[] legacy = org.opennms.plugins.prometheus.remotewriter.wal.proto.WalEntry.newBuilder()
                .setTimestampMs(1_000L).setMetricName("m").setValue(1.0).putLabels("node", "n1").build().toByteArray();
        long before = System.currentTimeMillis();
        MappedSample out = WalEntryCodec.decode(legacy);
        assertThat(out.enqueuedEpochMs()).isBetween(before, System.currentTimeMillis());
    }
}
