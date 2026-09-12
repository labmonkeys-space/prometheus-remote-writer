/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.Label;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.TimeSeries;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest;
import org.xerial.snappy.Snappy;

class RemoteWriteRequestBuilderTest {

    @Test
    void mapped_sample_rejects_labels_without_metric_name() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new MappedSample(Map.of("foo", "bar"), 1_000L, 1.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__name__");
    }

    @Test
    void empty_input_produces_empty_content() {
        BuildResult r = RemoteWriteRequestBuilder.build(List.of());
        assertThat(r.hasContent()).isFalse();
        assertThat(r.seriesCount()).isZero();
        assertThat(r.samplesWritten()).isZero();
        assertThat(r.samplesDroppedNonfinite()).isZero();
    }

    @Test
    void single_sample_emits_single_series_with_sorted_labels() throws Exception {
        MappedSample s = sample(Map.of(
                "zeta", "z",
                "__name__", "ifHCInOctets",
                "alpha", "a"), 1_000L, 42.0);

        BuildResult r = RemoteWriteRequestBuilder.build(List.of(s));
        WriteRequest parsed = parse(r);

        assertThat(r.seriesCount()).isEqualTo(1);
        assertThat(r.samplesWritten()).isEqualTo(1);

        TimeSeries ts = parsed.getTimeseries(0);
        // Labels must be emitted in lexicographic name order: __name__, alpha, zeta
        assertThat(ts.getLabelsList())
                .extracting(Label::getName)
                .containsExactly("__name__", "alpha", "zeta");
    }

    @Test
    void multiple_samples_same_labels_grouped_into_one_series_sorted_by_timestamp() throws Exception {
        Map<String, String> labels = Map.of("__name__", "foo", "node", "1:1");
        MappedSample later  = sample(labels, 2_000L, 2.0);
        MappedSample middle = sample(labels, 1_500L, 1.5);
        MappedSample early  = sample(labels, 1_000L, 1.0);

        BuildResult r = RemoteWriteRequestBuilder.build(List.of(later, early, middle));
        WriteRequest parsed = parse(r);

        assertThat(r.seriesCount()).isEqualTo(1);
        assertThat(r.samplesWritten()).isEqualTo(3);

        TimeSeries ts = parsed.getTimeseries(0);
        assertThat(ts.getSamplesList())
                .extracting(org.opennms.plugins.prometheus.remotewriter.wire.proto.Sample::getTimestamp)
                .containsExactly(1_000L, 1_500L, 2_000L);
    }

    @Test
    void different_label_sets_produce_separate_series() throws Exception {
        MappedSample a = sample(Map.of("__name__", "foo", "node", "1:1"), 1_000L, 1.0);
        MappedSample b = sample(Map.of("__name__", "foo", "node", "2:2"), 1_000L, 2.0);

        BuildResult r = RemoteWriteRequestBuilder.build(List.of(a, b));
        WriteRequest parsed = parse(r);

        assertThat(r.seriesCount()).isEqualTo(2);
        assertThat(parsed.getTimeseriesCount()).isEqualTo(2);
    }

    @Test
    void nan_and_infinity_are_dropped_and_counted() {
        Map<String, String> l = Map.of("__name__", "foo");
        BuildResult r = RemoteWriteRequestBuilder.build(List.of(
                sample(l, 1_000L, Double.NaN),
                sample(l, 2_000L, Double.POSITIVE_INFINITY),
                sample(l, 3_000L, Double.NEGATIVE_INFINITY),
                sample(l, 4_000L, 1.0)));

        assertThat(r.samplesDroppedNonfinite()).isEqualTo(3);
        assertThat(r.samplesWritten()).isEqualTo(1);
    }

    @Test
    void input_with_only_nonfinite_samples_produces_no_content() {
        Map<String, String> l = Map.of("__name__", "foo");
        BuildResult r = RemoteWriteRequestBuilder.build(List.of(
                sample(l, 1_000L, Double.NaN),
                sample(l, 2_000L, Double.POSITIVE_INFINITY)));

        assertThat(r.hasContent()).isFalse();
        assertThat(r.samplesDroppedNonfinite()).isEqualTo(2);
        assertThat(r.samplesWritten()).isZero();
    }

    @Test
    void duplicate_timestamps_collapse_last_write_wins() throws Exception {
        Map<String, String> labels = Map.of("__name__", "foo");
        // Three samples, two with the same timestamp (2_000L) — the later-
        // arriving value must win and the duplicate is counted as dropped.
        BuildResult r = RemoteWriteRequestBuilder.build(List.of(
                sample(labels, 1_000L, 1.0),
                sample(labels, 2_000L, 2.0),
                sample(labels, 2_000L, 2.5),
                sample(labels, 3_000L, 3.0)));

        assertThat(r.samplesDroppedDuplicate()).isEqualTo(1);
        assertThat(r.samplesWritten()).isEqualTo(3);

        WriteRequest parsed = parse(r);
        TimeSeries ts = parsed.getTimeseries(0);
        // The surviving sample at t=2000 must have the *later* value (2.5).
        assertThat(ts.getSamplesList())
                .extracting(org.opennms.plugins.prometheus.remotewriter.wire.proto.Sample::getValue)
                .containsExactly(1.0, 2.5, 3.0);
    }

    @Test
    void duplicates_in_different_series_do_not_collapse() {
        // Same timestamp, different label set → separate series, both survive.
        BuildResult r = RemoteWriteRequestBuilder.build(List.of(
                sample(Map.of("__name__", "foo", "node", "1:1"), 1_000L, 1.0),
                sample(Map.of("__name__", "foo", "node", "2:2"), 1_000L, 2.0)));

        assertThat(r.samplesDroppedDuplicate()).isZero();
        assertThat(r.samplesWritten()).isEqualTo(2);
    }

    @Test
    void round_trip_preserves_values_and_timestamps() throws Exception {
        MappedSample s = sample(Map.of("__name__", "foo", "node", "1:1"), 1_742_000L, 3.14);

        BuildResult r = RemoteWriteRequestBuilder.build(List.of(s));
        WriteRequest parsed = parse(r);

        TimeSeries ts = parsed.getTimeseries(0);
        assertThat(ts.getSamples(0).getValue()).isEqualTo(3.14);
        assertThat(ts.getSamples(0).getTimestamp()).isEqualTo(1_742_000L);
    }

    @Test
    void payload_is_snappy_encoded_not_raw_protobuf() throws Exception {
        // Sanity check: the BuildResult payload must decompress via Snappy and
        // then parse as a WriteRequest. If we forgot to compress, this fails.
        MappedSample s = sample(Map.of("__name__", "foo"), 1_000L, 1.0);
        BuildResult r = RemoteWriteRequestBuilder.build(List.of(s));

        byte[] decompressed = Snappy.uncompress(r.compressedPayload());
        assertThat(decompressed.length).isEqualTo(r.uncompressedSize());

        WriteRequest req = WriteRequest.parseFrom(decompressed);
        assertThat(req.getTimeseriesCount()).isEqualTo(1);
    }

    // ---------- helpers ------------------------------------------------------

    private static MappedSample sample(Map<String, String> labels, long ts, double v) {
        return new MappedSample(labels, ts, v);
    }

    private static WriteRequest parse(BuildResult r) throws Exception {
        return WriteRequest.parseFrom(Snappy.uncompress(r.compressedPayload()));
    }

    @Test
    void build_result_sums_the_enqueue_stamps_of_written_samples() {
        MappedSample a = new MappedSample(java.util.Map.of("__name__", "m", "i", "1"), 1_000L, 1.0, 5_000L);
        MappedSample b = new MappedSample(java.util.Map.of("__name__", "m", "i", "2"), 1_000L, 2.0, 7_000L);
        MappedSample nan = new MappedSample(java.util.Map.of("__name__", "m", "i", "3"), 1_000L, Double.NaN, 9_000L);
        RemoteWriteRequestBuilder.BuildResult r = RemoteWriteRequestBuilder.build(java.util.List.of(a, b, nan));
        assertThat(r.samplesWritten()).isEqualTo(2);
        assertThat(r.enqueuedEpochMsSum()).as("dropped samples are not in the latency sum").isEqualTo(12_000L);
    }

    @Test
    void samples_with_equal_labels_in_different_orders_share_one_series() throws Exception {
        java.util.Map<String, String> l1 = new java.util.LinkedHashMap<>(); l1.put("__name__", "m"); l1.put("node", "n");
        java.util.Map<String, String> l2 = new java.util.LinkedHashMap<>(); l2.put("node", "n"); l2.put("__name__", "m");
        RemoteWriteRequestBuilder.BuildResult r = RemoteWriteRequestBuilder.build(java.util.List.of(
                new MappedSample(l1, 1_000L, 1.0), new MappedSample(l2, 2_000L, 2.0)));
        org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest req =
                org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest.parseFrom(
                        org.xerial.snappy.Snappy.uncompress(r.compressedPayload()));
        assertThat(req.getTimeseriesCount()).isEqualTo(1);
        assertThat(req.getTimeseries(0).getSamplesCount()).isEqualTo(2);
        assertThat(req.getTimeseries(0).getLabels(0).getName()).isEqualTo("__name__");
    }
}
