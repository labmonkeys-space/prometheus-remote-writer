/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opennms.plugins.prometheus.remotewriter.wire.proto.Label;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.Sample;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.TimeSeries;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest;
import org.xerial.snappy.Snappy;

/**
 * Serializes a batch of {@link MappedSample}s into a snappy-compressed
 * Prometheus Remote Write v1 {@code WriteRequest} payload, grouped by
 * label-set identity and time-ordered within each series.
 *
 * <p>Non-finite sample values ({@code NaN}, {@code ±Infinity}) are filtered
 * pre-serialization; the count is exposed on {@link BuildResult} so that
 * the caller can increment a Dropwizard counter (wired in task group 11).
 */
public final class RemoteWriteRequestBuilder {

    public record BuildResult(
            byte[] compressedPayload,
            int uncompressedSize,
            int seriesCount,
            int samplesWritten,
            int samplesDroppedNonfinite,
            int samplesDroppedDuplicate,
            long enqueuedEpochMsSum) {
        public boolean hasContent() { return samplesWritten > 0; }
    }

    private RemoteWriteRequestBuilder() {}

    public static BuildResult build(Collection<MappedSample> samples) {
        int dropped = 0;
        int duplicates = 0;

        // Group by canonical (sorted) label set. LinkedHashMap preserves
        // first-seen series order, which gives stable output for tests.
        Map<SeriesKey, List<MappedSample>> bySeries = new LinkedHashMap<>();
        for (MappedSample s : samples) {
            if (!Double.isFinite(s.value())) {
                dropped++;
                continue;
            }
            bySeries.computeIfAbsent(s.key(), k -> new ArrayList<>()).add(s);
        }

        WriteRequest.Builder req = WriteRequest.newBuilder();
        int written = 0;

        long enqueuedSum = 0L;
        for (Map.Entry<SeriesKey, List<MappedSample>> entry : bySeries.entrySet()) {
            TimeSeries.Builder ts = TimeSeries.newBuilder();
            SeriesKey key = entry.getKey();
            for (int i = 0; i < key.size(); i++) {
                ts.addLabels(Label.newBuilder()
                        .setName(key.names()[i])
                        .setValue(key.values()[i])
                        .build());
            }
            List<MappedSample> seriesSamples = entry.getValue();
            // Sort by timestamp ascending — Prometheus rejects out-of-order samples
            // within a single TimeSeries on the remote-write ingest path.
            // TimSort is stable, so same-timestamp samples retain their
            // arrival order; the dedup loop below keeps the newest arrival.
            seriesSamples.sort((a, b) -> Long.compare(a.timestampMs(), b.timestampMs()));
            final int n = seriesSamples.size();
            for (int i = 0; i < n; i++) {
                MappedSample s = seriesSamples.get(i);
                // Last-write-wins: if the next sample has the same timestamp,
                // drop this one. Prometheus rejects a whole batch on duplicate
                // timestamps in one series; collapsing here keeps the batch
                // acceptable without requiring upstream callers to dedup.
                if (i + 1 < n && seriesSamples.get(i + 1).timestampMs() == s.timestampMs()) {
                    duplicates++;
                    continue;
                }
                ts.addSamples(Sample.newBuilder()
                        .setValue(s.value())
                        .setTimestamp(s.timestampMs())
                        .build());
                written++;
                enqueuedSum += s.enqueuedEpochMs();
            }
            req.addTimeseries(ts.build());
        }

        byte[] uncompressed = req.build().toByteArray();
        byte[] compressed;
        try {
            compressed = Snappy.compress(uncompressed);
        } catch (IOException e) {
            throw new UncheckedIOException("snappy compression failed", e);
        }
        return new BuildResult(
                compressed,
                uncompressed.length,
                bySeries.size(),
                written,
                dropped,
                duplicates,
                enqueuedSum);
    }

}
