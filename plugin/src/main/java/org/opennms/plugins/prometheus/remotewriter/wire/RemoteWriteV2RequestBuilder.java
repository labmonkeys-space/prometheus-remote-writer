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

import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.Request;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.Sample;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.TimeSeries;
import org.xerial.snappy.Snappy;

/**
 * Serialises a batch of {@link MappedSample}s into a snappy-compressed
 * Prometheus Remote Write v2.0.0 {@code Request} payload.
 *
 * <p>The v2 wire format differs from v1 in one operator-meaningful way:
 * label names and values are stored in a per-request {@code symbols}
 * table and {@code TimeSeries.labels_refs} reference into it as
 * {@code (name_idx, value_idx)} pairs. For typical OpenNMS batches —
 * where every sample carries the same dozen-or-so default labels
 * ({@code __name__}, {@code node}, {@code job}, {@code instance}, ...)
 * — interning eliminates the per-sample repetition of label names and
 * shared label values, yielding a 30-50% reduction in pre-snappy bytes.
 *
 * <p>The plugin does not populate {@code TimeSeries.metadata},
 * {@code TimeSeries.exemplars}, {@code TimeSeries.histograms}, or
 * {@code TimeSeries.created_timestamp} — see
 * {@code openspec/changes/add-remote-write-v2-support/design.md §3}.
 *
 * <p>Mirrors {@link RemoteWriteRequestBuilder} for non-finite filtering,
 * duplicate-timestamp dedup, and series-grouping. Returns the same
 * {@link BuildResult} record so callers (Flusher, WalFlusher) treat v1
 * and v2 outputs uniformly.
 */
public final class RemoteWriteV2RequestBuilder {

    /** v2 spec requires symbols[0] == "" — the empty-string sentinel. */
    private static final String EMPTY_SYMBOL = "";

    private RemoteWriteV2RequestBuilder() {}

    public static BuildResult build(Collection<MappedSample> samples) {
        int dropped = 0;
        int duplicates = 0;

        // Group by canonical (sorted) label set. LinkedHashMap preserves
        // first-seen series order, mirroring v1's stable test output.
        Map<SeriesKey, List<MappedSample>> bySeries = new LinkedHashMap<>();
        for (MappedSample s : samples) {
            if (!Double.isFinite(s.value())) {
                dropped++;
                continue;
            }
            bySeries.computeIfAbsent(s.key(), k -> new ArrayList<>()).add(s);
        }
        long enqueuedSum = 0L;

        // Symbol table — seeded with "" at index 0 per v2 spec.
        List<String> symbols = new ArrayList<>();
        Map<String, Integer> symbolIndex = new LinkedHashMap<>();
        symbols.add(EMPTY_SYMBOL);
        symbolIndex.put(EMPTY_SYMBOL, 0);

        Request.Builder req = Request.newBuilder();
        int written = 0;

        for (Map.Entry<SeriesKey, List<MappedSample>> entry : bySeries.entrySet()) {
            TimeSeries.Builder ts = TimeSeries.newBuilder();

            // Pack each label as (name_idx, value_idx) into labels_refs, in
            // the key's name-sorted order so the refs are deterministic.
            SeriesKey key = entry.getKey();
            for (int i = 0; i < key.size(); i++) {
                ts.addLabelsRefs(intern(key.names()[i], symbols, symbolIndex));
                ts.addLabelsRefs(intern(key.values()[i], symbols, symbolIndex));
            }

            List<MappedSample> seriesSamples = entry.getValue();
            // Same time-ordering + dedup invariant as v1.
            seriesSamples.sort((a, b) -> Long.compare(a.timestampMs(), b.timestampMs()));
            final int n = seriesSamples.size();
            for (int i = 0; i < n; i++) {
                MappedSample s = seriesSamples.get(i);
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

        // Symbols list is added once at the end of the build pass.
        req.addAllSymbols(symbols);

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

    /**
     * Intern a single string into the symbol table. Returns the index
     * (the value used in {@code labels_refs}). Idempotent — repeated
     * calls with the same string return the same index.
     */
    private static int intern(String s, List<String> symbols, Map<String, Integer> idx) {
        Integer existing = idx.get(s);
        if (existing != null) return existing;
        int next = symbols.size();
        symbols.add(s);
        idx.put(s, next);
        return next;
    }
}
