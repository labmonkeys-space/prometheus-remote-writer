/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opennms.plugins.prometheus.remotewriter.wal.proto.WalEntry;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Conversion between the in-memory {@link MappedSample} and the on-disk
 * {@link WalEntry} protobuf record. Pulls the {@code __name__} label into
 * its own field so the structurally-required metric name is not buried in
 * the generic label map — a micro-optimisation and a consistency guarantee
 * (a {@code WalEntry} with an empty {@code metric_name} is malformed and
 * fails the round-trip).
 */
public final class WalEntryCodec {

    private WalEntryCodec() { /* utility */ }

    /** Serialise a sample to the bytes that go inside a WAL frame. */
    public static byte[] encode(MappedSample sample) {
        WalEntry.Builder b = WalEntry.newBuilder()
                .setTimestampMs(sample.timestampMs())
                .setValue(sample.value())
                .setEnqueuedEpochMs(sample.enqueuedEpochMs());
        for (Map.Entry<String, String> e : sample.labels().entrySet()) {
            if (MappedSample.METRIC_NAME_LABEL.equals(e.getKey())) {
                b.setMetricName(e.getValue());
            } else {
                b.putLabels(e.getKey(), e.getValue());
            }
        }
        return b.build().toByteArray();
    }

    /**
     * Inverse of {@link #encode} — reconstruct a sample from the bytes
     * stored in a WAL frame. Throws if the bytes are not parseable or the
     * reconstructed sample would violate {@link MappedSample}'s invariants
     * (no metric_name, empty labels).
     */
    public static MappedSample decode(byte[] payload) {
        WalEntry entry;
        try {
            entry = WalEntry.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                "WAL entry is not a parseable WalEntry protobuf — segment may be "
                + "corrupted beyond torn-tail recovery", e);
        }
        // proto3 has no required fields — an all-zeros / empty-payload
        // WalEntry parses to a default instance with metric_name="". That
        // would reconstruct a labels map of {"__name__": ""} which
        // MappedSample accepts but Prometheus would reject at the wire.
        // Reject at the codec so callers see a clear signal instead of a
        // silent down-stream drop.
        if (entry.getMetricName().isEmpty()) {
            throw new IllegalStateException(
                "WAL entry has empty metric_name — entry is malformed or "
                + "payload bytes are not a WalEntry (frame CRC passed but "
                + "content is unusable)");
        }
        // LinkedHashMap so the reconstructed label map has deterministic
        // iteration order (__name__ first, then the protobuf-map keys in
        // their native order). Not strictly required by the wire — the
        // builder re-sorts — but makes debugging predictable.
        Map<String, String> labels = new LinkedHashMap<>(entry.getLabelsCount() + 1);
        labels.put(MappedSample.METRIC_NAME_LABEL, entry.getMetricName());
        labels.putAll(entry.getLabelsMap());
        long enqueued = entry.getEnqueuedEpochMs();
        // Legacy frame (field absent): count latency from replay, not from 1970.
        if (enqueued == 0L) enqueued = System.currentTimeMillis();
        return new MappedSample(labels, entry.getTimestampMs(), entry.getValue(), enqueued);
    }
}
