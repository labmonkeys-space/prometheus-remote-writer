/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.fuzz;

import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * A frame whose CRC passes still carries whatever was written, and after a
 * disk fault or a downgrade that can be bytes this build does not understand.
 * {@link WalEntryCodec#decode} is the boundary: it either returns a usable
 * sample or says the entry is unusable, and the replay loop is written
 * against exactly those two outcomes.
 *
 * <p>So the contract is the exception type. Anything else escaping here, a
 * protobuf parser throwing on a hostile varint or an allocation that a length
 * prefix drove out of memory, reaches a caller that does not catch it and
 * stops the drain of a bucket that was already backed up.
 */
public final class WalEntryCodecFuzzer {

    private WalEntryCodecFuzzer() { }

    public static void fuzzerTestOneInput(byte[] data) {
        MappedSample sample;
        try {
            sample = WalEntryCodec.decode(data);
        } catch (IllegalStateException expected) {
            // The documented answer for bytes that are not a usable entry.
            return;
        } catch (RuntimeException e) {
            throw new AssertionError("decode leaked " + e.getClass().getName()
                    + " instead of IllegalStateException", e);
        }
        if (sample == null) {
            throw new AssertionError("decode returned null rather than throwing");
        }
        String name = sample.labels().get(MappedSample.METRIC_NAME_LABEL);
        if (name == null || name.isEmpty()) {
            throw new AssertionError("decode accepted an entry with no metric name");
        }
        // A sample it accepted must survive its own encoder, or a replayed
        // WAL and a fresh one disagree about the same entry.
        MappedSample again = WalEntryCodec.decode(WalEntryCodec.encode(sample));
        if (!again.labels().equals(sample.labels())
                || again.timestampMs() != sample.timestampMs()) {
            throw new AssertionError("re-encoding an accepted entry changed it");
        }
        if (Double.compare(again.value(), sample.value()) != 0
                && !(Double.isNaN(again.value()) && Double.isNaN(sample.value()))) {
            throw new AssertionError("re-encoding an accepted entry changed its value");
        }
    }
}
