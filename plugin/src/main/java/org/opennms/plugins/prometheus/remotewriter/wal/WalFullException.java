/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.IOException;
import java.util.List;

/**
 * Thrown by {@link WalWriter#append(byte[])} when the WAL is at its
 * configured size cap and the overflow policy refuses to make room.
 *
 * <p>The caller ({@code PrometheusRemoteWriterStorage.store()} under
 * the wal-enabled path) converts this to an
 * {@code org.opennms.integration.api.v1.timeseries.StorageException}
 * and increments {@code samples_dropped_wal_full_total} — so the
 * failure mode is operator-visible at the same counter regardless of
 * which overflow policy is active.
 *
 * <p>{@link #evictedFramesBeforeFailure()} reports any frames the
 * writer had already evicted under DROP_OLDEST before giving up — may
 * be non-zero in the rare configuration where a single frame is
 * larger than the entire cap (which is always an operator
 * misconfiguration).
 */
public final class WalFullException extends IOException {

    private final int evictedFramesBeforeFailure;
    private final List<WalWriter.EvictedSegment> evictedSegmentsBeforeFailure;

    public WalFullException(String message, int evictedFramesBeforeFailure,
                            List<WalWriter.EvictedSegment> evictedSegmentsBeforeFailure) {
        super(message);
        this.evictedFramesBeforeFailure = evictedFramesBeforeFailure;
        this.evictedSegmentsBeforeFailure = List.copyOf(evictedSegmentsBeforeFailure);
    }

    public int evictedFramesBeforeFailure() {
        return evictedFramesBeforeFailure;
    }

    /** The segments those frames were in; see
     *  {@link WalWriter.AppendResult#evictedSegments()}. */
    public List<WalWriter.EvictedSegment> evictedSegmentsBeforeFailure() {
        return evictedSegmentsBeforeFailure;
    }
}
