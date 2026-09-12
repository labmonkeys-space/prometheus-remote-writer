/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import java.util.Map;
import java.util.Objects;

/**
 * A sample after label mapping, on its way to the wire.
 *
 * @param labels          post-mapping labels including {@code __name__}
 * @param timestampMs     sample time
 * @param value           sample value
 * @param key             series identity, computed once (see {@link SeriesKey})
 * @param enqueuedEpochMs wall-clock time the sample entered {@code store()};
 *                        summed at acknowledgement into
 *                        {@code sample_latency_ms_total}. Wall clock rather
 *                        than monotonic so it survives a WAL replay across
 *                        a restart.
 */
public record MappedSample(Map<String, String> labels, long timestampMs, double value,
                           SeriesKey key, long enqueuedEpochMs) {

    /** Prometheus reserves this label name for the metric name; every series must have it. */
    public static final String METRIC_NAME_LABEL = "__name__";

    /** Key computed from the labels, stamped now. */
    public MappedSample(Map<String, String> labels, long timestampMs, double value) {
        this(labels, timestampMs, value, System.currentTimeMillis());
    }

    /** Key computed from the labels, explicit stamp (WAL decode, tests). */
    public MappedSample(Map<String, String> labels, long timestampMs, double value, long enqueuedEpochMs) {
        this(labels, timestampMs, value, SeriesKey.of(Objects.requireNonNull(labels, "labels")), enqueuedEpochMs);
    }

    public MappedSample {
        Objects.requireNonNull(labels, "labels");
        Objects.requireNonNull(key, "key");
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("labels must not be empty");
        }
        if (!labels.containsKey(METRIC_NAME_LABEL)) {
            throw new IllegalArgumentException(
                "labels must contain " + METRIC_NAME_LABEL
                    + " — Prometheus rejects series without a metric name");
        }
    }
}
