/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * What the write path last saw about one OpenNMS resource: its string
 * attributes (OpenNMS key spelling, values verbatim, sorted by key), its
 * surveillance categories, and its interface speed in bits per second when
 * it has one. Immutable; the registry replaces the whole snapshot on change.
 *
 * @param resourceId the raw OpenNMS {@code resourceId}, unsanitised; the
 *                   emitter sanitises it exactly as the data series do so a
 *                   join on {@code resourceId} matches
 * @param ifSpeedBps null when the resource carries no usable speed
 */
public record ResourceMetadata(String resourceId,
                               SortedMap<String, String> attributes,
                               SortedSet<String> categories,
                               Long ifSpeedBps) {
    public ResourceMetadata {
        Objects.requireNonNull(resourceId, "resourceId");
        Objects.requireNonNull(attributes, "attributes");
        Objects.requireNonNull(categories, "categories");
    }

    /** True when there is nothing to put on the wire for this resource. */
    public boolean isEmpty() {
        return attributes.isEmpty() && categories.isEmpty() && ifSpeedBps == null;
    }
}
