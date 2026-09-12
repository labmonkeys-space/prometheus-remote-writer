/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import java.util.Arrays;
import java.util.Map;

/**
 * A sample's series identity: its label set in canonical (name-sorted)
 * order with a precomputed hash. Computed once when a sample is mapped and
 * carried on {@link MappedSample}, so grouping a batch into series is a hash
 * lookup instead of a sorted copy of every sample's labels (#164).
 *
 * <p>The two arrays are allocated per sample and live as long as the sample
 * does, so a deep queue backlog carries them; the strings inside are shared
 * with the label map, not copied.
 */
public final class SeriesKey {

    private final String[] names;
    private final String[] values;
    private final int hash;

    private SeriesKey(String[] names, String[] values) {
        this.names = names;
        this.values = values;
        this.hash = 31 * Arrays.hashCode(names) + Arrays.hashCode(values);
    }

    public static SeriesKey of(Map<String, String> labels) {
        String[] names = labels.keySet().toArray(new String[0]);
        Arrays.sort(names);
        String[] values = new String[names.length];
        for (int i = 0; i < names.length; i++) values[i] = labels.get(names[i]);
        return new SeriesKey(names, values);
    }

    /** Sorted label names. Do not mutate. */
    public String[] names()  { return names; }
    /** Values aligned with {@link #names()}. Do not mutate. */
    public String[] values() { return values; }
    public int size()        { return names.length; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SeriesKey k)) return false;
        return hash == k.hash && Arrays.equals(names, k.names) && Arrays.equals(values, k.values);
    }

    @Override public int hashCode() { return hash; }

    @Override public String toString() { return "SeriesKey" + Arrays.toString(names) + Arrays.toString(values); }
}
