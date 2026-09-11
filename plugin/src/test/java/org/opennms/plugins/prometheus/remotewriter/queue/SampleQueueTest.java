/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class SampleQueueTest {

    @Test
    void rejects_invalid_capacity() {
        assertThatThrownBy(() -> new SampleQueue(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void try_enqueue_under_capacity_succeeds_and_depth_reflects_it() {
        SampleQueue q = new SampleQueue(3);
        q.tryEnqueue(sample(1));
        q.tryEnqueue(sample(2));

        assertThat(q.depth()).isEqualTo(2);
        assertThat(q.getSamplesEnqueued()).isEqualTo(2);
    }

    @Test
    void try_enqueue_on_full_queue_returns_false_without_throwing() {
        SampleQueue q = new SampleQueue(2);
        assertThat(q.tryEnqueue(sample(1))).isTrue();
        assertThat(q.tryEnqueue(sample(2))).isTrue();

        assertThat(q.tryEnqueue(sample(3))).isFalse();
        assertThat(q.depth()).isEqualTo(2);
        assertThat(q.getSamplesEnqueued()).isEqualTo(2);
    }

    @Test
    void poll_batch_drains_up_to_max_batch() throws Exception {
        SampleQueue q = new SampleQueue(10);
        for (int i = 0; i < 5; i++) q.tryEnqueue(sample(i));

        List<MappedSample> batch = q.pollBatch(3, 10, TimeUnit.MILLISECONDS);

        assertThat(batch).hasSize(3);
        assertThat(q.depth()).isEqualTo(2);
        assertThat(q.getSamplesDequeued()).isEqualTo(3);
    }

    @Test
    void poll_batch_returns_empty_on_timeout_with_empty_queue() throws Exception {
        SampleQueue q = new SampleQueue(10);

        List<MappedSample> batch = q.pollBatch(5, 10, TimeUnit.MILLISECONDS);

        assertThat(batch).isEmpty();
        assertThat(q.getSamplesDequeued()).isZero();
    }

    @Test
    void try_enqueue_null_sample_throws_npe() {
        SampleQueue q = new SampleQueue(3);
        assertThatThrownBy(() -> q.tryEnqueue(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void drain_empties_the_queue_up_to_max() throws Exception {
        SampleQueue q = new SampleQueue(10);
        for (int i = 0; i < 5; i++) q.tryEnqueue(sample(i));

        List<MappedSample> batch = q.drain(10);

        assertThat(batch).hasSize(5);
        assertThat(q.depth()).isZero();
    }

    private static MappedSample sample(int i) {
        return new MappedSample(Map.of("__name__", "t", "i", Integer.toString(i)), i, (double) i);
    }
}
