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

        List<MappedSample> batch = q.pollBatch(3, 10, TimeUnit.MILLISECONDS, 0L).samples();

        assertThat(batch).hasSize(3);
        assertThat(q.depth()).isEqualTo(2);
        assertThat(q.getSamplesDequeued()).isEqualTo(3);
    }

    @Test
    void poll_batch_returns_empty_on_timeout_with_empty_queue() throws Exception {
        SampleQueue q = new SampleQueue(10);

        List<MappedSample> batch = q.pollBatch(5, 10, TimeUnit.MILLISECONDS, 0L).samples();

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

    // ---------- #162: linger ----------------------------------------------

    @Test
    void linger_zero_returns_as_soon_as_a_head_arrives() throws Exception {
        SampleQueue q = new SampleQueue(100);
        q.tryEnqueue(sample(1));
        long t0 = System.nanoTime();
        SampleQueue.Batch b = q.pollBatch(100, 1000, TimeUnit.MILLISECONDS, 0);
        assertThat(b.samples()).hasSize(1);
        assertThat(b.lingerNanos()).isZero();
        assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(200);
    }

    @Test
    void linger_coalesces_a_trickle_into_one_batch() throws Exception {
        SampleQueue q = new SampleQueue(100);
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                q.tryEnqueue(sample(i));
                try { Thread.sleep(10); } catch (InterruptedException e) { return; }
            }
        });
        producer.start();
        SampleQueue.Batch b = q.pollBatch(100, 1000, TimeUnit.MILLISECONDS, 300);
        producer.join();
        assertThat(b.samples()).hasSize(10);
        assertThat(b.lingerNanos() / 1_000_000).isBetween(200L, 400L);
    }

    @Test
    void linger_does_not_wait_when_the_batch_is_already_full() throws Exception {
        SampleQueue q = new SampleQueue(100);
        for (int i = 0; i < 5; i++) q.tryEnqueue(sample(i));
        long t0 = System.nanoTime();
        SampleQueue.Batch b = q.pollBatch(5, 1000, TimeUnit.MILLISECONDS, 500);
        assertThat(b.samples()).hasSize(5);
        assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(100);
        assertThat(b.lingerNanos()).isZero();
    }

    @Test
    void a_burst_that_fills_the_batch_ends_the_linger_early() throws Exception {
        SampleQueue q = new SampleQueue(200);
        q.tryEnqueue(sample(0));
        Thread producer = new Thread(() -> {
            try { Thread.sleep(20); } catch (InterruptedException e) { return; }
            for (int i = 1; i < 100; i++) q.tryEnqueue(sample(i));
        });
        producer.start();
        long t0 = System.nanoTime();
        SampleQueue.Batch b = q.pollBatch(100, 1000, TimeUnit.MILLISECONDS, 500);
        producer.join();
        assertThat(b.samples()).hasSize(100);
        assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(300);
    }

    @Test
    void interrupt_during_linger_returns_the_samples_already_taken() throws Exception {
        SampleQueue q = new SampleQueue(100);
        q.tryEnqueue(sample(1));
        q.tryEnqueue(sample(2));
        Thread me = Thread.currentThread();
        Thread interrupter = new Thread(() -> { try { Thread.sleep(50); } catch (InterruptedException e) { return; } me.interrupt(); });
        interrupter.start();
        SampleQueue.Batch b = q.pollBatch(100, 1000, TimeUnit.MILLISECONDS, 2000);
        // Read and clear the flag before join(): join() itself throws on a
        // set flag while the helper is still alive.
        boolean flagWasSet = Thread.interrupted();
        interrupter.join();
        assertThat(b.samples()).hasSize(2);
        assertThat(flagWasSet).as("interrupt flag left set for the caller").isTrue();
        assertThat(q.getSamplesDequeued()).isEqualTo(2);
    }

    private static MappedSample sample(int i) {
        return new MappedSample(Map.of("__name__", "t", "i", Integer.toString(i)), i, (double) i);
    }
}
