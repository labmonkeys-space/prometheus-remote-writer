/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;

/**
 * The flusher's count of payloads between builder and sender (#177).
 *
 * <p>A disk read waits for that count to reach zero, because the bucket's
 * reader is coupled to send outcomes. So the count has to be exact. One that
 * sticks above zero parks the disk drain forever, and the shard stops writing
 * until a restart builds a fresh flusher. That is what v0.8.0 did when the
 * sender settled a payload before the builder had counted it.
 *
 * <p>The interleaving is forced through test hooks rather than hoped for
 * under load, so these fail on the old ordering every time.
 */
class FlusherPipelineAccountingTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    private MockWebServer server;
    private RemoteWriteHttpClient http;
    private SampleQueue queue;
    private OverflowBucket bucket;
    private Flusher flusher;
    private PluginMetrics metrics;
    /** Released at teardown so a stubbed write never outlives its test. */
    private final CountDownLatch release = new CountDownLatch(1);

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.setDispatcher(new Dispatcher() {
            @Override public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(204);
            }
        });
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        c.setOverflowMaxSizeBytes(0);
        c.validate();
        http    = new RemoteWriteHttpClient(c);
        queue   = new SampleQueue(100);
        metrics = new PluginMetrics();
    }

    @AfterEach
    void tearDown() throws IOException {
        release.countDown();
        if (flusher != null) flusher.stop(1_000);
        if (bucket != null) bucket.close();
        http.shutdown();
        server.shutdown();
    }

    private void openBucket(Path dir) throws IOException {
        bucket = OverflowBucket.open(dir, 1L << 20, 64 * 1024,
                OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH, MAX_PAYLOAD, "shard-0");
    }

    private Flusher flusher(RemoteWriteHttpClient client, int batchSize) {
        return new Flusher(queue, bucket, null, client, batchSize, 50, 0, metrics,
                RemoteWriteRequestBuilders.forVersion(1), "test-flusher");
    }

    /** A client whose writes block until {@link #release}, ignoring interrupts
     *  the way a socket read does, and then succeed. */
    private RemoteWriteHttpClient stuckClient(CountDownLatch firstWrite) {
        RemoteWriteHttpClient stuck = mock(RemoteWriteHttpClient.class);
        when(stuck.write(any(byte[].class))).thenAnswer(inv -> {
            firstWrite.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
            return new WriteResult(WriteOutcome.SUCCESS, 204, 1, "");
        });
        return stuck;
    }

    private static MappedSample sample(long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut");
        return new MappedSample(labels, ts, (double) ts);
    }

    private long counter(String name) {
        return metrics.snapshot().get(name).longValue();
    }

    @Test
    void a_payload_settled_before_it_was_counted_leaves_nothing_outstanding(@TempDir Path dir)
            throws Exception {
        openBucket(dir);
        flusher = flusher(http, 10);
        // Stall the builder right after each handoff, as a preempted thread
        // would be, until the sender has sent and settled that payload.
        Semaphore settled = new Semaphore(0);
        flusher.afterSettleForTesting = settled::release;
        flusher.afterHandOffForTesting = () -> {
            try {
                settled.tryAcquire(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        flusher.start();

        queue.tryEnqueue(sample(1));
        await().atMost(Duration.ofSeconds(5)).until(() -> counter(PluginMetrics.SAMPLES_WRITTEN) == 1);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(flusher.outstandingPayloadsForTesting())
                        .as("payloads counted as outstanding by an idle flusher")
                        .isZero());

        // A backlog that reaches the bucket now, as a live spill's would, has
        // to drain through this same flusher. Well inside the 30 s gate.
        for (long t = 2; t <= 21; t++) bucket.append(sample(t));
        await().atMost(Duration.ofSeconds(5)).until(bucket::isEmpty);
        assertThat(counter(PluginMetrics.SAMPLES_DRAINED_FROM_OVERFLOW)).isEqualTo(20);
    }

    @Test
    void a_handoff_interrupted_at_forced_shutdown_is_not_left_counted() throws Exception {
        CountDownLatch firstWrite = new CountDownLatch(1);
        flusher = flusher(stuckClient(firstWrite), 1);
        Semaphore settled = new Semaphore(0);
        flusher.afterSettleForTesting = settled::release;
        flusher.start();

        // Batch 1 goes to the sender and blocks there, batch 2 fills the
        // handoff, and the builder blocks putting batch 3.
        for (long t = 1; t <= 3; t++) queue.tryEnqueue(sample(t));
        assertThat(firstWrite.await(5, TimeUnit.SECONDS)).as("sender never wrote").isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> queue.depth() == 0);
        Thread.sleep(200);   // let the builder reach the blocking put

        // Grace expires: the builder is interrupted in its put, and batch 2 is
        // an orphan in the handoff. Neither can be sent now.
        flusher.stop(100);
        release.countDown();
        assertThat(settled.tryAcquire(5, TimeUnit.SECONDS)).as("batch 1 never settled").isTrue();

        assertThat(counter(PluginMetrics.SAMPLES_DROPPED_SHUTDOWN)).isEqualTo(2);
        assertThat(flusher.outstandingPayloadsForTesting())
                .as("payloads still counted after the pipeline emptied")
                .isZero();
    }

    @Test
    void a_disk_read_that_gives_up_waiting_for_the_sender_is_recorded(@TempDir Path dir)
            throws Exception {
        openBucket(dir);
        CountDownLatch firstWrite = new CountDownLatch(1);
        flusher = flusher(stuckClient(firstWrite), 10);
        flusher.senderIdleBoundMs = 200;
        flusher.start();

        // A memory batch holds the sender, then a backlog lands on disk. The
        // disk read has to wait for the sender and gives up at the bound.
        queue.tryEnqueue(sample(1));
        assertThat(firstWrite.await(5, TimeUnit.SECONDS)).as("sender never wrote").isTrue();
        bucket.append(sample(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> flusher.senderIdleTimeouts.get() >= 1);

        // Once the sender comes back the drain proceeds.
        release.countDown();
        await().atMost(Duration.ofSeconds(5)).until(bucket::isEmpty);
    }

    @Test
    void a_healthy_drain_records_no_timeout(@TempDir Path dir) throws Exception {
        openBucket(dir);
        for (long t = 1; t <= 50; t++) bucket.append(sample(t));
        flusher = flusher(http, 10);
        flusher.senderIdleBoundMs = 1_000;
        flusher.start();

        await().atMost(Duration.ofSeconds(5)).until(bucket::isEmpty);
        assertThat(flusher.senderIdleTimeouts.get()).isZero();
    }
}
