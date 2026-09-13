/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;

/**
 * The receiver's own count of what it wrote, as Remote Write 2.0 requires it
 * to report, carried out of the client (#187).
 */
class RemoteWriteHttpClientWrittenHeaderTest {

    private static final String HEADER = "X-Prometheus-Remote-Write-Samples-Written";

    private MockWebServer server;
    private RemoteWriteHttpClient http;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/write").toString());
        c.setReadUrl(server.url("/").toString());
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(1);
        c.validate();
        http = new RemoteWriteHttpClient(c);
    }

    @AfterEach
    void tearDown() throws IOException {
        http.shutdown();
        server.shutdown();
    }

    private WriteResult write(MockResponse response) {
        server.enqueue(response);
        return http.write(new byte[] {1, 2, 3});
    }

    @Test
    void a_2xx_carries_the_receivers_written_count() {
        WriteResult r = write(new MockResponse().setResponseCode(204).setHeader(HEADER, "7"));
        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.samplesWrittenReported()).isEqualTo(7);
    }

    @Test
    void a_2xx_without_the_header_reports_nothing() {
        WriteResult r = write(new MockResponse().setResponseCode(204));
        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.samplesWrittenReported()).isEqualTo(-1);
    }

    @Test
    void an_unparseable_header_reports_nothing() {
        assertThat(write(new MockResponse().setResponseCode(204).setHeader(HEADER, "seven"))
                .samplesWrittenReported()).isEqualTo(-1);
        assertThat(write(new MockResponse().setResponseCode(204).setHeader(HEADER, "-3"))
                .samplesWrittenReported()).isEqualTo(-1);
    }

    @Test
    void a_failure_reports_nothing() {
        WriteResult r4 = write(new MockResponse().setResponseCode(400).setHeader(HEADER, "0"));
        assertThat(r4.outcome()).isEqualTo(WriteOutcome.DROPPED_4XX);
        assertThat(r4.samplesWrittenReported()).isEqualTo(-1);
        WriteResult r5 = write(new MockResponse().setResponseCode(503).setHeader(HEADER, "0"));
        assertThat(r5.outcome()).isEqualTo(WriteOutcome.DROPPED_5XX_EXHAUSTED);
        assertThat(r5.samplesWrittenReported()).isEqualTo(-1);
    }
}
