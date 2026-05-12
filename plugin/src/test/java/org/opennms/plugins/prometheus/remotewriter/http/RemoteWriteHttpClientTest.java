/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.config.HttpHeadersConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;

class RemoteWriteHttpClientTest {

    private static final byte[] PAYLOAD = new byte[] { 1, 2, 3, 4 };

    private MockWebServer server;
    private RemoteWriteHttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) client.shutdown();
        server.shutdown();
    }

    // ---------- success + headers ------------------------------------------

    @Test
    void success_emits_remote_write_v1_headers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(cfg(server));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.attemptsMade()).isEqualTo(1);
        assertThat(client.getWritesSuccessful()).isEqualTo(1);
        assertThat(client.getBytesWritten()).isEqualTo(PAYLOAD.length);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getMethod()).isEqualTo("POST");
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("0.1.0");
        assertThat(req.getHeader("User-Agent")).startsWith("org.opennms.plugins.prometheus-remote-writer/");
        assertThat(req.getBody().readByteArray()).isEqualTo(PAYLOAD);
    }

    @Test
    void v2_headers_use_proto_qualifier_and_version_2_0_0() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setWireProtocolVersion("2");
        client = newClient(c);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        // Content-Type carries the proto= qualifier per the v2 spec —
        // OkHttp may add a charset suffix; assert prefix.
        assertThat(req.getHeader("Content-Type"))
                .startsWith("application/x-protobuf;proto=io.prometheus.write.v2.Request");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("2.0.0");
    }

    // ---------- auth permutations ------------------------------------------

    @Test
    void basic_auth_header_is_attached() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBasicUsername("alice");
        c.setBasicPassword("s3cret");
        client = newClient(c);

        client.write(PAYLOAD);

        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("alice:s3cret".getBytes(StandardCharsets.UTF_8));
        assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo(expected);
    }

    @Test
    void bearer_auth_header_is_attached() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBearerToken("tok-abc");
        client = newClient(c);

        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo("Bearer tok-abc");
    }

    @Test
    void no_auth_header_when_none_configured() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(cfg(server));
        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("Authorization")).isNull();
    }

    @Test
    void tenant_header_is_attached_when_configured() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setTenantOrgId("team-a");
        client = newClient(c);

        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("X-Scope-OrgID")).isEqualTo("team-a");
    }

    // ---------- 4xx drop ---------------------------------------------------

    @Test
    void four_xx_drops_and_does_not_retry() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(400).setBody("bad labels"));
        client = newClient(cfg(server));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.DROPPED_4XX);
        assertThat(r.httpStatus()).isEqualTo(400);
        assertThat(r.attemptsMade()).isEqualTo(1);
        assertThat(r.detail()).contains("bad labels");
        assertThat(client.getWrites4xx()).isEqualTo(1);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    // ---------- 5xx retry --------------------------------------------------

    @Test
    void five_xx_then_success_retries_until_accepted() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(fastRetry(cfg(server)));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.attemptsMade()).isEqualTo(3);
        assertThat(server.getRequestCount()).isEqualTo(3);
        assertThat(client.getWritesSuccessful()).isEqualTo(1);
    }

    @Test
    void five_xx_exhausted_reports_dropped() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        PrometheusRemoteWriterConfig c = fastRetry(cfg(server));
        c.setRetryMaxAttempts(3);
        client = newClient(c);

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.DROPPED_5XX_EXHAUSTED);
        assertThat(r.httpStatus()).isEqualTo(500);
        assertThat(r.attemptsMade()).isEqualTo(3);
        assertThat(client.getWrites5xxExhausted()).isEqualTo(1);
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    // ---------- custom http.headers.* --------------------------------------

    @Test
    void custom_headers_reach_the_wire_alongside_managed_headers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        HttpHeadersConfig h = headers(Map.of(
            "http.headers.cf-access-client-id",     "abc123",
            "http.headers.cf-access-client-secret", "def456"));
        client = newClient(cfg(server), h);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("cf-access-client-id")).isEqualTo("abc123");
        assertThat(req.getHeader("cf-access-client-secret")).isEqualTo("def456");
        // Managed headers still present.
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("0.1.0");
    }

    @Test
    void custom_headers_coexist_with_basic_auth() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBasicUsername("alice");
        c.setBasicPassword("s3cret");
        HttpHeadersConfig h = headers(Map.of(
            "http.headers.x-custom-tenant", "team-alpha"));
        client = newClient(c, h);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        // Custom header attached
        assertThat(req.getHeader("x-custom-tenant")).isEqualTo("team-alpha");
        // Basic auth header still wins on Authorization (not overridden)
        String expected = "Basic " + Base64.getEncoder()
            .encodeToString("alice:s3cret".getBytes(StandardCharsets.UTF_8));
        assertThat(req.getHeader("Authorization")).isEqualTo(expected);
    }

    @Test
    void custom_headers_coexist_with_bearer_auth() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBearerToken("tok-abc");
        HttpHeadersConfig h = headers(Map.of(
            "http.headers.cf-access-client-id", "abc123"));
        client = newClient(c, h);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Authorization")).isEqualTo("Bearer tok-abc");
        assertThat(req.getHeader("cf-access-client-id")).isEqualTo("abc123");
    }

    @Test
    void custom_headers_coexist_with_tenant_org_id() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setTenantOrgId("team-a");
        HttpHeadersConfig h = headers(Map.of(
            "http.headers.cf-access-client-id", "abc123"));
        client = newClient(c, h);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("X-Scope-OrgID")).isEqualTo("team-a");
        assertThat(req.getHeader("cf-access-client-id")).isEqualTo("abc123");
    }

    @Test
    void empty_http_headers_preserves_v0_4_x_wire_shape() throws Exception {
        // No custom headers configured — assert the request shape is
        // identical to the baseline `success_emits_remote_write_v1_headers`
        // test, with no extra application-level headers leaking through.
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(cfg(server), HttpHeadersConfig.empty());

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("0.1.0");
        assertThat(req.getHeader("User-Agent"))
            .startsWith("org.opennms.plugins.prometheus-remote-writer/");
        assertThat(req.getHeader("Authorization")).isNull();
        assertThat(req.getHeader("X-Scope-OrgID")).isNull();
        // Negative assertion — guards against a future regression where
        // a header is added unconditionally to the write path. Anything
        // beyond the documented baseline above is a failure here.
        java.util.Set<String> seen = new java.util.HashSet<>(req.getHeaders().names());
        // Strip protocol/transport-managed headers that OkHttp adds itself.
        seen.removeAll(java.util.Set.of(
            "Content-Type", "Content-Encoding",
            "X-Prometheus-Remote-Write-Version", "User-Agent",
            "Content-Length", "Host", "Connection",
            "Accept-Encoding"));  // OkHttp's transparent gzip support adds this
        assertThat(seen)
            .as("only the documented baseline + transport-managed headers "
              + "should reach the wire when http.headers.* is empty")
            .isEmpty();
    }

    @Test
    void empty_http_headers_preserves_v2_wire_shape() throws Exception {
        // v2 baseline mirror of empty_http_headers_preserves_v0_4_x_wire_shape:
        // protocol-version header changes shape; everything else stays the
        // same; no unexpected application-level headers.
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setWireProtocolVersion("2");
        client = newClient(c, HttpHeadersConfig.empty());

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Content-Type"))
            .startsWith("application/x-protobuf;proto=io.prometheus.write.v2.Request");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("2.0.0");
        assertThat(req.getHeader("Authorization")).isNull();
        assertThat(req.getHeader("X-Scope-OrgID")).isNull();
        java.util.Set<String> seen = new java.util.HashSet<>(req.getHeaders().names());
        seen.removeAll(java.util.Set.of(
            "Content-Type", "Content-Encoding",
            "X-Prometheus-Remote-Write-Version", "User-Agent",
            "Content-Length", "Host", "Connection",
            "Accept-Encoding"));  // OkHttp's transparent gzip support adds this
        assertThat(seen).isEmpty();
    }

    @Test
    void user_agent_override_replaces_plugin_default() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        HttpHeadersConfig h = headers(Map.of(
            "http.headers.User-Agent", "onms-edge-site-7/1.0"));
        client = newClient(cfg(server), h);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        // Operator-supplied value wins; the default plugin User-Agent is gone.
        assertThat(req.getHeader("User-Agent")).isEqualTo("onms-edge-site-7/1.0");
    }

    // ---------- helpers ----------------------------------------------------

    private static PrometheusRemoteWriterConfig cfg(MockWebServer server) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        return c;
    }

    /** Drop retry backoff to sub-millisecond so tests run fast. */
    private static PrometheusRemoteWriterConfig fastRetry(PrometheusRemoteWriterConfig c) {
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(5);
        return c;
    }

    private static RemoteWriteHttpClient newClient(PrometheusRemoteWriterConfig c) {
        c.validate();
        return new RemoteWriteHttpClient(c);
    }

    private static RemoteWriteHttpClient newClient(PrometheusRemoteWriterConfig c,
                                                   HttpHeadersConfig h) {
        c.validate();
        return new RemoteWriteHttpClient(c, h);
    }

    private static HttpHeadersConfig headers(Map<String, String> props) {
        HttpHeadersConfig h = new HttpHeadersConfig();
        // Note: HttpHeadersConfig.updated() accepts Map<String, ?>; pass
        // through directly so we exercise the prefix-scan path.
        h.updated(java.util.Collections.unmodifiableMap(props));
        return h;
    }
}
