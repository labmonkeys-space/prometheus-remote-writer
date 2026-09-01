/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.opennms.plugins.prometheus.remotewriter.config.HttpHeadersConfig;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous HTTP client for Prometheus Remote Write v1.
 *
 * <p>Each call to {@link #write(byte[])} POSTs the supplied snappy-compressed
 * payload to the configured write URL with the required RW v1 headers, any
 * configured auth/tenant headers, and 5xx retry with exponential backoff
 * (bounded by {@code retry.max-attempts}). 4xx responses are dropped
 * immediately with the response body captured at WARN.
 *
 * <p>The client is thread-safe; the flusher (task group 8) will invoke
 * {@code write()} from a single flush thread in v0.1, but the underlying
 * {@link OkHttpClient} supports concurrency out of the box.
 */
public final class RemoteWriteHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteHttpClient.class);

    /** v1 Content-Type — bare protobuf, no proto qualifier. */
    private static final MediaType CONTENT_TYPE_V1 = MediaType.get("application/x-protobuf");
    /** v2 Content-Type — qualified with the v2 message name. */
    private static final MediaType CONTENT_TYPE_V2 =
            MediaType.get("application/x-protobuf;proto=io.prometheus.write.v2.Request");
    private static final String REMOTE_WRITE_VERSION_V1 = "0.1.0";
    private static final String REMOTE_WRITE_VERSION_V2 = "2.0.0";
    private static final String USER_AGENT =
            "org.opennms.plugins.prometheus-remote-writer/" + pluginVersion();

    public enum WriteOutcome {
        /** HTTP 2xx. */
        SUCCESS,
        /** HTTP 4xx — batch dropped, no retry. */
        DROPPED_4XX,
        /** HTTP 5xx on every attempt including retries. */
        DROPPED_5XX_EXHAUSTED,
        /** IOException on every attempt including retries. */
        TRANSPORT_ERROR
    }

    public record WriteResult(WriteOutcome outcome, int httpStatus, int attemptsMade, String detail) {}

    private final OkHttpClient http;
    private final PrometheusRemoteWriterConfig config;
    private final HttpHeadersConfig httpHeadersConfig;

    private final AtomicLong writesSuccessful     = new AtomicLong();
    private final AtomicLong writes4xx            = new AtomicLong();
    private final AtomicLong writes5xxExhausted   = new AtomicLong();
    private final AtomicLong writesTransportError = new AtomicLong();
    private final AtomicLong bytesWritten         = new AtomicLong();

    /**
     * Test-friendly constructor — no operator-supplied custom headers.
     * Production code wires the two-arg form so headers defined under
     * {@code http.headers.*} reach the wire.
     */
    public RemoteWriteHttpClient(PrometheusRemoteWriterConfig config) {
        this(config, HttpHeadersConfig.empty());
    }

    public RemoteWriteHttpClient(PrometheusRemoteWriterConfig config,
                                 HttpHeadersConfig httpHeadersConfig) {
        this.config = config;
        this.httpHeadersConfig = Objects.requireNonNull(httpHeadersConfig, "httpHeadersConfig");
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(config.getHttpMaxConnections());
        dispatcher.setMaxRequestsPerHost(config.getHttpMaxConnections());

        OkHttpClient.Builder b = new OkHttpClient.Builder()
                .connectTimeout(config.getHttpConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getHttpReadTimeoutMs(),       TimeUnit.MILLISECONDS)
                .writeTimeout(config.getHttpWriteTimeoutMs(),     TimeUnit.MILLISECONDS)
                .connectionPool(new ConnectionPool(
                        config.getHttpMaxConnections(), 5, TimeUnit.MINUTES))
                .dispatcher(dispatcher);
        TlsConfig.configure(b, config);
        this.http = b.build();
    }

    /**
     * POST the snappy-compressed WriteRequest payload. Blocks the calling
     * thread across retries.
     *
     * @param snappyCompressedPayload bytes produced by {@code RemoteWriteRequestBuilder}
     */
    public WriteResult write(byte[] snappyCompressedPayload) {
        Request request = buildRequest(snappyCompressedPayload);
        int maxAttempts = Math.max(1, config.getRetryMaxAttempts());
        long backoff = Math.max(1, config.getRetryInitialBackoffMs());
        // validate() guarantees retry.max-backoff-ms >= retry.initial-backoff-ms.
        long maxBackoff = Math.max(backoff, config.getRetryMaxBackoffMs());

        // Track the *last attempt's* failure kind so the final classification
        // reflects what actually happened on the last try, not a stale earlier
        // error. Without this, a run like "5xx, 5xx, IOException" would report
        // DROPPED_5XX_EXHAUSTED with a stale body (the real last failure was
        // the transport error).
        enum LastKind { NONE, HTTP_5XX, TRANSPORT }
        LastKind lastKind = LastKind.NONE;
        String lastBody = "";
        int lastStatus = 0;
        String lastIoMessage = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (Response resp = http.newCall(request).execute()) {
                int code = resp.code();
                if (resp.isSuccessful()) {
                    writesSuccessful.incrementAndGet();
                    bytesWritten.addAndGet(snappyCompressedPayload.length);
                    return new WriteResult(WriteOutcome.SUCCESS, code, attempt, null);
                }
                if (code >= 400 && code < 500) {
                    String body = readBodyQuiet(resp);
                    LOG.warn("Prometheus remote-write rejected with {}: {}", code, body);
                    writes4xx.incrementAndGet();
                    return new WriteResult(WriteOutcome.DROPPED_4XX, code, attempt, body);
                }
                // 5xx (or anything outside 2xx/4xx): retry
                lastKind = LastKind.HTTP_5XX;
                lastStatus = code;
                lastBody = readBodyQuiet(resp);
                LOG.warn("Prometheus remote-write returned {} on attempt {}/{}: {}",
                         code, attempt, maxAttempts, lastBody);
            } catch (IOException io) {
                lastKind = LastKind.TRANSPORT;
                lastIoMessage = io.getMessage();
                LOG.warn("Prometheus remote-write transport failure on attempt {}/{}: {}",
                         attempt, maxAttempts, io.getMessage());
            }

            if (attempt < maxAttempts) {
                if (!sleepInterruptibly(backoff)) {
                    // interrupted: stop retrying
                    return transportError(attempt, "interrupted during backoff");
                }
                backoff = Math.min(backoff * 2, maxBackoff);
            }
        }

        if (lastKind == LastKind.TRANSPORT) {
            writesTransportError.incrementAndGet();
            return new WriteResult(WriteOutcome.TRANSPORT_ERROR, 0, maxAttempts, lastIoMessage);
        }
        writes5xxExhausted.incrementAndGet();
        return new WriteResult(WriteOutcome.DROPPED_5XX_EXHAUSTED, lastStatus, maxAttempts, lastBody);
    }

    public void shutdown() {
        java.util.concurrent.ExecutorService exec = http.dispatcher().executorService();
        exec.shutdown();
        try {
            // Give in-flight requests a bounded window to finish before
            // tearing down the connection pool. Without the wait, a
            // retry-backoff Thread.sleep in progress can survive past
            // bundle-stop and hold a reference to the OSGi classloader.
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
                exec.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
        http.connectionPool().evictAll();
        if (config.isTlsInsecureSkipVerify()) {
            TlsConfig.stopInsecureWarn();
        }
    }

    // -- counter getters (wired to Dropwizard in task 11.1) -----------------

    public long getWritesSuccessful()     { return writesSuccessful.get(); }
    public long getWrites4xx()            { return writes4xx.get(); }
    public long getWrites5xxExhausted()   { return writes5xxExhausted.get(); }
    public long getWritesTransportError() { return writesTransportError.get(); }
    public long getBytesWritten()         { return bytesWritten.get(); }

    /** In-flight HTTP request count — running plus queued at the dispatcher. */
    public int getInFlightCalls() {
        return http.dispatcher().runningCallsCount() + http.dispatcher().queuedCallsCount();
    }

    // -- internals ----------------------------------------------------------

    private Request buildRequest(byte[] body) {
        // Wire-version-aware Content-Type and X-Prometheus-Remote-Write-Version.
        // Body bytes are produced by whichever builder the storage layer
        // selected via RemoteWriteRequestBuilders.forVersion(...) — this
        // method only sets the headers that match.
        boolean v2 = config.getWireProtocolVersion() == 2;
        MediaType contentType = v2 ? CONTENT_TYPE_V2 : CONTENT_TYPE_V1;
        String rwVersion = v2 ? REMOTE_WRITE_VERSION_V2 : REMOTE_WRITE_VERSION_V1;

        Request.Builder b = new Request.Builder()
                .url(config.getWriteUrl())
                .addHeader("Content-Encoding", "snappy")
                .addHeader("X-Prometheus-Remote-Write-Version", rwVersion)
                .addHeader("User-Agent", USER_AGENT);

        if (config.hasBasicAuth()) {
            String creds = config.getBasicUsername() + ":" + config.getBasicPassword();
            String encoded = Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
            b.addHeader("Authorization", "Basic " + encoded);
        } else if (config.hasBearerAuth()) {
            b.addHeader("Authorization", "Bearer " + config.getBearerToken());
        }
        if (config.hasTenant()) {
            b.addHeader("X-Scope-OrgID", config.getTenantOrgId());
        }
        // Operator-supplied custom headers — applied AFTER managed headers.
        // HttpHeadersConfig validation has already excluded reserved names,
        // so no conflict can arise here. User-Agent is the one allowed
        // override; OkHttp's addHeader is additive and Request.Builder
        // semantics on a duplicate header overwrites the earlier value
        // (we use the .header() vs .addHeader() distinction explicitly
        // for User-Agent to ensure the operator value wins).
        for (Map.Entry<String, String> h : httpHeadersConfig.headers().entrySet()) {
            if ("User-Agent".equalsIgnoreCase(h.getKey())) {
                b.header(h.getKey(), h.getValue());
            } else {
                b.addHeader(h.getKey(), h.getValue());
            }
        }
        b.post(RequestBody.create(body, contentType));
        return b.build();
    }

    private static String readBodyQuiet(Response resp) {
        try (ResponseBody body = resp.body()) {
            if (body == null) return "";
            String s = body.string();
            // Capped so a misbehaving backend can't blow up the log.
            return s.length() > 512 ? s.substring(0, 512) + "…(truncated)" : s;
        } catch (IOException io) {
            return "(body read error: " + io.getMessage() + ")";
        }
    }

    private static boolean sleepInterruptibly(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private WriteResult transportError(int attempt, String detail) {
        writesTransportError.incrementAndGet();
        return new WriteResult(WriteOutcome.TRANSPORT_ERROR, 0, attempt, detail);
    }

    private static String pluginVersion() {
        // Package attribute set by maven-bundle-plugin's manifest; fall back to
        // a compile-time constant so unit tests don't depend on the manifest.
        Package p = RemoteWriteHttpClient.class.getPackage();
        String v = p != null ? p.getImplementationVersion() : null;
        return v != null ? v : "0.1.0-dev";
    }
}
