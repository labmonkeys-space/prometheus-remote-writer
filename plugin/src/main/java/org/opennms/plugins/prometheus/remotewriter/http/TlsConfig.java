/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.OkHttpClient;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures TLS on an {@link OkHttpClient.Builder} from plugin config.
 * Three mutually exclusive modes:
 * <ol>
 *   <li>Default — JDK truststore only; no configuration applied.</li>
 *   <li>Custom CA bundle — {@code tls.ca-file} points at a PEM file; its
 *       certs become the only trust anchors.</li>
 *   <li>Insecure skip-verify — accepts any certificate with any hostname.
 *       Emits a WARN log on configure and every hour thereafter.</li>
 * </ol>
 */
public final class TlsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TlsConfig.class);
    private static final long INSECURE_WARN_INTERVAL_MS = 3_600_000L; // 1 hour

    /**
     * Hourly scheduler that re-emits the insecure-TLS WARN while at least one
     * client is still configured in insecure mode. One background thread per
     * JVM, started lazily, stopped when every client unregisters via
     * {@link #stopInsecureWarn()}.
     */
    private static final Object INSECURE_LOCK = new Object();
    private static int insecureActiveCount;
    private static ScheduledExecutorService insecureWarnExec;
    private static ScheduledFuture<?> insecureWarnFuture;

    private TlsConfig() {}

    public static void configure(OkHttpClient.Builder builder, PrometheusRemoteWriterConfig cfg) {
        if (cfg.isTlsInsecureSkipVerify()) {
            applyInsecureSkipVerify(builder);
            startInsecureWarn();
            return;
        }
        String caFile = cfg.getTlsCaFile();
        if (caFile != null && !caFile.isEmpty()) {
            applyCustomCa(builder, caFile);
        }
        // else: leave OkHttp defaults (JDK truststore)
    }

    // -- custom CA ----------------------------------------------------------

    private static void applyCustomCa(OkHttpClient.Builder builder, String caFile) {
        try {
            Path path = Path.of(caFile);
            if (!Files.exists(path)) {
                throw new IllegalStateException("tls.ca-file does not exist: " + caFile);
            }
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            try (InputStream in = Files.newInputStream(path)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                Collection<? extends Certificate> certs = cf.generateCertificates(in);
                int i = 0;
                for (Certificate c : certs) {
                    if (c instanceof X509Certificate) {
                        ks.setCertificateEntry("onms-ca-" + i++, c);
                    }
                }
                if (i == 0) {
                    throw new IllegalStateException(
                        "tls.ca-file contained no X.509 certificates: " + caFile);
                }
            }

            TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            TrustManager[] tms = tmf.getTrustManagers();
            X509TrustManager x509 = findX509(tms);
            if (x509 == null) {
                throw new IllegalStateException("No X509TrustManager from custom CA");
            }

            SSLContext sslCtx = SSLContext.getInstance("TLS");
            sslCtx.init(null, new TrustManager[] { x509 }, null);
            builder.sslSocketFactory(sslCtx.getSocketFactory(), x509);
            LOG.info("TLS configured with custom CA bundle: {}", caFile);
        } catch (IOException | CertificateException | NoSuchAlgorithmException
                 | java.security.KeyStoreException | java.security.KeyManagementException e) {
            throw new IllegalStateException(
                "failed to configure TLS from tls.ca-file=" + caFile + ": " + e.getMessage(), e);
        }
    }

    // -- insecure skip-verify ----------------------------------------------

    private static void applyInsecureSkipVerify(OkHttpClient.Builder builder) {
        logInsecureWarn();
        try {
            // CodeQL flags this as java/insecure-trustmanager (CWE-295) and that
            // is correct by the rule's letter — the TrustManager below accepts
            // any certificate. The behavior is the documented intent of this
            // opt-in mode (see the class javadoc); the alert is dismissed as
            // "won't fix" with that justification. See:
            // https://github.com/opennms-forge/prometheus-remote-writer/security/code-scanning/1
            X509TrustManager allTrusting = new X509TrustManager() {
                @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] { allTrusting }, null);
            builder.sslSocketFactory(ctx.getSocketFactory(), allTrusting);
            builder.hostnameVerifier((hostname, session) -> true);
        } catch (NoSuchAlgorithmException | java.security.KeyManagementException e) {
            throw new IllegalStateException("failed to install insecure TLS", e);
        }
    }

    private static void logInsecureWarn() {
        LOG.warn("tls.insecure-skip-verify is ENABLED — TLS cert and hostname "
               + "verification are disabled. This is unsafe in production.");
    }

    /**
     * Register one active insecure-TLS client. The first registration starts
     * a scheduled hourly WARN re-emitter; subsequent registrations bump a
     * refcount.
     */
    static void startInsecureWarn() {
        synchronized (INSECURE_LOCK) {
            insecureActiveCount++;
            if (insecureWarnExec == null) {
                insecureWarnExec = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "prometheus-remote-writer-insecure-tls-warn");
                    t.setDaemon(true);
                    return t;
                });
                insecureWarnFuture = insecureWarnExec.scheduleAtFixedRate(
                        TlsConfig::logInsecureWarn,
                        INSECURE_WARN_INTERVAL_MS, INSECURE_WARN_INTERVAL_MS,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Unregister one active insecure-TLS client. When the refcount drops to
     * zero the scheduler shuts down.
     */
    public static void stopInsecureWarn() {
        synchronized (INSECURE_LOCK) {
            if (insecureActiveCount > 0) insecureActiveCount--;
            if (insecureActiveCount == 0 && insecureWarnExec != null) {
                if (insecureWarnFuture != null) insecureWarnFuture.cancel(false);
                insecureWarnExec.shutdownNow();
                insecureWarnExec = null;
                insecureWarnFuture = null;
            }
        }
    }

    private static X509TrustManager findX509(TrustManager[] tms) {
        for (TrustManager tm : tms) {
            if (tm instanceof X509TrustManager x509) return x509;
        }
        return null;
    }
}
