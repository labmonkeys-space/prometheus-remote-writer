/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

/**
 * The Prometheus images the integration tests run against, in one place.
 * Both are deliberate references rather than "latest": bumping one is a
 * decision about what the suite proves, so Dependabot does not manage them.
 * The docs quote them through the {@code it-prometheus-v1} and
 * {@code it-prometheus-v2} attributes, and {@code make verify-versions}
 * fails when the two disagree.
 */
public final class PrometheusImages {

    /** The last 2.x line: the Remote Write v1 reference. */
    public static final String V1_REFERENCE = "prom/prometheus:v2.53.2";

    /** The first release with a stable Remote Write 2.0 receiver: the v2 reference. */
    public static final String V2_REFERENCE = "prom/prometheus:v3.0.1";

    private PrometheusImages() {}
}
