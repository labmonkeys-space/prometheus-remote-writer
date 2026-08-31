/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.shell;

import java.io.PrintStream;
import java.util.Map;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

/**
 * {@code opennms:prometheus-writer-stats} — print the plugin's Dropwizard
 * counters and gauges. Output is a stable name/value table; no ANSI colours,
 * sortable, grep-friendly.
 *
 * <p>Registration is the standard Karaf 4 extender pipeline: the bundle's
 * {@code Karaf-Commands} manifest header (see plugin/pom.xml) makes the
 * CommandExtender scan this package, and {@link Service} makes it register
 * this class — both are required; either alone registers nothing (#113).
 *
 * <p>{@link Reference} injects the storage by its CONCRETE type — the bean
 * is registered under both the OIA {@code TimeSeriesStorage} interface and
 * this class (see blueprint.xml), which resolves uniquely even when other
 * TSS plugins are installed and gives access to {@code getMetrics()}.
 * {@code optional = true} is load-bearing: a mandatory reference would
 * delay-activate the command into "Command not found" whenever the service
 * is momentarily unregistered (config reload), reproducing the very
 * ambiguity this registration fix removes; instead the command always
 * exists and reports the inactive state explicitly.
 */
@Command(scope = "opennms", name = "prometheus-writer-stats",
         description = "Print prometheus-remote-writer plugin metrics")
@Service
public class StatsCommand implements Action {

    @Reference(optional = true)
    private PrometheusRemoteWriterStorage storage;

    public StatsCommand() {}

    /** Constructor used by tests. */
    public StatsCommand(PrometheusRemoteWriterStorage storage) {
        this.storage = storage;
    }

    public void setStorage(PrometheusRemoteWriterStorage storage) {
        this.storage = storage;
    }

    @Override
    public Object execute() {
        render(System.out);
        return null;
    }

    /** Package-private for tests — renders the stats table to the given stream. */
    void render(PrintStream out) {
        if (storage == null) {
            out.println("prometheus-remote-writer is not active");
            return;
        }
        PluginMetrics metrics = storage.getMetrics();
        if (metrics == null) {
            out.println("prometheus-remote-writer has not been started");
            return;
        }

        Map<String, Number> snapshot = metrics.snapshot();
        int maxName = snapshot.keySet().stream().mapToInt(String::length).max().orElse(20);
        String fmt = "  %-" + maxName + "s  %s%n";

        out.println("prometheus-remote-writer metrics");
        out.println("================================");
        for (Map.Entry<String, Number> e : snapshot.entrySet()) {
            out.printf(fmt, e.getKey(), e.getValue());
        }
    }
}
