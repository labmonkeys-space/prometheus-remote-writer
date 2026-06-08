/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Name sanitization and metric-name assembly.
//!
//! One uniform rule is applied to every name segment: replace any character
//! outside `[A-Za-z0-9_]` with `_`, collapse runs of `_`, and trim leading and
//! trailing `_`. Casing is preserved so MIB-derived camelCase aliases stay
//! recognizable (`ifHCInOctets` is kept verbatim, never `if_hc_in_octets`).

/// Sanitize a single name segment. Preserves case; never converts camelCase.
pub fn sanitize(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_underscore = false;
    for c in input.chars() {
        let mapped = if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '_'
        };
        if mapped == '_' {
            if prev_underscore {
                continue;
            }
            prev_underscore = true;
        } else {
            prev_underscore = false;
        }
        out.push(mapped);
    }
    out.trim_matches('_').to_string()
}

/// Assemble a Prometheus metric name as `<namespace>_<group>_<name>`.
///
/// Empty segments are omitted (an empty group yields `<namespace>_<name>`).
/// Returns `None` when the leaf attribute name sanitizes to empty, since a
/// metric cannot be named.
pub fn metric_name(namespace: &str, group: &str, name: &str) -> Option<String> {
    let leaf = sanitize(name);
    if leaf.is_empty() {
        return None;
    }

    let mut parts: Vec<String> = Vec::with_capacity(3);
    let ns = sanitize(namespace);
    if !ns.is_empty() {
        parts.push(ns);
    }
    let group = sanitize(group);
    if !group.is_empty() {
        parts.push(group);
    }
    parts.push(leaf);

    let mut joined = parts.join("_");
    // A metric name must start with [A-Za-z_]; prefix `_` if it would start
    // with a digit (only possible when there is no namespace).
    if joined.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        joined.insert(0, '_');
    }
    Some(joined)
}

/// Sanitize a label name. Like [`sanitize`] but guarantees the result is a
/// valid Prometheus label name (does not start with a digit). Returns an empty
/// string when nothing valid remains.
pub fn label_name(input: &str) -> String {
    let s = sanitize(input);
    if s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("_{s}")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_camel_case_leaf() {
        assert_eq!(
            metric_name("onms", "mib2-tcp", "tcpCurrEstab").unwrap(),
            "onms_mib2_tcp_tcpCurrEstab"
        );
        assert_eq!(
            metric_name("onms", "mib2-interfaces", "ifHCInOctets").unwrap(),
            "onms_mib2_interfaces_ifHCInOctets"
        );
    }

    #[test]
    fn empty_group_omits_segment() {
        assert_eq!(
            metric_name("onms", "", "responseTime").unwrap(),
            "onms_responseTime"
        );
    }

    #[test]
    fn configurable_namespace() {
        assert_eq!(metric_name("acme", "g", "m").unwrap(), "acme_g_m");
        assert_eq!(metric_name("", "g", "m").unwrap(), "g_m");
    }

    #[test]
    fn cross_group_attribute_names_stay_distinct() {
        let a = metric_name("onms", "cisco-cpu", "usagePercent").unwrap();
        let b = metric_name("onms", "cisco-mem", "usagePercent").unwrap();
        assert_eq!(a, "onms_cisco_cpu_usagePercent");
        assert_eq!(b, "onms_cisco_mem_usagePercent");
        assert_ne!(a, b);
    }

    #[test]
    fn collapses_and_trims_underscores() {
        assert_eq!(sanitize("mib2--tcp"), "mib2_tcp");
        assert_eq!(sanitize("_leading.trailing_"), "leading_trailing");
        assert_eq!(sanitize("a..b__c"), "a_b_c");
    }

    #[test]
    fn empty_leaf_yields_none() {
        assert_eq!(metric_name("onms", "g", "..."), None);
    }

    #[test]
    fn label_name_does_not_start_with_digit() {
        assert_eq!(label_name("3com"), "_3com");
        assert_eq!(label_name("ifName"), "ifName");
    }

    #[test]
    fn digit_leading_metric_name_without_namespace_is_prefixed() {
        assert_eq!(metric_name("", "", "1minLoad").unwrap(), "_1minLoad");
    }
}
