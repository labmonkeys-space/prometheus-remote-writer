/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Maps an OpenNMS `CollectionSet` into Prometheus samples.
//!
//! Each `NumericAttribute` becomes one [`MappedSample`]. The metric name is
//! `<namespace>_<group>_<name>` (group is a name prefix, never a label) and the
//! leaf attribute name is preserved verbatim. Labels are derived from the
//! resource identity and the resource's `StringAttribute`s; the OpenNMS
//! resource instance is emitted as `resource_instance`, never the reserved
//! Prometheus label `instance`.

use crate::proto::{
    collection_set_resource::Resource, numeric_attribute, CollectionSet, CollectionSetResource,
    NodeLevelResource,
};
use crate::sanitize::{label_name, metric_name};

/// Label names the gateway derives from resource identity. A `StringAttribute`
/// may not override these — notably `__name__` (the metric name) and the
/// reserved Prometheus `instance` (which the gateway never emits).
const RESERVED_LABELS: &[&str] = &[
    "__name__",
    "instance",
    "node_id",
    "foreign_source",
    "foreign_id",
    "node_label",
    "location",
    "if_index",
    "resource_instance",
    "resource_type",
];

/// A single Prometheus sample, pre-wire and wire-version agnostic.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedSample {
    /// Label set including the reserved `__name__`. Not sorted.
    pub labels: Vec<(String, String)>,
    /// Epoch-millisecond timestamp (from the enclosing `CollectionSet`).
    pub timestamp_ms: i64,
    /// Sample value.
    pub value: f64,
    /// Whether the source attribute was typed COUNTER (carried as v2 metadata).
    pub is_counter: bool,
}

/// Maps `CollectionSet`s using a configurable metric-name namespace.
pub struct Mapper {
    namespace: String,
}

impl Mapper {
    /// Create a mapper with the given metric-name namespace (e.g. `"onms"`).
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }

    /// Flatten a `CollectionSet` into samples.
    pub fn map(&self, cs: &CollectionSet) -> Vec<MappedSample> {
        let timestamp_ms = cs.timestamp;
        let mut out = Vec::new();

        for resource in &cs.resource {
            let base = resource_labels(resource);

            for na in &resource.numeric {
                let Some(name) = metric_name(&self.namespace, &na.group, &na.name) else {
                    continue; // un-nameable attribute
                };

                // `metric_value` (the DoubleValue wrapper, mapped to Option<f64>)
                // is always serialized (even for zero) and is authoritative when
                // present; fall back to the bare `value`.
                let value = na.metric_value.unwrap_or(na.value);
                let is_counter = na.r#type() == numeric_attribute::Type::Counter;

                let mut labels = base.clone();
                labels.push(("__name__".to_string(), name));
                dedupe_by_name(&mut labels);

                out.push(MappedSample {
                    labels,
                    timestamp_ms,
                    value,
                    is_counter,
                });
            }
        }

        out
    }
}

/// Derive the label set shared by every sample of one resource.
fn resource_labels(resource: &CollectionSetResource) -> Vec<(String, String)> {
    let mut labels: Vec<(String, String)> = Vec::new();

    match &resource.resource {
        Some(Resource::Node(node)) => push_node(&mut labels, node),
        Some(Resource::Interface(iface)) => {
            if let Some(node) = &iface.node {
                push_node(&mut labels, node);
            }
            push_nonempty(&mut labels, "resource_instance", &iface.instance);
            if iface.if_index != 0 {
                labels.push(("if_index".to_string(), iface.if_index.to_string()));
            }
        }
        Some(Resource::Generic(generic)) => {
            if let Some(node) = &generic.node {
                push_node(&mut labels, node);
            }
            push_nonempty(&mut labels, "resource_type", &generic.r#type);
            push_nonempty(&mut labels, "resource_instance", &generic.instance);
        }
        Some(Resource::Response(response)) => {
            if let Some(node) = &response.node {
                push_node(&mut labels, node);
            }
            push_nonempty(&mut labels, "resource_instance", &response.instance);
            push_nonempty(&mut labels, "location", &response.location);
        }
        None => {}
    }

    // StringAttributes become labels (sanitized name, verbatim value). Skip
    // empty names/values (an empty-valued label changes series identity) and
    // reserved names so an attribute can never override a derived label or
    // smuggle in a reserved `instance`/`__name__`.
    for sa in &resource.string {
        let name = label_name(&sa.name);
        if name.is_empty() || sa.value.is_empty() || RESERVED_LABELS.contains(&name.as_str()) {
            continue;
        }
        labels.push((name, sa.value.clone()));
    }

    labels
}

fn push_node(labels: &mut Vec<(String, String)>, node: &NodeLevelResource) {
    if node.node_id != 0 {
        labels.push(("node_id".to_string(), node.node_id.to_string()));
    }
    push_nonempty(labels, "foreign_source", &node.foreign_source);
    push_nonempty(labels, "foreign_id", &node.foreign_id);
    push_nonempty(labels, "node_label", &node.node_label);
    push_nonempty(labels, "location", &node.location);
}

fn push_nonempty(labels: &mut Vec<(String, String)>, name: &str, value: &str) {
    if !value.is_empty() {
        labels.push((name.to_string(), value.to_string()));
    }
}

/// Remove duplicate label names, keeping the first occurrence. Guards against
/// e.g. a node `location` and a response-time `location` colliding.
fn dedupe_by_name(labels: &mut Vec<(String, String)>) {
    let mut seen = std::collections::HashSet::new();
    labels.retain(|(name, _)| seen.insert(name.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{InterfaceLevelResource, NumericAttribute, StringAttribute};

    fn node(id: i64) -> NodeLevelResource {
        NodeLevelResource {
            node_id: id,
            foreign_source: "Servers".into(),
            foreign_id: "router1".into(),
            node_label: "router1".into(),
            location: "Default".into(),
        }
    }

    fn numeric(group: &str, name: &str, value: f64, counter: bool) -> NumericAttribute {
        NumericAttribute {
            group: group.into(),
            name: name.into(),
            value,
            r#type: if counter {
                numeric_attribute::Type::Counter as i32
            } else {
                numeric_attribute::Type::Gauge as i32
            },
            metric_value: Some(value),
        }
    }

    fn label<'a>(s: &'a MappedSample, key: &str) -> Option<&'a str> {
        s.labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn one_sample_per_numeric_attribute() {
        let cs = CollectionSet {
            timestamp: 1_700_000_000_000,
            resource: vec![CollectionSetResource {
                resource: Some(Resource::Node(node(42))),
                resource_id: "node[42]".into(),
                resource_name: String::new(),
                resource_type_name: String::new(),
                string: vec![],
                numeric: vec![
                    numeric("mib2-tcp", "tcpCurrEstab", 123.0, false),
                    numeric("mib2-tcp", "tcpActiveOpens", 7.0, true),
                ],
            }],
        };
        let samples = Mapper::new("onms").map(&cs);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(
            label(&samples[0], "__name__"),
            Some("onms_mib2_tcp_tcpCurrEstab")
        );
        assert_eq!(label(&samples[0], "node_id"), Some("42"));
        assert_eq!(label(&samples[0], "node_label"), Some("router1"));
        assert_eq!(samples[0].value, 123.0);
        assert!(!samples[0].is_counter);
        assert!(samples[1].is_counter);
    }

    #[test]
    fn interface_instance_is_renamed_and_never_emits_instance() {
        let cs = CollectionSet {
            timestamp: 1,
            resource: vec![CollectionSetResource {
                resource: Some(Resource::Interface(InterfaceLevelResource {
                    node: Some(node(42)),
                    instance: "2".into(),
                    if_index: 2,
                })),
                resource_id: String::new(),
                resource_name: String::new(),
                resource_type_name: String::new(),
                string: vec![StringAttribute {
                    name: "ifName".into(),
                    value: "eth0".into(),
                }],
                numeric: vec![numeric("mib2-interfaces", "ifHCInOctets", 1.0, true)],
            }],
        };
        let s = &Mapper::new("onms").map(&cs)[0];
        assert_eq!(
            label(s, "__name__"),
            Some("onms_mib2_interfaces_ifHCInOctets")
        );
        assert_eq!(label(s, "resource_instance"), Some("2"));
        assert_eq!(label(s, "if_index"), Some("2"));
        assert_eq!(label(s, "ifName"), Some("eth0"));
        assert_eq!(
            label(s, "instance"),
            None,
            "must never emit reserved `instance`"
        );
    }

    #[test]
    fn cross_group_same_attribute_name_distinct_metrics() {
        let cs = CollectionSet {
            timestamp: 1,
            resource: vec![CollectionSetResource {
                resource: Some(Resource::Node(node(1))),
                resource_id: String::new(),
                resource_name: String::new(),
                resource_type_name: String::new(),
                string: vec![],
                numeric: vec![
                    numeric("cisco-cpu", "usagePercent", 10.0, false),
                    numeric("cisco-mem", "usagePercent", 20.0, false),
                ],
            }],
        };
        let samples = Mapper::new("onms").map(&cs);
        assert_eq!(
            label(&samples[0], "__name__"),
            Some("onms_cisco_cpu_usagePercent")
        );
        assert_eq!(
            label(&samples[1], "__name__"),
            Some("onms_cisco_mem_usagePercent")
        );
        // No `group` label is ever emitted.
        assert!(samples.iter().all(|s| label(s, "group").is_none()));
    }

    #[test]
    fn string_attributes_cannot_override_reserved_labels_or_be_empty() {
        let cs = CollectionSet {
            timestamp: 1,
            resource: vec![CollectionSetResource {
                resource: Some(Resource::Node(node(42))),
                resource_id: String::new(),
                resource_name: String::new(),
                resource_type_name: String::new(),
                string: vec![
                    StringAttribute {
                        name: "__name__".into(),
                        value: "evil".into(),
                    },
                    StringAttribute {
                        name: "instance".into(),
                        value: "1.2.3.4:9100".into(),
                    },
                    StringAttribute {
                        name: "ifAlias".into(),
                        value: String::new(), // empty value → dropped
                    },
                    StringAttribute {
                        name: "ifName".into(),
                        value: "eth0".into(),
                    },
                ],
                numeric: vec![numeric("mib2-tcp", "tcpCurrEstab", 1.0, false)],
            }],
        };
        let s = &Mapper::new("onms").map(&cs)[0];
        // Derived metric name wins; reserved `instance` is never emitted.
        assert_eq!(label(s, "__name__"), Some("onms_mib2_tcp_tcpCurrEstab"));
        assert_eq!(label(s, "instance"), None);
        // Empty-valued attribute is dropped; a normal one survives.
        assert_eq!(label(s, "ifAlias"), None);
        assert_eq!(label(s, "ifName"), Some("eth0"));
    }
}
