/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Prometheus Remote Write encoding (v1 and v2).
//!
//! Samples are grouped into time series by their (sorted) label set, the
//! protobuf payload is built for the selected wire version, and the result is
//! snappy block-compressed. [`encode`] returns the compressed body together
//! with the HTTP headers a Remote Write request requires.

use std::collections::BTreeMap;

use crate::mapping::MappedSample;
use crate::proto::{io::prometheus::write::v2, prometheus as v1};
use prost::Message;

/// Remote Write wire format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireVersion {
    /// v1 — `prometheus.WriteRequest`. Universally supported.
    V1,
    /// v2 — `io.prometheus.write.v2.Request`. Needs Prometheus 3.0+.
    V2,
}

impl WireVersion {
    /// Parse from the configured integer (`1` or `2`); defaults to V1.
    pub fn from_u8(v: u8) -> Self {
        match v {
            2 => WireVersion::V2,
            _ => WireVersion::V1,
        }
    }
}

/// An encoded Remote Write request: snappy-compressed body plus the headers it
/// must be sent with.
pub struct Encoded {
    /// Snappy block-compressed protobuf payload.
    pub body: Vec<u8>,
    /// `(name, value)` headers required for this wire version.
    pub headers: &'static [(&'static str, &'static str)],
}

const V1_HEADERS: &[(&str, &str)] = &[
    ("Content-Type", "application/x-protobuf"),
    ("Content-Encoding", "snappy"),
    ("X-Prometheus-Remote-Write-Version", "0.1.0"),
];

const V2_HEADERS: &[(&str, &str)] = &[
    (
        "Content-Type",
        "application/x-protobuf;proto=io.prometheus.write.v2.Request",
    ),
    ("Content-Encoding", "snappy"),
    ("X-Prometheus-Remote-Write-Version", "2.0.0"),
];

/// Encode and snappy-compress a batch of samples for the given wire version.
pub fn encode(version: WireVersion, samples: &[MappedSample]) -> Encoded {
    let (raw, headers) = match version {
        WireVersion::V1 => (encode_v1(samples), V1_HEADERS),
        WireVersion::V2 => (encode_v2(samples), V2_HEADERS),
    };
    Encoded {
        body: snappy_compress(&raw),
        headers,
    }
}

/// A canonical (sorted) label set used as a series key.
type LabelSet = Vec<(String, String)>;
/// Series keyed by label set, holding `(timestamp_ms, value)` points and
/// whether the series is a counter.
type SeriesMap = BTreeMap<LabelSet, (Vec<(i64, f64)>, bool)>;

/// Group samples by sorted label set, preserving counter-ness per series.
fn group(samples: &[MappedSample]) -> SeriesMap {
    let mut series: SeriesMap = BTreeMap::new();
    for s in samples {
        let mut labels = s.labels.clone();
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        let entry = series
            .entry(labels)
            .or_insert_with(|| (Vec::new(), s.is_counter));
        entry.0.push((s.timestamp_ms, s.value));
        entry.1 |= s.is_counter;
    }
    for (_, (points, _)) in series.iter_mut() {
        points.sort_by_key(|(ts, _)| *ts);
        // The Remote Write spec forbids duplicate timestamps within a series
        // (receivers 4xx the whole request); keep one sample per timestamp.
        points.dedup_by(|a, b| a.0 == b.0);
    }
    series
}

fn encode_v1(samples: &[MappedSample]) -> Vec<u8> {
    let timeseries = group(samples)
        .into_iter()
        .map(|(labels, (points, _))| v1::TimeSeries {
            labels: labels
                .into_iter()
                .map(|(name, value)| v1::Label { name, value })
                .collect(),
            samples: points
                .into_iter()
                .map(|(timestamp, value)| v1::Sample { value, timestamp })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        })
        .collect();

    v1::WriteRequest {
        timeseries,
        metadata: Vec::new(),
    }
    .encode_to_vec()
}

fn encode_v2(samples: &[MappedSample]) -> Vec<u8> {
    let mut interner = Interner::new();
    let mut timeseries = Vec::new();

    for (labels, (points, is_counter)) in group(samples) {
        let mut labels_refs = Vec::with_capacity(labels.len() * 2);
        for (name, value) in &labels {
            labels_refs.push(interner.intern(name));
            labels_refs.push(interner.intern(value));
        }

        let metric_type = if is_counter {
            v2::metadata::MetricType::Counter
        } else {
            v2::metadata::MetricType::Gauge
        };

        timeseries.push(v2::TimeSeries {
            labels_refs,
            samples: points
                .into_iter()
                .map(|(timestamp, value)| v2::Sample { value, timestamp })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
            metadata: Some(v2::Metadata {
                r#type: metric_type as i32,
                help_ref: 0,
                unit_ref: 0,
            }),
            created_timestamp: 0,
        });
    }

    v2::Request {
        symbols: interner.into_symbols(),
        timeseries,
    }
    .encode_to_vec()
}

/// String interning for the v2 symbols table. Per the v2 spec, `symbols[0]`
/// MUST be the empty string.
struct Interner {
    index: std::collections::HashMap<String, u32>,
    symbols: Vec<String>,
}

impl Interner {
    fn new() -> Self {
        let mut me = Self {
            index: std::collections::HashMap::new(),
            symbols: Vec::new(),
        };
        me.intern("");
        me
    }

    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = self.symbols.len() as u32;
        self.symbols.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }

    fn into_symbols(self) -> Vec<String> {
        self.symbols
    }
}

fn snappy_compress(raw: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(raw)
        .expect("snappy block compression is infallible for in-memory input")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, ts: i64, value: f64, counter: bool) -> MappedSample {
        MappedSample {
            labels: vec![
                ("__name__".to_string(), name.to_string()),
                ("node_id".to_string(), "42".to_string()),
            ],
            timestamp_ms: ts,
            value,
            is_counter: counter,
        }
    }

    fn snappy_decompress(body: &[u8]) -> Vec<u8> {
        snap::raw::Decoder::new().decompress_vec(body).unwrap()
    }

    #[test]
    fn v1_roundtrips_and_groups_series() {
        let samples = vec![
            sample("onms_m", 1000, 1.0, false),
            sample("onms_m", 2000, 2.0, false),
        ];
        let encoded = encode(WireVersion::V1, &samples);
        assert!(encoded
            .headers
            .iter()
            .any(|(k, _)| *k == "Content-Encoding"));
        let req = v1::WriteRequest::decode(&snappy_decompress(&encoded.body)[..]).unwrap();
        // Same labels → one series with two samples, time-ordered.
        assert_eq!(req.timeseries.len(), 1);
        let ts = &req.timeseries[0];
        assert_eq!(ts.samples.len(), 2);
        assert_eq!(ts.samples[0].timestamp, 1000);
        assert_eq!(ts.samples[1].timestamp, 2000);
        // Labels sorted by name.
        let names: Vec<_> = ts.labels.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, vec!["__name__", "node_id"]);
    }

    #[test]
    fn v1_header_is_v1() {
        let encoded = encode(WireVersion::V1, &[sample("m", 1, 1.0, false)]);
        let version = encoded
            .headers
            .iter()
            .find(|(k, _)| *k == "X-Prometheus-Remote-Write-Version")
            .unwrap()
            .1;
        assert_eq!(version, "0.1.0");
    }

    #[test]
    fn v2_interns_symbols_and_sets_counter_metadata() {
        let samples = vec![sample("onms_c", 1, 5.0, true)];
        let encoded = encode(WireVersion::V2, &samples);
        let version = encoded
            .headers
            .iter()
            .find(|(k, _)| *k == "X-Prometheus-Remote-Write-Version")
            .unwrap()
            .1;
        assert_eq!(version, "2.0.0");

        let req = v2::Request::decode(&snappy_decompress(&encoded.body)[..]).unwrap();
        assert_eq!(req.symbols[0], "", "symbols[0] must be empty per v2 spec");
        assert_eq!(req.timeseries.len(), 1);
        let ts = &req.timeseries[0];
        // labels_refs are name/value index pairs into symbols.
        assert_eq!(ts.labels_refs.len() % 2, 0);
        let meta = ts.metadata.as_ref().unwrap();
        assert_eq!(meta.r#type, v2::metadata::MetricType::Counter as i32);
        // Reconstruct first label name from the symbols table.
        let first_name = &req.symbols[ts.labels_refs[0] as usize];
        assert_eq!(first_name, "__name__");
    }

    #[test]
    fn duplicate_timestamps_within_a_series_are_collapsed() {
        // Two samples for the same series at the same timestamp would make the
        // request spec-invalid; only one point must survive.
        let samples = vec![
            sample("onms_m", 1000, 1.0, false),
            sample("onms_m", 1000, 2.0, false),
            sample("onms_m", 2000, 3.0, false),
        ];
        let encoded = encode(WireVersion::V1, &samples);
        let req = v1::WriteRequest::decode(&snappy_decompress(&encoded.body)[..]).unwrap();
        assert_eq!(req.timeseries.len(), 1);
        let ts = &req.timeseries[0];
        let stamps: Vec<i64> = ts.samples.iter().map(|s| s.timestamp).collect();
        assert_eq!(stamps, vec![1000, 2000]);
    }
}
