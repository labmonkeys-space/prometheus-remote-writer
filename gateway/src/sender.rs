/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Remote Write HTTP sender.
//!
//! Encodes a batch for the configured wire version and POSTs it. Transient
//! failures (5xx, transport errors) are retried with bounded exponential
//! backoff and never resolve until the write succeeds, which propagates
//! backpressure to the caller. A non-retryable 4xx drops the batch (counted)
//! and resolves so offsets may advance, matching the documented policy.

use std::time::Duration;

use anyhow::{Context, Result};
use gateway_core::{encode, MappedSample, WireVersion};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::StatusCode;
use tracing::{error, warn};

/// Outcome of a flush, used by the caller to decide offset handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Backend accepted the batch (2xx).
    Written,
    /// Backend rejected the batch (4xx); dropped and counted.
    Dropped,
}

/// Sends encoded batches to a Remote Write endpoint.
pub struct Sender {
    client: reqwest::Client,
    endpoint: String,
    version: WireVersion,
    extra_headers: HeaderMap,
    base_backoff: Duration,
    max_backoff: Duration,
}

impl Sender {
    /// Build a sender from the configured endpoint, wire version, and headers.
    pub fn new(
        endpoint: String,
        version: WireVersion,
        headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut extra_headers = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .with_context(|| format!("invalid header name `{k}`"))?;
            let value = HeaderValue::from_str(v)
                .with_context(|| format!("invalid header value for `{k}`"))?;
            extra_headers.insert(name, value);
        }

        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .context("building HTTP client")?,
            endpoint,
            version,
            extra_headers,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
        })
    }

    /// Encode and send a batch, retrying transient failures until success.
    pub async fn send(&self, samples: &[MappedSample]) -> FlushOutcome {
        let encoded = encode(self.version, samples);
        let mut backoff = self.base_backoff;

        loop {
            match self.try_post(&encoded.body, encoded.headers).await {
                Ok(Some(outcome)) => return outcome,
                Ok(None) => {
                    // Retryable status: fall through to backoff.
                }
                Err(err) => {
                    warn!(error = %err, "remote write transport error; retrying");
                }
            }

            metrics::counter!("gateway_remote_write_retries_total").increment(1);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(self.max_backoff);
        }
    }

    /// One POST attempt. `Ok(Some(_))` = terminal, `Ok(None)` = retryable,
    /// `Err(_)` = retryable transport error.
    async fn try_post(
        &self,
        body: &[u8],
        wire_headers: &[(&str, &str)],
    ) -> Result<Option<FlushOutcome>> {
        let mut req = self.client.post(&self.endpoint);
        for (k, v) in wire_headers {
            req = req.header(*k, *v);
        }
        req = req.headers(self.extra_headers.clone());

        let resp = req.body(body.to_vec()).send().await?;
        let status = resp.status();

        if status.is_success() {
            metrics::counter!("gateway_remote_write_batches_total", "result" => "written")
                .increment(1);
            Ok(Some(FlushOutcome::Written))
        } else if status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
            || status.is_server_error()
        {
            // 429/408 and 5xx are retryable per the Remote Write spec.
            warn!(%status, "remote write transient failure; retrying");
            Ok(None)
        } else if status.is_client_error() {
            let detail = resp.text().await.unwrap_or_default();
            error!(%status, detail = %detail.chars().take(256).collect::<String>(),
                "remote write rejected batch (4xx); dropping");
            metrics::counter!("gateway_remote_write_batches_total", "result" => "dropped")
                .increment(1);
            Ok(Some(FlushOutcome::Dropped))
        } else {
            // Unexpected (e.g. a 3xx that escaped redirect handling) — a config
            // error, not transient. Drop and count rather than spin forever.
            error!(%status, "unexpected remote write status; dropping batch");
            metrics::counter!("gateway_remote_write_batches_total", "result" => "dropped")
                .increment(1);
            Ok(Some(FlushOutcome::Dropped))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample() -> MappedSample {
        MappedSample {
            labels: vec![("__name__".to_string(), "onms_x".to_string())],
            timestamp_ms: 1,
            value: 1.0,
            is_counter: false,
        }
    }

    fn sender(uri: String) -> Sender {
        Sender::new(uri, WireVersion::V1, &BTreeMap::new()).unwrap()
    }

    #[tokio::test]
    async fn writes_on_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        assert_eq!(
            sender(server.uri()).send(&[sample()]).await,
            FlushOutcome::Written
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn drops_on_4xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;

        assert_eq!(
            sender(server.uri()).send(&[sample()]).await,
            FlushOutcome::Dropped
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    // wiremock evaluates mocks in registration order, so mount the limited
    // transient mock first; once its single hit is consumed the success mock
    // takes over — exercising the retry path to a terminal Written.
    #[tokio::test]
    async fn retries_5xx_then_writes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        assert_eq!(
            sender(server.uri()).send(&[sample()]).await,
            FlushOutcome::Written
        );
        assert!(server.received_requests().await.unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn retries_429_then_writes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        assert_eq!(
            sender(server.uri()).send(&[sample()]).await,
            FlushOutcome::Written
        );
        assert!(server.received_requests().await.unwrap().len() >= 2);
    }
}
