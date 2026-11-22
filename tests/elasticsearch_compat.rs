// SPDX-License-Identifier: AGPL-3.0-or-later
//! Optional live compatibility checks for Elasticsearch 7.10.2, 7.17 and 8.x.
//!
//! Set ES_7102_URL, ES_717_URL and/or ES_8X_URL to run against real clusters.
//! Optional credentials: ES_7102_USER/PASSWORD, ES_717_USER/PASSWORD,
//! ES_8X_USER/PASSWORD.

use dataspool::drivers::document::{
    CdcConfig, DocumentConfig, DocumentSink, DocumentSource, ElasticsearchCdcSource,
    switch_alias_after_catchup,
};
use dataspool::pipeline::{BackpressureConfig, OffsetValue, Record, RecordBatch, Sink, Source};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

fn configured_clusters() -> Vec<(&'static str, String, Option<String>, Option<String>)> {
    [
        ("ES_7102_URL", "ES_7102_USER", "ES_7102_PASSWORD", "7.10.2"),
        ("ES_717_URL", "ES_717_USER", "ES_717_PASSWORD", "7.17"),
        ("ES_8X_URL", "ES_8X_USER", "ES_8X_PASSWORD", "8.x"),
    ]
    .into_iter()
    .filter_map(|(url_key, user_key, pass_key, label)| {
        std::env::var(url_key).ok().map(|url| {
            (
                label,
                url.trim_end_matches('/').to_string(),
                std::env::var(user_key).ok(),
                std::env::var(pass_key).ok(),
            )
        })
    })
    .collect()
}

fn source_config_for_cdc(base_url: &str, change_index: &str) -> DocumentConfig {
    let mut config = DocumentConfig::local(change_index);
    config.urls = vec![base_url.to_string()];
    config
}

fn client(user: Option<&str>, password: Option<&str>) -> Client {
    let mut builder = Client::builder();
    if let (Some(user), Some(password)) = (user, password) {
        use base64::Engine;
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Basic {token}")).unwrap(),
        );
        builder = builder.default_headers(headers);
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn elasticsearch_pit_search_after_and_bulk_item_errors() {
    let clusters = configured_clusters();
    if clusters.is_empty() {
        eprintln!(
            "Skipping live Elasticsearch compatibility checks: no ES_*_URL variables configured"
        );
        return;
    }

    for (label, base_url, username, password) in clusters {
        let http = client(username.as_deref(), password.as_deref());
        let info: Value = http
            .get(format!("{base_url}/"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let version = info["version"]["number"].as_str().unwrap_or("unknown");
        assert!(
            version.starts_with("7.10.")
                || version.starts_with("7.17.")
                || version.starts_with("8."),
            "{label} endpoint reported unexpected version {version}"
        );

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let index = format!("dataspool-compat-{suffix}");
        let create = http
            .put(format!("{base_url}/{index}"))
            .json(&json!({"mappings":{"properties":{"number":{"type":"integer"}}}}))
            .send()
            .await
            .unwrap();
        assert!(
            create.status().is_success(),
            "{label}: index create failed: {}",
            create.status()
        );

        let bulk = r#"{"index":{"_id":"good-1"}}
{"number":7}
{"index":{"_id":"good-2"}}
{"number":8}
{"index":{"_id":"bad"}}
{"number":"not-an-integer"}
"#;
        let bulk_response: Value = http
            .post(format!("{base_url}/{index}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(bulk)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            bulk_response["items"].as_array().unwrap().len(),
            3,
            "{label}"
        );
        assert_eq!(bulk_response["items"][0]["index"]["status"], 201, "{label}");
        assert_eq!(bulk_response["items"][1]["index"]["status"], 201, "{label}");
        assert_eq!(bulk_response["items"][2]["index"]["status"], 400, "{label}");
        http.post(format!("{base_url}/{index}/_refresh"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let pit_response: Value = http
            .post(format!("{base_url}/{index}/_pit?keep_alive=1m"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let pit_id = pit_response["id"].as_str().expect("PIT id").to_string();

        let first_response = http
            .post(format!("{base_url}/_search"))
            .json(&json!({"size":1,"pit":{"id":&pit_id,"keep_alive":"1m"},"sort":[{"_doc":"asc"}]}))
            .send()
            .await
            .unwrap();
        let first_status = first_response.status();
        let first_body = first_response.text().await.unwrap();
        assert!(
            first_status.is_success(),
            "{label}: PIT search HTTP {first_status}: {first_body}"
        );
        let first: Value = serde_json::from_str(&first_body).unwrap();
        let current_pit_id = first["pit_id"].as_str().unwrap_or(&pit_id).to_string();
        let first_hit = &first["hits"]["hits"][0];
        assert!(!first_hit.is_null(), "{label}: PIT first page was empty");
        let sort = first_hit["sort"].clone();

        let second: Value = http
            .post(format!("{base_url}/_search"))
            .json(&json!({
                "size": 1,
                "pit": {"id": &current_pit_id, "keep_alive": "1m"},
                "sort": [{"_doc":"asc"}],
                "search_after": sort
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_ne!(
            first_hit["_id"], second["hits"]["hits"][0]["_id"],
            "{label}: search_after repeated a hit"
        );

        let current_pit_id = second["pit_id"]
            .as_str()
            .unwrap_or(&current_pit_id)
            .to_string();
        let close = http
            .delete(format!("{base_url}/_pit"))
            .json(&json!({"id": &current_pit_id}))
            .send()
            .await
            .unwrap();
        assert!(
            close.status().is_success() || close.status() == StatusCode::NOT_FOUND,
            "{label}: PIT close failed"
        );

        // Exercise the actual DocumentSource checkpoint contract across a source restart.
        let mut source_config = DocumentConfig::local(index.clone());
        source_config.urls = vec![base_url.clone()];
        source_config.batch_size = 1;
        let mut first_source = DocumentSource::new(source_config.clone());
        first_source.connect().await.unwrap();
        let (first_page, checkpoint) = first_source
            .read_batch(&OffsetValue::initial(), 1)
            .await
            .unwrap();
        assert_eq!(
            first_page.len(),
            1,
            "{label}: DocumentSource PIT first page"
        );
        drop(first_source); // emulate process termination without closing the PIT lease
        let mut resumed_source = DocumentSource::new(source_config);
        resumed_source.connect().await.unwrap();
        let (resumed_page, _) = resumed_source.read_batch(&checkpoint, 1).await.unwrap();
        assert_eq!(
            resumed_page.len(),
            1,
            "{label}: DocumentSource PIT resume page"
        );
        assert_ne!(
            first_page.records()[0].id,
            resumed_page.records()[0].id,
            "{label}: PIT resume duplicated a document"
        );
        resumed_source.close().await.unwrap();

        // CDC append-only feed, delete event encoding and alias cutover guard.
        let change_index = format!("{index}-changes");
        let create_changes = http
            .put(format!("{base_url}/{change_index}"))
            .send()
            .await
            .unwrap();
        assert!(
            create_changes.status().is_success(),
            "{label}: change index create"
        );
        let cdc_bulk = format!(
            "{{\"index\":{{\"_id\":\"evt-1\"}}}}\n{{\"sequence\":1,\"operation\":\"index\",\"document_id\":\"doc-1\",\"target_index\":\"{index}\",\"document\":{{\"number\":11}}}}\n{{\"index\":{{\"_id\":\"evt-2\"}}}}\n{{\"sequence\":2,\"operation\":\"delete\",\"document_id\":\"doc-2\",\"target_index\":\"{index}\"}}\n"
        );
        let cdc_response = http
            .post(format!("{base_url}/{change_index}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(cdc_bulk)
            .send()
            .await
            .unwrap();
        assert!(
            cdc_response.status().is_success(),
            "{label}: CDC bulk insert"
        );
        http.post(format!("{base_url}/{change_index}/_refresh"))
            .send()
            .await
            .unwrap();
        let cdc_config = CdcConfig {
            change_index: change_index.clone(),
            ..CdcConfig::default()
        };
        let mut cdc = ElasticsearchCdcSource::new(
            source_config_for_cdc(&base_url, &change_index),
            cdc_config,
        );
        cdc.connect().await.unwrap();
        let (events, watermark) = cdc.read_batch(&OffsetValue::initial(), 10).await.unwrap();
        assert_eq!(events.len(), 2, "{label}: CDC event count");
        assert_eq!(
            events.records()[1].get_metadata("_op"),
            Some("delete"),
            "{label}: CDC delete event"
        );
        assert_eq!(
            watermark,
            OffsetValue::Custom(json!({"driver":"elasticsearch_cdc","last_sequence":2})),
            "{label}: CDC watermark"
        );
        cdc.close().await.unwrap();

        let old_index = format!("{index}-old");
        let new_index = format!("{index}-new");
        for target in [&old_index, &new_index] {
            let response = http
                .put(format!("{base_url}/{target}"))
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "{label}: create alias target"
            );
        }
        let alias_setup = http
            .post(format!("{base_url}/_aliases"))
            .json(&json!({"actions":[{"add":{"index":old_index,"alias":"dataspool-live"}}]}))
            .send()
            .await
            .unwrap();
        assert!(
            alias_setup.status().is_success(),
            "{label}: initial alias setup"
        );
        assert!(
            switch_alias_after_catchup(
                &http,
                &base_url,
                "dataspool-live",
                Some(&old_index),
                &new_index,
                2,
                2
            )
            .await
            .is_ok(),
            "{label}: alias cutover"
        );
        let alias_state: Value = http
            .get(format!("{base_url}/_alias/dataspool-live"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            alias_state.get(&new_index).is_some(),
            "{label}: alias points to new index"
        );
        let blocked = switch_alias_after_catchup(
            &http,
            &base_url,
            "dataspool-live",
            Some(&new_index),
            &old_index,
            1,
            2,
        )
        .await;
        assert!(
            blocked.is_err(),
            "{label}: alias cutover must block before watermark catch-up"
        );

        // Exercise the production multi-worker sink with an intentionally high concurrency
        // window. The compatibility cluster config sets write queue_size=1 so a busy node
        // can emit real 429 item responses; DocumentSink must retry those items.
        let mut pressure_config = DocumentConfig::local(index.clone());
        pressure_config.urls = vec![base_url.clone()];
        let mut sink = DocumentSink::new(pressure_config, index.clone()).with_backpressure(
            BackpressureConfig {
                min_batch_size: 32,
                max_batch_size: 128,
                initial_batch_size: 64,
                min_in_flight: 16,
                max_in_flight: 16,
                target_latency: std::time::Duration::from_secs(10),
                high_queue_utilization: 0.5,
            },
        );
        sink.connect().await.unwrap();
        let retry_records = (0..2048)
            .map(|n| Record::with_id(format!("sink-retry-{n}"), "compat", json!({"number":n})))
            .collect::<Vec<_>>();
        let sink_result = sink
            .write_batch(RecordBatch::from(retry_records))
            .await
            .unwrap();
        assert_eq!(
            sink_result.success_count, 2048,
            "{label}: sink must finish all retried documents: {:?}",
            sink_result.errors
        );
        assert_eq!(
            sink_result.failed_count, 0,
            "{label}: no records may be lost after retry"
        );
        eprintln!(
            "Elasticsearch {label}: DocumentSink retried {} rejected documents",
            sink_result.retried_count
        );
        sink.close().await.unwrap();

        // A bounded real-cluster overload probe: issue parallel bulks and count HTTP 429s.
        // On an uncongested/custom cluster this can remain zero; the sink retry assertion above
        // still validates end-to-end delivery and the per-item retry path is covered by unit tests.
        let mut overload_jobs = tokio::task::JoinSet::new();
        for worker in 0..24 {
            let http = http.clone();
            let base_url = base_url.clone();
            let index = index.clone();
            overload_jobs.spawn(async move {
                let mut body = String::new();
                for n in 0..250 {
                    body.push_str(&format!(
                        "{{\"index\":{{\"_id\":\"pressure-{worker}-{n}\"}}}}\n{{\"number\":{n}}}\n"
                    ));
                }
                http.post(format!("{base_url}/{index}/_bulk"))
                    .header("content-type", "application/x-ndjson")
                    .body(body)
                    .send()
                    .await
                    .map(|r| r.status().as_u16())
            });
        }
        let mut observed_429 = 0;
        while let Some(joined) = overload_jobs.join_next().await {
            if let Ok(Ok(429)) = joined {
                observed_429 += 1;
            }
        }
        eprintln!(
            "Elasticsearch {label} ({version}): PIT restart, CDC, alias cutover passed; overload probe observed {observed_429} HTTP 429 responses"
        );

        let _ = http
            .delete(format!("{base_url}/{change_index}"))
            .send()
            .await;
        let _ = http
            .delete(format!("{base_url}/{old_index},{new_index}"))
            .send()
            .await;
        let _ = http.delete(format!("{base_url}/{index}")).send().await;
    }
}
