// SPDX-License-Identifier: AGPL-3.0-or-later
//! Optional live compatibility checks for Elasticsearch 7.10.2, 7.17, 8.x and 9.x.
//!
//! Set ES_7102_URL, ES_717_URL, ES_8X_URL and/or ES_9X_URL to run against real clusters.
//! Optional credentials: ES_7102_USER/PASSWORD, ES_717_USER/PASSWORD,
//! ES_8X_USER/PASSWORD, ES_9X_USER/PASSWORD.

use dataspool::drivers::document::{
    CdcConfig, DocumentConfig, DocumentSink, DocumentSource, ElasticsearchCdcSource,
    ElasticsearchOnlineMigrationSource, switch_alias_after_catchup,
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
        ("ES_9X_URL", "ES_9X_USER", "ES_9X_PASSWORD", "9.x"),
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
        if std::env::var("ES_REQUIRE_LIVE").as_deref() == Ok("1") {
            panic!("ES_REQUIRE_LIVE=1 but no ES_*_URL variables are configured");
        }
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
                || version.starts_with("8.")
                || version.starts_with("9."),
            "{label} endpoint reported unexpected version {version}"
        );
        let expected_nodes = std::env::var("ES_EXPECT_NODES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1);
        let node_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
        let node_count = loop {
            let nodes: Value = http
                .get(format!("{base_url}/_nodes?filter_path=nodes"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            let count = nodes["nodes"].as_object().map_or(0, |n| n.len());
            if count >= expected_nodes {
                break count;
            }
            assert!(
                tokio::time::Instant::now() < node_deadline,
                "{label}: expected at least {expected_nodes} nodes, discovered {count} after 90s"
            );
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        };

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let index = format!("dataspool-compat-{suffix}");
        let create = http
            .put(format!("{base_url}/{index}"))
            .json(&json!({"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"number":{"type":"integer"}}}}))
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
            .json(
                &json!({"size":1,"pit":{"id":&pit_id,"keep_alive":"1m"},"sort":[{"number":"asc"}]}),
            )
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
                "sort": [{"number":"asc"}],
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
        assert_eq!(first_page.len(), 1, "{label}: first source page");
        drop(first_source); // emulate process termination without closing the server-side cursor
        let mut resumed_source = DocumentSource::new(source_config.clone());
        resumed_source.connect().await.unwrap();
        let (resumed_page, _) = resumed_source.read_batch(&checkpoint, 1).await.unwrap();
        assert_eq!(
            resumed_page.len(),
            1,
            "{label}: DocumentSource cursor resume page"
        );
        assert_ne!(
            first_page.records()[0].id,
            resumed_page.records()[0].id,
            "{label}: cursor resume duplicated a document"
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
        assert!(
            cdc.is_continuous(),
            "{label}: CDC source must stay active when idle"
        );
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

        // Verify the complete online source: capture the change-log watermark,
        // copy the source snapshot, then consume only events newer than the watermark.
        let mut online_change_connection = source_config.clone();
        online_change_connection.index = change_index.clone();
        let mut online = ElasticsearchOnlineMigrationSource::new(
            source_config.clone(),
            online_change_connection,
            CdcConfig {
                change_index: change_index.clone(),
                ..CdcConfig::default()
            },
        );
        online.connect().await.unwrap();
        let late_event = format!(
            "{{\"index\":{{\"_id\":\"evt-3\"}}}}\n{{\"sequence\":3,\"operation\":\"index\",\"document_id\":\"doc-3\",\"target_index\":\"{index}\",\"document\":{{\"number\":33}},\"routing\":\"tenant-3\"}}\n"
        );
        let late_response = http
            .post(format!("{base_url}/{change_index}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(late_event)
            .send()
            .await
            .unwrap();
        assert!(
            late_response.status().is_success(),
            "{label}: append late CDC event"
        );
        http.post(format!("{base_url}/{change_index}/_refresh"))
            .send()
            .await
            .unwrap();

        let (snapshot_page_one, snapshot_offset) =
            online.read_batch(&OffsetValue::initial(), 1).await.unwrap();
        assert_eq!(
            snapshot_page_one.len(),
            1,
            "{label}: online snapshot page one"
        );
        let (snapshot_page_two, snapshot_offset) =
            online.read_batch(&snapshot_offset, 1).await.unwrap();
        assert_eq!(
            snapshot_page_two.len(),
            1,
            "{label}: online snapshot page two"
        );
        let (cdc_page, online_offset) = online.read_batch(&snapshot_offset, 1).await.unwrap();
        assert_eq!(cdc_page.len(), 1, "{label}: online CDC catch-up event");
        assert_eq!(
            cdc_page.records()[0].get_metadata("_cdc_sequence"),
            Some("3"),
            "{label}: online CDC starts after captured watermark"
        );
        assert_eq!(
            cdc_page.records()[0].get_metadata("_routing"),
            Some("tenant-3"),
            "{label}: online CDC preserves routing"
        );
        let mut resumed_change_connection = source_config.clone();
        resumed_change_connection.index = change_index.clone();
        let mut resumed_online = ElasticsearchOnlineMigrationSource::new(
            source_config.clone(),
            resumed_change_connection,
            CdcConfig {
                change_index: change_index.clone(),
                ..CdcConfig::default()
            },
        );
        resumed_online.connect().await.unwrap();
        let (after_restart, _) = resumed_online.read_batch(&online_offset, 1).await.unwrap();
        assert!(
            after_restart.is_empty(),
            "{label}: online CDC checkpoint resume"
        );
        online.close().await.unwrap();
        resumed_online.close().await.unwrap();

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
                min_in_flight: 4,
                max_in_flight: 4,
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
        let mut total_retried = sink_result.retried_count;
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
        let soak_seconds = std::env::var("ES_SOAK_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if soak_seconds > 0 {
            let mut soak_config = DocumentConfig::local(index.clone());
            soak_config.urls = vec![base_url.clone()];
            let mut soak_sink = DocumentSink::new(soak_config, index.clone()).with_backpressure(
                BackpressureConfig {
                    min_batch_size: 32,
                    max_batch_size: 128,
                    initial_batch_size: 128,
                    min_in_flight: 4,
                    max_in_flight: 4,
                    target_latency: std::time::Duration::from_secs(10),
                    high_queue_utilization: 0.95,
                },
            );
            soak_sink.connect().await.unwrap();
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_secs(soak_seconds);
            let mut sequence = 0_u64;
            let mut soak_written = 0_u64;
            while tokio::time::Instant::now() < deadline {
                let batch = (0..4096)
                    .map(|n| {
                        Record::with_id(
                            format!("soak-{sequence}-{n}"),
                            "compat-soak",
                            json!({"number":sequence+n as u64}),
                        )
                    })
                    .collect::<Vec<_>>();
                let result = soak_sink
                    .write_batch(RecordBatch::from(batch))
                    .await
                    .unwrap();
                assert_eq!(
                    result.failed_count, 0,
                    "{label}: long soak encountered permanent/transient failures: {:?}",
                    result.errors
                );
                assert_eq!(
                    result.rejected_count, 0,
                    "{label}: long soak left rejected documents after retry"
                );
                soak_written += result.success_count as u64;
                total_retried += result.retried_count;
                sequence += 4096;
            }
            soak_sink.close().await.unwrap();
            eprintln!(
                "Elasticsearch {label}: {soak_seconds}s soak delivered {soak_written} docs, retry_count={total_retried}"
            );
        }
        if std::env::var("ES_REQUIRE_429").as_deref() == Ok("1") {
            assert!(
                total_retried > 0,
                "{label}: multi-node stress did not exercise DocumentSink's real 429 retry path"
            );
        }
        eprintln!(
            "Elasticsearch {label} ({version}): nodes={node_count}, PIT restart, CDC, alias cutover passed; overload probe observed {observed_429} HTTP 429 responses; sink_retries={total_retried}"
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
