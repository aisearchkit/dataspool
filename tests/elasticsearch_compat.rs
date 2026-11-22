// SPDX-License-Identifier: AGPL-3.0-or-later
//! Optional live compatibility checks for Elasticsearch 7.10.2, 7.17 and 8.x.
//!
//! Set ES_7102_URL, ES_717_URL and/or ES_8X_URL to run against real clusters.
//! Optional credentials: ES_7102_USER/PASSWORD, ES_717_USER/PASSWORD,
//! ES_8X_USER/PASSWORD.

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

        let first: Value = http
            .post(format!("{base_url}/_search"))
            .json(&json!({
                "size": 1,
                "pit": {"id": &pit_id, "keep_alive": "1m"},
                "sort": [{"_shard_doc":"asc"}]
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let current_pit_id = first["pit_id"].as_str().unwrap_or(&pit_id).to_string();
        let first_hit = &first["hits"]["hits"][0];
        assert!(!first_hit.is_null(), "{label}: PIT first page was empty");
        let sort = first_hit["sort"].clone();

        let second: Value = http
            .post(format!("{base_url}/_search"))
            .json(&json!({
                "size": 1,
                "pit": {"id": &current_pit_id, "keep_alive": "1m"},
                "sort": [{"_shard_doc":"asc"}],
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

        let _ = http.delete(format!("{base_url}/{index}")).send().await;
        eprintln!(
            "Elasticsearch {label} ({version}): PIT, search_after and bulk item errors passed"
        );
    }
}
