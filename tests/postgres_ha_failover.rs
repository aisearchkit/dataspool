// SPDX-License-Identifier: AGPL-3.0-or-later
//! Verifies Dataspool Outbox continuity across a real PostgreSQL standby promotion.

use dataspool::drivers::relational::PostgresTransactionalOutbox;
use serde_json::json;
use uuid::Uuid;

const STREAM: &str = "dataspool-primary-promotion-e2e";

#[tokio::test]
async fn postgres_outbox_survives_primary_promotion() {
    let url = std::env::var("DATASPOOL_TEST_POSTGRES_HA_URL")
        .expect("DATASPOOL_TEST_POSTGRES_HA_URL must target the active primary");
    let phase = std::env::var("DATASPOOL_TEST_POSTGRES_HA_PHASE").expect("HA phase is required");
    let outbox = PostgresTransactionalOutbox::connect(&url, STREAM)
        .await
        .unwrap();
    outbox.migrate().await.unwrap();

    match phase.as_str() {
        "seed" => {
            sqlx::query("DELETE FROM dataspool_cdc_outbox WHERE stream_key = $1")
                .bind(STREAM)
                .execute(outbox.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE dataspool_cdc_outbox_gate SET paused = FALSE, last_sequence = 0 WHERE stream_key = $1")
                .bind(STREAM).execute(outbox.pool()).await.unwrap();
            let mut writer = outbox.begin_write().await.unwrap();
            writer
                .append_event(
                    Uuid::new_v4(),
                    "index",
                    "promoted-doc",
                    "promoted-target",
                    &json!({"value":1}),
                )
                .await
                .unwrap();
            writer.commit().await.unwrap();
            assert_eq!(outbox.watermark().await.unwrap(), 1);
        }
        "verify" => {
            assert_eq!(
                outbox.watermark().await.unwrap(),
                1,
                "committed gate watermark must survive standby promotion"
            );
            let events = outbox.read_after(0, 10).await.unwrap();
            assert_eq!(
                events.len(),
                1,
                "committed Outbox event must survive standby promotion"
            );
            assert_eq!(events[0].document["value"], 1);
            let mut writer = outbox.begin_write().await.unwrap();
            writer
                .append_event(
                    Uuid::new_v4(),
                    "index",
                    "after-promotion",
                    "promoted-target",
                    &json!({"value":2}),
                )
                .await
                .unwrap();
            writer.commit().await.unwrap();
            assert_eq!(
                outbox.watermark().await.unwrap(),
                2,
                "new writes must succeed on the promoted primary"
            );
        }
        other => panic!("unsupported PostgreSQL HA test phase: {other}"),
    }
}
