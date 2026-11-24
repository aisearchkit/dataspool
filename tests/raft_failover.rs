// SPDX-License-Identifier: AGPL-3.0-or-later
//! Three-member Raft failover integration: a leader is stopped, a new leader
//! commits a higher shard fencing generation, and the stale generation is rejected.

use dataspool::agent::ShardClaimer;
use dataspool::pipeline::OffsetValue;
use dataspool::raft::{Command, RaftNode, raft_config};
use dataspool::storage::RocksDbStore;
use dataspool::task::{ShardMeta, TaskMeta, TaskStore};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn three_member_raft_leader_failover_fences_stale_worker() {
    let token = "raft-failover-integration-token-0123456789".to_string();
    let ids = [101_u64, 202_u64, 303_u64];
    let mut directories = Vec::new();
    let mut stores = Vec::new();
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();

    for _ in 0..3 {
        let directory = tempdir().unwrap();
        let store = RocksDbStore::open(directory.path().join("app")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        directories.push(directory);
        stores.push(store);
        listeners.push(listener);
    }

    let members = ids
        .iter()
        .zip(&addresses)
        .map(|(id, address)| (*id, BasicNode::new(address.clone())))
        .collect::<BTreeMap<_, _>>();
    let config = raft_config(Duration::from_millis(150), Duration::from_millis(600));
    let mut nodes = Vec::new();
    let mut servers = Vec::new();
    for index in 0..3 {
        let store = TaskStore::new(stores[index].clone());
        let node = RaftNode::new(
            ids[index],
            directories[index].path().join("raft"),
            store,
            config.clone(),
            token.clone(),
        )
        .await
        .unwrap();
        let router = dataspool::raft_router(node.handle(), token.clone());
        let listener = listeners.remove(0);
        servers.push(tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        }));
        nodes.push(node);
    }

    nodes[0].initialize(members).await.unwrap();
    let leader_id = nodes[0]
        .wait_for_leader(Duration::from_secs(12))
        .await
        .unwrap();
    let leader_index = ids.iter().position(|id| *id == leader_id).unwrap();
    let task_store = Arc::new(TaskStore::new(stores[(leader_index + 1) % 3].clone()));
    let task = TaskMeta::new("raft-failover-fencing".into(), "{}".into(), "{}".into());
    let task_id = task.task_id;
    let shard = ShardMeta::new(task_id, 0, "{}".into());
    let shard_id = shard.shard_id;
    let created = nodes[leader_index]
        .propose(Command::CreateTask {
            request_id: format!("failover-create:{}", task_id),
            actor: "integration-test".into(),
            task: Box::new(task),
            shards: vec![shard],
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);

    let worker_index = (leader_index + 1) % 3;
    let first_claimer = ShardClaimer::new(
        "worker-before-failover".into(),
        task_store.clone(),
        Arc::new(nodes[worker_index].clone()),
    )
    .with_lease_seconds(3)
    .unwrap();
    let first_lease = first_claimer.try_claim().await.unwrap().unwrap();
    let first_fence = first_lease.fencing_token;

    // Stop the actual Raft leader while retaining the other two live members.
    nodes[leader_index].shutdown().await.unwrap();
    let surviving_index = (leader_index + 1) % 3;
    let new_leader_id = nodes[surviving_index]
        .wait_for_leader(Duration::from_secs(12))
        .await
        .unwrap();
    assert_ne!(
        new_leader_id, leader_id,
        "a different member must lead after failover"
    );
    let remaining = (first_lease.expires_at - chrono::Utc::now())
        .to_std()
        .unwrap_or_default();
    tokio::time::sleep(remaining + Duration::from_millis(150)).await;
    let recovery_index = ids.iter().position(|id| *id == new_leader_id).unwrap();
    assert!(nodes[recovery_index].ensure_linearizable().await.is_ok());
    let recovery_store = Arc::new(TaskStore::new(stores[recovery_index].clone()));
    let recovery_claimer = ShardClaimer::new(
        "worker-after-failover".into(),
        recovery_store.clone(),
        Arc::new(nodes[recovery_index].clone()),
    )
    .with_lease_seconds(3)
    .unwrap();
    let recovered_lease = recovery_claimer.try_claim().await.unwrap().unwrap();
    assert!(recovered_lease.fencing_token > first_fence);

    let stale_write = nodes[recovery_index]
        .propose(Command::UpdateShardProgress {
            shard_id: shard_id.to_string(),
            node_id: first_lease.node_id.to_string(),
            fencing_token: first_fence,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            processed_records: 999,
            processed_bytes: 999,
            offset: serde_json::to_string(&OffsetValue::DocumentCount(999)).unwrap(),
            batch_id: None,
        })
        .await
        .unwrap();
    assert!(
        !stale_write.success,
        "stale fencing generation must be rejected"
    );

    for store in &stores {
        let persisted = TaskStore::new(store.clone())
            .get_shard(&shard_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.fencing_token, recovered_lease.fencing_token);
        assert_eq!(
            persisted.records_processed, 0,
            "stale progress must not be applied"
        );
    }

    for (index, node) in nodes.iter().enumerate() {
        if index != leader_index {
            node.shutdown().await.unwrap();
        }
    }
    for server in servers {
        server.abort();
    }
}
