// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task metadata definitions

use crate::types::{TaskId, TaskState, TaskStats};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum TaskPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Task metadata containing all information about a data migration task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Unique task identifier
    pub task_id: TaskId,

    /// Human-readable task name
    pub name: String,

    /// Task description (optional)
    pub description: Option<String>,

    /// Current task state (includes timestamps and stats)
    pub state: TaskState,

    /// Task priority
    pub priority: TaskPriority,

    /// Source configuration (JSON string)
    pub source_config: String,

    /// Sink configuration (JSON string)
    pub sink_config: String,

    /// Transform configurations (optional, JSON array string)
    pub transform_configs: Option<String>,

    /// Sharding strategy and parameters (optional)
    pub sharding_config: Option<String>,

    /// Total number of shards
    pub shard_count: u32,

    /// Number of completed shards
    pub completed_shards: u32,

    /// Number of failed shards
    pub failed_shards: u32,

    /// Additional metadata (key-value pairs)
    pub metadata: HashMap<String, String>,

    /// Task creation timestamp
    pub created_at: SystemTime,

    /// Task last update timestamp
    pub updated_at: SystemTime,
}

impl TaskMeta {
    /// Create a new task metadata
    pub fn new(name: String, source_config: String, sink_config: String) -> Self {
        let now = SystemTime::now();
        Self {
            task_id: TaskId::new(),
            name,
            description: None,
            state: TaskState::Pending,
            priority: TaskPriority::default(),
            source_config,
            sink_config,
            transform_configs: None,
            sharding_config: None,
            shard_count: 0,
            completed_shards: 0,
            failed_shards: 0,
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Update task state
    pub fn set_state(&mut self, new_state: TaskState) {
        self.state = new_state;
        self.updated_at = SystemTime::now();
    }

    /// Mark a shard as completed
    pub fn mark_shard_completed(&mut self) {
        self.completed_shards += 1;
        self.updated_at = SystemTime::now();

        // Check if all shards are done
        if self.completed_shards == self.shard_count {
            let stats = TaskStats {
                docs_processed: 0, // Would be aggregated from shards
                bytes_processed: 0,
                duration_ms: 0,
            };
            self.state = TaskState::Completed {
                finished_at: chrono::Utc::now(),
                stats,
            };
        }
    }

    /// Mark a shard as failed
    pub fn mark_shard_failed(&mut self) {
        self.failed_shards += 1;
        self.updated_at = SystemTime::now();
    }

    /// Set error and mark task as failed
    pub fn mark_failed(&mut self, error: String) {
        self.state = TaskState::Failed {
            error,
            failed_at: chrono::Utc::now(),
        };
        self.updated_at = SystemTime::now();
    }

    /// Start the task
    pub fn start(&mut self, shard_count: u32) {
        self.shard_count = shard_count;
        self.state = TaskState::Running {
            started_at: chrono::Utc::now(),
            shard_count,
        };
        self.updated_at = SystemTime::now();
    }

    /// Calculate task progress (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.shard_count == 0 {
            return 0.0;
        }
        self.completed_shards as f64 / self.shard_count as f64
    }

    /// Check if task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed { .. } | TaskState::Failed { .. }
        )
    }

    /// Check if task is running
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_meta_new() {
        let task = TaskMeta::new("test-task".to_string(), "{}".to_string(), "{}".to_string());

        assert_eq!(task.name, "test-task");
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.priority, TaskPriority::Normal);
        assert_eq!(task.shard_count, 0);
        assert_eq!(task.completed_shards, 0);
        assert!(!task.is_terminal());
    }

    #[test]
    fn test_task_start() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());

        task.start(10);
        assert!(task.is_running());
        assert_eq!(task.shard_count, 10);
    }

    #[test]
    fn test_shard_tracking() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());
        task.start(10);

        assert_eq!(task.progress(), 0.0);

        for _ in 0..5 {
            task.mark_shard_completed();
        }
        assert_eq!(task.completed_shards, 5);
        assert_eq!(task.progress(), 0.5);

        task.mark_shard_failed();
        assert_eq!(task.failed_shards, 1);

        // Complete remaining shards
        for _ in 0..4 {
            task.mark_shard_completed();
        }
        assert_eq!(task.completed_shards, 9);

        task.mark_shard_completed();
        assert!(task.is_terminal());
        assert_eq!(task.progress(), 1.0);
    }

    #[test]
    fn test_task_failure() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());

        task.mark_failed("connection error".to_string());
        assert!(task.is_terminal());

        match &task.state {
            TaskState::Failed { error, .. } => {
                assert_eq!(error, "connection error");
            }
            _ => panic!("Expected Failed state"),
        }
    }

    #[test]
    fn test_priority_ordering() {
        assert!(TaskPriority::Critical > TaskPriority::High);
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
    }
}
