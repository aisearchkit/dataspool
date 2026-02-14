// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! License and feature authorization module
//!
//! This module controls access to premium features based on customer licenses.
//! 
//! **License Tiers**:
//! - **Community**: Document migration only (ES/OpenSearch) - Free, AGPL-3.0
//! - **Basic**: + Relational databases (MySQL/PostgreSQL)
//! - **Standard**: + Streaming (Kafka/Pulsar/AutoMQ)
//! - **Professional**: + NoSQL (MongoDB/Redis) + Object Storage
//! - **Enterprise**: All features + Priority Support + Custom Development
//!
//! **Enforcement Points**:
//! - Driver factory instantiation
//! - API endpoint access
//! - Task submission
//! - Worker execution

mod manager;

pub use manager::{LicenseManager, LicenseStats};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

/// Licensed features in Dataspool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    /// Document database migration (Elasticsearch, OpenSearch)
    DocumentMigration,
    /// Relational database migration (MySQL, PostgreSQL, SQLite)
    RelationalMigration,
    /// Streaming platform integration (Kafka, Pulsar, AutoMQ)
    StreamingMigration,
    /// NoSQL database migration (MongoDB, Redis, Cassandra)
    NoSqlMigration,
    /// Object storage operations (S3, NFS snapshots, Parquet)
    ObjectStorageMigration,
    /// Local shard reader (zero-copy ES migration)
    LocalShardReader,
    /// Advanced transforms (scripting, complex mappings)
    AdvancedTransforms,
    /// Multi-tenancy and RBAC
    MultiTenancy,
    /// High availability (3+ nodes)
    HighAvailability,
    /// Priority support and SLA
    PrioritySupport,
}

impl Feature {
    /// Get human-readable feature name
    pub fn display_name(&self) -> &str {
        match self {
            Feature::DocumentMigration => "Document Database Migration",
            Feature::RelationalMigration => "Relational Database Migration",
            Feature::StreamingMigration => "Streaming Platform Integration",
            Feature::NoSqlMigration => "NoSQL Database Migration",
            Feature::ObjectStorageMigration => "Object Storage Migration",
            Feature::LocalShardReader => "Local Shard Reader",
            Feature::AdvancedTransforms => "Advanced Transforms",
            Feature::MultiTenancy => "Multi-Tenancy & RBAC",
            Feature::HighAvailability => "High Availability Clustering",
            Feature::PrioritySupport => "Priority Support & SLA",
        }
    }

    /// Check if feature is free (Community tier)
    pub fn is_free(&self) -> bool {
        matches!(self, Feature::DocumentMigration)
    }
}

/// License tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicenseTier {
    Community,
    Basic,
    Standard,
    Professional,
    Enterprise,
}

impl LicenseTier {
    /// Get all features included in this tier
    pub fn features(&self) -> HashSet<Feature> {
        let mut features = HashSet::new();
        features.insert(Feature::DocumentMigration);

        if matches!(
            self,
            LicenseTier::Basic
                | LicenseTier::Standard
                | LicenseTier::Professional
                | LicenseTier::Enterprise
        ) {
            features.insert(Feature::RelationalMigration);
        }

        if matches!(
            self,
            LicenseTier::Standard | LicenseTier::Professional | LicenseTier::Enterprise
        ) {
            features.insert(Feature::StreamingMigration);
        }

        if matches!(self, LicenseTier::Professional | LicenseTier::Enterprise) {
            features.insert(Feature::NoSqlMigration);
            features.insert(Feature::ObjectStorageMigration);
            features.insert(Feature::LocalShardReader);
            features.insert(Feature::AdvancedTransforms);
        }

        if matches!(self, LicenseTier::Enterprise) {
            features.insert(Feature::MultiTenancy);
            features.insert(Feature::HighAvailability);
            features.insert(Feature::PrioritySupport);
        }

        features
    }
}

/// License type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicenseType {
    Community,
    Trial,
    Perpetual,
    Subscription,
    Floating,
}

/// License metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct License {
    pub license_id: String,
    pub organization: String,
    pub tier: LicenseTier,
    pub license_type: LicenseType,
    pub features: HashSet<Feature>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_nodes: Option<usize>,
    pub max_tasks_per_day: Option<usize>,
    pub hardware_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

impl License {
    pub fn community() -> Self {
        Self {
            license_id: "COMMUNITY-FREE".to_string(),
            organization: "Open Source Community".to_string(),
            tier: LicenseTier::Community,
            license_type: LicenseType::Community,
            features: LicenseTier::Community.features(),
            issued_at: Utc::now(),
            expires_at: None,
            max_nodes: None,
            max_tasks_per_day: None,
            hardware_id: None,
            signature: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn trial(organization: String) -> Self {
        let mut features = LicenseTier::Enterprise.features();
        features.remove(&Feature::PrioritySupport);

        Self {
            license_id: format!("TRIAL-{}", uuid::Uuid::new_v4()),
            organization,
            tier: LicenseTier::Professional,
            license_type: LicenseType::Trial,
            features,
            issued_at: Utc::now(),
            expires_at: Some(Utc::now() + chrono::Duration::days(30)),
            max_nodes: Some(3),
            max_tasks_per_day: Some(100),
            hardware_id: None,
            signature: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn is_valid(&self) -> Result<(), LicenseError> {
        if let Some(expires_at) = self.expires_at {
            if Utc::now() > expires_at {
                return Err(LicenseError::Expired { expired_at: expires_at });
            }
        }
        Ok(())
    }

    pub fn has_feature(&self, feature: Feature) -> bool {
        if feature.is_free() {
            return true;
        }
        self.features.contains(&feature)
    }

    pub fn days_until_expiry(&self) -> Option<i64> {
        self.expires_at.map(|exp| (exp - Utc::now()).num_days())
    }

    pub fn is_in_grace_period(&self) -> bool {
        if let Some(days) = self.days_until_expiry() {
            days < 0 && days > -7
        } else {
            false
        }
    }
}

/// License errors
#[derive(Error, Debug)]
pub enum LicenseError {
    #[error("License expired on {expired_at}")]
    Expired { expired_at: DateTime<Utc> },

    #[error("Feature '{feature}' not licensed (requires {required_tier:?} or higher)")]
    FeatureNotLicensed {
        feature: String,
        required_tier: LicenseTier,
    },

    #[error("Node limit exceeded: {current} nodes active, license allows {max}")]
    NodeLimitExceeded { current: usize, max: usize },

    #[error("Task limit exceeded: {current} tasks today, license allows {max}")]
    TaskLimitExceeded { current: usize, max: usize },

    #[error("Invalid license signature")]
    InvalidSignature,

    #[error("License file not found: {path}")]
    FileNotFound { path: String },

    #[error("Failed to parse license: {0}")]
    ParseError(String),

    #[error("Hardware mismatch: license is node-locked to different hardware")]
    HardwareMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_community_license() {
        let license = License::community();
        assert_eq!(license.tier, LicenseTier::Community);
        assert!(license.has_feature(Feature::DocumentMigration));
        assert!(!license.has_feature(Feature::RelationalMigration));
        assert!(license.is_valid().is_ok());
    }

    #[test]
    fn test_trial_license() {
        let license = License::trial("Acme Corp".to_string());
        assert_eq!(license.license_type, LicenseType::Trial);
        assert!(license.has_feature(Feature::DocumentMigration));
        assert!(license.has_feature(Feature::RelationalMigration));
        assert!(license.has_feature(Feature::StreamingMigration));
        assert!(license.is_valid().is_ok());
    }

    #[test]
    fn test_license_tiers() {
        let basic = LicenseTier::Basic.features();
        assert!(basic.contains(&Feature::DocumentMigration));
        assert!(basic.contains(&Feature::RelationalMigration));
        assert!(!basic.contains(&Feature::StreamingMigration));

        let enterprise = LicenseTier::Enterprise.features();
        assert!(enterprise.contains(&Feature::MultiTenancy));
    }
}
