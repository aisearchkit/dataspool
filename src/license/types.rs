// SPDX-License-Identifier: AGPL-3.0-or-later
//! License data model and validation errors.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicenseTier {
    Community,
    Basic,
    Standard,
    Professional,
    Enterprise,
    Trial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    DocumentMigration,
    RelationalMigration,
    StreamingMigration,
    NoSqlMigration,
    ObjectStorageMigration,
    LocalShardReader,
    AdvancedTransforms,
    MultiTenancy,
    HighAvailability,
    PrioritySupport,
}

impl Feature {
    pub fn display_name(self) -> &'static str {
        match self {
            Self::DocumentMigration => "Document Migration",
            Self::RelationalMigration => "Relational Migration",
            Self::StreamingMigration => "Streaming Platform Integration",
            Self::NoSqlMigration => "NoSQL Migration",
            Self::ObjectStorageMigration => "Object Storage Migration",
            Self::LocalShardReader => "Local Shard Reader",
            Self::AdvancedTransforms => "Advanced Transforms",
            Self::MultiTenancy => "Multi-tenancy",
            Self::HighAvailability => "High Availability",
            Self::PrioritySupport => "Priority Support",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct License {
    pub license_id: String,
    pub organization: String,
    pub tier: LicenseTier,
    #[serde(default)]
    pub features: Vec<Feature>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_tasks_per_day: Option<usize>,
    pub max_nodes: Option<usize>,
    #[serde(default)]
    pub grace_period_days: i64,
}

impl License {
    pub fn community() -> Self {
        Self {
            license_id: "community".into(),
            organization: "Community".into(),
            tier: LicenseTier::Community,
            features: vec![Feature::DocumentMigration],
            issued_at: Utc::now(),
            expires_at: None,
            max_tasks_per_day: None,
            max_nodes: None,
            grace_period_days: 0,
        }
    }

    pub fn trial(organization: String) -> Self {
        Self {
            license_id: format!("trial-{}", Utc::now().timestamp()),
            organization,
            tier: LicenseTier::Trial,
            features: vec![
                Feature::DocumentMigration,
                Feature::RelationalMigration,
                Feature::StreamingMigration,
                Feature::NoSqlMigration,
                Feature::ObjectStorageMigration,
                Feature::LocalShardReader,
                Feature::AdvancedTransforms,
            ],
            issued_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::days(30)),
            max_tasks_per_day: Some(100),
            max_nodes: Some(3),
            grace_period_days: 7,
        }
    }

    pub fn has_feature(&self, feature: Feature) -> bool {
        if self.features.contains(&feature) {
            return true;
        }
        match self.tier {
            LicenseTier::Community => feature == Feature::DocumentMigration,
            LicenseTier::Basic => matches!(
                feature,
                Feature::DocumentMigration | Feature::RelationalMigration
            ),
            LicenseTier::Standard => matches!(
                feature,
                Feature::DocumentMigration
                    | Feature::RelationalMigration
                    | Feature::StreamingMigration
            ),
            LicenseTier::Professional => matches!(
                feature,
                Feature::DocumentMigration
                    | Feature::RelationalMigration
                    | Feature::StreamingMigration
                    | Feature::NoSqlMigration
                    | Feature::ObjectStorageMigration
                    | Feature::LocalShardReader
                    | Feature::AdvancedTransforms
            ),
            LicenseTier::Enterprise => true,
            LicenseTier::Trial => false,
        }
    }

    pub fn is_valid(&self) -> Result<(), LicenseError> {
        if let Some(expiry) = self.expires_at {
            if Utc::now() > expiry + Duration::days(self.grace_period_days.max(0)) {
                return Err(LicenseError::Expired { expires_at: expiry });
            }
        }
        Ok(())
    }

    pub fn days_until_expiry(&self) -> Option<i64> {
        self.expires_at
            .map(|expiry| (expiry - Utc::now()).num_days())
    }

    pub fn is_in_grace_period(&self) -> bool {
        self.expires_at.is_some_and(|expiry| {
            let now = Utc::now();
            now > expiry && now <= expiry + Duration::days(self.grace_period_days.max(0))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_enable_expected_feature_sets() {
        let mut license = License::community();
        assert!(license.has_feature(Feature::DocumentMigration));
        assert!(!license.has_feature(Feature::RelationalMigration));

        license.tier = LicenseTier::Basic;
        assert!(license.has_feature(Feature::RelationalMigration));
        assert!(!license.has_feature(Feature::StreamingMigration));

        license.tier = LicenseTier::Standard;
        assert!(license.has_feature(Feature::StreamingMigration));
        assert!(!license.has_feature(Feature::AdvancedTransforms));

        license.tier = LicenseTier::Professional;
        assert!(license.has_feature(Feature::AdvancedTransforms));
        assert!(!license.has_feature(Feature::HighAvailability));

        license.tier = LicenseTier::Enterprise;
        assert!(license.has_feature(Feature::HighAvailability));
    }
}

#[derive(Debug, Error)]
pub enum LicenseError {
    #[error("License file not found: {path}")]
    FileNotFound { path: String },
    #[error("License parse error: {0}")]
    ParseError(String),
    #[error("License expired at {expires_at}")]
    Expired { expires_at: DateTime<Utc> },
    #[error("Feature '{feature}' requires {required_tier:?} tier")]
    FeatureNotLicensed {
        feature: String,
        required_tier: LicenseTier,
    },
    #[error("Daily task limit exceeded: {current}/{max}")]
    TaskLimitExceeded { current: usize, max: usize },
    #[error("Node limit exceeded: {current}/{max}")]
    NodeLimitExceeded { current: usize, max: usize },
}
