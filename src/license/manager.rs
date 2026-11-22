// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! License manager for loading, validating, and enforcing licenses

use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

use super::{Feature, License, LicenseError};

/// License manager singleton
pub struct LicenseManager {
    /// Current active license
    license: Arc<RwLock<License>>,

    /// Path to license file
    license_file: Option<PathBuf>,

    /// Statistics
    stats: Arc<RwLock<LicenseStats>>,
}

/// License usage statistics
#[derive(Debug, Clone, Default)]
pub struct LicenseStats {
    /// Features checked
    pub features_checked: std::collections::HashMap<String, u64>,

    /// Features denied
    pub features_denied: std::collections::HashMap<String, u64>,

    /// Tasks executed today
    pub tasks_today: usize,

    /// Active nodes
    pub active_nodes: usize,
}

impl LicenseManager {
    /// Create a new license manager with community license
    pub fn new() -> Self {
        let license = License::community();
        info!("Initialized with Community license (free tier)");

        Self {
            license: Arc::new(RwLock::new(license)),
            license_file: None,
            stats: Arc::new(RwLock::new(LicenseStats::default())),
        }
    }

    /// Load license from file
    pub async fn load_from_file<P: AsRef<Path>>(&mut self, path: P) -> Result<(), LicenseError> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(LicenseError::FileNotFound {
                path: path.display().to_string(),
            });
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| LicenseError::ParseError(e.to_string()))?;

        let license: License =
            serde_yaml::from_str(&content).map_err(|e| LicenseError::ParseError(e.to_string()))?;

        // Validate license
        license.is_valid()?;

        info!(
            "Loaded {} license for {} (ID: {})",
            format!("{:?}", license.tier),
            license.organization,
            license.license_id
        );

        if let Some(expires_at) = license.expires_at {
            let days = license.days_until_expiry().unwrap_or(0);
            if days <= 30 {
                warn!("License expires in {} days ({})", days, expires_at);
            }
        }

        *self.license.write() = license;
        self.license_file = Some(path.to_path_buf());

        Ok(())
    }

    /// Save current license to file
    pub async fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), LicenseError> {
        let snapshot = self.license.read().clone();
        let content = serde_yaml::to_string(&snapshot)
            .map_err(|e| LicenseError::ParseError(e.to_string()))?;

        tokio::fs::write(path.as_ref(), content)
            .await
            .map_err(|e| LicenseError::ParseError(e.to_string()))?;

        Ok(())
    }

    /// Check if a feature is licensed
    pub fn check_feature(&self, feature: Feature) -> Result<(), LicenseError> {
        let license = self.license.read();

        // Update stats
        {
            let mut stats = self.stats.write();
            let key = format!("{:?}", feature);
            *stats.features_checked.entry(key.clone()).or_insert(0) += 1;

            if !license.has_feature(feature) {
                *stats.features_denied.entry(key).or_insert(0) += 1;
            }
        }

        // Check license validity
        license.is_valid()?;

        // Check feature
        if !license.has_feature(feature) {
            return Err(LicenseError::FeatureNotLicensed {
                feature: feature.display_name().to_string(),
                required_tier: required_tier_for_feature(feature),
            });
        }

        Ok(())
    }

    /// Get current license info
    pub fn get_license(&self) -> License {
        self.license.read().clone()
    }

    /// Get license statistics
    pub fn get_stats(&self) -> LicenseStats {
        self.stats.read().clone()
    }

    /// Increment task counter and check limits
    pub fn check_task_limit(&self) -> Result<(), LicenseError> {
        let license = self.license.read();
        let mut stats = self.stats.write();

        stats.tasks_today += 1;

        if let Some(max_tasks) = license.max_tasks_per_day
            && stats.tasks_today > max_tasks
        {
            return Err(LicenseError::TaskLimitExceeded {
                current: stats.tasks_today,
                max: max_tasks,
            });
        }

        Ok(())
    }

    /// Update active node count and check limits
    pub fn check_node_limit(&self, node_count: usize) -> Result<(), LicenseError> {
        let license = self.license.read();
        let mut stats = self.stats.write();

        stats.active_nodes = node_count;

        if let Some(max_nodes) = license.max_nodes
            && node_count > max_nodes
        {
            return Err(LicenseError::NodeLimitExceeded {
                current: node_count,
                max: max_nodes,
            });
        }

        Ok(())
    }

    /// Reset daily task counter (call at midnight)
    pub fn reset_daily_stats(&self) {
        let mut stats = self.stats.write();
        stats.tasks_today = 0;
    }

    /// Check if license is in grace period
    pub fn is_in_grace_period(&self) -> bool {
        self.license.read().is_in_grace_period()
    }
}

impl Default for LicenseManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Determine required tier for a feature
fn required_tier_for_feature(feature: Feature) -> super::LicenseTier {
    use super::LicenseTier;

    match feature {
        Feature::DocumentMigration => LicenseTier::Community,
        Feature::RelationalMigration => LicenseTier::Basic,
        Feature::StreamingMigration => LicenseTier::Standard,
        Feature::NoSqlMigration
        | Feature::ObjectStorageMigration
        | Feature::LocalShardReader
        | Feature::AdvancedTransforms => LicenseTier::Professional,
        Feature::MultiTenancy | Feature::HighAvailability | Feature::PrioritySupport => {
            LicenseTier::Enterprise
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_license_manager_default() {
        let manager = LicenseManager::new();
        let license = manager.get_license();

        assert_eq!(license.tier, super::super::LicenseTier::Community);
        assert!(manager.check_feature(Feature::DocumentMigration).is_ok());
        assert!(manager.check_feature(Feature::RelationalMigration).is_err());
    }

    #[tokio::test]
    async fn test_feature_check() {
        let manager = LicenseManager::new();

        // Community feature should work
        assert!(manager.check_feature(Feature::DocumentMigration).is_ok());

        // Premium features should fail
        let result = manager.check_feature(Feature::StreamingMigration);
        assert!(result.is_err());

        if let Err(LicenseError::FeatureNotLicensed { feature, .. }) = result {
            assert_eq!(feature, "Streaming Platform Integration");
        }
    }

    #[tokio::test]
    async fn test_task_limits() {
        let manager = LicenseManager::new();

        // Load trial license with task limits
        let trial_license = License::trial("Test Org".to_string());
        *manager.license.write() = trial_license;

        // Should allow tasks up to limit
        for _ in 0..100 {
            assert!(manager.check_task_limit().is_ok());
        }

        // Should fail after limit
        assert!(manager.check_task_limit().is_err());
    }

    #[tokio::test]
    async fn test_node_limits() {
        let manager = LicenseManager::new();
        let trial_license = License::trial("Test Org".to_string());
        *manager.license.write() = trial_license;

        assert!(manager.check_node_limit(3).is_ok());
        assert!(manager.check_node_limit(4).is_err());
    }
}
