// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Remote license validation via Keystone API

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct ValidateLicenseRequest {
    pub license_key: String,
    pub hardware_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ValidateLicenseResponse {
    pub valid: bool,
    pub tier: Option<String>,
    pub features: Option<Vec<String>>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KeystoneClient {
    base_url: String,
    client: reqwest::Client,
}

impl KeystoneClient {
    pub fn new(base_url: impl Into<String>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            base_url: base_url.into(),
            client,
        })
    }

    /// Validate a license key against Keystone server
    pub async fn validate_license(
        &self,
        license_key: &str,
        hardware_fingerprint: Option<String>,
    ) -> anyhow::Result<ValidateLicenseResponse> {
        let url = format!("{}/api/v1/licenses/validate", self.base_url);

        let request = ValidateLicenseRequest {
            license_key: license_key.to_string(),
            hardware_fingerprint,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .json::<ValidateLicenseResponse>()
            .await?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Only run with actual Keystone server
    async fn test_validate_license_integration() {
        let client = KeystoneClient::new("http://localhost:8080").unwrap();

        let result = client.validate_license("test-key", None).await;

        // Should fail with invalid key but not error
        assert!(result.is_ok());
    }
}
