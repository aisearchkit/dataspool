// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! Document store configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;
use crate::error::DriverError;

/// Document store connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentConfig {
    /// Document store cluster URLs (e.g., ["http://localhost:9200"])
    pub urls: Vec<String>,

    /// Optional username for authentication
    pub username: Option<String>,

    /// Optional password for authentication
    pub password: Option<String>,

    /// Optional API key for authentication
    pub api_key: Option<String>,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Enable TLS certificate validation
    #[serde(default = "default_true")]
    pub verify_ssl: bool,

    /// Index pattern to read from (e.g., "logs-*")
    pub index: String,

    /// Optional query filter (Query DSL)
    pub query: Option<serde_json::Value>,

    /// Scroll/PIT timeout in minutes
    #[serde(default = "default_scroll_timeout")]
    pub scroll_timeout_mins: u64,

    /// Batch size for read operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Force use Scroll API (disable PIT)
    #[serde(default)]
    pub force_scroll: bool,
}

fn default_timeout() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

fn default_scroll_timeout() -> u64 {
    5
}

fn default_batch_size() -> usize {
    1000
}

impl DocumentConfig {
    /// Create minimal config for local testing
    pub fn local(index: impl Into<String>) -> Self {
        Self {
            urls: vec!["http://localhost:9200".to_string()],
            username: None,
            password: None,
            api_key: None,
            timeout_secs: 30,
            verify_ssl: false,
            index: index.into(),
            query: None,
            scroll_timeout_mins: 5,
            batch_size: 1000,
            force_scroll: false,
        }
    }

    /// Parse and validate URLs
    pub fn parse_urls(&self) -> crate::error::Result<Vec<Url>> {
        self.urls
            .iter()
            .map(|s| {
                Url::parse(s).map_err(|e| {
                    DriverError::DataError(format!("Invalid document store URL '{}': {}", s, e)).into()
                })
            })
            .collect()
    }

    /// Get connection timeout
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Get scroll/PIT timeout
    pub fn scroll_timeout(&self) -> Duration {
        Duration::from_secs(self.scroll_timeout_mins * 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_config() {
        let config = DocumentConfig::local("test-index");
        assert_eq!(config.index, "test-index");
        assert_eq!(config.urls.len(), 1);
        assert_eq!(config.batch_size, 1000);
    }

    #[test]
    fn test_parse_urls() {
        let config = DocumentConfig {
            urls: vec![
                "http://localhost:9200".to_string(),
                "https://db.example.com:443".to_string(),
            ],
            username: None,
            password: None,
            api_key: None,
            timeout_secs: 30,
            verify_ssl: true,
            index: "test".to_string(),
            query: None,
            scroll_timeout_mins: 5,
            batch_size: 1000,
            force_scroll: false,
        };

        let urls = config.parse_urls().unwrap();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "http://localhost:9200/");
        assert_eq!(urls[1].as_str(), "https://db.example.com/");
    }

    #[test]
    fn test_invalid_url() {
        let config = DocumentConfig {
            urls: vec!["not-a-url".to_string()],
            username: None,
            password: None,
            api_key: None,
            timeout_secs: 30,
            verify_ssl: true,
            index: "test".to_string(),
            query: None,
            scroll_timeout_mins: 5,
            batch_size: 1000,
            force_scroll: false,
        };

        assert!(config.parse_urls().is_err());
    }
}
