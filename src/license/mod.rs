// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! License management and validation

mod manager;
mod remote;
mod types;

pub use manager::{LicenseManager, LicenseStats};
pub use remote::{KeystoneClient, ValidateLicenseResponse};
pub use types::{Feature, License, LicenseError, LicenseTier};
