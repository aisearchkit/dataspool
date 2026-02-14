// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! License management and validation

mod manager;
mod remote;

pub use manager::{Feature, License, LicenseManager, LicenseTier};
pub use remote::{KeystoneClient, ValidateLicenseResponse};
