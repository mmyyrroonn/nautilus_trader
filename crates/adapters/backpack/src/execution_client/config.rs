// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Caller-owned account labels, credentials and finite lifecycle budgets.
use std::{
    any::Any,
    path::{Path, PathBuf},
    time::Duration,
};

use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::AccountId;

use super::telemetry::BackpackAccountTelemetry;
use crate::{
    account::pagination::BackpackReadBudget,
    common::credential::BackpackCredential,
    config::BackpackConfig,
    http::{client::BackpackHttpPolicy, quota::BackpackQuota},
    identity::BackpackClientIdNamespace,
};
/// Finite account session, recovery and delivery bounds.
#[derive(Clone, Copy, Debug)]
pub struct BackpackExecutionPolicy {
    pub connect_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub recovery_interval: Duration,
    pub recovery_lookback: Duration,
    pub input_capacity: usize,
    pub fill_capacity: usize,
}
impl Default for BackpackExecutionPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(20),
            shutdown_timeout: Duration::from_secs(3),
            recovery_interval: Duration::from_secs(30),
            recovery_lookback: Duration::from_secs(3600),
            input_capacity: 256,
            fill_capacity: 100_000,
        }
    }
}
impl BackpackExecutionPolicy {
    /// Validates finite connection, shutdown, recovery and memory bounds.
    ///
    /// # Errors
    /// Returns an error for zero/excessive bounds.
    pub fn validate(self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !self.connect_timeout.is_zero() && self.connect_timeout <= Duration::from_secs(60),
            "invalid connect budget"
        );
        anyhow::ensure!(
            !self.shutdown_timeout.is_zero() && self.shutdown_timeout <= Duration::from_secs(30),
            "invalid shutdown budget"
        );
        anyhow::ensure!(
            self.recovery_interval >= Duration::from_millis(100)
                && self.recovery_interval <= Duration::from_secs(3600),
            "invalid recovery interval"
        );
        anyhow::ensure!(
            !self.recovery_lookback.is_zero()
                && self.recovery_lookback <= Duration::from_secs(86400),
            "invalid recovery lookback"
        );
        anyhow::ensure!(
            (1..=4096).contains(&self.input_capacity)
                && (1..=1_000_000).contains(&self.fill_capacity),
            "invalid delivery bound"
        );
        Ok(self)
    }
}
/// Immutable read-only configuration. Construction performs no I/O or environment lookup.
/// The account ID is a configured engine label, never venue-verified account identity.
/// Clones share a quota and exclusive telemetry claim; only one factory client owns them.
#[derive(Clone, Debug)]
pub struct BackpackExecutionClientConfig {
    pub(crate) scope: BackpackConfig,
    pub(crate) credential: BackpackCredential,
    pub(crate) account_id: AccountId,
    pub(crate) namespace: BackpackClientIdNamespace,
    pub(crate) identity_directory: PathBuf,
    pub(crate) policy: BackpackExecutionPolicy,
    pub(crate) read_budget: BackpackReadBudget,
    pub(crate) http_policy: BackpackHttpPolicy,
    pub(crate) quota: BackpackQuota,
    pub(crate) telemetry: BackpackAccountTelemetry,
}
impl BackpackExecutionClientConfig {
    /// Creates explicit audience-bound configuration without opening files/sockets.
    /// Production connection requires an explicit caller invocation of connect.
    ///
    /// # Errors
    /// Returns an error for audience/issuer mismatch, empty directory or invalid finite policy.
    #[expect(clippy::too_many_arguments)]
    pub fn new_read_only(
        scope: BackpackConfig,
        credential: BackpackCredential,
        account_id: AccountId,
        namespace: BackpackClientIdNamespace,
        identity_directory: PathBuf,
        policy: BackpackExecutionPolicy,
        read_budget: BackpackReadBudget,
        quota: BackpackQuota,
    ) -> anyhow::Result<Self> {
        credential.check_audience(scope.endpoints())?;
        anyhow::ensure!(
            account_id.get_issuer().as_str() == "BACKPACK",
            "invalid Backpack account issuer"
        );
        anyhow::ensure!(
            !identity_directory.as_os_str().is_empty(),
            "identity directory is required"
        );
        let policy = policy.validate()?;
        Ok(Self {
            scope,
            credential,
            account_id,
            namespace,
            identity_directory,
            policy,
            read_budget,
            quota,
            http_policy: BackpackHttpPolicy::default(),
            telemetry: BackpackAccountTelemetry::default(),
        })
    }
    #[must_use]
    pub const fn scope(&self) -> &BackpackConfig {
        &self.scope
    }
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    #[must_use]
    pub fn identity_directory(&self) -> &Path {
        &self.identity_directory
    }
    #[must_use]
    pub const fn policy(&self) -> BackpackExecutionPolicy {
        self.policy
    }
    #[must_use]
    pub fn telemetry(&self) -> BackpackAccountTelemetry {
        self.telemetry.clone()
    }
}
impl ClientConfig for BackpackExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_native_readonly_account_issuer_is_checked_before_identity_io() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("must-not-be-created");
        for label in [
            "ASTER-SYNTHETIC",
            "backpack-SYNTHETIC",
            "BACKPACKX-SYNTHETIC",
        ] {
            let result = BackpackExecutionClientConfig::new_read_only(
                BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
                BackpackCredential::production("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                    .unwrap(),
                AccountId::new_checked(label).unwrap(),
                BackpackClientIdNamespace::new_checked("production", "synthetic-account", None)
                    .unwrap(),
                path.clone(),
                BackpackExecutionPolicy::default(),
                BackpackReadBudget::new(10, 1, 10, Duration::from_secs(1)).unwrap(),
                BackpackQuota::default(),
            );
            assert!(result.is_err());
            assert!(!path.exists());
        }
    }
}
