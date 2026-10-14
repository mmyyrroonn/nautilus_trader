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

//! Owner-thread observations and durable economic consumption; no financial commands.
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use nautilus_model::reports::FillReport;

use super::{client::BackpackLoopbackControlRuntime, telemetry::BackpackAccountHealth};
use crate::execution::owner::BackpackShutdownReport;

#[derive(Debug, Default)]
struct Attachment {
    used: bool,
    runtime: Weak<BackpackLoopbackControlRuntime>,
    shutdown: Option<BackpackShutdownReport>,
}
/// Weak native owner handle. It cannot inject account facts or dispatch trading requests.
#[derive(Clone, Debug, Default)]
pub struct BackpackProductionControl {
    inner: Rc<RefCell<Attachment>>,
}
impl BackpackProductionControl {
    pub(super) fn check_unattached(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.inner.borrow().used,
            "production control was already attached"
        );
        Ok(())
    }
    pub(super) fn attach(
        &self,
        runtime: &Rc<BackpackLoopbackControlRuntime>,
    ) -> anyhow::Result<()> {
        self.check_unattached()?;
        let mut state = self.inner.borrow_mut();
        state.used = true;
        state.runtime = Rc::downgrade(runtime);
        Ok(())
    }
    pub(super) fn remember_shutdown(&self, report: Option<BackpackShutdownReport>) {
        if let Some(report) = report {
            self.inner.borrow_mut().shutdown = Some(report);
        }
    }
    fn runtime(&self) -> anyhow::Result<Rc<BackpackLoopbackControlRuntime>> {
        self.inner
            .borrow()
            .runtime
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("production client is not attached or was disposed"))
    }
    pub fn health(&self) -> anyhow::Result<BackpackAccountHealth> {
        self.runtime()?.health()
    }
    pub fn pending(&self) -> anyhow::Result<Vec<FillReport>> {
        self.runtime()?.pending()
    }
    pub fn persist_economics(&self) -> anyhow::Result<serde_json::Value> {
        self.runtime()?.persist_economics()
    }
    pub fn reconcile_terminal_evidence(&self) -> anyhow::Result<serde_json::Value> {
        let runtime = self.runtime()?;
        runtime.reconcile_production_terminal_evidence()
    }
    /// Reports current native admission without dispatching a financial request.
    pub fn admission_status_json(&self) -> anyhow::Result<serde_json::Value> {
        self.runtime()?.production_admission_status()
    }
    /// Extends expiry without accepting new permissions, account facts or risk limits.
    pub fn renew_authority(&self, expires_at_ms: u64) -> anyhow::Result<()> {
        self.runtime()?.renew_production_authority(expires_at_ms)
    }
    /// Returns the full authenticated REST reading only while its private revision is current.
    pub fn account_snapshot_json(&self) -> anyhow::Result<serde_json::Value> {
        self.runtime()?.production_account_snapshot()
    }
    #[must_use]
    pub fn shutdown(&self) -> Option<BackpackShutdownReport> {
        if let Ok(runtime) = self.runtime() {
            self.remember_shutdown(runtime.shutdown());
        }
        self.inner.borrow().shutdown
    }
}
