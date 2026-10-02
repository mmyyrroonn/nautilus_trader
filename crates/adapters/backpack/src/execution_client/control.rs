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

//! Owner-thread, weakly attached control for a single synthetic local peer client.
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use nautilus_model::{identifiers::InstrumentId, reports::FillReport};

use super::{
    client::BackpackLoopbackControlRuntime, restricted::BackpackLoopbackSession,
    telemetry::BackpackAccountHealth,
};
use crate::execution::{guard::BackpackLoopbackAccountFacts, owner::BackpackShutdownReport};

#[derive(Debug, Default)]
struct Attachment {
    used: bool,
    runtime: Weak<BackpackLoopbackControlRuntime>,
    shutdown: Option<BackpackShutdownReport>,
}

/// A no-I/O control plan, attached once by the real native factory on its owner thread.
/// It retains no client, cache, identity lock or credential. A dropped client permanently
/// invalidates the attachment; building another client cannot redirect old controls or tokens.
#[derive(Clone, Debug, Default)]
pub struct BackpackLoopbackControl {
    inner: Rc<RefCell<Attachment>>,
}
impl BackpackLoopbackControl {
    pub(super) fn check_unattached(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.inner.borrow().used,
            "loopback control was already attached"
        );
        Ok(())
    }
    pub(super) fn attach(
        &self,
        runtime: &Rc<BackpackLoopbackControlRuntime>,
    ) -> anyhow::Result<()> {
        let mut state = self.inner.borrow_mut();
        anyhow::ensure!(!state.used, "loopback control was already attached");
        state.used = true;
        state.runtime = Rc::downgrade(runtime);
        Ok(())
    }
    pub(super) fn remember_shutdown(&self, shutdown: Option<BackpackShutdownReport>) {
        if let Some(shutdown) = shutdown {
            self.inner.borrow_mut().shutdown = Some(shutdown);
        }
    }
    fn runtime(&self) -> anyhow::Result<Rc<BackpackLoopbackControlRuntime>> {
        self.inner
            .borrow()
            .runtime
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("loopback client is not attached or was disposed"))
    }
    /// Starts a new explicit session bound to the actual public/private owner generations.
    ///
    /// # Errors
    /// Returns an error before attachment, after disposal, or for stale/disconnected owners.
    pub fn begin_session(&self) -> anyhow::Result<BackpackLoopbackSession> {
        self.runtime()?.begin_session()
    }
    /// Accepts explicit complete facts for the exact configured synthetic namespace.
    ///
    /// # Errors
    /// Returns an error for a stale token, unsupported policy, scope mismatch or stale facts.
    pub fn accept_account(
        &self,
        token: BackpackLoopbackSession,
        facts: BackpackLoopbackAccountFacts,
    ) -> anyhow::Result<()> {
        self.runtime()?.accept_account(token, facts)
    }
    /// Refreshes from the actual owner-thread native cache and validated account metadata.
    ///
    /// # Errors
    /// Returns an error for a stale token, absent quote/metadata or stale public evidence.
    pub fn refresh_market(
        &self,
        token: BackpackLoopbackSession,
        instrument: InstrumentId,
    ) -> anyhow::Result<()> {
        self.runtime()?.refresh_market(token, instrument)
    }
    /// Invalidates only the currently matched session; old tokens cannot freeze a new run.
    ///
    /// # Errors
    /// Returns an error before attachment, after disposal or for an old session.
    pub fn invalidate(&self, token: BackpackLoopbackSession) -> anyhow::Result<()> {
        self.runtime()?.invalidate(token)
    }
    /// Returns genuine pending reports without acknowledging economic application.
    ///
    /// # Errors
    /// Returns an error before attachment or after client disposal.
    pub fn pending(&self) -> anyhow::Result<Vec<FillReport>> {
        self.runtime()?.pending()
    }
    /// Returns a versioned actual runtime health snapshot, never a private verification proof.
    ///
    /// # Errors
    /// Returns an error before attachment or after client disposal.
    pub fn health(&self) -> anyhow::Result<BackpackAccountHealth> {
        self.runtime()?.health()
    }
    /// Returns sticky stop evidence, preserved as a value after disposal.
    #[must_use]
    pub fn shutdown(&self) -> Option<BackpackShutdownReport> {
        if let Ok(runtime) = self.runtime() {
            self.remember_shutdown(runtime.shutdown());
        }
        self.inner.borrow().shutdown
    }
}
