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

//! Static local-peer plans; native factory admission verifies actual public ownership later.
use std::any::Any;

use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_model::identifiers::TraderId;

use super::{
    client::BackpackExecutionClient, config::BackpackExecutionClientConfig,
    control::BackpackLoopbackControl,
};
use crate::{
    config::BackpackDataClientConfig,
    execution::{guard::BackpackExecutionAuthority, owner::BackpackMutationPolicy},
    instruments::BackpackEconomicsSource,
};

/// A separately typed, synthetic-only plan. Construction does not claim files or sockets.
#[derive(Clone, Debug)]
pub struct BackpackLoopbackExecutionClientConfig {
    pub(crate) account: BackpackExecutionClientConfig,
    pub(crate) public: BackpackDataClientConfig,
    pub(crate) authority: BackpackExecutionAuthority,
    pub(crate) mutation: BackpackMutationPolicy,
    pub(crate) control: BackpackLoopbackControl,
}
impl BackpackLoopbackExecutionClientConfig {
    /// Validates exact numeric local origins, scope, finite authority and synthetic economics.
    /// Actual public telemetry ownership is checked only after DataClient construction.
    ///
    /// # Errors
    /// Returns an error for production/mismatched peers, scope, provenance or invalid limits.
    pub fn new_checked(
        account: BackpackExecutionClientConfig,
        public: BackpackDataClientConfig,
        authority: BackpackExecutionAuthority,
        mutation: BackpackMutationPolicy,
    ) -> anyhow::Result<Self> {
        let endpoints = account.scope.endpoints();
        anyhow::ensure!(endpoints.is_loopback(), "production writes unsupported");
        anyhow::ensure!(
            account.namespace.matches_loopback_peer(endpoints),
            "loopback namespace must match exact origins"
        );
        anyhow::ensure!(
            endpoints == public.scope().endpoints()
                && account.scope.symbols() == public.scope().symbols(),
            "public/private scope mismatch"
        );
        anyhow::ensure!(
            public
                .economics()
                .values()
                .all(|economics| economics.source == BackpackEconomicsSource::Synthetic),
            "explicit synthetic economics required"
        );
        authority.validate()?;
        anyhow::ensure!(
            !mutation.budget.is_zero() && mutation.budget <= std::time::Duration::from_secs(60),
            "invalid mutation budget"
        );
        Ok(Self {
            account,
            public,
            authority,
            mutation,
            control: BackpackLoopbackControl::default(),
        })
    }
    /// Returns the weak owner-thread handle; this cannot manufacture an admitted session.
    #[must_use]
    pub fn control(&self) -> BackpackLoopbackControl {
        self.control.clone()
    }
}
impl ClientConfig for BackpackLoopbackExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
/// Creates the actual guarded native client after the public data owner is registered.
#[derive(Debug, Default)]
pub struct BackpackLoopbackExecutionClientFactory;
impl ExecutionClientFactory for BackpackLoopbackExecutionClientFactory {
    fn create(
        &self,
        trader: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let plan = config
            .as_any()
            .downcast_ref::<BackpackLoopbackExecutionClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("invalid loopback execution configuration"))?;
        plan.control.check_unattached()?;
        // DataClient claims actual endpoint/quota telemetry before this factory is called
        let account = plan.account.clone().with_loopback_execution(
            plan.authority.clone(),
            plan.mutation,
            &plan.account.quota,
            plan.public.telemetry().clone(),
        )?;
        let client = BackpackExecutionClient::new(trader, name, account, cache)?;
        client.attach_loopback_control(&plan.control)?;
        Ok(Box::new(client))
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    fn config_type(&self) -> &'static str {
        "BackpackLoopbackExecutionClientConfig"
    }
}
