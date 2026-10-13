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

//! Explicit production-protocol execution through the native execution engine.
use std::{any::Any, path::PathBuf};

use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_model::identifiers::TraderId;

use super::{
    client::BackpackExecutionClient,
    config::{BackpackExecutionClientConfig, BackpackLoopbackExecutionMode},
    production_control::BackpackProductionControl,
};
use crate::{
    config::BackpackDataClientConfig,
    execution::{guard::BackpackExecutionAuthority, owner::BackpackMutationPolicy},
    instruments::BackpackEconomicsSource,
};

/// An immutable, explicitly authorized native writer plan. Construction performs no I/O.
#[derive(Clone, Debug)]
pub struct BackpackProductionExecutionClientConfig {
    pub(crate) account: BackpackExecutionClientConfig,
    pub(crate) public: BackpackDataClientConfig,
    pub(crate) authority: BackpackExecutionAuthority,
    pub(crate) mutation: BackpackMutationPolicy,
    pub(crate) economic_state_directory: PathBuf,
    pub(crate) control: BackpackProductionControl,
}
impl BackpackProductionExecutionClientConfig {
    /// Validates matching native public/private scope, durable storage, and finite limits.
    /// Numeric loopback transports are supported for signed production-protocol simulations.
    pub fn new_checked(
        account: BackpackExecutionClientConfig,
        public: BackpackDataClientConfig,
        authority: BackpackExecutionAuthority,
        mutation: BackpackMutationPolicy,
        economic_state_directory: PathBuf,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            account.restricted.is_none(),
            "production plan requires a read-only base configuration"
        );
        anyhow::ensure!(
            account
                .namespace
                .matches_credential_peer(&account.credential, account.scope.endpoints()),
            "production identity must bind the actual peer credential authority"
        );
        anyhow::ensure!(
            account.scope.endpoints() == public.scope().endpoints()
                && account.scope.symbols() == public.scope().symbols(),
            "public/private scope mismatch"
        );
        anyhow::ensure!(
            !economic_state_directory.as_os_str().is_empty(),
            "durable economic directory is required"
        );
        if !account.scope.endpoints().is_loopback() {
            anyhow::ensure!(
                account.namespace.matches_credential(&account.credential),
                "production identity must bind the actual credential authority"
            );
            anyhow::ensure!(
                public
                    .economics()
                    .values()
                    .all(|e| e.source != BackpackEconomicsSource::Synthetic),
                "synthetic economics cannot authorize production"
            );
        }
        authority.validate()?;
        anyhow::ensure!(
            authority.max_order_notional <= rust_decimal::Decimal::from(100)
                && authority.max_reserved_notional <= rust_decimal::Decimal::from(500)
                && authority.max_reserved_margin <= rust_decimal::Decimal::from(500)
                && authority.max_unsettled_orders <= 64,
            "production authority exceeds the native USD 100 per order / USD 500 aggregate envelope"
        );
        anyhow::ensure!(
            !mutation.budget.is_zero() && mutation.budget <= std::time::Duration::from_secs(60),
            "invalid mutation budget"
        );
        Ok(Self {
            account,
            public,
            authority,
            mutation,
            economic_state_directory,
            control: BackpackProductionControl::default(),
        })
    }
    #[must_use]
    pub fn control(&self) -> BackpackProductionControl {
        self.control.clone()
    }
}
impl ClientConfig for BackpackProductionExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Registers one native writer; every financial command still originates in ExecutionEngine.
#[derive(Debug, Default)]
pub struct BackpackProductionExecutionClientFactory;
impl ExecutionClientFactory for BackpackProductionExecutionClientFactory {
    fn create(
        &self,
        trader: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let plan = config
            .as_any()
            .downcast_ref::<BackpackProductionExecutionClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("invalid production execution configuration"))?;
        plan.control.check_unattached()?;
        anyhow::ensure!(
            plan.public
                .telemetry()
                .owns_scope(plan.account.scope.endpoints(), &plan.account.quota),
            "public data owner must be registered first"
        );
        let mut account = plan.account.clone();
        account.restricted = Some(BackpackLoopbackExecutionMode {
            authority: plan.authority.clone(),
            mutation_policy: plan.mutation,
            public_telemetry: plan.public.telemetry().clone(),
            production: true,
        });
        let mut client = BackpackExecutionClient::new(trader, name, account, cache)?;
        client.configure_economic_consumer(&plan.economic_state_directory)?;
        client.attach_production_control(&plan.control)?;
        Ok(Box::new(client))
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    fn config_type(&self) -> &'static str {
        "BackpackProductionExecutionClientConfig"
    }
}
