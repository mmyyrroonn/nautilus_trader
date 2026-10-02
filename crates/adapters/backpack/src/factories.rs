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

//! Native registry factory for credential-free Backpack public sessions.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    config::BackpackDataClientConfig, data::BackpackDataClient, data_error::BackpackDataError,
    http::quota::BackpackQuota,
};
/// Creates public data owners sharing this factory's caller-owned REST quota.
#[derive(Clone, Debug, Default)]
pub struct BackpackDataClientFactory {
    quota: BackpackQuota,
}
impl BackpackDataClientFactory {
    /// Creates a public-only factory without network or credential lookup.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Shares an explicit REST scope with future account/execution factories.
    #[must_use]
    pub fn with_quota(quota: BackpackQuota) -> Self {
        Self { quota }
    }
}
impl DataClientFactory for BackpackDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        if name.trim().is_empty() {
            return Err(BackpackDataError::Configuration("empty client name").into());
        }
        let config = config
            .as_any()
            .downcast_ref::<BackpackDataClientConfig>()
            .ok_or(BackpackDataError::Configuration("factory config type"))?;
        Ok(Box::new(BackpackDataClient::with_quota(
            ClientId::new_checked(name)
                .map_err(|_| BackpackDataError::Configuration("invalid client name"))?,
            config.clone(),
            self.quota.clone(),
        )?))
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    fn config_type(&self) -> &'static str {
        "BackpackDataClientConfig"
    }
}
