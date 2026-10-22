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

//! Factories connecting Arcus clients to LiveNode.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use crate::{
    config::{ArcusDataClientConfig, ArcusExecutionClientConfig},
    consts::{ARCUS, ARCUS_VENUE},
    data::ArcusDataClient,
    execution::ArcusExecutionClient,
};

/// Creates Arcus public data clients.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.arcus", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.arcus")
)]
pub struct ArcusDataClientFactory;

impl DataClientFactory for ArcusDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<ArcusDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("Expected ArcusDataClientConfig"))?
            .clone();
        config.validate()?;
        Ok(Box::new(ArcusDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        ARCUS
    }

    fn config_type(&self) -> &'static str {
        stringify!(ArcusDataClientConfig)
    }
}

/// Creates Arcus netting margin execution clients.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.arcus", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.arcus")
)]
pub struct ArcusExecutionClientFactory;

impl ExecutionClientFactory for ArcusExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<ArcusExecutionClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("Expected ArcusExecutionClientConfig"))?
            .clone();
        config.validate()?;
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *ARCUS_VENUE,
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache,
        );
        Ok(Box::new(ArcusExecutionClient::new(core, config)?))
    }

    fn name(&self) -> &'static str {
        ARCUS
    }

    fn config_type(&self) -> &'static str {
        stringify!(ArcusExecutionClientConfig)
    }
}
