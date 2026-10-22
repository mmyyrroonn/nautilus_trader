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

//! Python constructors for Arcus configuration.

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use pyo3::prelude::*;

use crate::config::{ArcusDataClientConfig, ArcusEnvironment, ArcusExecutionClientConfig};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ArcusEnvironment {
    const fn __hash__(&self) -> isize {
        *self as isize
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ArcusDataClientConfig {
    /// Configures public data, with optional instrument selection and endpoint overrides.
    #[new]
    #[pyo3(signature = (environment=None, load_ids=None, base_url_http=None, base_url_ws=None,
        http_timeout_secs=None, ws_idle_timeout_secs=None, queue_capacity=None))]
    fn py_new(
        environment: Option<ArcusEnvironment>,
        load_ids: Option<Vec<InstrumentId>>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_idle_timeout_secs: Option<u64>,
        queue_capacity: Option<usize>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            environment: environment.unwrap_or(defaults.environment),
            load_ids: load_ids.unwrap_or(defaults.load_ids),
            base_url_http,
            base_url_ws,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_idle_timeout_secs: ws_idle_timeout_secs.unwrap_or(defaults.ws_idle_timeout_secs),
            queue_capacity: queue_capacity.unwrap_or(defaults.queue_capacity),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ArcusExecutionClientConfig {
    /// Configures live execution using a registered Ed25519 API seed.
    ///
    /// Unspecified credentials use environment variables. Mainnet uses ARCUS_API_SECRET,
    /// ARCUS_API_KEY and ARCUS_ADDRESS; testnet uses the ARCUS_TESTNET prefix.
    /// Secrets have no getters and are excluded from representations and serialization.
    #[new]
    #[pyo3(signature = (account_id=None, environment=None, address=None, subaccount_id=None,
        api_key=None, api_secret=None, load_ids=None, base_url_http=None, base_url_ws=None,
        http_timeout_secs=None, ws_idle_timeout_secs=None, queue_capacity=None,
        reconciliation_interval_secs=None, market_slippage_bps=None))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        account_id: Option<AccountId>,
        environment: Option<ArcusEnvironment>,
        address: Option<String>,
        subaccount_id: Option<u8>,
        api_key: Option<String>,
        api_secret: Option<String>,
        load_ids: Option<Vec<InstrumentId>>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_idle_timeout_secs: Option<u64>,
        queue_capacity: Option<usize>,
        reconciliation_interval_secs: Option<u64>,
        market_slippage_bps: Option<u32>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            account_id: account_id.unwrap_or(defaults.account_id),
            environment: environment.unwrap_or(defaults.environment),
            address,
            subaccount_id: subaccount_id.unwrap_or(defaults.subaccount_id),
            api_key,
            api_secret,
            load_ids: load_ids.unwrap_or(defaults.load_ids),
            base_url_http,
            base_url_ws,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_idle_timeout_secs: ws_idle_timeout_secs.unwrap_or(defaults.ws_idle_timeout_secs),
            queue_capacity: queue_capacity.unwrap_or(defaults.queue_capacity),
            reconciliation_interval_secs: reconciliation_interval_secs
                .unwrap_or(defaults.reconciliation_interval_secs),
            market_slippage_bps: market_slippage_bps.unwrap_or(defaults.market_slippage_bps),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
