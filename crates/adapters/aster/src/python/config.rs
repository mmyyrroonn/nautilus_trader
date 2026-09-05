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

//! Python bindings for Aster configuration.

use nautilus_binance::config::BinanceInstrumentProviderConfig;
use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::{AccountId, Venue};
use pyo3::{prelude::*, pymethods};

use crate::{
    common::enums::AsterEnvironment,
    config::{AsterDataClientConfig, AsterExecutionClientConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl AsterDataClientConfig {
    /// Configuration for the Aster live market-data client.
    ///
    /// Aster's Futures API is Binance-USD-M compatible, so instrument loading reuses
    /// `BinanceInstrumentProviderConfig`. Its `load_ids` entries must use the `ASTER`
    /// venue, e.g. `NVDAUSDT-PERP.ASTER`.
    #[new]
    #[pyo3(signature = (
        environment = None,
        base_url_http = None,
        base_url_ws = None,
        instrument_provider = None,
        instrument_refresh_interval_secs = None,
        instrument_status_poll_secs = None,
        proxy_url = None,
        venue = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<AsterEnvironment>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        instrument_provider: Option<BinanceInstrumentProviderConfig>,
        instrument_refresh_interval_secs: Option<u64>,
        instrument_status_poll_secs: Option<u64>,
        proxy_url: Option<String>,
        venue: Option<Venue>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            environment: environment.unwrap_or(defaults.environment),
            base_url_http: base_url_http.or(defaults.base_url_http),
            base_url_ws: base_url_ws.or(defaults.base_url_ws),
            instrument_provider: instrument_provider.unwrap_or(defaults.instrument_provider),
            instrument_refresh_interval_secs: instrument_refresh_interval_secs
                .unwrap_or(defaults.instrument_refresh_interval_secs),
            instrument_status_poll_secs: instrument_status_poll_secs
                .unwrap_or(defaults.instrument_status_poll_secs),
            proxy_url: proxy_url.or(defaults.proxy_url),
            venue: venue.or(defaults.venue),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        stringify!(AsterDataClientConfig).to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl AsterExecutionClientConfig {
    /// Configuration for the Aster live execution client.
    ///
    /// Aster's Futures V3 trading endpoints are EIP-712 signed, so this client takes an API
    /// wallet private key rather than an API key/secret pair. `signer_private_key` falls back
    /// to the `ASTER_SIGNER_PRIVATE_KEY` environment variable, `signer_address` to
    /// `ASTER_SIGNER_ADDRESS`, and `user_address` to `ASTER_USER_ADDRESS`.
    ///
    /// The private key is accepted but never exposed: it has no Python getter and does not
    /// appear in `repr()`.
    #[new]
    #[pyo3(signature = (
        account_id = None,
        environment = None,
        user_address = None,
        signer_address = None,
        signer_private_key = None,
        base_url_http = None,
        base_url_ws = None,
        instrument_provider = None,
        http_timeout_secs = None,
        ws_heartbeat_secs = None,
        ws_connect_timeout_secs = None,
        proxy_url = None,
        treat_expired_as_canceled = None,
        venue = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        account_id: Option<AccountId>,
        environment: Option<AsterEnvironment>,
        user_address: Option<String>,
        signer_address: Option<String>,
        signer_private_key: Option<String>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        instrument_provider: Option<BinanceInstrumentProviderConfig>,
        http_timeout_secs: Option<u64>,
        ws_heartbeat_secs: Option<u64>,
        ws_connect_timeout_secs: Option<u64>,
        proxy_url: Option<String>,
        treat_expired_as_canceled: Option<bool>,
        venue: Option<Venue>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            account_id: account_id.unwrap_or(defaults.account_id),
            environment: environment.unwrap_or(defaults.environment),
            user_address: user_address.or(defaults.user_address),
            signer_address: signer_address.or(defaults.signer_address),
            signer_private_key: signer_private_key.or(defaults.signer_private_key),
            base_url_http: base_url_http.or(defaults.base_url_http),
            base_url_ws: base_url_ws.or(defaults.base_url_ws),
            instrument_provider: instrument_provider.unwrap_or(defaults.instrument_provider),
            http_timeout_secs: http_timeout_secs.or(defaults.http_timeout_secs),
            ws_heartbeat_secs: ws_heartbeat_secs.or(defaults.ws_heartbeat_secs),
            ws_connect_timeout_secs: ws_connect_timeout_secs.or(defaults.ws_connect_timeout_secs),
            proxy_url: proxy_url.or(defaults.proxy_url),
            treat_expired_as_canceled: treat_expired_as_canceled
                .unwrap_or(defaults.treat_expired_as_canceled),
            venue: venue.or(defaults.venue),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    /// Returns whether a signing key was supplied on the configuration itself.
    ///
    /// A `False` result does not mean the client cannot sign: the key may still come from the
    /// `ASTER_SIGNER_PRIVATE_KEY` environment variable.
    #[pyo3(name = "has_explicit_credentials")]
    fn py_has_explicit_credentials(&self) -> bool {
        self.has_explicit_credentials()
    }

    /// Never renders the signer private key.
    fn __repr__(&self) -> String {
        stringify!(AsterExecutionClientConfig).to_string()
    }
}
