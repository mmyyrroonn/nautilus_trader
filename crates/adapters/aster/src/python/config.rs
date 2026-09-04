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
use nautilus_model::identifiers::Venue;
use pyo3::{prelude::*, pymethods};

use crate::{common::enums::AsterEnvironment, config::AsterDataClientConfig};

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
