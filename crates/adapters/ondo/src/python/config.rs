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

//! Python bindings for Ondo Perps configuration.

use nautilus_model::identifiers::{AccountId, InstrumentId};
use pyo3::{prelude::*, pymethods};

use crate::{
    common::enums::OndoEnvironment,
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoDataClientConfig {
    /// Configuration for the Ondo Perps live market-data client.
    ///
    /// This surface carries no credential member at all: the data client reads public market
    /// data and never signs a request, so there is nothing to pass. `deny_unknown_fields` on the
    /// Rust type means an `api_key`, `secret`, `token` or `passphrase` keyword is rejected rather
    /// than ignored.
    ///
    /// `load_ids` narrows the published instruments to the listed IDs (e.g.
    /// `NVDA-USD-PERP.ONDO`); an empty list loads every market the venue publishes. Loading never
    /// widens a non-empty list into the whole venue.
    ///
    /// `base_url_http` and `base_url_ws` exist for the official host of the selected environment
    /// or an explicit local test server, and must never be used to reach production from a
    /// sandbox configuration.
    ///
    /// `raw_md_run_id` is the run id the raw public-frame recording must carry. The application sets
    /// it to its process stamp - the same string every tape record of the run carries - so the raw
    /// frames and the tape join by construction; without it the recorder derives the run id from
    /// `raw_md_path`'s parent directory name and says so in the run header.
    #[new]
    #[pyo3(signature = (
        environment = None,
        load_ids = None,
        base_url_http = None,
        base_url_ws = None,
        http_timeout_secs = None,
        ws_heartbeat_secs = None,
        book_limit = None,
        raw_md_path = None,
        raw_md_run_id = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<OndoEnvironment>,
        load_ids: Option<Vec<InstrumentId>>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_heartbeat_secs: Option<u64>,
        book_limit: Option<u32>,
        raw_md_path: Option<String>,
        raw_md_run_id: Option<String>,
    ) -> PyResult<Self> {
        let defaults = Self::default();

        Ok(Self {
            environment: environment.unwrap_or(defaults.environment),
            load_ids: load_ids.unwrap_or(defaults.load_ids),
            base_url_http: base_url_http.or(defaults.base_url_http),
            base_url_ws: base_url_ws.or(defaults.base_url_ws),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_heartbeat_secs: ws_heartbeat_secs.unwrap_or(defaults.ws_heartbeat_secs),
            book_limit: book_limit.unwrap_or(defaults.book_limit),
            raw_md_path: raw_md_path.or(defaults.raw_md_path),
            raw_md_run_id: raw_md_run_id.or(defaults.raw_md_run_id),
        })
    }

    /// Never renders a credential: this configuration holds none.
    fn __repr__(&self) -> String {
        stringify!(OndoDataClientConfig).to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoExecutionClientConfig {
    /// Configuration for the Ondo Perps live execution client.
    ///
    /// `environment` defaults to `SANDBOX`, and production is not reachable: the authenticated
    /// surface is refused for it before a credential is read, whatever `base_url_http` carries.
    ///
    /// `account_id` is required. `api_key` and `api_secret` are optional explicit overrides - the
    /// same pair the Aster execution configuration takes - and when either is absent the client
    /// reads `ONDO_SANDBOX_API_KEY` and `ONDO_SANDBOX_API_SECRET` from the environment instead,
    /// erroring if they are missing rather than falling back to another account. Neither the key
    /// nor the secret is readable from Python: there is no property for either and `__repr__`
    /// renders neither.
    ///
    /// `allow_production_orders` exists so that asking for production order entry can be refused
    /// by name. Setting it does not enable anything: this phase returns an unsupported error.
    ///
    /// `dms_timeout_secs` and `reconcile_interval_secs` fix the configuration surface for the
    /// dead man's switch and the reconciliation interval that a later phase arms.
    #[new]
    #[pyo3(signature = (
        environment = None,
        account_id = None,
        api_key = None,
        api_secret = None,
        base_url_http = None,
        http_timeout_secs = None,
        dms_timeout_secs = None,
        reconcile_interval_secs = None,
        allow_production_orders = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<OndoEnvironment>,
        account_id: Option<AccountId>,
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url_http: Option<String>,
        http_timeout_secs: Option<u64>,
        dms_timeout_secs: Option<u64>,
        reconcile_interval_secs: Option<u64>,
        allow_production_orders: Option<bool>,
    ) -> Self {
        let defaults = Self::default();

        Self {
            environment: environment.unwrap_or(defaults.environment),
            account_id,
            api_key,
            api_secret,
            base_url_http: base_url_http.or(defaults.base_url_http),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            dms_timeout_secs: dms_timeout_secs.unwrap_or(defaults.dms_timeout_secs),
            reconcile_interval_secs: reconcile_interval_secs
                .unwrap_or(defaults.reconcile_interval_secs),
            allow_production_orders: allow_production_orders
                .unwrap_or(defaults.allow_production_orders),
        }
    }

    /// Never renders a credential: the key and the secret are both redacted by the Rust `Debug`.
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
