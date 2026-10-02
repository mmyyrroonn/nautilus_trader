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

//! Narrow Python configuration wrappers; native factories own all protocol work.

use std::collections::BTreeMap;

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    common::endpoints::BackpackEndpoints,
    config::{BackpackConfig, BackpackDataClientConfig, BackpackPublicLifecycleConfig},
    instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics},
    parsing::decimal,
};

/// Explicit exact economics; provenance does not establish account verification.
#[pyclass(
    name = "BackpackInstrumentEconomics",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackInstrumentEconomics {
    pub(crate) inner: BackpackInstrumentEconomics,
}

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackInstrumentEconomics {
    /// Validates four exact decimal strings and explicit economic provenance.
    #[new]
    #[pyo3(signature = (margin_init, margin_maint, maker_fee, taker_fee, source, source_reference))]
    fn py_new(
        margin_init: &str,
        margin_maint: &str,
        maker_fee: &str,
        taker_fee: &str,
        source: &str,
        source_reference: String,
    ) -> PyResult<Self> {
        let source = match source {
            "Configured" => BackpackEconomicsSource::Configured,
            "VenueObserved" => BackpackEconomicsSource::VenueObserved,
            "Synthetic" => BackpackEconomicsSource::Synthetic,
            _ => return Err(to_pyvalue_err("unsupported Backpack economics source")),
        };
        let inner = BackpackInstrumentEconomics::new_checked(
            decimal(margin_init, "margin_init").map_err(to_pyvalue_err)?,
            decimal(margin_maint, "margin_maint").map_err(to_pyvalue_err)?,
            decimal(maker_fee, "maker_fee").map_err(to_pyvalue_err)?,
            decimal(taker_fee, "taker_fee").map_err(to_pyvalue_err)?,
            source,
            source_reference,
        )
        .map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }
    #[getter]
    fn margin_init(&self) -> String {
        self.inner.margin_init.to_string()
    }
    #[getter]
    fn margin_maint(&self) -> String {
        self.inner.margin_maint.to_string()
    }
    #[getter]
    fn maker_fee(&self) -> String {
        self.inner.maker_fee.to_string()
    }
    #[getter]
    fn taker_fee(&self) -> String {
        self.inner.taker_fee.to_string()
    }
    #[getter]
    fn source(&self) -> &'static str {
        match self.inner.source {
            BackpackEconomicsSource::Configured => "Configured",
            BackpackEconomicsSource::VenueObserved => "VenueObserved",
            BackpackEconomicsSource::Synthetic => "Synthetic",
        }
    }
    #[getter]
    fn source_reference(&self) -> String {
        self.inner.source_reference.clone()
    }
    fn __repr__(&self) -> &'static str {
        "BackpackInstrumentEconomics"
    }
}

/// Checked public configuration; no credential field or environment lookup exists.
#[pyclass(
    name = "BackpackDataClientConfig",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackDataClientConfig {
    pub(crate) inner: BackpackDataClientConfig,
}

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackDataClientConfig {
    /// Constructs a public plan without clients, credentials, sockets or runtime tasks.
    /// Economics must cover exactly the explicit native-symbol allowlist.
    #[new]
    #[pyo3(signature = (symbols, economics, *, base_url_http=None, base_url_ws=None,
        http_timeout_secs=None, ws_connect_timeout_secs=None, ws_heartbeat_secs=None,
        ws_idle_timeout_secs=None, reconnect_timeout_secs=None, shutdown_timeout_secs=None,
        quote_stale_after_ms=None, depth_snapshot_limit=None, max_buffer_frames=None,
        max_levels_per_side=None, max_ws_message_bytes=None))]
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit checked public lifecycle configuration"
    )]
    fn py_new(
        symbols: Vec<String>,
        economics: BTreeMap<String, PyBackpackInstrumentEconomics>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_connect_timeout_secs: Option<u64>,
        ws_heartbeat_secs: Option<u64>,
        ws_idle_timeout_secs: Option<u64>,
        reconnect_timeout_secs: Option<u64>,
        shutdown_timeout_secs: Option<u64>,
        quote_stale_after_ms: Option<u64>,
        depth_snapshot_limit: Option<usize>,
        max_buffer_frames: Option<usize>,
        max_levels_per_side: Option<usize>,
        max_ws_message_bytes: Option<usize>,
    ) -> PyResult<Self> {
        let endpoints = match (base_url_http, base_url_ws) {
            (None, None) => BackpackEndpoints::production(),
            (Some(http), Some(ws)) => {
                BackpackEndpoints::loopback_override(&http, &ws).map_err(to_pyvalue_err)?
            }
            _ => {
                return Err(to_pyvalue_err(
                    "Backpack loopback overrides require both HTTP and WS origins",
                ));
            }
        };
        let scope =
            BackpackConfig::with_endpoints_checked(symbols, endpoints).map_err(to_pyvalue_err)?;
        let defaults = BackpackPublicLifecycleConfig::default();
        let lifecycle = BackpackPublicLifecycleConfig {
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_connect_timeout_secs: ws_connect_timeout_secs
                .unwrap_or(defaults.ws_connect_timeout_secs),
            ws_heartbeat_secs: ws_heartbeat_secs.unwrap_or(defaults.ws_heartbeat_secs),
            ws_idle_timeout_secs: ws_idle_timeout_secs.unwrap_or(defaults.ws_idle_timeout_secs),
            reconnect_timeout_secs: reconnect_timeout_secs
                .unwrap_or(defaults.reconnect_timeout_secs),
            shutdown_timeout_secs: shutdown_timeout_secs.unwrap_or(defaults.shutdown_timeout_secs),
            quote_stale_after_ms: quote_stale_after_ms.unwrap_or(defaults.quote_stale_after_ms),
            depth_snapshot_limit: depth_snapshot_limit.unwrap_or(defaults.depth_snapshot_limit),
            max_buffer_frames: max_buffer_frames.unwrap_or(defaults.max_buffer_frames),
            max_levels_per_side: max_levels_per_side.unwrap_or(defaults.max_levels_per_side),
            max_ws_message_bytes: max_ws_message_bytes.unwrap_or(defaults.max_ws_message_bytes),
        };
        let inner = BackpackDataClientConfig::new_checked(
            scope,
            economics.into_iter().map(|(s, e)| (s, e.inner)).collect(),
        )
        .map_err(to_pyvalue_err)?
        .with_lifecycle_checked(lifecycle)
        .map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }
    #[getter]
    fn symbols(&self) -> Vec<String> {
        self.inner.scope().symbols().iter().cloned().collect()
    }
    #[getter]
    fn economics(&self) -> BTreeMap<String, PyBackpackInstrumentEconomics> {
        self.inner
            .economics()
            .iter()
            .map(|(s, e)| {
                (
                    s.clone(),
                    PyBackpackInstrumentEconomics { inner: e.clone() },
                )
            })
            .collect()
    }
    #[getter]
    fn base_url_http(&self) -> String {
        self.inner.scope().endpoints().rest_url().to_string()
    }
    #[getter]
    fn base_url_ws(&self) -> String {
        self.inner.scope().endpoints().websocket_url().to_string()
    }
    #[getter]
    fn http_timeout_secs(&self) -> u64 {
        self.inner.lifecycle().http_timeout_secs
    }
    #[getter]
    fn ws_connect_timeout_secs(&self) -> u64 {
        self.inner.lifecycle().ws_connect_timeout_secs
    }
    #[getter]
    fn ws_heartbeat_secs(&self) -> u64 {
        self.inner.lifecycle().ws_heartbeat_secs
    }
    #[getter]
    fn ws_idle_timeout_secs(&self) -> u64 {
        self.inner.lifecycle().ws_idle_timeout_secs
    }
    #[getter]
    fn reconnect_timeout_secs(&self) -> u64 {
        self.inner.lifecycle().reconnect_timeout_secs
    }
    #[getter]
    fn shutdown_timeout_secs(&self) -> u64 {
        self.inner.lifecycle().shutdown_timeout_secs
    }
    #[getter]
    fn quote_stale_after_ms(&self) -> u64 {
        self.inner.lifecycle().quote_stale_after_ms
    }
    #[getter]
    fn depth_snapshot_limit(&self) -> usize {
        self.inner.lifecycle().depth_snapshot_limit
    }
    #[getter]
    fn max_buffer_frames(&self) -> usize {
        self.inner.lifecycle().max_buffer_frames
    }
    #[getter]
    fn max_levels_per_side(&self) -> usize {
        self.inner.lifecycle().max_levels_per_side
    }
    #[getter]
    fn max_ws_message_bytes(&self) -> usize {
        self.inner.lifecycle().max_ws_message_bytes
    }
    /// Returns actual sanitized per-run observations, distinct from capability support.
    fn telemetry_snapshot_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner.telemetry().snapshot()).map_err(to_pyvalue_err)
    }
    fn __repr__(&self) -> &'static str {
        "BackpackDataClientConfig"
    }
}
