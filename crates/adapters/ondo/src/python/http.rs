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

//! Python bindings for the public Ondo Perps REST client.
//!
//! This is the surface the application's `ondo_preflight` CLI reads market metadata through. It
//! wraps [`OndoHttpClient`] (the plan's single public REST client) rather than re-implementing any
//! request: the environment resolves the endpoint set through
//! [`crate::common::consts::http_base_url`], the request targets are
//! [`crate::http::query`]'s path constants, and the schema check is
//! [`crate::http::models::parse_markets`].
//!
//! No method takes a credential and none is read from the environment or a `.env` file, and every
//! decimal the venue sends stays the string the venue sent: JSON numbers that are not integers are
//! passed through as `decimal.Decimal`, never as an `f64`.
//!
//! Constructing the client performs no I/O: it builds an HTTP connection pool with no open socket
//! and no request, so it is safe to construct offline. Every method that talks to the venue is a
//! Python awaitable.

use std::str::FromStr;

use nautilus_core::{python::to_pyvalue_err, time::get_atomic_clock_realtime};
use nautilus_model::{identifiers::InstrumentId, python::instruments::instrument_any_to_pyobject};
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    types::{PyDict, PyList},
};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    common::{consts::http_base_url, enums::OndoEnvironment},
    http::{
        client::OndoHttpClient,
        models::{parse_instruments, parse_markets},
        query::{MARKETS_PATH, OndoRequestTarget},
    },
};

/// Converts a decoded JSON document into Python objects, preserving exact decimals.
///
/// A JSON string stays a string, which is what the venue's `baseIncrement`, `quoteIncrement` and
/// fee members are. A JSON integer becomes an `int`. Any other JSON number becomes a
/// `decimal.Decimal` built from its exact lexeme, so nothing routes through `f64`; a number wider
/// than `Decimal` can hold is passed through as its verbatim text rather than rounded.
fn json_value_to_pyobject(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(value) => (*value).into_py_any(py),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return value.into_py_any(py);
            }

            if let Some(value) = number.as_u64() {
                return value.into_py_any(py);
            }

            let text = number.to_string();

            match Decimal::from_str(&text) {
                Ok(value) => value.into_py_any(py),
                Err(_) => text.into_py_any(py),
            }
        }
        Value::String(value) => value.as_str().into_py_any(py),
        Value::Array(items) => {
            let mut objects = Vec::with_capacity(items.len());

            for item in items {
                objects.push(json_value_to_pyobject(py, item)?);
            }

            PyList::new(py, objects)?.into_py_any(py)
        }
        Value::Object(members) => {
            let dict = PyDict::new(py);

            for (key, member) in members {
                dict.set_item(key, json_value_to_pyobject(py, member)?)?;
            }

            dict.into_py_any(py)
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoHttpClient {
    /// Provides a public HTTP client for the [Ondo Perps](https://docs.ondoperps.xyz) REST API.
    ///
    /// `environment` selects the endpoint set (`PRODUCTION` or `SANDBOX`); `timeout_secs` is the
    /// per-request timeout and defaults to 15. The client holds no credentials, and constructing
    /// it performs no request: it opens a connection pool and nothing else.
    ///
    /// The client's own rate budget is a conservative one request per second. The factories give
    /// the clients they create the *environment's* budget, which the data and execution surfaces of
    /// one environment share; a standalone client built here has its own, so it does not contend
    /// with a running node.
    #[new]
    #[pyo3(signature = (environment, timeout_secs = 15))]
    fn py_new(environment: OndoEnvironment, timeout_secs: u64) -> PyResult<Self> {
        Self::builder()
            .base_url(http_base_url(environment).to_string())
            .timeout_secs(timeout_secs)
            .build()
            .map_err(to_pyvalue_err)
    }

    /// Calls `GET /status` and returns the venue's response as a `dict`.
    ///
    /// The body this hands back is only as informative as [`OndoHttpClient::get_status`], which
    /// unwraps a `GenericResponse` `result` member when one is present: a body that carried the
    /// envelope and one that did not both arrive here looking the same, so read that method's note
    /// before treating anything about this endpoint's shape as settled. `/status` has answered 200
    /// on every call this project has made, always returning `marketStatus` and nothing else.
    /// Nothing here is read as a price, quantity or fee; this endpoint is diagnostics only.
    ///
    /// # Errors
    ///
    /// Raises on a transport or classification error, or on a body that is not UTF-8 JSON.
    #[pyo3(name = "get_status")]
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing",)))]
    fn py_get_status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status = client.get_status().await.map_err(to_pyvalue_err)?;

            Python::attach(|py| json_value_to_pyobject(py, &status))
        })
    }

    /// Calls `GET /v1/markets` and returns the unwrapped `result` object as a `dict`.
    ///
    /// The body is checked by the crate's own fail-closed market schema rule
    /// ([`crate::http::models::parse_markets`]) before it is handed over, so a response that is
    /// not market metadata raises instead of being read as empty metadata. The returned mapping is
    /// the `result` member the venue sent — the `GenericResponse` envelope's `success` flag and
    /// nesting are not exposed — and every increment and fee keeps the decimal string the venue
    /// sent, because the member is unwrapped as JSON text rather than re-serialized.
    ///
    /// # Errors
    ///
    /// Raises on a transport or classification error, on a body that is not market metadata per the
    /// rules above, or on a body that is not UTF-8 JSON.
    #[pyo3(name = "get_markets")]
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing",)))]
    fn py_get_markets<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let body = client
                .get_raw(&OndoRequestTarget::new(MARKETS_PATH))
                .await
                .map_err(to_pyvalue_err)?;
            let payload = std::str::from_utf8(&body).map_err(to_pyvalue_err)?;

            parse_markets(payload).map_err(to_pyvalue_err)?;

            let document: Value = serde_json::from_str(payload).map_err(to_pyvalue_err)?;

            let result = document
                .get("result")
                .filter(|member| !member.is_null())
                .cloned()
                .ok_or_else(|| {
                    to_pyvalue_err(format!(
                        "{MARKETS_PATH} response carries no 'result' object to return"
                    ))
                })?;

            Python::attach(|py| json_value_to_pyobject(py, &result))
        })
    }

    /// Calls `GET /v1/perps/contracts` and returns one `dict` per contract.
    ///
    /// The response envelope is unwrapped by the Rust client, which keeps each contract as its
    /// exact JSON text; this method decodes that text per entry. The contract schema was sampled
    /// live twice on 2026-09-14 (`reports/ondo-acceptance/`: `rest-capture/contracts.json` and
    /// `preflight/contracts.json`); no member is mapped to a field here - each is handed back as it
    /// arrived.
    ///
    /// # Errors
    ///
    /// Raises on a transport or classification error, on an envelope that is not the documented
    /// `GenericResponse` shape, and on a venue that publishes no contracts at all.
    #[pyo3(name = "get_contracts")]
    #[gen_stub(override_return_type(type_repr = "list[dict[str, typing.Any]]", imports = ("typing",)))]
    fn py_get_contracts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contracts = client.get_contracts().await.map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let mut objects = Vec::with_capacity(contracts.len());

                for contract in &contracts {
                    let document: Value =
                        serde_json::from_str(contract.get()).map_err(to_pyvalue_err)?;
                    objects.push(json_value_to_pyobject(py, &document)?);
                }

                Ok(PyList::new(py, objects)?.into_any().unbind())
            })
        })
    }

    /// Loads Nautilus instrument definitions for `load_ids` from `GET /v1/markets`.
    ///
    /// `load_ids` are Nautilus instrument IDs as strings, e.g. `"NVDA-USD-PERP.ONDO"`. An empty
    /// list loads every market the venue publishes; a non-empty list is never widened, and a
    /// requested market the payload does not carry fails the whole load rather than silently
    /// loading fewer instruments. `ts_init` for the built instruments is the local time of this
    /// call.
    ///
    /// The conversion is [`crate::http::models::parse_instruments`] — the crate's single schema and
    /// precision boundary — so precision comes from the venue's own increments, and a market whose
    /// status cannot be classified fails the load.
    ///
    /// # Errors
    ///
    /// Raises on a transport or classification error, or when the payload carries no market data,
    /// does not carry a requested instrument, carries an unusable increment for one, or reports a
    /// status for one that cannot be classified.
    #[pyo3(name = "load_instrument_definitions")]
    #[gen_stub(override_return_type(type_repr = "list[typing.Any]", imports = ("typing",)))]
    fn py_load_instrument_definitions<'py>(
        &self,
        py: Python<'py>,
        load_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let body = client
                .get_raw(&OndoRequestTarget::new(MARKETS_PATH))
                .await
                .map_err(to_pyvalue_err)?;
            let payload = std::str::from_utf8(&body).map_err(to_pyvalue_err)?;

            let instrument_ids: Vec<InstrumentId> = load_ids
                .iter()
                .map(|load_id| InstrumentId::from(load_id.as_str()))
                .collect();

            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let instruments =
                parse_instruments(payload, &instrument_ids, ts_init).map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let mut objects = Vec::with_capacity(instruments.len());

                for instrument in instruments {
                    objects.push(instrument_any_to_pyobject(py, instrument)?);
                }

                Ok(PyList::new(py, objects)?.into_any().unbind())
            })
        })
    }
}
