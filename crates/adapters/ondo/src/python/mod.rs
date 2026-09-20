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

//! Python bindings from `pyo3`.
//!
//! Ondo's Python surface is deliberately narrow: the venue constants, the environment, the data
//! client configuration and its factory, the execution client configuration and its factory, and
//! the public HTTP client the `ondo_preflight` CLI reads market metadata and instruments through.
//!
//! Both clients are consumed through the Rust trait surface, exactly as Aster's and Lighter's are:
//! the factories registered here are what a Python `LiveNode` wires the adapter with, and this
//! phase adds no second execution implementation on the Python side.
//!
//! The credentials live on [`crate::config::OndoExecutionClientConfig`] and nowhere else on this
//! surface: it carries the optional `api_key`/`api_secret` pair (both absent from its `__repr__`,
//! its properties and its serialization) and the `allow_production_orders` flag, which this phase
//! refuses whatever it is set to. The data configuration and the HTTP client accept no key, secret,
//! token or passphrase at all.
//!
//! `ONDO` in this module is the Ondo Perps **venue**, never the `ONDO` crypto asset ticker.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

pub mod config;
pub mod factories;
pub mod http;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::{ONDO, ONDO_CLIENT_ID, ONDO_VENUE},
        enums::OndoEnvironment,
    },
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
    factories::{OndoDataClientFactory, OndoExecutionClientFactory},
    http::client::OndoHttpClient,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_ondo_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<OndoDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract OndoDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_ondo_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<OndoDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract OndoDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_ondo_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<OndoExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract OndoExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_ondo_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<OndoExecutionClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract OndoExecutionClientConfig: {e}"
        ))),
    }
}

/// Ondo Perps adapter Python module.
///
/// Exposed through `nautilus_trader.adapters.ondo`.
#[pymodule]
pub fn ondo(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(ONDO), ONDO)?;
    m.add(stringify!(ONDO_CLIENT_ID), *ONDO_CLIENT_ID)?;
    m.add(stringify!(ONDO_VENUE), *ONDO_VENUE)?;
    m.add_class::<OndoEnvironment>()?;
    m.add_class::<OndoDataClientConfig>()?;
    m.add_class::<OndoDataClientFactory>()?;
    m.add_class::<OndoExecutionClientConfig>()?;
    m.add_class::<crate::production::OndoExecutionEnvelopeConfig>()?;
    m.add_class::<OndoExecutionClientFactory>()?;
    m.add_class::<OndoHttpClient>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) = registry.register_factory_extractor(ONDO.to_string(), extract_ondo_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Ondo data factory extractor: {e}"
        )));
    }

    if let Err(e) =
        registry.register_exec_factory_extractor(ONDO.to_string(), extract_ondo_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Ondo exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_config_extractor("OndoDataClientConfig".to_string(), extract_ondo_data_config)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Ondo data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "OndoExecutionClientConfig".to_string(),
        extract_ondo_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Ondo exec config extractor: {e}"
        )));
    }

    Ok(())
}
