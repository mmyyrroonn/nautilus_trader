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
//! Aster's Python surface is intentionally narrow: constants, environment selection,
//! configuration, and the data and execution client factories. The market-data client is the
//! Binance USD-M client; execution is Aster's own EIP-712 signed client. Both are consumed
//! through the Rust trait surface.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

pub mod config;
pub mod factories;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::{ASTER, ASTER_CLIENT_ID, ASTER_VENUE},
        enums::AsterEnvironment,
    },
    config::{AsterDataClientConfig, AsterExecutionClientConfig},
    factories::{AsterDataClientFactory, AsterExecutionClientFactory},
};

#[expect(clippy::needless_pass_by_value)]
fn extract_aster_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<AsterDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AsterDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_aster_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<AsterDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AsterDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_aster_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<AsterExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AsterExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_aster_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<AsterExecutionClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AsterExecutionClientConfig: {e}"
        ))),
    }
}

/// Aster adapter Python module.
///
/// Exposed through `nautilus_trader.adapters.aster`.
#[pymodule]
pub fn aster(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(ASTER), ASTER)?;
    m.add(stringify!(ASTER_CLIENT_ID), *ASTER_CLIENT_ID)?;
    m.add(stringify!(ASTER_VENUE), *ASTER_VENUE)?;
    m.add_class::<AsterEnvironment>()?;
    m.add_class::<AsterDataClientConfig>()?;
    m.add_class::<AsterDataClientFactory>()?;
    m.add_class::<AsterExecutionClientConfig>()?;
    m.add_class::<AsterExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(ASTER.to_string(), extract_aster_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Aster data factory extractor: {e}"
        )));
    }

    if let Err(e) =
        registry.register_exec_factory_extractor(ASTER.to_string(), extract_aster_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Aster exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "AsterDataClientConfig".to_string(),
        extract_aster_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Aster data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "AsterExecutionClientConfig".to_string(),
        extract_aster_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Aster exec config extractor: {e}"
        )));
    }

    Ok(())
}
