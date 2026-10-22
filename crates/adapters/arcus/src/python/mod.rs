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

//! Python configuration and LiveNode factory registration for Arcus.

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
    config::{ArcusDataClientConfig, ArcusEnvironment, ArcusExecutionClientConfig},
    consts::{ARCUS, ARCUS_CLIENT_ID, ARCUS_VENUE},
    factories::{ArcusDataClientFactory, ArcusExecutionClientFactory},
};

#[expect(clippy::needless_pass_by_value)]
fn extract_arcus_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<ArcusDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ArcusDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_arcus_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ArcusDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ArcusDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_arcus_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<ArcusExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ArcusExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_arcus_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ArcusExecutionClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ArcusExecutionClientConfig: {e}"
        ))),
    }
}

/// Arcus adapter Python module.
///
/// Exposed through `nautilus_trader.adapters.arcus`.
#[pymodule]
pub fn arcus(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::instruments::settlement_currency().map_err(to_pyruntime_err)?;
    m.add(stringify!(ARCUS), ARCUS)?;
    m.add(stringify!(ARCUS_CLIENT_ID), *ARCUS_CLIENT_ID)?;
    m.add(stringify!(ARCUS_VENUE), *ARCUS_VENUE)?;
    m.add_class::<ArcusEnvironment>()?;
    m.add_class::<ArcusDataClientConfig>()?;
    m.add_class::<ArcusDataClientFactory>()?;
    m.add_class::<ArcusExecutionClientConfig>()?;
    m.add_class::<ArcusExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(ARCUS.to_string(), extract_arcus_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Arcus data factory extractor: {e}"
        )));
    }

    if let Err(e) =
        registry.register_exec_factory_extractor(ARCUS.to_string(), extract_arcus_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Arcus exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ArcusDataClientConfig".to_string(),
        extract_arcus_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Arcus data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ArcusExecutionClientConfig".to_string(),
        extract_arcus_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Arcus exec config extractor: {e}"
        )));
    }

    Ok(())
}
