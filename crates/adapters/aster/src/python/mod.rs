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
//! configuration, and the data client factory. The underlying market-data client is the
//! Binance USD-M client, consumed through the Rust trait surface.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

pub mod config;
pub mod factories;

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::{ASTER, ASTER_CLIENT_ID, ASTER_VENUE},
        enums::AsterEnvironment,
    },
    config::AsterDataClientConfig,
    factories::AsterDataClientFactory,
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

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(ASTER.to_string(), extract_aster_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Aster data factory extractor: {e}"
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

    Ok(())
}
