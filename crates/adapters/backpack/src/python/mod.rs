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

//! Native public factory projection and registry extraction.
#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented by checked native constructors"
)]

pub mod config;
pub mod replay;

use config::{PyBackpackDataClientConfig, PyBackpackInstrumentEconomics};
use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::consts::{BACKPACK, BACKPACK_CLIENT_ID, BACKPACK_VENUE},
    factories::BackpackDataClientFactory,
};

/// Native data factory; constructing it performs no I/O.
#[pyclass(
    name = "BackpackDataClientFactory",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug, Default)]
pub struct PyBackpackDataClientFactory {
    inner: BackpackDataClientFactory,
}
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyBackpackDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: BackpackDataClientFactory::new(),
        }
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    #[getter]
    fn config_type(&self) -> &'static str {
        "BackpackDataClientConfig"
    }
    /// Reports compiled support only; consult the configuration telemetry for run evidence.
    fn capabilities_json(&self) -> &'static str {
        r#"{"schema_version":1,"public_market_data":true,"read_only_account":false,"restricted_execution":false,"funding_rate_fraction":false,"full_depth_coverage":false}"#
    }
    fn __repr__(&self) -> &'static str {
        "BackpackDataClientFactory"
    }
}

#[expect(clippy::needless_pass_by_value, reason = "registry callback signature")]
fn extract_data_factory(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn DataClientFactory>> {
    let wrapper = value
        .extract::<PyBackpackDataClientFactory>(py)
        .map_err(|_| to_pyvalue_err("invalid BackpackDataClientFactory"))?;
    Ok(Box::new(wrapper.inner))
}
#[expect(clippy::needless_pass_by_value, reason = "registry callback signature")]
fn extract_data_config(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    let wrapper = value
        .extract::<PyBackpackDataClientConfig>(py)
        .map_err(|_| to_pyvalue_err("invalid BackpackDataClientConfig"))?;
    Ok(Box::new(wrapper.inner))
}

/// Exposes implemented native public capabilities through the normal adapter registry.
#[pymodule]
pub fn backpack(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BACKPACK", BACKPACK)?;
    m.add("BACKPACK_CLIENT_ID", *BACKPACK_CLIENT_ID)?;
    m.add("BACKPACK_VENUE", *BACKPACK_VENUE)?;
    m.add_class::<PyBackpackInstrumentEconomics>()?;
    m.add_class::<PyBackpackDataClientConfig>()?;
    m.add_class::<PyBackpackDataClientFactory>()?;
    m.add_class::<replay::PyBackpackPublicReplay>()?;
    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor("BACKPACK".to_string(), extract_data_factory)
        .map_err(to_pyruntime_err)?;
    registry
        .register_config_extractor("BackpackDataClientConfig".to_string(), extract_data_config)
        .map_err(to_pyruntime_err)?;
    Ok(())
}
