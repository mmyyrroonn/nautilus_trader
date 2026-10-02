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

//! Standard native execution registry projection. Every command remains read-only.
use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
use nautilus_core::python::to_pyvalue_err;
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use super::account::PyBackpackExecutionClientConfig;
use crate::execution_client::BackpackExecutionClientFactory;

/// Native read-only execution factory; construction performs no I/O.
#[pyclass(
    name = "BackpackExecutionClientFactory",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug, Default)]
pub struct PyBackpackExecutionClientFactory;
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    #[getter]
    fn config_type(&self) -> &'static str {
        "BackpackExecutionClientConfig"
    }
    /// Compiled support only, never live account verification or economic application ACK.
    fn capabilities_json(&self) -> &'static str {
        r#"{"schema_version":1,"public_market_data":false,"read_only_account":true,"restricted_execution":false,"production_writes":false,"private_subscription_confirmed":false,"durable_economic_ack":false}"#
    }
    fn __repr__(&self) -> &'static str {
        "BackpackExecutionClientFactory(<read-only>)"
    }
}
#[expect(clippy::needless_pass_by_value, reason = "registry callback signature")]
fn extract_factory(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn ExecutionClientFactory>> {
    value
        .extract::<PyBackpackExecutionClientFactory>(py)
        .map_err(|_| to_pyvalue_err("invalid BackpackExecutionClientFactory"))?;
    Ok(Box::new(BackpackExecutionClientFactory))
}
#[expect(clippy::needless_pass_by_value, reason = "registry callback signature")]
fn extract_config(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    let wrapper = value
        .extract::<PyBackpackExecutionClientConfig>(py)
        .map_err(|_| to_pyvalue_err("invalid BackpackExecutionClientConfig"))?;
    Ok(Box::new(wrapper.inner))
}
pub(super) fn register_execution() -> PyResult<()> {
    let registry = get_global_pyo3_registry();
    registry
        .register_exec_factory_extractor("BACKPACK".into(), extract_factory)
        .map_err(|_| to_pyvalue_err("Backpack execution factory registration failed"))?;
    registry
        .register_config_extractor("BackpackExecutionClientConfig".into(), extract_config)
        .map_err(|_| to_pyvalue_err("Backpack account configuration registration failed"))?;
    Ok(())
}
