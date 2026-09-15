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

//! Python bindings for the Ondo Perps client factories.

use pyo3::prelude::*;

use crate::{
    common::consts::ONDO,
    factories::{OndoDataClientFactory, OndoExecutionClientFactory},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoDataClientFactory {
    /// Factory for creating Ondo Perps data clients.
    ///
    /// One factory is created per adapter environment and owns one REST budget, which every
    /// client it creates draws on (plan §4.4). Registering the factory with a Python `LiveNode`
    /// is what makes the `ONDO` client name resolvable; the data client's configuration is the
    /// `OndoDataClientConfig` above.
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    /// Returns the venue name this factory registers as, `"ONDO"`.
    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ONDO
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoExecutionClientFactory {
    /// Factory for creating Ondo Perps execution clients.
    ///
    /// One factory is created per adapter environment and owns one REST budget, which every client
    /// it creates draws on (plan §4.4); pass the same budget the data factory holds so the two
    /// surfaces pace against one bucket. The client it builds is the native Rust execution client
    /// behind the `ExecutionClient` trait, and the credential is resolved inside it - this factory
    /// carries none, and its `__repr__` renders none.
    ///
    /// Registering the factory with a Python `LiveNode` is what makes the `ONDO` execution client
    /// resolvable; its configuration is the `OndoExecutionClientConfig` above.
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    /// Returns the venue name this factory registers as, `"ONDO"`.
    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ONDO
    }
}
