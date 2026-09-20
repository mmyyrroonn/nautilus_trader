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

use pyo3::{prelude::*, types::PyDict};

use crate::{
    common::consts::ONDO,
    factories::{OndoDataClientFactory, OndoExecutionClientFactory},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OndoDataClientFactory {
    /// Factory for creating Ondo Perps data clients.
    ///
    /// One factory is created per adapter environment, and every client it creates draws on that
    /// environment's REST budget (plan §4.4) - the same one an execution client of the same
    /// environment draws on, resolved from the `environment` its configuration names rather than
    /// passed between surfaces. Registering the factory with a Python `LiveNode` is what makes the
    /// `ONDO` client name resolvable; the data client's configuration is the
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
    /// One factory is created per adapter environment, and every client it creates draws on that
    /// environment's REST budget (plan §4.4) - the same bucket a data client configured for the
    /// same environment draws on, so a metadata refresh and a cancel cannot each hold half of the
    /// venue's limit. The client it builds is the native Rust execution client behind the
    /// `ExecutionClient` trait, and the credential is resolved inside it - this factory carries
    /// none, and its `__repr__` renders none.
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

    /// Returns whether this build's native execution client implements the ordered bounded
    /// shutdown lifecycle.
    ///
    /// This is a read-only capability fact about the installed wheel, not a venue protocol
    /// result and not a claim about any particular account. `true` means the native
    /// `ExecutionClient` lifecycle implements the ordered stop - cancel this run's own
    /// orders, confirm their terminal state, release the dead man's switch only when nothing
    /// is unconfirmed, then close the private transport, with a fail-closed synchronous
    /// fallback. It does **not** assert that a credentialled session has been exercised
    /// against the venue, that the REST/WS protocol is verified, or that a run left the
    /// account clean.
    ///
    /// The application reads this with `getattr(factory, "supports_ordered_shutdown", ...)`
    /// and treats anything other than the exact boolean `true` as unimplemented, so an older
    /// wheel that lacks the attribute fails closed.
    #[getter]
    #[pyo3(name = "supports_ordered_shutdown")]
    #[must_use]
    pub const fn py_supports_ordered_shutdown(&self) -> bool {
        true
    }

    /// Returns a sanitized snapshot of the most recent execution client's diagnostics, or `None`.
    ///
    /// This is a read-only, bounded view of the native run, keyed by the application's contract
    /// (`native-readonly/interface.md`): the run token the configuration supplied, whether the
    /// venue accepted a login, the acknowledged report channels, the private run state,
    /// reconnect/recovery counts, the count of verified account states published, the account
    /// identity (`matched` / `mismatch` / `unknown`) and the owned shutdown status. It is read from
    /// the native execution client the factory created and never fabricated here.
    ///
    /// The mapping carries no frame, credential, account id, order id or monetary field. A `None`
    /// means this factory has not created an execution client yet; an older wheel without this
    /// attribute fails closed when the application uses `getattr(...)`.
    #[pyo3(name = "read_only_snapshot")]
    #[gen_stub(
        override_return_type(
            type_repr = "dict[str, typing.Any] | None",
            imports = ("typing",),
        )
    )]
    fn py_read_only_snapshot<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(snapshot) = self.read_only_snapshot() else {
            return Ok(None);
        };

        let subscriptions = PyDict::new(py);

        for label in ["ordersPerps", "fillsPerps"] {
            subscriptions.set_item(label, snapshot.subscriptions_acked.contains(&label))?;
        }

        let dict = PyDict::new(py);
        dict.set_item("run_id", snapshot.run_id)?;
        dict.set_item("logged_in", snapshot.logged_in)?;
        dict.set_item("subscriptions_acked", subscriptions)?;
        dict.set_item("run_state", snapshot.run_state)?;
        dict.set_item("reconnects", snapshot.reconnects)?;
        dict.set_item("recoveries", snapshot.recoveries)?;
        dict.set_item("account_state_events", snapshot.account_state_events)?;
        dict.set_item("identity_match", snapshot.identity_match)?;
        dict.set_item("shutdown_status", snapshot.shutdown_status)?;

        Ok(Some(dict))
    }
    #[getter]
    #[pyo3(name = "supports_production_trade_envelope")]
    pub const fn py_supports_production_trade_envelope(&self) -> bool {
        true
    }

    #[pyo3(name = "production_trade_snapshot")]
    #[gen_stub(override_return_type(type_repr="dict[str, typing.Any] | None", imports=("typing",)))]
    fn py_production_trade_snapshot<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(snapshot) = self.production_trade_snapshot() else {
            return Ok(None);
        };
        let value = PyModule::import(py, "json")?.call_method1("loads", (snapshot.to_string(),))?;
        Ok(Some(value.cast_into::<PyDict>()?))
    }
}
