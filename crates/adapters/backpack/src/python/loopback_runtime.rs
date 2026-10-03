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

//! Native factory attachment and session control; no Python economic acknowledgement surface.
use std::time::Duration;

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::identifiers::InstrumentId;
use pyo3::prelude::*;

use super::{
    account::PyBackpackExecutionClientConfig,
    config::PyBackpackDataClientConfig,
    loopback::{PyBackpackLoopbackAccountFacts, PyBackpackLoopbackExecutionAuthority},
};
use crate::{
    execution::owner::BackpackMutationPolicy,
    execution_client::{
        control::BackpackLoopbackControl, loopback_config::BackpackLoopbackExecutionClientConfig,
        restricted::BackpackLoopbackSession,
    },
    identity::BackpackClientIdNamespace,
    signing::BackpackReceiveWindow,
};

/// Unforgeable admitted session; generation alone never attests a production account.
#[pyclass(
    name = "BackpackLoopbackSession",
    module = "nautilus_trader.adapters.backpack",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Copy, Debug)]
pub struct PyBackpackLoopbackSession {
    inner: BackpackLoopbackSession,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackSession {
    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }
    fn __repr__(&self) -> &'static str {
        "BackpackLoopbackSession(<synthetic-only>)"
    }
}
/// Weak owner-thread observation/control, retaining neither the native node nor identity lock.
#[pyclass(
    name = "BackpackLoopbackControl",
    module = "nautilus_trader.adapters.backpack",
    unsendable,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackLoopbackControl {
    inner: BackpackLoopbackControl,
    namespace: BackpackClientIdNamespace,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackControl {
    /// Starts only after both actual native public/private owners are current.
    fn begin_session(&self) -> PyResult<PyBackpackLoopbackSession> {
        self.inner
            .begin_session()
            .map(|inner| PyBackpackLoopbackSession { inner })
            .map_err(|_| to_pyruntime_err("loopback session admission refused"))
    }
    /// Supplies explicit complete synthetic facts; no account/private attestation is minted.
    fn accept_account(
        &self,
        session: &PyBackpackLoopbackSession,
        facts: PyBackpackLoopbackAccountFacts,
    ) -> PyResult<()> {
        let facts = crate::execution::guard::BackpackLoopbackAccountFacts {
            namespace: self.namespace.clone(),
            generation: session.inner.generation(),
            observed_at_ms: facts.observed_at_ms,
            available_margin: facts.available_margin,
            margin_per_notional: facts.margin_per_notional,
            fee_buffer_per_notional: facts.fee_buffer_per_notional,
            economics_reference: facts.economics_reference,
            net_positions: facts.net_positions,
            auto_borrow: false,
            auto_lend: false,
            auto_repay: false,
            liquidating: false,
            complete: true,
        };
        self.inner
            .accept_account(session.inner, facts)
            .map_err(|_| to_pyruntime_err("loopback account admission refused"))
    }
    /// Reads the actual native cache and validated metadata; caller-created quotes are not accepted.
    fn refresh_market(
        &self,
        session: &PyBackpackLoopbackSession,
        instrument_id: &str,
    ) -> PyResult<()> {
        let instrument = InstrumentId::from_as_ref(instrument_id)
            .map_err(|_| to_pyvalue_err("invalid loopback instrument scope"))?;
        self.inner
            .refresh_market(session.inner, instrument)
            .map_err(|_| to_pyruntime_err("loopback market admission refused"))
    }
    fn invalidate(&self, session: &PyBackpackLoopbackSession) -> PyResult<()> {
        self.inner
            .invalidate(session.inner)
            .map_err(|_| to_pyruntime_err("loopback session invalidation refused"))
    }
    /// Uses native retained terminal observations and durable receipts; accepts no caller evidence.
    fn reconcile_terminal_evidence(&self, session: &PyBackpackLoopbackSession) -> PyResult<String> {
        let result = self
            .inner
            .reconcile_terminal_evidence(session.inner)
            .map_err(|_| to_pyruntime_err("native terminal reconciliation refused"))?;
        serde_json::to_string(&result)
            .map_err(|_| to_pyruntime_err("terminal snapshot serialization failed"))
    }
    /// Persists actual native consumer state and receipt before economic ACK.
    /// No caller-created state, trade selector or acknowledgement callback is accepted.
    fn persist_economics(&self) -> PyResult<String> {
        let result = self
            .inner
            .persist_economics()
            .map_err(|_| to_pyruntime_err("native economic checkpoint refused"))?;
        serde_json::to_string(&result)
            .map_err(|_| to_pyruntime_err("economic snapshot serialization failed"))
    }
    /// Returns actual staged economics. Observation never advances dedup or releases order capacity.
    fn pending_fills_json(&self) -> PyResult<String> {
        let pending = self
            .inner
            .pending()
            .map_err(|_| to_pyruntime_err("loopback client unavailable"))?;
        serde_json::to_string(&pending)
            .map_err(|_| to_pyruntime_err("loopback snapshot serialization failed"))
    }
    fn telemetry_snapshot_json(&self) -> PyResult<String> {
        let health = self
            .inner
            .health()
            .map_err(|_| to_pyruntime_err("loopback client unavailable"))?;
        serde_json::to_string(&health)
            .map_err(|_| to_pyruntime_err("loopback snapshot serialization failed"))
    }
    /// Returns persisted bounded-stop observations, or None before an owner has stopped.
    fn shutdown_report_json(&self) -> PyResult<Option<String>> {
        self.inner
            .shutdown()
            .map(|report| {
                serde_json::to_string(&serde_json::json!({
            "schema_version": 1, "dirty": report.dirty, "unsent": report.unsent,
            "unknown": report.unknown, "observed_unreconciled": report.observed_unreconciled,
            "pending_cancellations": report.pending_cancellations,
            "positions_unknown_or_nonzero": report.positions_unknown_or_nonzero,
        })).map_err(|_| to_pyruntime_err("loopback snapshot serialization failed"))
            })
            .transpose()
    }
    fn __repr__(&self) -> &'static str {
        "BackpackLoopbackControl(<weak synthetic owner>)"
    }
}

/// Explicit numeric local peer plan; configuration and factory construction perform no I/O.
#[pyclass(
    name = "BackpackLoopbackExecutionClientConfig",
    module = "nautilus_trader.adapters.backpack",
    unsendable,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackLoopbackExecutionClientConfig {
    pub(crate) inner: BackpackLoopbackExecutionClientConfig,
    account: PyBackpackExecutionClientConfig,
    public: PyBackpackDataClientConfig,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackExecutionClientConfig {
    #[new]
    #[pyo3(signature = (read_only_config, public_config, *, authority, mutation_budget_ms, receive_window_ms, economic_state_directory=None))]
    fn py_new(
        read_only_config: PyBackpackExecutionClientConfig,
        public_config: PyBackpackDataClientConfig,
        authority: PyBackpackLoopbackExecutionAuthority,
        mutation_budget_ms: u64,
        receive_window_ms: u64,
        economic_state_directory: Option<String>,
    ) -> PyResult<Self> {
        let window = BackpackReceiveWindow::new(receive_window_ms)
            .map_err(|_| to_pyvalue_err("invalid loopback receive window"))?;
        let mutation = BackpackMutationPolicy {
            window,
            budget: Duration::from_millis(mutation_budget_ms),
        };
        let inner = BackpackLoopbackExecutionClientConfig::new_checked(
            read_only_config.inner.clone(),
            public_config.inner.clone(),
            authority.inner,
            mutation,
        )
        .map_err(|_| to_pyvalue_err("invalid synthetic loopback execution configuration"))?;
        let inner = match economic_state_directory {
            Some(directory) => inner
                .with_economic_state_directory(directory.into())
                .map_err(|_| to_pyvalue_err("invalid economic consumer directory"))?,
            None => inner,
        };
        Ok(Self {
            inner,
            account: read_only_config,
            public: public_config,
        })
    }
    #[getter]
    fn read_only_config(&self) -> PyBackpackExecutionClientConfig {
        self.account.clone()
    }
    #[getter]
    fn public_config(&self) -> PyBackpackDataClientConfig {
        self.public.clone()
    }
    #[getter]
    fn authority(&self) -> PyBackpackLoopbackExecutionAuthority {
        PyBackpackLoopbackExecutionAuthority {
            inner: self.inner.authority.clone(),
        }
    }
    #[getter]
    fn mutation_budget_ms(&self) -> u64 {
        self.inner
            .mutation
            .budget
            .as_millis()
            .try_into()
            .expect("validated budget")
    }
    #[getter]
    fn receive_window_ms(&self) -> u64 {
        self.inner.mutation.window.milliseconds()
    }
    #[getter]
    fn control(&self) -> PyBackpackLoopbackControl {
        PyBackpackLoopbackControl {
            inner: self.inner.control(),
            namespace: self.inner.account.namespace.clone(),
        }
    }
    fn __repr__(&self) -> &'static str {
        "BackpackLoopbackExecutionClientConfig(<synthetic-only>)"
    }
}
/// Standard native factory projection. Submit/cancel travel through the actual native engine.
#[pyclass(
    name = "BackpackLoopbackExecutionClientFactory",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug, Default)]
pub struct PyBackpackLoopbackExecutionClientFactory;
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    #[getter]
    fn config_type(&self) -> &'static str {
        "BackpackLoopbackExecutionClientConfig"
    }
    fn capabilities_json(&self) -> &'static str {
        r#"{"schema_version":1,"read_only_account":true,"restricted_execution":true,"numeric_loopback_only":true,"production_writes":false,"private_subscription_confirmed":false,"durable_economic_ack":false}"#
    }
    fn __repr__(&self) -> &'static str {
        "BackpackLoopbackExecutionClientFactory(<synthetic-only>)"
    }
}
