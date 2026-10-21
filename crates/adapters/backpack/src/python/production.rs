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

//! Finite production permission and standard native execution configuration.
use std::time::Duration;

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use pyo3::prelude::*;
use rust_decimal::Decimal;

use super::{account::PyBackpackExecutionClientConfig, config::PyBackpackDataClientConfig};
use crate::{
    execution::{guard::BackpackExecutionAuthority, owner::BackpackMutationPolicy},
    execution_client::{
        production_config::BackpackProductionExecutionClientConfig,
        production_control::BackpackProductionControl,
    },
    identity::BackpackClientIdNamespace,
    parsing::decimal,
    signing::BackpackReceiveWindow,
};
fn exact(value: &str) -> PyResult<Decimal> {
    decimal(value, "execution permission")
        .map_err(|_| to_pyvalue_err("invalid exact execution permission"))
}
/// Finite caller permission, distinct from authenticated account and market observations.
#[pyclass(
    name = "BackpackProductionExecutionAuthority",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackProductionExecutionAuthority {
    pub(crate) inner: BackpackExecutionAuthority,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackProductionExecutionAuthority {
    /// Validates every finite limit and explicit permission without filesystem or network I/O.
    #[new]
    #[pyo3(signature = (*, expires_at_ms, max_account_age_ms, max_market_age_ms,
        max_order_notional, max_reserved_notional, max_reserved_margin, max_unsettled_orders,
        allow_new_risk, allow_reduction, allow_owned_cancel))]
    #[expect(
        clippy::too_many_arguments,
        reason = "all finite permissions are explicit"
    )]
    fn py_new(
        expires_at_ms: u64,
        max_account_age_ms: u64,
        max_market_age_ms: u64,
        max_order_notional: &str,
        max_reserved_notional: &str,
        max_reserved_margin: &str,
        max_unsettled_orders: usize,
        allow_new_risk: bool,
        allow_reduction: bool,
        allow_owned_cancel: bool,
    ) -> PyResult<Self> {
        let inner = BackpackExecutionAuthority {
            expires_at_ms,
            max_account_age_ms,
            max_market_age_ms,
            max_order_notional: exact(max_order_notional)?,
            max_reserved_notional: exact(max_reserved_notional)?,
            max_reserved_margin: exact(max_reserved_margin)?,
            max_unsettled_orders,
            allow_new_risk,
            allow_reduction,
            allow_owned_cancel,
        };
        inner
            .validate()
            .map_err(|_| to_pyvalue_err("invalid finite production execution authority"))?;
        Ok(Self { inner })
    }
    #[getter]
    fn expires_at_ms(&self) -> u64 {
        self.inner.expires_at_ms
    }
    #[getter]
    fn max_account_age_ms(&self) -> u64 {
        self.inner.max_account_age_ms
    }
    #[getter]
    fn max_market_age_ms(&self) -> u64 {
        self.inner.max_market_age_ms
    }
    #[getter]
    fn max_order_notional(&self) -> String {
        self.inner.max_order_notional.to_string()
    }
    #[getter]
    fn max_reserved_notional(&self) -> String {
        self.inner.max_reserved_notional.to_string()
    }
    #[getter]
    fn max_reserved_margin(&self) -> String {
        self.inner.max_reserved_margin.to_string()
    }
    #[getter]
    fn max_unsettled_orders(&self) -> usize {
        self.inner.max_unsettled_orders
    }
    #[getter]
    fn allow_new_risk(&self) -> bool {
        self.inner.allow_new_risk
    }
    #[getter]
    fn allow_reduction(&self) -> bool {
        self.inner.allow_reduction
    }
    #[getter]
    fn allow_owned_cancel(&self) -> bool {
        self.inner.allow_owned_cancel
    }
    fn __repr__(&self) -> &'static str {
        "BackpackProductionExecutionAuthority(<finite permissions>)"
    }
}

/// Weak native observation handle; it cannot manufacture account facts or sessions.
#[pyclass(
    name = "BackpackProductionControl",
    module = "nautilus_trader.adapters.backpack",
    unsendable,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackProductionControl {
    inner: BackpackProductionControl,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackProductionControl {
    /// Extends the current finite expiry by at most five minutes from now, keeping all limits.
    fn renew_authority(&self, expires_at_ms: u64) -> PyResult<()> {
        self.inner
            .renew_authority(expires_at_ms)
            .map_err(|_| to_pyvalue_err("production authority renewal refused"))
    }
    /// Returns the complete revision-fenced authenticated account reading, without any I/O.
    fn account_snapshot_json(&self) -> PyResult<String> {
        let value = self
            .inner
            .account_snapshot_json()
            .map_err(|_| to_pyruntime_err("current production account snapshot unavailable"))?;
        serde_json::to_string(&value)
            .map_err(|_| to_pyruntime_err("account snapshot serialization failed"))
    }
    /// Checks current native admission facts without dispatching financial requests.
    fn admission_status_json(&self) -> PyResult<String> {
        let value = self
            .inner
            .admission_status_json()
            .map_err(|_| to_pyruntime_err("production admission status unavailable"))?;
        serde_json::to_string(&value)
            .map_err(|_| to_pyruntime_err("admission status serialization failed"))
    }
    /// Persists the actual native cache and receipts before advancing economic acknowledgement.
    fn persist_economics(&self) -> PyResult<String> {
        let value = self
            .inner
            .persist_economics()
            .map_err(|_| to_pyruntime_err("native economic checkpoint refused"))?;
        serde_json::to_string(&value)
            .map_err(|_| to_pyruntime_err("economic snapshot serialization failed"))
    }
    /// Reconciles native terminal observations against independently durable economic receipts.
    fn reconcile_terminal_evidence(&self) -> PyResult<String> {
        let value = self
            .inner
            .reconcile_terminal_evidence()
            .map_err(|_| to_pyruntime_err("native terminal reconciliation refused"))?;
        serde_json::to_string(&value)
            .map_err(|_| to_pyruntime_err("terminal snapshot serialization failed"))
    }
    /// Observes pending fills without acknowledging them.
    fn pending_fills_json(&self) -> PyResult<String> {
        let value = self
            .inner
            .pending()
            .map_err(|_| to_pyruntime_err("production client unavailable"))?;
        serde_json::to_string(&value)
            .map_err(|_| to_pyruntime_err("pending fill serialization failed"))
    }
    fn telemetry_snapshot_json(&self) -> PyResult<String> {
        let value = self
            .inner
            .health()
            .map_err(|_| to_pyruntime_err("production client unavailable"))?;
        serde_json::to_string(&value).map_err(|_| to_pyruntime_err("health serialization failed"))
    }
    /// Returns sticky bounded-stop evidence after the native owner has stopped.
    fn shutdown_report_json(&self) -> PyResult<Option<String>> {
        self.inner
            .shutdown()
            .map(|report| {
                serde_json::to_string(&serde_json::json!({
                    "schema_version":1, "dirty":report.dirty, "unsent":report.unsent,
                    "unknown":report.unknown, "observed_unreconciled":report.observed_unreconciled,
                    "pending_cancellations":report.pending_cancellations,
                    "positions_unknown_or_nonzero":report.positions_unknown_or_nonzero,
                    "positions_unknown":report.positions_unknown,
                    "positions_nonzero":report.positions_nonzero,
                    "complete_account_proof":report.complete_account_proof,
                    "account_observed_at_ms":report.account_observed_at_ms,
                    "account_completed_at_ms":report.account_completed_at_ms,
                    "evaluated_at_ms":report.evaluated_at_ms,
                    "private_generation":report.private_generation,
                    "private_epoch":report.private_epoch,
                    "private_revision":report.private_revision,
                    "reason":report.reason,
                }))
                .map_err(|_| to_pyruntime_err("shutdown serialization failed"))
            })
            .transpose()
    }
    fn __repr__(&self) -> &'static str {
        "BackpackProductionControl(<weak native owner>)"
    }
}
/// Checked execution plan; construction performs no credential discovery or transport I/O.
#[pyclass(
    name = "BackpackProductionExecutionClientConfig",
    module = "nautilus_trader.adapters.backpack",
    unsendable,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackProductionExecutionClientConfig {
    pub(crate) inner: BackpackProductionExecutionClientConfig,
    account: PyBackpackExecutionClientConfig,
    public: PyBackpackDataClientConfig,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackProductionExecutionClientConfig {
    #[new]
    #[pyo3(signature=(account_config,public_config,*,authority,mutation_budget_ms,receive_window_ms,economic_state_directory))]
    fn py_new(
        mut account_config: PyBackpackExecutionClientConfig,
        public_config: PyBackpackDataClientConfig,
        authority: PyBackpackProductionExecutionAuthority,
        mutation_budget_ms: u64,
        receive_window_ms: u64,
        economic_state_directory: String,
    ) -> PyResult<Self> {
        let subaccount = account_config
            .inner
            .namespace
            .asserted_subaccount()
            .map(|value| value.to_string());
        account_config.inner.namespace = BackpackClientIdNamespace::credential_scope(
            &account_config.inner.credential,
            account_config.inner.scope.endpoints(),
            subaccount.as_deref(),
        )
        .map_err(|_| to_pyvalue_err("invalid production credential scope"))?;
        let window = BackpackReceiveWindow::new(receive_window_ms)
            .map_err(|_| to_pyvalue_err("invalid execution receive window"))?;
        let mutation = BackpackMutationPolicy {
            window,
            budget: Duration::from_millis(mutation_budget_ms),
        };
        let inner = BackpackProductionExecutionClientConfig::new_checked(
            account_config.inner.clone(),
            public_config.inner.clone(),
            authority.inner,
            mutation,
            economic_state_directory.into(),
        )
        .map_err(|_| to_pyvalue_err("invalid production execution configuration"))?;
        Ok(Self {
            inner,
            account: account_config,
            public: public_config,
        })
    }
    #[getter]
    fn account_config(&self) -> PyBackpackExecutionClientConfig {
        self.account.clone()
    }
    #[getter]
    fn public_config(&self) -> PyBackpackDataClientConfig {
        self.public.clone()
    }
    #[getter]
    fn authority(&self) -> PyBackpackProductionExecutionAuthority {
        PyBackpackProductionExecutionAuthority {
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
            .expect("validated mutation budget")
    }
    #[getter]
    fn receive_window_ms(&self) -> u64 {
        self.inner.mutation.window.milliseconds()
    }
    #[getter]
    fn economic_state_directory(&self) -> String {
        self.inner
            .economic_state_directory
            .to_string_lossy()
            .into_owned()
    }
    #[getter]
    fn control(&self) -> PyBackpackProductionControl {
        PyBackpackProductionControl {
            inner: self.inner.control(),
        }
    }
    fn __repr__(&self) -> &'static str {
        "BackpackProductionExecutionClientConfig"
    }
}
/// Standard native execution factory; its support declaration never attests a live account.
#[pyclass(
    name = "BackpackProductionExecutionClientFactory",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug, Default)]
pub struct PyBackpackProductionExecutionClientFactory;
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackProductionExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    #[getter]
    fn config_type(&self) -> &'static str {
        "BackpackProductionExecutionClientConfig"
    }
    /// Compiled protocol support; runtime observations and receipts remain separate.
    fn capabilities_json(&self) -> &'static str {
        r#"{"schema_version":1,"public_market_data":false,"read_only_account":false,"restricted_execution":true,"production_writes":true,"private_subscription_confirmed":false,"durable_economic_ack":false}"#
    }
    fn __repr__(&self) -> &'static str {
        "BackpackProductionExecutionClientFactory"
    }
}
