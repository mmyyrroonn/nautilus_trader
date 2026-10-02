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

//! Explicit opaque credentials, shared quotas and native read-only account configuration.
use std::{path::PathBuf, sync::Arc, time::Duration};

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::AccountId;
use pyo3::prelude::*;

use crate::{
    account::pagination::BackpackReadBudget,
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    config::BackpackConfig,
    execution_client::{BackpackExecutionClientConfig, BackpackExecutionPolicy},
    http::quota::BackpackQuota,
    identity::BackpackClientIdNamespace,
};

fn endpoints(http: Option<&str>, ws: Option<&str>) -> PyResult<BackpackEndpoints> {
    match (http, ws) {
        (None, None) => Ok(BackpackEndpoints::production()),
        (Some(http), Some(ws)) => BackpackEndpoints::loopback_override(http, ws)
            .map_err(|_| to_pyvalue_err("invalid Backpack endpoint audience")),
        _ => Err(to_pyvalue_err(
            "Backpack loopback audience requires both origins",
        )),
    }
}

/// Opaque explicit seed bound to production or exactly one validated local peer.
/// No signer, seed getter, serializer or environment discovery is exposed.
#[pyclass(
    name = "BackpackCredential",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackCredential {
    pub(crate) inner: BackpackCredential,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackCredential {
    #[new]
    #[pyo3(signature = (seed_base64, *, base_url_http=None, base_url_ws=None))]
    fn py_new(
        seed_base64: &str,
        base_url_http: Option<&str>,
        base_url_ws: Option<&str>,
    ) -> PyResult<Self> {
        let audience = endpoints(base_url_http, base_url_ws)?;
        let inner = if audience.is_loopback() {
            BackpackCredential::loopback_peer(seed_base64, &audience)
        } else {
            BackpackCredential::production(seed_base64)
        }
        .map_err(|_| to_pyvalue_err("invalid Backpack credential"))?;
        Ok(Self { inner })
    }
    fn __repr__(&self) -> &'static str {
        "BackpackCredential(<redacted>)"
    }
}

/// Opaque in-process REST quota scope, shared by public factory and account config.
/// Contains no raw HTTP access; external processes require separate coordination.
#[pyclass(
    name = "BackpackQuota",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackQuota {
    pub(crate) inner: BackpackQuota,
}
impl Default for PyBackpackQuota {
    fn default() -> Self {
        Self {
            inner: BackpackQuota::default(),
        }
    }
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackQuota {
    #[new]
    #[pyo3(signature = (*, standard_period_ms=32, historical_period_ms=2100))]
    fn py_new(standard_period_ms: u64, historical_period_ms: u64) -> PyResult<Self> {
        if standard_period_ms > 60_000 || historical_period_ms > 300_000 {
            return Err(to_pyvalue_err("invalid Backpack quota period"));
        }
        Ok(Self {
            inner: BackpackQuota::with_periods(
                Duration::from_millis(standard_period_ms),
                Duration::from_millis(historical_period_ms),
            )
            .map_err(|_| to_pyvalue_err("invalid Backpack quota period"))?,
        })
    }
    /// True only when both handles share the same actual native in-process limiter.
    fn shares_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(
            &self.inner.clone().into_limiter(),
            &other.inner.clone().into_limiter(),
        )
    }
    fn __repr__(&self) -> &'static str {
        "BackpackQuota"
    }
}

/// Checked read-only plan; construction never opens the identity directory or network.
/// Factory client creation acquires the durable identity directory's OS lock.
/// Account labels and namespace components remain caller claims, not verified identity.
#[pyclass(
    name = "BackpackExecutionClientConfig",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackExecutionClientConfig {
    pub(crate) inner: BackpackExecutionClientConfig,
    identity_account: String,
    subaccount: Option<String>,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackExecutionClientConfig {
    #[new]
    #[pyo3(signature = (symbols, credential, account_id, identity_account, identity_directory, *,
        quota, subaccount=None, base_url_http=None, base_url_ws=None, connect_timeout_ms=20000,
        shutdown_timeout_ms=3000, recovery_interval_ms=30000, recovery_lookback_ms=3600000,
        input_capacity=256, fill_capacity=100000, page_size=1000, max_pages=10,
        max_items=10000, read_timeout_ms=30000))]
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit native account policy and read limits"
    )]
    fn py_new(
        symbols: Vec<String>,
        credential: PyBackpackCredential,
        account_id: &str,
        identity_account: &str,
        identity_directory: String,
        quota: PyBackpackQuota,
        subaccount: Option<&str>,
        base_url_http: Option<&str>,
        base_url_ws: Option<&str>,
        connect_timeout_ms: u64,
        shutdown_timeout_ms: u64,
        recovery_interval_ms: u64,
        recovery_lookback_ms: u64,
        input_capacity: usize,
        fill_capacity: usize,
        page_size: u64,
        max_pages: u64,
        max_items: u64,
        read_timeout_ms: u64,
    ) -> PyResult<Self> {
        let audience = endpoints(base_url_http, base_url_ws)?;
        let namespace = if audience.is_loopback() {
            BackpackClientIdNamespace::loopback_peer(&audience, identity_account, subaccount)
        } else {
            BackpackClientIdNamespace::new_checked("production", identity_account, subaccount)
        }
        .map_err(|_| to_pyvalue_err("invalid Backpack identity namespace"))?;
        let scope = BackpackConfig::with_endpoints_checked(symbols, audience)
            .map_err(|_| to_pyvalue_err("invalid Backpack account symbol scope"))?;
        let account_id = AccountId::new_checked(account_id)
            .map_err(|_| to_pyvalue_err("invalid Backpack engine account label"))?;
        let policy = BackpackExecutionPolicy {
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms),
            recovery_interval: Duration::from_millis(recovery_interval_ms),
            recovery_lookback: Duration::from_millis(recovery_lookback_ms),
            input_capacity,
            fill_capacity,
        };
        let budget = BackpackReadBudget::new(
            page_size,
            max_pages,
            max_items,
            Duration::from_millis(read_timeout_ms),
        )
        .map_err(|_| to_pyvalue_err("invalid Backpack account read budget"))?;
        let inner = BackpackExecutionClientConfig::new_read_only(
            scope,
            credential.inner,
            account_id,
            namespace,
            PathBuf::from(identity_directory),
            policy,
            budget,
            quota.inner,
        )
        .map_err(|_| to_pyvalue_err("invalid Backpack read-only account configuration"))?;
        Ok(Self {
            inner,
            identity_account: identity_account.to_string(),
            subaccount: subaccount.map(str::to_string),
        })
    }
    #[getter]
    fn identity_account(&self) -> String {
        self.identity_account.clone()
    }
    #[getter]
    fn subaccount(&self) -> Option<String> {
        self.subaccount.clone()
    }
    #[getter]
    fn symbols(&self) -> Vec<String> {
        self.inner.scope().symbols().iter().cloned().collect()
    }
    #[getter]
    fn account_id(&self) -> String {
        self.inner.account_id().to_string()
    }
    #[getter]
    fn identity_directory(&self) -> String {
        self.inner
            .identity_directory()
            .to_string_lossy()
            .into_owned()
    }
    #[getter]
    fn quota(&self) -> PyBackpackQuota {
        PyBackpackQuota {
            inner: self.inner.quota.clone(),
        }
    }
    #[getter]
    fn base_url_http(&self) -> String {
        self.inner.scope().endpoints().rest_url().to_string()
    }
    #[getter]
    fn base_url_ws(&self) -> String {
        self.inner.scope().endpoints().websocket_url().to_string()
    }
    #[getter]
    fn connect_timeout_ms(&self) -> u64 {
        self.inner
            .policy
            .connect_timeout
            .as_millis()
            .try_into()
            .expect("validated millisecond budget")
    }
    #[getter]
    fn shutdown_timeout_ms(&self) -> u64 {
        self.inner
            .policy
            .shutdown_timeout
            .as_millis()
            .try_into()
            .expect("validated millisecond budget")
    }
    #[getter]
    fn recovery_interval_ms(&self) -> u64 {
        self.inner
            .policy
            .recovery_interval
            .as_millis()
            .try_into()
            .expect("validated millisecond budget")
    }
    #[getter]
    fn recovery_lookback_ms(&self) -> u64 {
        self.inner
            .policy
            .recovery_lookback
            .as_millis()
            .try_into()
            .expect("validated millisecond budget")
    }
    #[getter]
    fn input_capacity(&self) -> usize {
        self.inner.policy.input_capacity
    }
    #[getter]
    fn fill_capacity(&self) -> usize {
        self.inner.policy.fill_capacity
    }
    #[getter]
    fn page_size(&self) -> u64 {
        self.inner.read_budget.page_size
    }
    #[getter]
    fn max_pages(&self) -> u64 {
        self.inner.read_budget.max_pages
    }
    #[getter]
    fn max_items(&self) -> u64 {
        self.inner.read_budget.max_items
    }
    #[getter]
    fn read_timeout_ms(&self) -> u64 {
        self.inner
            .read_budget
            .timeout
            .as_millis()
            .try_into()
            .expect("validated millisecond budget")
    }
    /// Sanitized versioned per-run observations; enqueue is never durable economic ACK.
    fn telemetry_snapshot_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner.telemetry().snapshot())
            .map_err(|_| to_pyvalue_err("Backpack account telemetry unavailable"))
    }
    fn __repr__(&self) -> &'static str {
        "BackpackExecutionClientConfig(<read-only>)"
    }
}
