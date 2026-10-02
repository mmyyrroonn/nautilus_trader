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

//! Exact caller authority and explicitly synthetic account observations.
use std::collections::BTreeMap;

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::InstrumentId;
use pyo3::prelude::*;
use rust_decimal::Decimal;

use crate::{execution::guard::BackpackExecutionAuthority, parsing::decimal};

fn exact(value: &str) -> PyResult<Decimal> {
    decimal(value, "local peer value").map_err(|_| to_pyvalue_err("invalid exact loopback value"))
}

/// Finite caller permission for a synthetic peer. This cannot authorize production writes.
#[pyclass(
    name = "BackpackLoopbackExecutionAuthority",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackLoopbackExecutionAuthority {
    pub(crate) inner: BackpackExecutionAuthority,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackExecutionAuthority {
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
            .map_err(|_| to_pyvalue_err("invalid finite loopback authority"))?;
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
        "BackpackLoopbackExecutionAuthority(<synthetic-only>)"
    }
}

/// Explicit complete synthetic observations. Neither positions nor policy are inferred from absence.
#[pyclass(
    name = "BackpackLoopbackAccountFacts",
    module = "nautilus_trader.adapters.backpack",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Clone, Debug)]
pub struct PyBackpackLoopbackAccountFacts {
    pub(crate) observed_at_ms: u64,
    pub(crate) available_margin: Decimal,
    pub(crate) margin_per_notional: Decimal,
    pub(crate) fee_buffer_per_notional: Decimal,
    pub(crate) economics_reference: String,
    pub(crate) net_positions: BTreeMap<InstrumentId, Decimal>,
}
#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackLoopbackAccountFacts {
    /// Requires explicit disabled policy flags and complete facts; freshness is checked at admission.
    #[new]
    #[pyo3(signature = (*, observed_at_ms, available_margin, margin_per_notional,
        fee_buffer_per_notional, economics_reference, net_positions, auto_borrow,
        auto_lend, auto_repay, liquidating, complete))]
    #[expect(
        clippy::too_many_arguments,
        reason = "unknown policy never gets an implicit default"
    )]
    fn py_new(
        observed_at_ms: u64,
        available_margin: &str,
        margin_per_notional: &str,
        fee_buffer_per_notional: &str,
        economics_reference: String,
        net_positions: BTreeMap<String, String>,
        auto_borrow: bool,
        auto_lend: bool,
        auto_repay: bool,
        liquidating: bool,
        complete: bool,
    ) -> PyResult<Self> {
        let available_margin = exact(available_margin)?;
        let margin_per_notional = exact(margin_per_notional)?;
        let fee_buffer_per_notional = exact(fee_buffer_per_notional)?;
        if observed_at_ms == 0
            || observed_at_ms > i64::MAX as u64
            || available_margin < Decimal::ZERO
            || !(Decimal::ZERO..=Decimal::ONE).contains(&margin_per_notional)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&fee_buffer_per_notional)
            || economics_reference.trim().is_empty()
            || economics_reference.len() > 256
            || net_positions.is_empty()
            || net_positions.len() > 256
            || auto_borrow
            || auto_lend
            || auto_repay
            || liquidating
            || !complete
        {
            return Err(to_pyvalue_err("invalid complete synthetic account facts"));
        }
        let mut positions = BTreeMap::new();
        for (instrument, quantity) in net_positions {
            let id = InstrumentId::from_as_ref(&instrument)
                .map_err(|_| to_pyvalue_err("invalid loopback position scope"))?;
            if id.venue.as_str() != "BACKPACK" {
                return Err(to_pyvalue_err("invalid loopback position scope"));
            }
            positions.insert(id, exact(&quantity)?);
        }
        Ok(Self {
            observed_at_ms,
            available_margin,
            margin_per_notional,
            fee_buffer_per_notional,
            economics_reference,
            net_positions: positions,
        })
    }
    #[getter]
    fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }
    #[getter]
    fn available_margin(&self) -> String {
        self.available_margin.to_string()
    }
    #[getter]
    fn margin_per_notional(&self) -> String {
        self.margin_per_notional.to_string()
    }
    #[getter]
    fn fee_buffer_per_notional(&self) -> String {
        self.fee_buffer_per_notional.to_string()
    }
    #[getter]
    fn economics_reference(&self) -> String {
        self.economics_reference.clone()
    }
    #[getter]
    fn net_positions(&self) -> BTreeMap<String, String> {
        self.net_positions
            .iter()
            .map(|(id, quantity)| (id.to_string(), quantity.to_string()))
            .collect()
    }
    #[getter]
    fn auto_borrow(&self) -> bool {
        false
    }
    #[getter]
    fn auto_lend(&self) -> bool {
        false
    }
    #[getter]
    fn auto_repay(&self) -> bool {
        false
    }
    #[getter]
    fn liquidating(&self) -> bool {
        false
    }
    #[getter]
    fn complete(&self) -> bool {
        true
    }
    fn __repr__(&self) -> &'static str {
        "BackpackLoopbackAccountFacts(<synthetic-only>)"
    }
}
