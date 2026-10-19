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

//! Readonly final cumulative-fill evidence for application order reconciliation.

use std::{collections::VecDeque, sync::Arc};

use ahash::AHashMap;
use nautilus_model::{
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    reports::OrderStatusReport,
};
use parking_lot::Mutex;
use serde::Serialize;

const EVIDENCE_CAPACITY: usize = 100_000;

#[derive(Clone, Debug, Serialize)]
struct TerminalFillEvidence {
    client_order_id: ClientOrderId,
    instrument_id: InstrumentId,
    venue_order_id: VenueOrderId,
    final_filled_quantity: String,
    source: String,
    ts_event_ns: u64,
    conflicting: bool,
    #[serde(skip)]
    http_conflicting: bool,
    #[serde(skip)]
    stream_conflict_ts_ns: u64,
}

#[derive(Debug, Default)]
struct EvidenceState {
    attached: bool,
    evidence: AHashMap<ClientOrderId, TerminalFillEvidence>,
    order: VecDeque<ClientOrderId>,
}

/// Readonly, single-client observations of venue terminal cumulative fills.
///
/// Missing or conflicting evidence cannot certify completion. Callers compare
/// the explicit final quantity with their durable, deduplicated fill ledger.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.lighter", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.lighter")
)]
pub struct LighterExecutionControl {
    inner: Arc<Mutex<EvidenceState>>,
}

impl LighterExecutionControl {
    /// Checks a new HTTP statement without creating or upgrading valid evidence.
    pub(crate) fn invalidate_http_terminal_conflict(&self, report: &OrderStatusReport) {
        if !report.order_status.is_closed() {
            return;
        }
        let Some(cloid) = report.client_order_id else {
            return;
        };
        if let Some(prior) = self.inner.lock().evidence.get_mut(&cloid)
            && prior.source == "http_order_status"
            && (prior.final_filled_quantity
                != report.filled_qty.as_decimal().normalize().to_string()
                || prior.instrument_id != report.instrument_id
                || prior.venue_order_id != report.venue_order_id)
        {
            prior.conflicting = true;
            prior.http_conflicting = true;
        }
    }

    pub(crate) fn invalidate(&self, cloid: ClientOrderId, source: &str) {
        if let Some(proof) = self.inner.lock().evidence.get_mut(&cloid) {
            proof.conflicting = true;
            proof.http_conflicting |= source == "http_order_status";
        }
    }

    pub(crate) fn attach(&self) -> anyhow::Result<()> {
        let mut state = self.inner.lock();
        anyhow::ensure!(
            !state.attached,
            "Lighter execution control already attached"
        );
        state.attached = true;
        Ok(())
    }

    pub(crate) fn observe(&self, report: &OrderStatusReport, source: &str) {
        let Some(cloid) = report.client_order_id else {
            return;
        };
        if !report.order_status.is_closed() {
            return;
        }
        let mut state = self.inner.lock();
        if source == "websocket_terminal_order"
            && let Some(prior) = state.evidence.get_mut(&cloid)
            && prior.source == "http_order_status"
        {
            if report.ts_last.as_u64() > prior.ts_event_ns
                && report.filled_qty.as_decimal().normalize().to_string()
                    != prior.final_filled_quantity
            {
                prior.conflicting = true;
                prior.stream_conflict_ts_ns = report.ts_last.as_u64();
            }
            return;
        }
        if report.quantity.is_zero()
            || report.filled_qty > report.quantity
            || (report.order_status == nautilus_model::enums::OrderStatus::Filled
                && report.filled_qty != report.quantity)
        {
            if let Some(prior) = state.evidence.get_mut(&cloid) {
                prior.conflicting = true;
                prior.http_conflicting |= source == "http_order_status";
            }
            return;
        }
        let quantity = report.filled_qty.as_decimal().normalize().to_string();
        let http_conflicting = state.evidence.get(&cloid).is_some_and(|prior| {
            prior.http_conflicting
                || (source == "http_order_status"
                    && prior.source == source
                    && (prior.final_filled_quantity != quantity
                        || prior.instrument_id != report.instrument_id
                        || prior.venue_order_id != report.venue_order_id))
        });
        let conflicting = state.evidence.get(&cloid).is_some_and(|prior| {
            let authoritative_upgrade = source == "http_order_status"
                && prior.instrument_id == report.instrument_id
                && prior.venue_order_id == report.venue_order_id
                && report.ts_last.as_u64()
                    >= prior.stream_conflict_ts_ns.max(
                        if prior.source == "websocket_terminal_order" {
                            prior.ts_event_ns
                        } else {
                            0
                        },
                    )
                && prior
                    .final_filled_quantity
                    .parse::<rust_decimal::Decimal>()
                    .is_ok_and(|quantity| report.filled_qty.as_decimal() >= quantity);
            http_conflicting
                || (!authoritative_upgrade
                    && (prior.conflicting
                        || prior.final_filled_quantity != quantity
                        || prior.instrument_id != report.instrument_id
                        || prior.venue_order_id != report.venue_order_id))
        });
        let stream_conflict_ts_ns = state
            .evidence
            .get(&cloid)
            .map_or(0, |prior| prior.stream_conflict_ts_ns);
        if !state.evidence.contains_key(&cloid) {
            state.order.push_back(cloid);
        }
        state.evidence.insert(
            cloid,
            TerminalFillEvidence {
                client_order_id: cloid,
                instrument_id: report.instrument_id,
                venue_order_id: report.venue_order_id,
                final_filled_quantity: quantity,
                source: source.into(),
                ts_event_ns: report.ts_last.as_u64(),
                conflicting,
                http_conflicting,
                stream_conflict_ts_ns,
            },
        );
        while state.order.len() > EVIDENCE_CAPACITY {
            if let Some(old) = state.order.pop_front() {
                state.evidence.remove(&old);
            }
        }
    }

    /// Returns venue final cumulative quantity and identity, or no evidence.
    ///
    /// Evidence is retained over reconnects in a bounded process replay horizon.
    /// It does not survive restart; the original order must be queried again.
    /// HTTP confirmation can resolve a stream conflict with a nondecreasing quantity.
    /// Contradictory HTTP observations remain conflicting.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored evidence cannot be serialized.
    pub fn terminal_fill_evidence_json(
        &self,
        client_order_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let cloid = ClientOrderId::new_checked(client_order_id)?;
        self.inner
            .lock()
            .evidence
            .get(&cloid)
            .map(serde_json::to_string)
            .transpose()
            .map_err(Into::into)
    }
}
