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

//! At-least-once economic staging; a durable delivery acknowledgement precedes dedup.

use std::collections::BTreeMap;

use nautilus_model::{
    identifiers::{InstrumentId, TradeId},
    reports::FillReport,
};
use serde::{Deserialize, Serialize};

use super::BackpackAccountError;

/// Native symbol and true trade ID, shared across REST/private stream reconciliation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BackpackFillKey {
    pub instrument_id: InstrumentId,
    pub trade_id: TradeId,
}

/// Exact economic fingerprint durably coupled to the consumer's applied state.
/// It excludes receipt time/report UUID and therefore survives transport changes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackpackAppliedFill {
    pub key: BackpackFillKey,
    pub fingerprint: String,
}

/// Result of staging a valid native fill, including pending retry versus applied duplicate.
#[derive(Clone, Debug)]
pub enum BackpackFillStage {
    Pending(Box<FillReport>),
    AlreadyApplied,
}

/// Bounded pending and applied records, with no unacknowledged record treated as applied.
///
/// The execution owner must atomically persist economic application and the returned
/// acknowledgement record in its own journal. On restart, load only durably applied
/// records using `from_applied`; uncommitted fills are delivered again. The consumer
/// must be idempotent by `BackpackFillKey`. This is deliberately at-least-once, not a
/// claim of atomic delivery to an external engine or a standalone journal.
#[derive(Debug)]
pub struct BackpackFillReconciler {
    capacity: usize,
    pending: BTreeMap<BackpackFillKey, (BackpackAppliedFill, FillReport)>,
    applied: BTreeMap<BackpackFillKey, BackpackAppliedFill>,
}
impl BackpackFillReconciler {
    /// Restores dedup only from consumer-confirmed durable economic application.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/oversized capacity, conflicting or duplicate records.
    pub fn from_applied(
        capacity: usize,
        records: impl IntoIterator<Item = BackpackAppliedFill>,
    ) -> Result<Self, BackpackAccountError> {
        if capacity == 0 || capacity > 1_000_000 {
            return Err(BackpackAccountError::InvalidBudget);
        }
        let mut result = Self {
            capacity,
            pending: BTreeMap::new(),
            applied: BTreeMap::new(),
        };
        for record in records {
            if record.fingerprint.len() != 64
                || !record.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(BackpackAccountError::InvalidField("fill acknowledgement"));
            }
            if result.applied.len() >= capacity {
                return Err(BackpackAccountError::Budget);
            }
            if result.applied.insert(record.key, record).is_some() {
                return Err(BackpackAccountError::Conflict);
            }
        }
        Ok(result)
    }

    /// Stages a true native fill; pending duplicates remain deliverable until acknowledged.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting same-ID economics or bounded capacity exhaustion.
    pub fn stage(&mut self, report: FillReport) -> Result<BackpackFillStage, BackpackAccountError> {
        let record = applied_record(&report)?;
        if let Some(applied) = self.applied.get(&record.key) {
            if applied != &record {
                return Err(BackpackAccountError::Conflict);
            }
            return Ok(BackpackFillStage::AlreadyApplied);
        }
        if let Some((pending, previous)) = self.pending.get_mut(&record.key) {
            if pending != &record
                || (previous.client_order_id.is_some()
                    && report.client_order_id.is_some()
                    && previous.client_order_id != report.client_order_id)
            {
                return Err(BackpackAccountError::Conflict);
            }
            // A command ACK may establish attribution after the first fill arrived.
            // Qualify only still-pending delivery; never change applied economics.
            if previous.client_order_id.is_none() && report.client_order_id.is_some() {
                *previous = report;
            }
            return Ok(BackpackFillStage::Pending(Box::new(previous.clone())));
        }
        if self.pending.len() + self.applied.len() >= self.capacity {
            return Err(BackpackAccountError::Budget);
        }
        self.pending.insert(record.key, (record, report.clone()));
        Ok(BackpackFillStage::Pending(Box::new(report)))
    }

    /// Enumerates pending fills by event time, including first/late fills before order ACK.
    /// No terminal order flag suppresses a valid later fill.
    #[must_use]
    pub fn pending_in_event_order(&self) -> Vec<&FillReport> {
        let mut reports: Vec<_> = self.pending.values().map(|(_, report)| report).collect();
        reports.sort_by_key(|report| (report.ts_event, report.instrument_id, report.trade_id));
        reports
    }

    /// Commits dedup only through the owner's durable economic-application transaction.
    ///
    /// `commit` must acknowledge actual economic delivery and durably commit this record
    /// atomically with consumer state. A failed callback leaves the fill pending. It may
    /// already have applied externally: retry with the same idempotent key, never guess
    /// that an error proves no economic delivery. This store never writes before ACK.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown keys or unsuccessful consumer acknowledgement.
    pub fn acknowledge_with(
        &mut self,
        key: BackpackFillKey,
        commit: impl FnOnce(&BackpackAppliedFill) -> Result<(), BackpackAccountError>,
    ) -> Result<(), BackpackAccountError> {
        let (record, _) = self
            .pending
            .get(&key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        commit(record)?;
        let (record, _) = self
            .pending
            .remove(&key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        self.applied.insert(key, record);
        Ok(())
    }

    /// Clones the immutable pending receipt so consumer application can run outside owner locks.
    /// This is staging evidence, never proof that economic application has occurred.
    ///
    /// # Errors
    /// Returns an error for an unknown pending fill.
    pub fn pending_acknowledgement(
        &self,
        key: BackpackFillKey,
    ) -> Result<BackpackAppliedFill, BackpackAccountError> {
        self.pending
            .get(&key)
            .map(|(record, _)| record.clone())
            .ok_or(BackpackAccountError::UnknownAcknowledgement)
    }

    /// Marks a receipt applied only after the caller durably commits actual economic application.
    /// The consumer must be idempotent: competing callers may commit the same immutable receipt.
    /// A matching already-applied receipt succeeds; conflicting receipt economics never do.
    ///
    /// # Errors
    /// Returns an error for unknown or conflicting acknowledgements.
    pub fn acknowledge_committed(
        &mut self,
        receipt: &BackpackAppliedFill,
    ) -> Result<(), BackpackAccountError> {
        if let Some(applied) = self.applied.get(&receipt.key) {
            return if applied == receipt {
                Ok(())
            } else {
                Err(BackpackAccountError::Conflict)
            };
        }
        let (pending, _) = self
            .pending
            .get(&receipt.key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        if pending != receipt {
            return Err(BackpackAccountError::Conflict);
        }
        let (record, _) = self
            .pending
            .remove(&receipt.key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        self.applied.insert(receipt.key, record);
        Ok(())
    }

    /// Returns records suitable for the owner's durable applied-state checkpoint.
    pub fn applied_records(&self) -> impl Iterator<Item = &BackpackAppliedFill> {
        self.applied.values()
    }
}

fn applied_record(report: &FillReport) -> Result<BackpackAppliedFill, BackpackAccountError> {
    let key = BackpackFillKey {
        instrument_id: report.instrument_id,
        trade_id: report.trade_id,
    };
    let exact = serde_json::to_vec(&(
        report.account_id,
        report.instrument_id,
        report.venue_order_id,
        report.trade_id,
        report.order_side,
        report.last_qty,
        report.last_px,
        report.commission,
        report.liquidity_side,
        report.ts_event,
        report.venue_position_id,
    ))
    .map_err(|_| BackpackAccountError::Decode)?;
    Ok(BackpackAppliedFill {
        key,
        fingerprint: blake3::hash(&exact).to_hex().to_string(),
    })
}
