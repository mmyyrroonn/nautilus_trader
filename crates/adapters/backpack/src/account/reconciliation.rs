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

use nautilus_core::UnixNanos;
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

/// The two authenticated transports publish different event instants for one trade.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum BackpackFillTimeSource {
    RestHistory,
    PrivateEngine,
}

/// Original source instants, separate from the immutable consumer receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BackpackFillTimeEvidence {
    pub(crate) source: BackpackFillTimeSource,
    pub(crate) first: UnixNanos,
    pub(crate) fine: Option<UnixNanos>,
}

impl BackpackFillTimeEvidence {
    pub(crate) fn new(source: BackpackFillTimeSource, first: UnixNanos) -> Self {
        Self {
            source,
            first,
            fine: (!first.as_u64().is_multiple_of(1_000_000)).then_some(first),
        }
    }

    pub(crate) fn compatible(&self, next: UnixNanos) -> bool {
        fill_time_precision_compatible(self.first, next)
            && self
                .fine
                .is_none_or(|fine| fill_time_precision_compatible(fine, next))
    }

    pub(crate) fn observe(&mut self, next: UnixNanos) {
        if !next.as_u64().is_multiple_of(1_000_000) {
            self.fine.get_or_insert(next);
        }
    }
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
    applied_reports: BTreeMap<BackpackFillKey, FillReport>,
    fine_event_times: BTreeMap<BackpackFillKey, UnixNanos>,
    source_times: BTreeMap<BackpackFillKey, Vec<BackpackFillTimeEvidence>>,
    last_conflict_fields: Vec<&'static str>,
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
            applied_reports: BTreeMap::new(),
            fine_event_times: BTreeMap::new(),
            source_times: BTreeMap::new(),
            last_conflict_fields: Vec::new(),
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
        self.stage_inner(report, None)
    }

    /// Cross-transport dedup compares every identity/economic field, while each source
    /// retains its own event-time invariant. It never changes the original receipt.
    pub(crate) fn stage_from(
        &mut self,
        report: FillReport,
        source: BackpackFillTimeSource,
    ) -> Result<BackpackFillStage, BackpackAccountError> {
        self.stage_inner(report, Some(source))
    }

    /// Rechecks retained attribution only at an instant already observed on its source.
    pub(crate) fn stage_retained(
        &mut self,
        report: FillReport,
    ) -> Result<BackpackFillStage, BackpackAccountError> {
        let key = BackpackFillKey {
            instrument_id: report.instrument_id,
            trade_id: report.trade_id,
        };
        let source = self.source_times.get(&key).and_then(|times| {
            times
                .iter()
                .find(|time| time.first == report.ts_event || time.fine == Some(report.ts_event))
                .map(|time| time.source)
        });
        self.stage_inner(report, source)
    }

    fn stage_inner(
        &mut self,
        report: FillReport,
        source: Option<BackpackFillTimeSource>,
    ) -> Result<BackpackFillStage, BackpackAccountError> {
        let record = applied_record(&report)?;
        let time_conflict = if let Some(source) = source {
            self.source_times
                .get(&record.key)
                .and_then(|times| times.iter().find(|time| time.source == source))
                .is_some_and(|time| !time.compatible(report.ts_event))
        } else {
            let original = self
                .applied_reports
                .get(&record.key)
                .or_else(|| self.pending.get(&record.key).map(|(_, previous)| previous));
            self.fine_event_times
                .get(&record.key)
                .is_some_and(|previous| !fill_time_precision_compatible(*previous, report.ts_event))
                || self.source_times.get(&record.key).is_some_and(|times| {
                    times
                        .iter()
                        .filter(|time| {
                            original.is_some_and(|previous| time.first == previous.ts_event)
                        })
                        .any(|time| !time.compatible(report.ts_event))
                })
        };
        if let Some(applied) = self.applied.get(&record.key) {
            let previous = self.applied_reports.get(&record.key);
            let compatible = if let Some(previous) = previous {
                compatible_record(previous, applied, &report, source.is_some())?
            } else {
                applied == &record
            };
            if time_conflict || !compatible {
                self.last_conflict_fields = self.applied_reports.get(&record.key).map_or_else(
                    || vec!["fingerprint"],
                    |previous| conflict_fields(previous, &report),
                );
                if time_conflict && !self.last_conflict_fields.contains(&"ts_event") {
                    self.last_conflict_fields.push("ts_event");
                }
                return Err(BackpackAccountError::Conflict);
            }
            self.observe_time(record.key, report.ts_event, source);
            return Ok(BackpackFillStage::AlreadyApplied);
        }
        if let Some((pending, previous)) = self.pending.get_mut(&record.key) {
            if time_conflict || !compatible_record(previous, pending, &report, source.is_some())? {
                self.last_conflict_fields = conflict_fields(previous, &report);
                if time_conflict && !self.last_conflict_fields.contains(&"ts_event") {
                    self.last_conflict_fields.push("ts_event");
                }
                return Err(BackpackAccountError::Conflict);
            }
            // A command ACK may establish attribution after the first fill arrived.
            // Qualify only still-pending delivery; never change applied economics.
            if previous.client_order_id.is_none() && report.client_order_id.is_some() {
                previous.client_order_id = report.client_order_id;
            }
            let retained = previous.clone();
            self.observe_time(record.key, report.ts_event, source);
            return Ok(BackpackFillStage::Pending(Box::new(retained)));
        }
        if self.pending.len() + self.applied.len() >= self.capacity {
            return Err(BackpackAccountError::Budget);
        }
        self.observe_time(record.key, report.ts_event, source);
        self.pending.insert(record.key, (record, report.clone()));
        Ok(BackpackFillStage::Pending(Box::new(report)))
    }

    fn observe_time(
        &mut self,
        key: BackpackFillKey,
        time: UnixNanos,
        source: Option<BackpackFillTimeSource>,
    ) {
        if let Some(source) = source {
            let times = self.source_times.entry(key).or_default();
            if let Some(previous) = times.iter_mut().find(|time| time.source == source) {
                previous.observe(time);
            } else {
                times.push(BackpackFillTimeEvidence::new(source, time));
                times.sort_by_key(|time| time.source);
            }
        } else if !time.as_u64().is_multiple_of(1_000_000) {
            self.fine_event_times.entry(key).or_insert(time);
        }
    }

    pub(crate) fn time_evidence(&self) -> BTreeMap<BackpackFillKey, Vec<BackpackFillTimeEvidence>> {
        self.source_times.clone()
    }

    pub(crate) fn restore_time_evidence(
        &mut self,
        key: BackpackFillKey,
        times: Vec<BackpackFillTimeEvidence>,
    ) -> Result<(), BackpackAccountError> {
        if !self.applied_reports.contains_key(&key) || times.len() > 2 {
            return Err(BackpackAccountError::Conflict);
        }
        let mut sources = std::collections::BTreeSet::new();
        for time in &times {
            if !sources.insert(time.source)
                || time.fine.is_some_and(|fine| {
                    fine.as_u64() % 1_000_000 == 0
                        || !fill_time_precision_compatible(time.first, fine)
                })
                || (time.first.as_u64() % 1_000_000 != 0 && time.fine != Some(time.first))
            {
                return Err(BackpackAccountError::Conflict);
            }
        }
        if !times.is_empty() {
            self.source_times.insert(key, times);
        }
        Ok(())
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
        let (record, report) = self
            .pending
            .remove(&key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        self.applied.insert(key, record);
        self.applied_reports.insert(key, report);
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
        let (record, report) = self
            .pending
            .remove(&receipt.key)
            .ok_or(BackpackAccountError::UnknownAcknowledgement)?;
        self.applied.insert(receipt.key, record);
        self.applied_reports.insert(receipt.key, report);
        Ok(())
    }

    /// Restores only reports whose immutable receipt is already durably applied.
    /// Retained reports permit field-name diagnostics without changing dedup rules.
    pub(crate) fn restore_applied_report(
        &mut self,
        report: FillReport,
    ) -> Result<(), BackpackAccountError> {
        let record = applied_record(&report)?;
        if self.applied.get(&record.key) != Some(&record) {
            return Err(BackpackAccountError::Conflict);
        }
        if !report.ts_event.as_u64().is_multiple_of(1_000_000) {
            self.fine_event_times.insert(record.key, report.ts_event);
        }
        self.applied_reports.insert(record.key, report);
        Ok(())
    }

    pub(crate) fn last_conflict_fields(&self) -> &[&'static str] {
        &self.last_conflict_fields
    }

    /// Returns records suitable for the owner's durable applied-state checkpoint.
    pub fn applied_records(&self) -> impl Iterator<Item = &BackpackAppliedFill> {
        self.applied.values()
    }
}

/// Known REST fills can truncate private engine time to the containing millisecond.
/// This is a precision relation, never an arbitrary timestamp tolerance.
pub(crate) fn fill_time_precision_compatible(left: UnixNanos, right: UnixNanos) -> bool {
    let left = left.as_u64();
    let right = right.as_u64();
    left == right
        || (left / 1_000_000 == right / 1_000_000
            && (left.is_multiple_of(1_000_000) || right.is_multiple_of(1_000_000)))
}

fn compatible_record(
    previous: &FillReport,
    original: &BackpackAppliedFill,
    next: &FillReport,
    cross_transport: bool,
) -> Result<bool, BackpackAccountError> {
    if (!cross_transport && !fill_time_precision_compatible(previous.ts_event, next.ts_event))
        || previous.commission.currency != next.commission.currency
        || (previous.client_order_id.is_some()
            && next.client_order_id.is_some()
            && previous.client_order_id != next.client_order_id)
    {
        return Ok(false);
    }
    // Recompute the entire legacy fingerprint at the original event instant.
    // Every other serialized economic/identity field must still match exactly;
    // the durable original report and receipt are never modified.
    let mut comparable = next.clone();
    comparable.ts_event = previous.ts_event;
    Ok(applied_record(&comparable)? == *original)
}

fn conflict_fields(previous: &FillReport, next: &FillReport) -> Vec<&'static str> {
    let mut fields = Vec::new();
    for (name, changed) in [
        ("account_id", previous.account_id != next.account_id),
        (
            "instrument_id",
            previous.instrument_id != next.instrument_id,
        ),
        (
            "venue_order_id",
            previous.venue_order_id != next.venue_order_id,
        ),
        ("trade_id", previous.trade_id != next.trade_id),
        ("order_side", previous.order_side != next.order_side),
        (
            "last_qty",
            previous.last_qty.as_decimal() != next.last_qty.as_decimal(),
        ),
        (
            "quantity_precision",
            previous.last_qty.precision != next.last_qty.precision,
        ),
        (
            "last_px",
            previous.last_px.as_decimal() != next.last_px.as_decimal(),
        ),
        (
            "price_precision",
            previous.last_px.precision != next.last_px.precision,
        ),
        (
            "commission",
            previous.commission.as_decimal() != next.commission.as_decimal(),
        ),
        (
            "commission_currency",
            previous.commission.currency != next.commission.currency,
        ),
        (
            "liquidity_side",
            previous.liquidity_side != next.liquidity_side,
        ),
        ("ts_event", previous.ts_event != next.ts_event),
        (
            "venue_position_id",
            previous.venue_position_id != next.venue_position_id,
        ),
        (
            "client_order_id",
            previous.client_order_id.is_some()
                && next.client_order_id.is_some()
                && previous.client_order_id != next.client_order_id,
        ),
    ] {
        if changed {
            fields.push(name);
        }
    }
    if fields.is_empty() {
        fields.push("serialized_representation");
    }
    fields
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

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide},
        identifiers::{AccountId, ClientOrderId, VenueOrderId},
        types::{Currency, Money, Price, Quantity},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;

    const COARSE: u64 = 1_791_868_544_780_000_000;
    const FINE: u64 = COARSE + 856_000;

    fn report(timestamp: u64) -> FillReport {
        FillReport::new(
            AccountId::from("BACKPACK-SYNTHETIC"),
            InstrumentId::from("BTC_USDC_PERP.BACKPACK"),
            VenueOrderId::from("synthetic-owned-order"),
            TradeId::from("99738585"),
            OrderSide::Buy,
            Quantity::from_decimal_dp(Decimal::from_str_exact("0.00005").unwrap(), 5).unwrap(),
            Price::from_decimal_dp(Decimal::from_str_exact("83600.8").unwrap(), 1).unwrap(),
            Money::from_decimal(
                Decimal::from_str_exact("0.000419").unwrap(),
                Currency::USDC(),
            )
            .unwrap(),
            LiquiditySide::Maker,
            Some(ClientOrderId::from("O-SYNTHETIC-1")),
            None,
            UnixNanos::from(timestamp),
            UnixNanos::from(timestamp),
            None,
        )
    }

    #[rstest]
    fn millisecond_legacy_receipt_preserves_original_report_and_hash() {
        let original = report(FINE);
        let receipt = applied_record(&original).unwrap();
        let mut restored = BackpackFillReconciler::from_applied(2, [receipt.clone()]).unwrap();
        restored.restore_applied_report(original.clone()).unwrap();
        assert!(matches!(
            restored.stage(report(COARSE)).unwrap(),
            BackpackFillStage::AlreadyApplied
        ));
        assert_eq!(restored.applied_records().next(), Some(&receipt));
        assert_eq!(
            serde_json::to_value(&restored.applied_reports[&receipt.key]).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        assert!(restored.pending_in_event_order().is_empty());
    }

    #[rstest]
    #[case(COARSE, FINE)]
    #[case(FINE, COARSE)]
    fn millisecond_pending_keeps_first_event_time_and_receipt(
        #[case] first: u64,
        #[case] next: u64,
    ) {
        let original = report(first);
        let receipt = applied_record(&original).unwrap();
        let mut pending = BackpackFillReconciler::from_applied(2, []).unwrap();
        pending.stage(original.clone()).unwrap();
        let duplicate = match pending.stage(report(next)).unwrap() {
            BackpackFillStage::Pending(report) => report,
            _ => panic!("unapplied fill was dropped"),
        };
        assert_eq!(duplicate.ts_event, original.ts_event);
        assert_eq!(
            pending.pending_acknowledgement(receipt.key).unwrap(),
            receipt
        );
        assert!(matches!(
            pending.stage(report(FINE + 1_000)),
            Err(BackpackAccountError::Conflict)
        ));
    }

    #[rstest]
    fn millisecond_compatibility_rejects_fine_time_bucket_fee_and_ownership_changes() {
        let original = report(FINE);
        let receipt = applied_record(&original).unwrap();
        let mut restored = BackpackFillReconciler::from_applied(2, [receipt.clone()]).unwrap();
        restored.restore_applied_report(original).unwrap();
        let mut fee = report(COARSE);
        fee.commission = Money::from_decimal(
            Decimal::from_str_exact("0.000418").unwrap(),
            Currency::USDC(),
        )
        .unwrap();
        let mut owner = report(COARSE);
        owner.client_order_id = Some(ClientOrderId::from("O-SYNTHETIC-OTHER"));
        for changed in [report(FINE + 1_000), report(COARSE + 1_000_000), fee, owner] {
            assert!(matches!(
                restored.stage(changed),
                Err(BackpackAccountError::Conflict)
            ));
        }
        assert_eq!(restored.applied_records().next(), Some(&receipt));
    }

    #[rstest]
    fn millisecond_hash_only_restore_cannot_guess_time_precision() {
        let receipt = applied_record(&report(FINE)).unwrap();
        let mut restored = BackpackFillReconciler::from_applied(2, [receipt.clone()]).unwrap();
        assert!(matches!(
            restored.stage(report(COARSE)),
            Err(BackpackAccountError::Conflict)
        ));
        assert_eq!(restored.last_conflict_fields(), &["fingerprint"]);
        assert_eq!(restored.applied_records().next(), Some(&receipt));
    }

    const MAKER_ENGINE: u64 = 1_791_885_452_846_264_000;
    const MAKER_HISTORY: u64 = 1_791_885_452_845_000_000;

    #[rstest]
    #[case(
        BackpackFillTimeSource::PrivateEngine,
        MAKER_ENGINE,
        BackpackFillTimeSource::RestHistory,
        MAKER_HISTORY
    )]
    #[case(
        BackpackFillTimeSource::RestHistory,
        MAKER_HISTORY,
        BackpackFillTimeSource::PrivateEngine,
        MAKER_ENGINE
    )]
    fn cross_source_pending_preserves_first_report_and_receipt(
        #[case] first_source: BackpackFillTimeSource,
        #[case] first_time: u64,
        #[case] next_source: BackpackFillTimeSource,
        #[case] next_time: u64,
    ) {
        let original = report(first_time);
        let receipt = applied_record(&original).unwrap();
        let mut reconciler = BackpackFillReconciler::from_applied(2, []).unwrap();
        reconciler
            .stage_from(original.clone(), first_source)
            .unwrap();
        let BackpackFillStage::Pending(duplicate) = reconciler
            .stage_from(report(next_time), next_source)
            .unwrap()
        else {
            panic!("unacknowledged fill was lost");
        };
        assert_eq!(duplicate.ts_event, original.ts_event);
        assert_eq!(
            reconciler.pending_acknowledgement(receipt.key).unwrap(),
            receipt
        );
        assert_eq!(reconciler.time_evidence()[&receipt.key].len(), 2);
        assert!(matches!(
            reconciler.stage(report(next_time)),
            Err(BackpackAccountError::Conflict)
        ));
        assert!(matches!(
            reconciler.stage_from(report(first_time + 1_000_000), first_source),
            Err(BackpackAccountError::Conflict)
        ));
    }

    #[rstest]
    fn cross_source_legacy_restore_preserves_applied_economics_and_persists_source_times() {
        let original = report(MAKER_ENGINE);
        let receipt = applied_record(&original).unwrap();
        let mut restored = BackpackFillReconciler::from_applied(2, [receipt.clone()]).unwrap();
        restored.restore_applied_report(original.clone()).unwrap();
        assert!(restored.time_evidence().is_empty()); // Legacy source must not be guessed.
        assert!(matches!(
            restored
                .stage_from(report(MAKER_HISTORY), BackpackFillTimeSource::RestHistory)
                .unwrap(),
            BackpackFillStage::AlreadyApplied
        ));
        assert!(matches!(
            restored
                .stage_from(original.clone(), BackpackFillTimeSource::PrivateEngine)
                .unwrap(),
            BackpackFillStage::AlreadyApplied
        ));
        let times = restored.time_evidence().remove(&receipt.key).unwrap();
        let bytes = serde_json::to_vec(&times).unwrap();
        let mut restarted = BackpackFillReconciler::from_applied(2, [receipt.clone()]).unwrap();
        restarted.restore_applied_report(original.clone()).unwrap();
        restarted
            .restore_time_evidence(receipt.key, serde_json::from_slice(&bytes).unwrap())
            .unwrap();
        assert!(matches!(
            restarted
                .stage_from(report(MAKER_HISTORY), BackpackFillTimeSource::RestHistory)
                .unwrap(),
            BackpackFillStage::AlreadyApplied
        ));
        for (source, timestamp) in [
            (
                BackpackFillTimeSource::RestHistory,
                MAKER_HISTORY + 1_000_000,
            ),
            (BackpackFillTimeSource::PrivateEngine, MAKER_ENGINE + 1_000),
        ] {
            assert!(matches!(
                restarted.stage_from(report(timestamp), source),
                Err(BackpackAccountError::Conflict)
            ));
        }
        assert_eq!(restarted.applied_records().next(), Some(&receipt));
        assert_eq!(
            serde_json::to_value(&restarted.applied_reports[&receipt.key]).unwrap(),
            serde_json::to_value(original).unwrap()
        );
        assert!(restarted.pending_in_event_order().is_empty());
    }

    #[rstest]
    fn cross_source_does_not_bridge_distinct_fine_times_or_accept_economic_changes() {
        let mut reconciler = BackpackFillReconciler::from_applied(2, []).unwrap();
        reconciler
            .stage_from(report(COARSE), BackpackFillTimeSource::PrivateEngine)
            .unwrap();
        reconciler
            .stage_from(report(FINE), BackpackFillTimeSource::PrivateEngine)
            .unwrap();
        reconciler
            .stage_from(report(MAKER_HISTORY), BackpackFillTimeSource::RestHistory)
            .unwrap();
        assert!(matches!(
            reconciler.stage(report(FINE + 1_000)),
            Err(BackpackAccountError::Conflict)
        ));
        assert!(matches!(
            reconciler.stage_from(report(FINE + 1_000), BackpackFillTimeSource::PrivateEngine),
            Err(BackpackAccountError::Conflict)
        ));
        let mut changed = report(MAKER_HISTORY);
        changed.commission = Money::from_decimal(
            Decimal::from_str_exact("0.000418").unwrap(),
            Currency::USDC(),
        )
        .unwrap();
        assert!(matches!(
            reconciler.stage_from(changed, BackpackFillTimeSource::RestHistory),
            Err(BackpackAccountError::Conflict)
        ));
        assert!(reconciler.last_conflict_fields().contains(&"commission"));
    }

    #[rstest]
    fn cross_source_hash_only_restore_cannot_skip_economic_verification() {
        let receipt = applied_record(&report(MAKER_ENGINE)).unwrap();
        let mut restored = BackpackFillReconciler::from_applied(2, [receipt]).unwrap();
        assert!(matches!(
            restored.stage_from(report(MAKER_HISTORY), BackpackFillTimeSource::RestHistory),
            Err(BackpackAccountError::Conflict)
        ));
        assert!(restored.time_evidence().is_empty());
    }
}
