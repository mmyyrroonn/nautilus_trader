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

//! Runtime tokens and retained true observations for explicitly guarded local peers.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use nautilus_model::identifiers::{ClientOrderId, InstrumentId, VenueOrderId};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::{
    account::{
        models::{BackpackFill, BackpackOrder},
        reconciliation::{BackpackFillKey, BackpackFillTimeEvidence, BackpackFillTimeSource},
    },
    execution::{
        guard::{BackpackLoopbackAccountFacts, BackpackLoopbackMarketFacts},
        owner::{BackpackOrderOwner, BackpackShutdownReport},
    },
    identity::BackpackClientIdNamespace,
};

/// An opaque local-peer observation token. It never attests a production account.
/// The client rejects tokens from an older run, private connection or admitted session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackLoopbackSession {
    pub(crate) run: u64,
    pub(crate) client_generation: u64,
    pub(crate) private_epoch: u64,
    pub(crate) generation: u64,
}
impl BackpackLoopbackSession {
    /// Generation that every explicit account/market fact must name.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Debug)]
pub(crate) struct LoopbackExecution {
    pub owner: Arc<BackpackOrderOwner>,
    pub commands: Arc<Semaphore>,
    released: Mutex<BTreeSet<ClientOrderId>>,
    pub native_orders: Mutex<BTreeMap<ClientOrderId, nautilus_model::orders::OrderAny>>,
    pub pending_submissions: Mutex<BTreeSet<ClientOrderId>>,
    pub pending: Mutex<BTreeMap<BackpackFillKey, BackpackFill>>,
    // Preserve the first fine wire timestamp alongside the original raw report.
    // A coarse baseline must never permit two different fine event instants.
    fine_fill_times: Mutex<BTreeMap<BackpackFillKey, (nautilus_core::UnixNanos, String)>>,
    source_fill_times: Mutex<BTreeMap<BackpackFillKey, Vec<BackpackFillTimeEvidence>>>,
    pub orders: Mutex<BTreeMap<(InstrumentId, VenueOrderId), BackpackOrder>>,
    pub terminal_published: Mutex<BTreeSet<ClientOrderId>>,
    session: Mutex<(u64, Option<BackpackLoopbackSession>)>,
    capacity: usize,
}
impl LoopbackExecution {
    pub(crate) fn new(
        owner: Arc<BackpackOrderOwner>,
        capacity: usize,
        commands: usize,
    ) -> anyhow::Result<Self> {
        let generation = owner.guard().lock()?.generation;
        Ok(Self {
            owner,
            commands: Arc::new(Semaphore::new(commands)),
            released: Mutex::new(BTreeSet::new()),
            native_orders: Mutex::new(BTreeMap::new()),
            pending_submissions: Mutex::new(BTreeSet::new()),
            pending: Mutex::new(BTreeMap::new()),
            fine_fill_times: Mutex::new(BTreeMap::new()),
            source_fill_times: Mutex::new(BTreeMap::new()),
            orders: Mutex::new(BTreeMap::new()),
            terminal_published: Mutex::new(BTreeSet::new()),
            session: Mutex::new((generation, None)),
            capacity,
        })
    }
    pub(crate) fn begin(
        &self,
        run: u64,
        generation: u64,
        epoch: u64,
        namespace: &BackpackClientIdNamespace,
        public: crate::telemetry::BackpackPublicTelemetry,
    ) -> anyhow::Result<BackpackLoopbackSession> {
        let mut state = self.session.lock();
        let next = state
            .0
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("execution generation exhausted"))?;
        self.owner.guard().begin_session(namespace, next)?;
        // A failed public binding still consumes the generation accepted by the owner
        *state = (next, None);
        if let Err(error) = self.owner.guard().bind_public(public) {
            self.owner.guard().invalidate();
            state.1 = None;
            return Err(error.into());
        }
        let token = BackpackLoopbackSession {
            run,
            client_generation: generation,
            private_epoch: epoch,
            generation: next,
        };
        *state = (next, Some(token));
        Ok(token)
    }
    pub(crate) fn retain_native(
        &self,
        id: ClientOrderId,
        order: nautilus_model::orders::OrderAny,
    ) -> anyhow::Result<()> {
        let mut orders = self.native_orders.lock();
        anyhow::ensure!(
            !orders.contains_key(&id),
            "original native submission is already owned"
        );
        anyhow::ensure!(
            orders.len() < self.capacity,
            "native order evidence bound exhausted"
        );
        orders.insert(id, order);
        Ok(())
    }
    pub(crate) fn release(&self, id: ClientOrderId) {
        self.released.lock().insert(id);
    }
    pub(crate) fn released(&self, id: ClientOrderId) -> bool {
        self.released.lock().contains(&id)
    }
    pub(crate) fn token(&self) -> anyhow::Result<BackpackLoopbackSession> {
        self.session
            .lock()
            .1
            .ok_or_else(|| anyhow::anyhow!("guarded peer session is not admitted"))
    }
    pub(crate) fn current(&self, token: BackpackLoopbackSession) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.session.lock().1 == Some(token),
            "stale guarded session"
        );
        Ok(())
    }
    pub(crate) fn account(
        &self,
        token: BackpackLoopbackSession,
        facts: BackpackLoopbackAccountFacts,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        let state = self.session.lock();
        anyhow::ensure!(
            state.1 == Some(token) && facts.generation == token.generation,
            "stale account evidence"
        );
        self.owner.guard().update_account(facts, now_ms)?;
        Ok(())
    }
    pub(crate) fn market(
        &self,
        token: BackpackLoopbackSession,
        facts: BackpackLoopbackMarketFacts,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        let state = self.session.lock();
        anyhow::ensure!(
            state.1 == Some(token) && facts.generation == token.generation,
            "stale market evidence"
        );
        self.owner.guard().update_market(facts, now_ms)?;
        Ok(())
    }
    pub(crate) fn invalidate(&self) {
        let mut state = self.session.lock();
        state.1 = None;
        self.owner.guard().invalidate();
    }
    pub(crate) fn stop(&self) -> anyhow::Result<BackpackShutdownReport> {
        let mut state = self.session.lock();
        state.1 = None;
        Ok(self.owner.stop()?)
    }
    pub(crate) fn stop_with_account(
        &self,
        proof: Result<
            crate::execution::owner::BackpackShutdownAccountProof,
            crate::execution::owner::BackpackShutdownReason,
        >,
    ) -> anyhow::Result<BackpackShutdownReport> {
        let mut state = self.session.lock();
        state.1 = None;
        Ok(self.owner.stop_with_account(proof)?)
    }
    pub(crate) fn retain_fill_from(
        &self,
        key: BackpackFillKey,
        raw: BackpackFill,
        source: BackpackFillTimeSource,
    ) -> anyhow::Result<()> {
        self.retain_fill(key, raw, Some(source))
    }

    fn retain_fill(
        &self,
        key: BackpackFillKey,
        raw: BackpackFill,
        source: Option<BackpackFillTimeSource>,
    ) -> anyhow::Result<()> {
        let mut pending = self.pending.lock();
        let timestamp = crate::account::reports::fill_time(&raw.timestamp)?;
        let mut fine_times = self.fine_fill_times.lock();
        let mut source_times = self.source_fill_times.lock();
        anyhow::ensure!(
            if let Some(source) = source {
                source_times
                    .get(&key)
                    .and_then(|times| times.iter().find(|time| time.source == source))
                    .is_none_or(|time| time.compatible(timestamp))
            } else {
                fine_times.get(&key).is_none_or(|(previous, _)| {
                    crate::account::reconciliation::fill_time_precision_compatible(
                        *previous, timestamp,
                    )
                })
            },
            "conflicting pending fill economics or attribution"
        );
        if let Some(previous) = pending.get_mut(&key) {
            // WS civil datetime rendering can omit trailing fractional zeroes while
            // REST can additionally truncate to the containing millisecond.
            // Preserve the original wire string and require exact economics.
            let same_instant = source.is_some()
                || crate::account::reconciliation::fill_time_precision_compatible(
                    crate::account::reports::fill_time(&previous.timestamp)?,
                    timestamp,
                );
            anyhow::ensure!(
                previous.order_id == raw.order_id
                    && previous.symbol == raw.symbol
                    && previous.trade_id == raw.trade_id
                    && previous.price == raw.price
                    && previous.quantity == raw.quantity
                    && previous.fee == raw.fee
                    && previous.fee_symbol == raw.fee_symbol
                    && previous.is_maker == raw.is_maker
                    && previous.side == raw.side
                    && same_instant
                    && previous.system_order_type == raw.system_order_type
                    && (previous.client_id.is_none()
                        || raw.client_id.is_none()
                        || previous.client_id == raw.client_id),
                "conflicting pending fill economics or attribution"
            );
            if previous.client_id.is_none() {
                previous.client_id = raw.client_id;
            }
        } else {
            anyhow::ensure!(
                pending.len() < self.capacity,
                "pending attribution bound exhausted"
            );
            pending.insert(key, raw.clone());
        }
        if let Some(source) = source {
            let times = source_times.entry(key).or_default();
            if let Some(previous) = times.iter_mut().find(|time| time.source == source) {
                previous.observe(timestamp);
            } else {
                times.push(BackpackFillTimeEvidence::new(source, timestamp));
            }
        } else if timestamp.as_u64() % 1_000_000 != 0 {
            fine_times.entry(key).or_insert((timestamp, raw.timestamp));
        }
        Ok(())
    }
    pub(crate) fn retain_order(
        &self,
        instrument: InstrumentId,
        raw: BackpackOrder,
    ) -> anyhow::Result<()> {
        let id = VenueOrderId::new_checked(&raw.id)?;
        let mut orders = self.orders.lock();
        anyhow::ensure!(
            orders.contains_key(&(instrument, id)) || orders.len() < self.capacity,
            "pending order observation bound exhausted"
        );
        let mut raw = raw;
        if let Some(old) = orders.get(&(instrument, id)) {
            anyhow::ensure!(
                old.symbol == raw.symbol
                    && old.side == raw.side
                    && old.order_type == raw.order_type
                    && old.time_in_force == raw.time_in_force
                    && old.system_order_type == raw.system_order_type
                    && compatible(old.quantity, raw.quantity)
                    && compatible(old.price, raw.price)
                    && compatible(old.post_only, raw.post_only)
                    && compatible(old.reduce_only, raw.reduce_only)
                    && compatible(old.quote_quantity, raw.quote_quantity)
                    && compatible(old.client_id, raw.client_id),
                "conflicting pending order economics or attribution"
            );
            raw.client_id = raw.client_id.or(old.client_id);
            raw.quantity = raw.quantity.or(old.quantity);
            raw.price = raw.price.or(old.price);
            raw.post_only = raw.post_only.or(old.post_only);
            raw.reduce_only = raw.reduce_only.or(old.reduce_only);
            raw.quote_quantity = raw.quote_quantity.or(old.quote_quantity);
            let old_terminal = terminal(&old.status);
            let new_terminal = terminal(&raw.status);
            if old_terminal && new_terminal {
                let later_full_fill = matches!(old.status.as_str(), "Cancelled" | "Expired")
                    && raw.status == "Filled"
                    && old
                        .executed_quantity
                        .zip(raw.executed_quantity)
                        .is_some_and(|(previous, next)| next.0 > previous.0);
                anyhow::ensure!(
                    old.status == raw.status || later_full_fill,
                    "conflicting terminal order observations"
                );
                if later_full_fill {
                    raw.status.clone_from(&old.status);
                }
            }
            match (old.executed_quantity, raw.executed_quantity) {
                (Some(previous), Some(next)) if next.0 < previous.0 => return Ok(()),
                (Some(_), None) => return Ok(()),
                _ => {}
            }
            if old_terminal && !new_terminal {
                // A late trade can increase cumulative economics without undoing the
                // terminal lifecycle. Keep that higher watermark for reconciliation.
                raw.status.clone_from(&old.status);
                raw.expiry_reason.clone_from(&old.expiry_reason);
            }
            if old.executed_quantity == raw.executed_quantity {
                anyhow::ensure!(
                    compatible(old.executed_quote_quantity, raw.executed_quote_quantity),
                    "conflicting cumulative quote economics"
                );
                raw.executed_quote_quantity =
                    raw.executed_quote_quantity.or(old.executed_quote_quantity);
            }
        }
        orders.insert((instrument, id), raw);
        Ok(())
    }
}

fn compatible<T: Copy + PartialEq>(previous: Option<T>, next: Option<T>) -> bool {
    previous.is_none() || next.is_none() || previous == next
}
fn terminal(status: &str) -> bool {
    matches!(status, "Filled" | "Cancelled" | "Expired")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
        config::BackpackConfig,
        execution::{
            guard::BackpackExecutionAuthority,
            owner::{BackpackMutationPolicy, BackpackOrderOwnerConfig},
        },
        http::{
            client::{BackpackClock, BackpackSystemClock},
            quota::BackpackQuota,
        },
        identity::BackpackClientIdStore,
    };
    fn control() -> (tempfile::TempDir, LoopbackExecution) {
        let directory = tempfile::tempdir().unwrap();
        let endpoints =
            BackpackEndpoints::loopback_override("http://127.0.0.1:8123", "ws://127.0.0.1:8123")
                .unwrap();
        let namespace =
            BackpackClientIdNamespace::loopback_peer(&endpoints, "synthetic", None).unwrap();
        let clock = Arc::new(BackpackSystemClock);
        let authority = BackpackExecutionAuthority {
            expires_at_ms: clock.timestamp_ms().unwrap() + 60_000,
            max_account_age_ms: 1000,
            max_market_age_ms: 1000,
            max_order_notional: Decimal::ONE,
            max_reserved_notional: Decimal::ONE,
            max_reserved_margin: Decimal::ONE,
            max_unsettled_orders: 1,
            allow_new_risk: true,
            allow_reduction: true,
            allow_owned_cancel: true,
        };
        let owner = BackpackOrderOwner::new_checked(BackpackOrderOwnerConfig {
            config: BackpackConfig::with_endpoints_checked(
                vec!["BTC_USDC_PERP".into()],
                endpoints.clone(),
            )
            .unwrap(),
            credential: BackpackCredential::loopback_peer(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                &endpoints,
            )
            .unwrap(),
            endpoints,
            quota: BackpackQuota::default(),
            clock,
            policy: BackpackMutationPolicy {
                window: Default::default(),
                budget: std::time::Duration::from_secs(1),
            },
            identities: BackpackClientIdStore::open(directory.path(), &namespace).unwrap(),
            namespace,
            authority,
        })
        .unwrap();
        (
            directory,
            LoopbackExecution::new(Arc::new(owner), 16, 2).unwrap(),
        )
    }
    fn fixture(name: &str) -> Value {
        serde_json::from_str::<Value>(include_str!("../../test_data/account/synthetic.json"))
            .unwrap()[name]
            .clone()
    }
    fn key() -> BackpackFillKey {
        BackpackFillKey {
            instrument_id: InstrumentId::from("BTC_USDC_PERP.BACKPACK"),
            trade_id: nautilus_model::identifiers::TradeId::from("9007199254740993"),
        }
    }
    #[rstest]
    #[case("fee",json!("0.000001"))]
    #[case("feeSymbol",json!("BTC"))]
    #[case("price",json!("100.2"))]
    #[case("quantity",json!("0.00002"))]
    #[case("isMaker",json!(false))]
    #[case("side",json!("Ask"))]
    #[case("timestamp",json!("2026-10-02T00:00:00.123457"))]
    #[case("orderId",json!("different-order"))]
    #[case("symbol",json!("ETH_USDC_PERP"))]
    #[case("clientId",json!("2"))]
    #[case("systemOrderType",json!("Liquidation"))]
    fn retained_fill_rejects_conflicting_economics_before_any_command_ack(
        #[case] field: &str,
        #[case] value: Value,
    ) {
        let (_directory, control) = control();
        let raw = fixture("fill");
        control
            .retain_fill(key(), serde_json::from_value(raw.clone()).unwrap(), None)
            .unwrap();
        let mut conflicting = raw;
        conflicting[field] = value;
        assert!(
            control
                .retain_fill(key(), serde_json::from_value(conflicting).unwrap(), None)
                .is_err()
        );
        assert_eq!(
            control.pending.lock()[&key()].fee.0,
            Decimal::from_str_exact("-0.000001").unwrap()
        );
    }
    #[rstest]
    fn retained_fill_allows_only_missing_to_known_candidate_upgrade() {
        let (_directory, control) = control();
        let mut raw: BackpackFill = serde_json::from_value(fixture("fill")).unwrap();
        raw.client_id = None;
        control.retain_fill(key(), raw.clone(), None).unwrap();
        raw.client_id = Some("1".into());
        control.retain_fill(key(), raw.clone(), None).unwrap();
        raw.client_id = None;
        control.retain_fill(key(), raw, None).unwrap();
        assert_eq!(
            control.pending.lock()[&key()].client_id.as_deref(),
            Some("1")
        );
    }
    #[rstest]
    #[case(true)]
    #[case(false)]
    fn millisecond_retained_fill_preserves_original_raw_and_rejects_distinct_fine(
        #[case] fine_first: bool,
    ) {
        let (_directory, control) = control();
        let mut fine: BackpackFill = serde_json::from_value(fixture("fill")).unwrap();
        fine.timestamp = "2026-10-02T00:00:00.123456".into();
        let mut coarse = fine.clone();
        coarse.timestamp = "2026-10-02T00:00:00.123".into();
        let (original, duplicate) = if fine_first {
            (fine.clone(), coarse)
        } else {
            (coarse, fine.clone())
        };
        control.retain_fill(key(), original.clone(), None).unwrap();
        control.retain_fill(key(), duplicate, None).unwrap();
        assert_eq!(control.pending.lock()[&key()].timestamp, original.timestamp);
        fine.timestamp = "2026-10-02T00:00:00.123457".into();
        assert!(control.retain_fill(key(), fine, None).is_err());
    }
    #[rstest]
    #[case(true)]
    #[case(false)]
    fn cross_source_retained_fill_preserves_wire_time_and_rejects_same_source_changes(
        #[case] private_first: bool,
    ) {
        let (_directory, control) = control();
        let mut private: BackpackFill = serde_json::from_value(fixture("fill")).unwrap();
        private.timestamp = "2026-10-13T09:57:32.846264".into();
        let mut history = private.clone();
        history.timestamp = "2026-10-13T09:57:32.845".into();
        let (first, first_source, next, next_source) = if private_first {
            (
                private.clone(),
                BackpackFillTimeSource::PrivateEngine,
                history.clone(),
                BackpackFillTimeSource::RestHistory,
            )
        } else {
            (
                history.clone(),
                BackpackFillTimeSource::RestHistory,
                private.clone(),
                BackpackFillTimeSource::PrivateEngine,
            )
        };
        control
            .retain_fill_from(key(), first.clone(), first_source)
            .unwrap();
        control.retain_fill_from(key(), next, next_source).unwrap();
        assert_eq!(control.pending.lock()[&key()].timestamp, first.timestamp);
        assert_eq!(control.source_fill_times.lock()[&key()].len(), 2);
        private.timestamp = "2026-10-13T09:57:32.846265".into();
        assert!(
            control
                .retain_fill_from(key(), private, BackpackFillTimeSource::PrivateEngine)
                .is_err()
        );
        history.timestamp = "2026-10-13T09:57:32.846".into();
        assert!(
            control
                .retain_fill_from(key(), history, BackpackFillTimeSource::RestHistory)
                .is_err()
        );
        let mut fee = first;
        fee.fee.0 += Decimal::ONE;
        assert!(control.retain_fill_from(key(), fee, first_source).is_err());
    }

    #[rstest]
    fn cross_source_retained_fill_cannot_bridge_distinct_fine_times() {
        let (_directory, control) = control();
        let mut raw: BackpackFill = serde_json::from_value(fixture("fill")).unwrap();
        raw.timestamp = "2026-10-13T09:57:32.846".into();
        control
            .retain_fill_from(key(), raw.clone(), BackpackFillTimeSource::PrivateEngine)
            .unwrap();
        raw.timestamp = "2026-10-13T09:57:32.846264".into();
        control
            .retain_fill_from(key(), raw.clone(), BackpackFillTimeSource::PrivateEngine)
            .unwrap();
        raw.timestamp = "2026-10-13T09:57:32.845".into();
        control
            .retain_fill_from(key(), raw.clone(), BackpackFillTimeSource::RestHistory)
            .unwrap();
        raw.timestamp = "2026-10-13T09:57:32.846265".into();
        assert!(
            control
                .retain_fill_from(key(), raw, BackpackFillTimeSource::PrivateEngine)
                .is_err()
        );
    }

    #[rstest]
    fn retained_order_preserves_high_cumulative_terminal_and_known_candidate() {
        let (_directory, control) = control();
        let instrument = key().instrument_id;
        let mut raw: BackpackOrder = serde_json::from_value(fixture("resting_order")).unwrap();
        raw.status = "Cancelled".into();
        control.retain_order(instrument, raw.clone()).unwrap();
        let mut older = raw.clone();
        older.status = "New".into();
        older.client_id = None;
        older.executed_quantity = Some(crate::account::models::BackpackDecimal(Decimal::ZERO));
        control.retain_order(instrument, older).unwrap();
        let id = VenueOrderId::from("synthetic-order-A");
        assert_eq!(control.orders.lock()[&(instrument, id)].status, "Cancelled");
        assert_eq!(control.orders.lock()[&(instrument, id)].client_id, Some(1));
        raw.client_id = None;
        raw.status = "Filled".into();
        raw.executed_quantity = Some(crate::account::models::BackpackDecimal(
            Decimal::from_str_exact("0.00002").unwrap(),
        ));
        control.retain_order(instrument, raw).unwrap();
        let orders = control.orders.lock();
        let latest = &orders[&(instrument, id)];
        assert_eq!(latest.status, "Cancelled");
        assert_eq!(latest.client_id, Some(1));
        assert_eq!(
            latest.executed_quantity.unwrap().0,
            Decimal::from_str_exact("0.00002").unwrap()
        );
    }
}
