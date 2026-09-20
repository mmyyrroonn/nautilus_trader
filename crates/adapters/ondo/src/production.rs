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

//! Exact, immutable limits for a separately authorized production probe.

use crate::http::{models::MarketInfo, orders::client_order_lookup_target};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{data::QuoteTick, identifiers::InstrumentId};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::Write,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The complete approved entry and cleanup envelope for one production run.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object, frozen)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoExecutionEnvelopeConfig {
    /// The sole permitted instrument.
    pub instrument_id: InstrumentId,
    /// The approved opening side.
    pub entry_side: String,
    /// The approved opening quantity ceiling.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub entry_max_quantity: Decimal,
    /// The directional opening limit price.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub entry_worst_price: Decimal,
    /// The approved opening notional ceiling.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub entry_max_notional_usd: Decimal,
    /// The side that closes the opening fill.
    pub close_side: String,
    /// The maximum closing quantity.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub close_max_quantity: Decimal,
    /// The directional closing limit price.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub close_worst_price: Decimal,
    /// The maximum number of closing attempts.
    pub max_close_attempts: u32,
    /// The order notional limit, at most USD 50.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub max_notional_per_order_usd: Decimal,
    /// The gross exposure limit, at most USD 100.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub max_gross_exposure_usd: Decimal,
    /// The approved minimum available margin, at least 25 USDC, without conversion.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub min_available_margin_usdc: Decimal,
    /// The total number of creates, at most three.
    pub max_orders: u32,
    /// The number of opening attempts, exactly one.
    pub max_new_risk_requests: u32,
    /// The shared create and own-cancel request count.
    pub max_app_requests: u32,
    /// The immutable opening deadline.
    pub entry_deadline_unix_nanos: u64,
    /// The immutable cleanup deadline.
    pub cleanup_deadline_unix_nanos: u64,
    /// Requires a verified whole-account flat start.
    pub require_flat_start: bool,
}

fn deserialize_exact_decimal<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Decimal, D::Error> {
    let value = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&value).map_err(serde::de::Error::custom)
}

impl OndoExecutionEnvelopeConfig {
    /// Validates exact limits and a maximum ten-minute run, without changing any value.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, inconsistent, expired or excessive limits.
    pub fn validate(&self, now: u64) -> Result<(), String> {
        let fail = || Err("invalid or excessive production execution envelope".to_string());
        if !self.require_flat_start
            || self.instrument_id.to_string() != "NVDA-USD-PERP.ONDO"
            || crate::common::parse::instrument_id_to_market(&self.instrument_id).is_err()
            || !matches!(
                (self.entry_side.as_str(), self.close_side.as_str()),
                ("buy", "sell") | ("sell", "buy")
            )
            || self.min_available_margin_usdc < Decimal::from(25)
            || self.entry_max_quantity <= Decimal::ZERO
            || self.close_max_quantity <= Decimal::ZERO
            || self.close_max_quantity > self.entry_max_quantity
            || self.entry_worst_price <= Decimal::ZERO
            || self.close_worst_price <= Decimal::ZERO
            || self.entry_max_notional_usd < Decimal::from(10)
            || self.entry_max_notional_usd > Decimal::from(20)
            || self.max_notional_per_order_usd <= Decimal::ZERO
            || self.max_notional_per_order_usd > Decimal::from(50)
            || self.max_gross_exposure_usd <= Decimal::ZERO
            || self.max_gross_exposure_usd > Decimal::from(100)
            || self.entry_max_notional_usd > self.max_notional_per_order_usd
            || self.entry_max_notional_usd > self.max_gross_exposure_usd
            || !(1..=2).contains(&self.max_close_attempts)
            || !(2..=3).contains(&self.max_orders)
            || self.max_orders > 1 + self.max_close_attempts
            || self.max_new_risk_requests != 1
            || self.max_app_requests < self.max_orders
            || self.max_app_requests > 6
            || now >= self.entry_deadline_unix_nanos
            || self.entry_deadline_unix_nanos >= self.cleanup_deadline_unix_nanos
            || self.cleanup_deadline_unix_nanos.saturating_sub(now) > 600_000_000_000
        {
            return fail();
        }
        let Some(entry) = self.entry_max_quantity.checked_mul(self.entry_worst_price) else {
            return fail();
        };
        if entry > self.entry_max_notional_usd {
            return fail();
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProductionEvidence {
    pub ready: bool,
    pub identity_matched: bool,
    pub journal_healthy: bool,
    pub dms_verified: bool,
    pub available_margin_usdc: Option<Decimal>,
    pub orders: BTreeMap<String, (Decimal, bool, Option<String>)>,
    pub unknown: usize,
}

type EvidenceReader = dyn Fn(bool) -> Result<ProductionEvidence, String> + Send + Sync;

/// A native run authority whose constructor and mutation paths are crate-private.
///
/// Public HTTP constructors can receive this type, but cannot manufacture an authority.
pub struct ProductionAuthority {
    envelope: OndoExecutionEnvelopeConfig,
    run_id: String,
    created_at: u64,
    evidence: OnceLock<Arc<EvidenceReader>>,
    state: Mutex<ProductionState>,
    journal_binding: PathBuf,
    expected_identity: String,
    dms_timeout_nanos: u64,
}

impl fmt::Debug for ProductionAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProductionAuthority")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct ProductionState {
    creates: BTreeMap<String, (Decimal, bool)>,
    requests: u32,
    closes: u32,
    entry: Option<String>,
    started: bool,
    frozen: bool,
    activity: u64,
    activity_at: u64,
    generation: u64,
    metadata: Option<(MarketInfo, u64)>,
    contexts: BTreeMap<String, (Vec<u8>, QuoteTick, Option<Decimal>)>,
    underlying_closed: Option<bool>,
    stopped: bool,
    snapshot: Option<serde_json::Value>,
    dms_confirmed_deadline: Option<u64>,
    dms_pending_sent: Option<u64>,
    release_pending: bool,
    release_acked: bool,
    known_zero: BTreeSet<String>,
    identity_verified: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireOrder {
    market: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    size: String,
    price: String,
    time_in_force: String,
    post_only: bool,
    reduce_only: bool,
    client_order_id: String,
}

impl ProductionAuthority {
    pub(crate) fn new(
        envelope: OndoExecutionEnvelopeConfig,
        run_id: String,
        identity: &str,
        journal: &str,
        dms_timeout_secs: u64,
    ) -> Result<Arc<Self>, String> {
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        envelope.validate(now)?;
        let journal_binding = PathBuf::from(format!("{journal}.production-run"));
        if let Some(parent) = journal_binding.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|_| "production journal directory cannot be created")?;
        }
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&journal_binding)
            .map_err(|_| "production journal is already bound or not writable; use readonly recovery and a new run path")?;
        let binding=serde_json::to_vec(&serde_json::json!({"run_id":run_id,"expected_venue_account_id":identity,"envelope":envelope})).map_err(|_| "production binding serialization failed")?;
        file.write_all(&binding)
            .and_then(|()| file.sync_all())
            .map_err(|_| "production journal binding could not be persisted")?;
        Ok(Arc::new(Self {
            envelope,
            run_id,
            created_at: now,
            evidence: OnceLock::new(),
            state: Mutex::new(ProductionState::default()),
            journal_binding,
            expected_identity: identity.to_string(),
            dms_timeout_nanos: dms_timeout_secs * 1_000_000_000,
        }))
    }

    pub(crate) fn bind(&self, reader: Arc<EvidenceReader>) -> Result<(), String> {
        self.evidence
            .set(reader)
            .map_err(|_| "production authority is already bound".to_string())
    }

    fn evidence(&self, checkpoint: bool) -> Result<ProductionEvidence, String> {
        (self
            .evidence
            .get()
            .ok_or("production runtime is not bound")?)(checkpoint)
    }

    pub(crate) fn envelope(&self) -> &OndoExecutionEnvelopeConfig {
        &self.envelope
    }

    pub(crate) fn metadata(&self, info: Option<MarketInfo>, closed: Option<bool>, now: u64) {
        let mut state = self.state.lock();
        state.metadata = info.map(|i| (i, now));
        state.underlying_closed = closed;
    }

    pub(crate) fn prepare(
        &self,
        id: String,
        body: Vec<u8>,
        quote: Option<QuoteTick>,
        minimum: Option<Decimal>,
    ) -> Result<(), String> {
        if minimum.is_some_and(|v| v <= Decimal::ZERO) {
            return Err("published minimum notional is invalid".into());
        }
        let quote = quote.ok_or("native quote is unavailable")?;
        if quote.instrument_id != self.envelope.instrument_id {
            return Err("quote instrument does not match approval".into());
        }
        let mut state = self.state.lock();
        if state.contexts.contains_key(&id)
            || state.contexts.len() >= self.envelope.max_orders as usize
        {
            return Err("duplicate or excessive production command".into());
        }
        state.contexts.insert(id, (body, quote, minimum));
        Ok(())
    }

    pub(crate) fn verify_identity(&self, actual: Option<String>) -> Result<(), String> {
        let matched = actual.as_deref() == Some(self.expected_identity.as_str());
        self.state.lock().identity_verified = matched;
        if matched {
            Ok(())
        } else {
            Err("production authenticated identity missing or mismatched".into())
        }
    }

    pub(crate) fn begin_dms_send(&self, sent_at: u64, renewal: bool) -> bool {
        let mut state = self.state.lock();
        if state.dms_pending_sent.is_some()
            || (renewal
                && state
                    .dms_confirmed_deadline
                    .is_none_or(|deadline| sent_at >= deadline))
        {
            return false;
        }
        state.dms_pending_sent = Some(sent_at);
        true
    }

    pub(crate) fn confirm_dms(&self, now: u64) -> bool {
        let mut state = self.state.lock();
        let Some(sent) = state.dms_pending_sent.take() else {
            return false;
        };
        let Some(deadline) = sent.checked_add(self.dms_timeout_nanos) else {
            return false;
        };
        if now >= deadline {
            state.dms_confirmed_deadline = None;
            return false;
        }
        state.dms_confirmed_deadline = Some(deadline);
        true
    }

    pub(crate) fn invalidate(&self) {
        let mut state = self.state.lock();
        if !state.stopped {
            state.dms_confirmed_deadline = None;
            state.dms_pending_sent = None;
        }
        if !state.frozen {
            state.snapshot = None;
        }
    }

    pub(crate) fn activity(&self) {
        let mut state = self.state.lock();
        state.activity += 1;
        state.activity_at = get_atomic_clock_realtime().get_time_ns().as_u64();
        if state.started {
            state.snapshot = None;
        }
    }

    pub(crate) fn definite_zero(&self, id: &str) {
        self.state.lock().known_zero.insert(id.to_string());
    }
    pub(crate) fn owns(&self, id: &str) -> bool {
        self.state.lock().creates.contains_key(id)
    }

    pub(crate) fn activity_generation(&self) -> u64 {
        self.state.lock().activity
    }

    fn market_ready(
        &self,
        state: &ProductionState,
        now: u64,
    ) -> Result<(Decimal, Decimal), String> {
        let (info, at) = state
            .metadata
            .as_ref()
            .ok_or("missing production instrument metadata")?;
        if *at < self.created_at || now.saturating_sub(*at) > 65_000_000_000 || !info.is_tradable()
        {
            return Err("stale or disabled production instrument metadata".into());
        }
        let (tick, step) = info
            .try_increments()
            .map_err(|_| "invalid production instrument increments")?;
        let tick = tick.as_decimal();
        let step = step.as_decimal();
        for quantity in [
            self.envelope.entry_max_quantity,
            self.envelope.close_max_quantity,
        ] {
            if quantity % step != Decimal::ZERO {
                return Err("approved quantity is not on the venue step".into());
            }
        }
        for price in [
            self.envelope.entry_worst_price,
            self.envelope.close_worst_price,
        ] {
            if price % tick != Decimal::ZERO {
                return Err("approved price is not on the venue tick".into());
            }
        }
        if state.underlying_closed != Some(false) {
            return Err("underlying market hours are closed or unknown".into());
        }
        Ok((tick, step))
    }

    pub(crate) fn authorize_post(&self, target: &str, body: &[u8]) -> Result<(), String> {
        if target != "/v1/perps/orders" {
            return Err("production supports one limit IOC create only".into());
        }
        let order: WireOrder =
            serde_json::from_slice(body).map_err(|_| "invalid production create body")?;
        let quantity = Decimal::from_str_exact(&order.size)
            .map_err(|_| "invalid exact production quantity")?;
        let price =
            Decimal::from_str_exact(&order.price).map_err(|_| "invalid exact production price")?;
        let instrument = crate::common::parse::market_to_instrument_id(&order.market)
            .map_err(|_| "invalid production market")?;
        let mut state = self.state.lock();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let (tick, step) = self.market_ready(&state, now)?;
        let (expected, quote, minimum) = state
            .contexts
            .get(&order.client_order_id)
            .ok_or("no native command context for this body")?;
        if expected.as_slice() != body
            || quote.ts_init.as_u64() < self.created_at
            || quote.ts_init.as_u64() > now
            || now.saturating_sub(quote.ts_init.as_u64()) > 2_000_000_000
            || quote.bid_price.as_decimal() <= Decimal::ZERO
            || quote.ask_price < quote.bid_price
            || quote.bid_size.as_decimal() <= Decimal::ZERO
            || quote.ask_size.as_decimal() <= Decimal::ZERO
        {
            return Err("serialized command differs or native quote is stale/invalid".into());
        }
        let minimum = *minimum;
        let e = &self.envelope;
        if state.frozen
            || state.stopped
            || !state.started
            || instrument != e.instrument_id
            || order.order_type != "limit"
            || order.time_in_force != "IOC"
            || order.post_only
            || quantity <= Decimal::ZERO
            || price <= Decimal::ZERO
            || quantity % step != Decimal::ZERO
            || price % tick != Decimal::ZERO
            || state.creates.contains_key(&order.client_order_id)
            || state.creates.len() >= e.max_orders as usize
            || state.requests >= e.max_app_requests
            || now >= e.cleanup_deadline_unix_nanos
        {
            return Err("production create outside immutable envelope".into());
        }
        let notional = quantity
            .checked_mul(price)
            .ok_or("production notional overflow")?;
        if minimum.is_some_and(|v| notional < v)
            || notional > e.max_notional_per_order_usd
            || notional > e.max_gross_exposure_usd
        {
            return Err("production hard notional ceiling exceeded".into());
        }
        let evidence = self.evidence(false)?;
        if !state.identity_verified
            || !evidence.ready
            || !evidence.identity_matched
            || !evidence.journal_healthy
            || !evidence.dms_verified
            || evidence.unknown != 0
            || state
                .dms_confirmed_deadline
                .is_none_or(|deadline| now >= deadline)
        {
            return Err(
                "production native readiness, identity, journal or protection is unverified".into(),
            );
        }
        let (side, worst) = if order.reduce_only {
            (&e.close_side, e.close_worst_price)
        } else {
            (&e.entry_side, e.entry_worst_price)
        };
        if order.side != *side
            || (side == "buy" && price > worst)
            || (side == "sell" && price < worst)
        {
            return Err("production side or directional price exceeds approval".into());
        }
        if order.reduce_only {
            if state.closes >= e.max_close_attempts || quantity > e.close_max_quantity {
                return Err("production close attempt limit".into());
            }
            let entry = state
                .entry
                .as_ref()
                .ok_or("production has no own opening order")?;
            let (filled, settled, _) = evidence
                .orders
                .get(entry)
                .ok_or("opening fill is unknown")?;
            if !settled {
                return Err("opening order has not settled".into());
            }
            let mut consumed = Decimal::ZERO;
            for (id, (reserved, closing)) in &state.creates {
                if !closing {
                    continue;
                }
                if state.known_zero.contains(id) {
                    continue;
                }
                consumed += match evidence.orders.get(id) {
                    Some((filled, true, _)) => *filled,
                    _ => *reserved,
                };
            }
            if consumed + quantity > *filled {
                return Err("production close exceeds confirmed own residual".into());
            }
        } else if state.entry.is_some()
            || now >= e.entry_deadline_unix_nanos
            || quantity > e.entry_max_quantity
            || notional > e.entry_max_notional_usd
            || evidence
                .available_margin_usdc
                .is_none_or(|v| v < e.min_available_margin_usdc)
        {
            return Err("production opening exceeds approval or margin".into());
        }
        // The tracked command must be durable before any signed byte can leave this process
        let persisted = self.evidence(true)?;
        if !persisted.journal_healthy {
            return Err("production pre-send journal checkpoint failed".into());
        }
        let reservation=serde_json::to_vec(&serde_json::json!({"client_order_id":order.client_order_id,"size":order.size,"price":order.price,"reduce_only":order.reduce_only})).map_err(|_| "production reservation serialization failed")?;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.journal_binding)
            .map_err(|_| "production reservation journal unavailable")?;
        file.write_all(b"\n")
            .and_then(|()| file.write_all(&reservation))
            .and_then(|()| file.sync_all())
            .map_err(|_| "production reservation journal write failed")?;
        let send_now = get_atomic_clock_realtime().get_time_ns().as_u64();
        if send_now >= e.cleanup_deadline_unix_nanos
            || (!order.reduce_only && send_now >= e.entry_deadline_unix_nanos)
            || send_now.saturating_sub(quote.ts_init.as_u64()) > 2_000_000_000
            || state
                .dms_confirmed_deadline
                .is_none_or(|deadline| send_now >= deadline)
            || !self.evidence(false)?.ready
        {
            return Err("production admission expired during durable reservation".into());
        }
        state.requests += 1;
        if order.reduce_only {
            state.closes += 1;
        } else {
            state.entry = Some(order.client_order_id.clone());
        }
        state
            .creates
            .insert(order.client_order_id, (quantity, order.reduce_only));
        state.activity += 1;
        state.activity_at = now;
        state.snapshot = None;
        Ok(())
    }

    pub(crate) fn authorize_delete(&self, target: &str) -> Result<(), String> {
        let mut state = self.state.lock();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        if now >= self.envelope.cleanup_deadline_unix_nanos
            || state.requests >= self.envelope.max_app_requests
        {
            return Err("production cancellation budget or deadline exhausted".into());
        }
        let evidence = self.evidence(false)?;
        let owned = state.creates.keys().any(|id| {
            client_order_lookup_target(id).as_str() == target
                || evidence
                    .orders
                    .get(id)
                    .and_then(|(_, _, venue)| venue.as_ref())
                    .is_some_and(|venue| {
                        crate::http::orders::order_lookup_target(venue).as_str() == target
                    })
        });
        if !owned {
            return Err("production cancel target is not owned by this run".into());
        }
        state.requests += 1;
        state.activity += 1;
        state.activity_at = now;
        state.snapshot = None;
        Ok(())
    }

    pub(crate) fn reconcile(
        &self,
        reading: &crate::reconciliation::AccountReading,
        clean: bool,
        generation: u64,
    ) {
        let mut state = self.state.lock();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let Ok(evidence) = self.evidence(false) else {
            state.snapshot = None;
            return;
        };
        let foreign = reading
            .orders
            .iter()
            .filter(|o| {
                !o.status.is_terminal()
                    && !o
                        .client_order_id
                        .as_ref()
                        .is_some_and(|id| state.creates.contains_key(id))
            })
            .count();
        let own = reading
            .orders
            .iter()
            .filter(|o| {
                !o.status.is_terminal()
                    && o.client_order_id
                        .as_ref()
                        .is_some_and(|id| state.creates.contains_key(id))
            })
            .count();
        let all_flat = reading.positions.iter().all(|p| p.signed == Decimal::ZERO);
        let position = reading
            .positions
            .iter()
            .filter(|p| p.instrument_id == Some(self.envelope.instrument_id))
            .map(|p| p.signed)
            .sum::<Decimal>();
        let metadata_fresh = self.market_ready(&state, now).is_ok();

        if !state.started {
            if state
                .dms_confirmed_deadline
                .is_none_or(|deadline| now >= deadline)
                || !state.identity_verified
                || !clean
                || !all_flat
                || foreign != 0
                || own != 0
                || !evidence.ready
                || !evidence.identity_matched
                || !evidence.journal_healthy
                || evidence
                    .available_margin_usdc
                    .is_none_or(|v| v < self.envelope.min_available_margin_usdc)
                || !evidence.dms_verified
                || !metadata_fresh
            {
                return;
            }
            state.started = true;
            state.generation += 1;
            state.snapshot = Some(
                serde_json::json!({"run_id":self.run_id,"phase":"start","generation":state.generation,"snapshot_unix_nanos":now,"identity_match":"matched","account_flat":true,"coverage_complete":true,"foreign_open_orders":0,"own_open_orders":0,"native_ready":true,"metadata_fresh":true,"trading_enabled":true,"underlying_market_closed":false,"dms_verified":true,"available_margin_usdc":evidence.available_margin_usdc.map(|v|v.to_string()),"minimum_notional_usd":null,"minimum_notional_policy":"enforce_if_published"}),
            );
        } else if (state.stopped || !state.contexts.is_empty())
            && generation == state.activity
            && clean
            && position == Decimal::ZERO
            && all_flat
            && foreign == 0
            && own == 0
            && evidence.unknown == 0
            && state.creates.keys().all(|id| {
                state.known_zero.contains(id)
                    || evidence
                        .orders
                        .get(id)
                        .is_some_and(|(_, settled, _)| *settled)
            })
        {
            state.frozen = true;
            state.generation += 1;
            state.snapshot = Some(
                serde_json::json!({"run_id":self.run_id,"phase":"reconciled","generation":state.generation,"snapshot_unix_nanos":now,"latest_activity_generation":state.activity,"latest_activity_unix_nanos":state.activity_at,"reconciled_activity_generation":state.activity,"reconciliation_unix_nanos":now,"complete":true,"snapshot_fresh":true,"instrument_id":self.envelope.instrument_id.to_string(),"position_qty":position.to_string(),"reconciled_flat":true,"own_open_orders":0,"foreign_open_orders":0,"unknown_submissions":0,"late_fills":0,"execution_cost":null}),
            );
        }
    }

    pub(crate) fn verify_dms_reading(
        &self,
        reading: &crate::reconciliation::AccountReading,
    ) -> Result<(), String> {
        let state = self.state.lock();
        if reading.orders.iter().any(|o| {
            !o.status.is_terminal()
                && !o
                    .client_order_id
                    .as_ref()
                    .is_some_and(|id| state.creates.contains_key(id))
        }) {
            return Err("production DMS found foreign open orders".into());
        }
        if state.entry.is_none() {
            if self.entry_remaining().is_zero() {
                return Err("production opening deadline expired before DMS arm".into());
            }
            if reading.positions.iter().any(|p| p.signed != Decimal::ZERO) {
                return Err("production initial account is occupied".into());
            }
        } else {
            if reading.positions.iter().any(|p| {
                p.signed != Decimal::ZERO && p.instrument_id != Some(self.envelope.instrument_id)
            }) {
                return Err("production DMS found a foreign position".into());
            }
            let evidence = self.evidence(false)?;
            let mut own = Decimal::ZERO;
            for (id, (_, closing)) in &state.creates {
                let (filled, _, _) = evidence
                    .orders
                    .get(id)
                    .ok_or("production own order quantity is unknown")?;
                own += if *closing { -*filled } else { *filled };
            }
            if self.envelope.entry_side == "sell" {
                own = -own;
            }
            let venue = reading.positions.iter().map(|p| p.signed).sum::<Decimal>();
            if venue != own {
                return Err("production DMS position differs from own confirmed fills".into());
            }
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Option<serde_json::Value> {
        self.state.lock().snapshot.clone()
    }
    pub(crate) fn stop_creates(&self) {
        let mut state = self.state.lock();
        if !state.stopped && !state.frozen {
            state.activity += 1;
            state.activity_at = get_atomic_clock_realtime().get_time_ns().as_u64();
            state.snapshot = None;
        }
        state.stopped = true;
    }

    pub(crate) fn expire(&self) {
        let mut state = self.state.lock();
        state.stopped = true;
        state.dms_confirmed_deadline = None;
        state.dms_pending_sent = None;
        if let Some(snapshot) = state.snapshot.as_mut() {
            if snapshot["phase"] == "start" {
                state.snapshot = None;
            } else {
                snapshot["shutdown_status"] = serde_json::json!("uncertain");
            }
        }
    }
    pub(crate) fn entry_remaining(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(
            self.envelope
                .entry_deadline_unix_nanos
                .saturating_sub(get_atomic_clock_realtime().get_time_ns().as_u64()),
        )
    }

    pub(crate) fn remaining(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(
            self.envelope
                .cleanup_deadline_unix_nanos
                .saturating_sub(get_atomic_clock_realtime().get_time_ns().as_u64()),
        )
    }
    pub(crate) fn begin_release(&self) -> Result<(), String> {
        let mut state = self.state.lock();
        if state.release_pending || state.release_acked {
            return Err("production DMS release already sent or confirmed".into());
        }
        state.release_pending = true;
        Ok(())
    }

    pub(crate) fn confirm_release(&self) {
        let mut state = self.state.lock();
        if state.release_pending && !self.remaining().is_zero() {
            state.release_pending = false;
            state.release_acked = true;
        }
    }

    pub(crate) fn release_acked(&self) -> bool {
        self.state.lock().release_acked
    }

    pub(crate) fn validate_direct_switch(
        &self,
        frame: &crate::reconciliation::DeadMansSwitchMessage,
    ) -> Result<(), String> {
        let state = self.state.lock();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let evidence = self.evidence(false)?;
        let proof = state
            .snapshot
            .as_ref()
            .ok_or("production release has no authoritative reconciliation proof")?;
        if !state.stopped
            || !state.frozen
            || !state.identity_verified
            || !evidence.identity_matched
            || state
                .dms_confirmed_deadline
                .is_none_or(|deadline| now >= deadline)
            || frame.op != crate::websocket::messages::WsOp::Unsubscribe
            || frame.channel != "cancelAllOrdersAfterPerps"
            || frame.timeout_seconds != self.dms_timeout_nanos / 1_000_000_000
            || self.remaining().is_zero()
            || proof["phase"] != "reconciled"
            || proof["complete"] != true
            || proof["reconciled_flat"] != true
            || proof["latest_activity_generation"].as_u64() != Some(state.activity)
            || evidence.unknown != 0
            || [
                "own_open_orders",
                "foreign_open_orders",
                "unknown_submissions",
                "late_fills",
            ]
            .iter()
            .any(|k| proof[*k] != 0)
            || !state.creates.keys().all(|id| {
                state.known_zero.contains(id)
                    || evidence
                        .orders
                        .get(id)
                        .is_some_and(|(_, settled, _)| *settled)
            })
        {
            return Err("production DMS release requires current, flat, fully reconciled owned shutdown proof".into());
        }
        Ok(())
    }

    pub(crate) fn permits_dms(&self) -> bool {
        self.remaining() > std::time::Duration::ZERO
    }
    pub(crate) fn finish(&self, clean: bool) {
        let mut state = self.state.lock();
        state.frozen = true;
        let confirmed_release = state.release_acked;
        if let Some(snapshot) = state.snapshot.as_mut() {
            if snapshot["phase"] == "reconciled"
                && clean
                && confirmed_release
                && !self.remaining().is_zero()
            {
                snapshot["phase"] = serde_json::json!("final");
                snapshot["shutdown_status"] = serde_json::json!("clean");
            } else {
                snapshot["shutdown_status"] = serde_json::json!("uncertain");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn authority(timeout: u64) -> (Arc<ProductionAuthority>, PathBuf) {
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let envelope:OndoExecutionEnvelopeConfig=serde_json::from_value(serde_json::json!({
            "instrument_id":"NVDA-USD-PERP.ONDO","entry_side":"buy","entry_max_quantity":"0.05","entry_worst_price":"230","entry_max_notional_usd":"15",
            "close_side":"sell","close_max_quantity":"0.05","close_worst_price":"225","max_close_attempts":2,"max_notional_per_order_usd":"50","max_gross_exposure_usd":"100","min_available_margin_usdc":"25",
            "max_orders":3,"max_new_risk_requests":1,"max_app_requests":6,"entry_deadline_unix_nanos":now+60_000_000_000_u64,"cleanup_deadline_unix_nanos":now+120_000_000_000_u64,"require_flat_start":true
        })).unwrap();
        let path = std::env::temp_dir().join(format!(
            "ondo-production-authority-{}",
            nautilus_core::UUID4::new()
        ));
        let guard = ProductionAuthority::new(
            envelope,
            "unit-run".into(),
            "unit-account",
            path.to_str().unwrap(),
            timeout,
        )
        .unwrap();
        let binding = guard.journal_binding.clone();
        (guard, binding)
    }

    #[rstest]
    fn test_dms_delayed_ack_cannot_extend_past_original_send_deadline() {
        let (guard, path) = authority(2);
        let sent = get_atomic_clock_realtime().get_time_ns().as_u64();
        assert!(guard.begin_dms_send(sent, false));
        assert!(!guard.begin_dms_send(sent + 1_000_000_000, false));
        assert!(!guard.confirm_dms(sent + 2_000_000_001));
        assert!(guard.state.lock().dms_confirmed_deadline.is_none());
        std::fs::remove_file(path).unwrap();
    }

    #[rstest]
    fn test_dms_socket_send_does_not_extend_confirmed_protection() {
        let (guard, path) = authority(4);
        let sent = get_atomic_clock_realtime().get_time_ns().as_u64();
        assert!(guard.begin_dms_send(sent, false));
        assert!(guard.confirm_dms(sent + 500_000_000));
        assert_eq!(
            guard.state.lock().dms_confirmed_deadline,
            Some(sent + 4_000_000_000)
        );
        assert!(guard.begin_dms_send(sent + 2_000_000_000, true));
        assert!(!guard.begin_dms_send(sent + 3_000_000_000, true));
        assert_eq!(
            guard.state.lock().dms_confirmed_deadline,
            Some(sent + 4_000_000_000)
        );
        assert!(!guard.confirm_dms(sent + 6_000_000_001));
        assert!(guard.state.lock().dms_confirmed_deadline.is_none());
        std::fs::remove_file(path).unwrap();
    }
    #[derive(Debug)]
    struct AccountStillReady;
    impl crate::http::client::OndoNewRiskGuard for AccountStillReady {
        fn revalidate(&self, _permit: crate::http::client::NewRiskPermit) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_slow_checkpoint_crossing_genuine_dms_deadline_sends_no_http_bytes() {
        use crate::{
            common::{
                credential::OndoCredential,
                enums::{OndoAuthenticationScope, OndoEnvironment},
            },
            http::{
                client::{NewRiskPermit, OndoHttpClient, OndoNewRiskSendError},
                query::OndoRequestTarget,
                rate_limit::OndoRequestPriority,
            },
        };
        use nautilus_model::types::{Price, Quantity};
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let (guard, path) = authority(1);
        let entered = Arc::new(AtomicBool::new(false));
        let deadline = Arc::new(AtomicU64::new(0));
        let checkpoint_entered = Arc::clone(&entered);
        let confirmed_deadline = Arc::clone(&deadline);
        guard
            .bind(Arc::new(move |checkpoint| {
                if checkpoint {
                    checkpoint_entered.store(true, Ordering::SeqCst);
                    let remaining = confirmed_deadline
                        .load(Ordering::SeqCst)
                        .saturating_sub(get_atomic_clock_realtime().get_time_ns().as_u64());
                    std::thread::sleep(
                        std::time::Duration::from_nanos(remaining)
                            + std::time::Duration::from_millis(25),
                    );
                }
                Ok(ProductionEvidence {
                    ready: true,
                    identity_matched: true,
                    journal_healthy: true,
                    dms_verified: true,
                    available_margin_usdc: Some(Decimal::from(25)),
                    orders: BTreeMap::new(),
                    unknown: 0,
                })
            }))
            .unwrap();
        guard.verify_identity(Some("unit-account".into())).unwrap();
        let now = get_atomic_clock_realtime().get_time_ns();
        let info = crate::http::models::parse_markets(include_str!(
            "../test_data/rest/markets_synthetic.json"
        ))
        .unwrap()
        .market_infos(now)
        .unwrap()
        .into_iter()
        .find(|i| i.instrument_id() == guard.envelope.instrument_id)
        .unwrap();
        guard.metadata(Some(info), Some(false), now.as_u64());
        let body=serde_json::to_vec(&serde_json::json!({"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.05","price":"230.00","timeInForce":"IOC","postOnly":false,"reduceOnly":false,"clientOrderId":"slow-checkpoint"})).unwrap();
        let quote = QuoteTick::new(
            guard.envelope.instrument_id,
            Price::from("229.99"),
            Price::from("230.00"),
            Quantity::from("1.00"),
            Quantity::from("1.00"),
            now,
            now,
        );
        guard
            .prepare("slow-checkpoint".into(), body.clone(), Some(quote), None)
            .unwrap();
        let credential = Arc::new(
            OndoCredential::new(
                OndoEnvironment::Production,
                "ondoKeyId_UNIT_TEST_ONLY".into(),
                "ondoApiSecret_UNIT_TEST_ONLY".into(),
            )
            .unwrap(),
        );
        let client = OndoHttpClient::builder()
            .base_url(format!("http://{}", listener.local_addr().unwrap()))
            .credential(credential)
            .authentication_scope(OndoAuthenticationScope::ProductionTrading)
            .production_guard(Arc::clone(&guard))
            .new_risk_guard(
                Arc::new(AccountStillReady) as Arc<dyn crate::http::client::OndoNewRiskGuard>
            )
            .timeout_secs(1)
            .build()
            .unwrap();
        let sent = get_atomic_clock_realtime().get_time_ns().as_u64();
        deadline.store(sent + 1_000_000_000, Ordering::SeqCst);
        assert!(guard.begin_dms_send(sent, false));
        assert!(guard.confirm_dms(sent));
        guard.reconcile(&crate::reconciliation::AccountReading::default(), true, 0);
        assert!(guard.snapshot().is_some());
        let result = client
            .post_signed_raw(
                &OndoRequestTarget::new("/v1/perps/orders"),
                body,
                OndoRequestPriority::Normal,
                NewRiskPermit::new(1),
            )
            .await;
        assert!(
            entered.load(Ordering::SeqCst),
            "the checkpoint must start before protection expires"
        );
        assert!(
            matches!(result,Err(OndoNewRiskSendError::Refused {ref reason}) if reason.contains("expired during durable reservation")),
            "{result:?}"
        );
        assert!(
            listener
                .accept()
                .is_err_and(|e| e.kind() == std::io::ErrorKind::WouldBlock),
            "no TCP connection or POST bytes may leave after the genuine deadline"
        );
        assert!(guard.state.lock().creates.is_empty());
        std::fs::remove_file(path).unwrap();
    }
}
