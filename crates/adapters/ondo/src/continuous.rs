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

//! Continuous admission retains the same native transport authority and genuine DMS proof.
//! Market notional limits use current native quotes as admission estimates, without an execution
//! price bound.

use std::collections::VecDeque;

use super::*;

/// Explicit limits for continuous execution across configured Ondo perpetual instruments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object, frozen)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoContinuousExecutionConfig {
    /// The perpetual instruments this execution session may trade.
    pub instrument_ids: Vec<InstrumentId>,
    /// The exact per-order notional ceiling, at most USD 100.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub max_notional_per_order_usd: Decimal,
    /// The exact gross position and outstanding entry exposure ceiling, at most USD 500.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub max_gross_exposure_usd: Decimal,
    /// The minimum authoritative available USDC margin for additional risk.
    #[serde(deserialize_with = "deserialize_exact_decimal")]
    pub min_available_margin_usdc: Decimal,
    /// The maximum simultaneous prepared, outstanding and unresolved orders.
    pub max_live_orders: u32,
    /// The maximum application creates and owned cancellations in a rolling second.
    pub max_requests_per_second: u32,
    /// The maximum unresolved submission count before all additional creates are suspended.
    pub max_unresolved_orders: u32,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OndoContinuousExecutionConfig {
    instrument_ids: Vec<InstrumentId>,
    max_notional_per_order_usd: Decimal,
    max_gross_exposure_usd: Decimal,
    min_available_margin_usdc: Decimal,
    max_live_orders: u32,
    max_requests_per_second: u32,
    max_unresolved_orders: u32,
});

impl OndoContinuousExecutionConfig {
    /// Validates explicit limits without widening them or selecting an instrument implicitly.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid instruments, duplicate instruments or invalid limits.
    pub fn validate(&self) -> Result<(), String> {
        if self.instrument_ids.is_empty()
            || self.instrument_ids.iter().collect::<BTreeSet<_>>().len()
                != self.instrument_ids.len()
            || self.instrument_ids.iter().any(|id| {
                crate::common::parse::instrument_id_to_market(id).is_err()
                    || !id.to_string().ends_with("-PERP.ONDO")
            })
            || self.max_notional_per_order_usd <= Decimal::ZERO
            || self.max_notional_per_order_usd > Decimal::from(100)
            || self.max_gross_exposure_usd <= Decimal::ZERO
            || self.max_gross_exposure_usd > Decimal::from(500)
            || self.max_notional_per_order_usd > self.max_gross_exposure_usd
            || self.min_available_margin_usdc <= Decimal::ZERO
            || self.max_live_orders == 0
            || self.max_requests_per_second == 0
            || self.max_unresolved_orders == 0
            || self.max_unresolved_orders > self.max_live_orders
        {
            return Err("invalid continuous execution instruments or admission limits".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct ContinuousState {
    pub(super) metadata: BTreeMap<InstrumentId, (MarketInfo, Option<bool>, u64)>,
    pub(super) released: bool,
    intents: BTreeMap<String, ContinuousIntent>,
    request_times: VecDeque<u64>,
    reading: Option<(crate::reconciliation::AccountReading, bool, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContinuousIntent {
    client_order_id: String,
    instrument_id: InstrumentId,
    side: String,
    quantity: Decimal,
    price: Option<Decimal>,
    reduce_only: bool,
    body: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContinuousWireOrder {
    market: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    size: String,
    price: Option<String>,
    time_in_force: Option<String>,
    post_only: bool,
    reduce_only: bool,
    client_order_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum ContinuousJournalRecord {
    Binding {
        run_id: String,
        expected_venue_account_id: String,
        continuous: OndoContinuousExecutionConfig,
    },
    Intent {
        intent: ContinuousIntent,
    },
    Baseline {
        baseline: CashflowBaseline,
    },
    DefiniteZero {
        client_order_id: String,
    },
}

impl ProductionAuthority {
    /// Restores original order identities when this exact continuous run journal already exists.
    pub(crate) fn new_continuous(
        config: OndoContinuousExecutionConfig,
        run_id: String,
        identity: &str,
        journal: &str,
        dms_timeout_secs: u64,
    ) -> Result<Arc<Self>, String> {
        config.validate()?;
        if run_id.is_empty() || identity.is_empty() || dms_timeout_secs == 0 {
            return Err("continuous execution requires run, identity and DMS timeout".into());
        }
        let dms_timeout_nanos = dms_timeout_secs
            .checked_mul(1_000_000_000)
            .ok_or("continuous DMS timeout overflow")?;
        let journal_binding = PathBuf::from(format!("{journal}.production-run"));
        if let Some(parent) = journal_binding.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|_| "continuous journal directory cannot be created")?;
        }
        // OS ownership is released after a crash, while the original journal remains durable
        let journal_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(format!("{journal}.production-run.lock"))
            .map_err(|_| "continuous journal ownership lock is unavailable")?;
        journal_lock
            .try_lock()
            .map_err(|_| "continuous journal is already owned by another execution client")?;
        let binding = ContinuousJournalRecord::Binding {
            run_id: run_id.clone(),
            expected_venue_account_id: identity.to_string(),
            continuous: config.clone(),
        };
        let mut state = ProductionState::default();
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&journal_binding)
        {
            Ok(mut file) => {
                let bytes = serde_json::to_vec(&binding)
                    .map_err(|_| "continuous binding serialization failed")?;
                file.write_all(&bytes)
                    .and_then(|()| file.write_all(b"\n"))
                    .and_then(|()| file.sync_all())
                    .map_err(|_| "continuous journal binding could not be persisted")?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let text = std::fs::read_to_string(&journal_binding)
                    .map_err(|_| "continuous recovery journal cannot be read")?;
                if !text.ends_with('\n') {
                    return Err("continuous recovery journal has an incomplete record".into());
                }
                let mut records = text.lines();
                let first: ContinuousJournalRecord = serde_json::from_str(
                    records
                        .next()
                        .ok_or("continuous recovery binding is missing")?,
                )
                .map_err(|_| "continuous recovery binding is invalid")?;
                if !matches!(first, ContinuousJournalRecord::Binding {
                    run_id: saved_run, expected_venue_account_id: saved_identity, continuous: saved_config,
                } if saved_run == run_id && saved_identity == identity && saved_config == config)
                {
                    return Err("continuous recovery binding differs from configured run".into());
                }
                for line in records {
                    match serde_json::from_str::<ContinuousJournalRecord>(line)
                        .map_err(|_| "continuous recovery record is incomplete or invalid")?
                    {
                        ContinuousJournalRecord::Intent { intent } => {
                            if Self::continuous_intent(&intent.body)?.1 != intent
                                || !config.instrument_ids.contains(&intent.instrument_id)
                                || intent.price.is_some_and(|price| {
                                    intent.quantity.checked_mul(price).is_none_or(|value| {
                                        value > config.max_notional_per_order_usd
                                    })
                                })
                            {
                                return Err(
                                    "continuous recovery intent differs from its original body"
                                        .into(),
                                );
                            }
                            if state
                                .continuous
                                .intents
                                .contains_key(&intent.client_order_id)
                            {
                                return Err(
                                    "duplicate identity in continuous recovery journal".into()
                                );
                            }
                            state.creates.insert(
                                intent.client_order_id.clone(),
                                (intent.quantity, intent.reduce_only),
                            );
                            state
                                .continuous
                                .intents
                                .insert(intent.client_order_id.clone(), intent);
                        }
                        ContinuousJournalRecord::DefiniteZero { client_order_id } => {
                            state.known_zero.insert(client_order_id);
                        }
                        ContinuousJournalRecord::Baseline { baseline } => {
                            if state.cashflow_baseline.is_some() || baseline.read_at == 0 {
                                return Err(
                                    "invalid or duplicate continuous cashflow baseline".into()
                                );
                            }
                            state.cashflow_baseline = Some(baseline);
                        }
                        ContinuousJournalRecord::Binding { .. } => {
                            return Err("duplicate continuous recovery binding".into());
                        }
                    }
                }
            }
            Err(_) => return Err("continuous journal binding is not writable".into()),
        }
        Ok(Arc::new(Self {
            envelope: None,
            continuous: Some(config),
            run_id,
            created_at: get_atomic_clock_realtime().get_time_ns().as_u64(),
            evidence: OnceLock::new(),
            state: Mutex::new(state),
            journal_binding,
            continuous_journal_lock: Mutex::new(Some(journal_lock)),
            expected_identity: identity.to_string(),
            dms_timeout_nanos,
        }))
    }

    pub(crate) fn is_continuous(&self) -> bool {
        self.continuous.is_some()
    }

    /// Releases OS ownership only after the native client has drained tasks and stopped its stream.
    pub(crate) fn release_continuous_ownership(&self) {
        if !self.is_continuous() {
            return;
        }
        let mut state = self.state.lock();
        state.continuous.released = true;
        state.frozen = true;
        state.stopped = true;
        state.dms_confirmed_deadline = None;
        state.dms_pending_sent = None;
        self.continuous_journal_lock.lock().take();
    }

    pub(crate) fn continuous_ownership_released(&self) -> bool {
        self.state.lock().continuous.released
    }

    pub(crate) fn continuous_new_risk_ready(&self) -> bool {
        let state = self.state.lock();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let Ok(evidence) = self.evidence(false) else {
            return false;
        };
        state.started
            && !state.stopped
            && !state.frozen
            && !state.continuous.released
            && Self::continuous_live_orders(&state, &evidence)
                < self.continuous_config().max_live_orders as usize
            && self
                .instrument_ids()
                .iter()
                .all(|id| self.continuous_market_ready(&state, *id, now).is_ok())
            && self
                .check_continuous_account(&state, &evidence, now, true)
                .is_ok()
    }

    pub(crate) fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.continuous.as_ref().map_or_else(
            || vec![self.envelope().instrument_id],
            |config| config.instrument_ids.clone(),
        )
    }

    pub(crate) fn minimum_available_margin_usdc(&self) -> Decimal {
        self.continuous.as_ref().map_or_else(
            || self.envelope().min_available_margin_usdc,
            |config| config.min_available_margin_usdc,
        )
    }

    pub(crate) fn metadata_for(
        &self,
        instrument: InstrumentId,
        info: Option<MarketInfo>,
        closed: Option<bool>,
        now: u64,
    ) {
        if !self.is_continuous() {
            if instrument == self.envelope().instrument_id {
                self.metadata(info, closed, now);
            }
            return;
        }
        let mut state = self.state.lock();
        state.continuous.metadata.remove(&instrument);
        if self.instrument_ids().contains(&instrument)
            && let Some(info) = info
            && info.instrument_id() == instrument
        {
            state
                .continuous
                .metadata
                .insert(instrument, (info, closed, now));
        }
    }

    fn continuous_config(&self) -> &OndoContinuousExecutionConfig {
        self.continuous
            .as_ref()
            .expect("continuous authority required")
    }

    fn append_continuous_record(&self, record: &ContinuousJournalRecord) -> Result<(), String> {
        let bytes =
            serde_json::to_vec(record).map_err(|_| "continuous intent serialization failed")?;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.journal_binding)
            .map_err(|_| "continuous intent journal is unavailable")?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|_| "continuous intent journal write failed".to_string())
    }

    pub(super) fn persist_continuous_zero(&self, id: &str) -> Result<(), String> {
        self.append_continuous_record(&ContinuousJournalRecord::DefiniteZero {
            client_order_id: id.to_string(),
        })
    }

    fn continuous_intent(body: &[u8]) -> Result<(ContinuousWireOrder, ContinuousIntent), String> {
        let order: ContinuousWireOrder =
            serde_json::from_slice(body).map_err(|_| "invalid continuous create body")?;
        let quantity = Decimal::from_str_exact(&order.size)
            .map_err(|_| "invalid exact continuous quantity")?;
        let price = order
            .price
            .as_deref()
            .map(Decimal::from_str_exact)
            .transpose()
            .map_err(|_| "invalid exact continuous price")?;
        let valid_type = match order.order_type.as_str() {
            "limit" => {
                price.is_some_and(|price| price > Decimal::ZERO)
                    && matches!(order.time_in_force.as_deref(), Some("IOC" | "GTC"))
                    && (!order.post_only || order.time_in_force.as_deref() == Some("GTC"))
            }
            "market" => {
                let fields: serde_json::Value =
                    serde_json::from_slice(body).map_err(|_| "invalid continuous create body")?;
                !order.post_only
                    && fields.get("price").is_none()
                    && fields.get("timeInForce").is_none()
            }
            _ => false,
        };
        let instrument_id = crate::common::parse::market_to_instrument_id(&order.market)
            .map_err(|_| "invalid continuous market")?;
        if order.client_order_id.is_empty()
            || !valid_type
            || !matches!(order.side.as_str(), "buy" | "sell")
            || quantity <= Decimal::ZERO
        {
            return Err(
                "continuous execution requires a valid limit IOC/GTC or market order".into(),
            );
        }
        let intent = ContinuousIntent {
            client_order_id: order.client_order_id.clone(),
            instrument_id,
            side: order.side.clone(),
            quantity,
            price,
            reduce_only: order.reduce_only,
            body: body.to_vec(),
        };
        Ok((order, intent))
    }

    pub(super) fn prepare_continuous(
        &self,
        id: String,
        body: Vec<u8>,
        quote: Option<QuoteTick>,
        minimum: Option<Decimal>,
    ) -> Result<(), String> {
        let (_, intent) = Self::continuous_intent(&body)?;
        let mut quote = quote.ok_or("native continuous quote is unavailable")?;
        let config = self.continuous_config();
        if id != intent.client_order_id
            || !config.instrument_ids.contains(&intent.instrument_id)
            || quote.instrument_id != intent.instrument_id
            || minimum.is_some_and(|v| v <= Decimal::ZERO)
        {
            return Err("continuous command, instrument or published minimum is invalid".into());
        }
        let evidence = self.evidence(false)?;
        if intent.price.is_none() {
            quote = *evidence
                .quotes
                .get(&intent.instrument_id)
                .ok_or("native continuous market quote is unavailable")?;
        }
        let mut state = self.state.lock();
        if state.frozen
            || state.stopped
            || state.contexts.contains_key(&id)
            || state.creates.contains_key(&id)
            || state.known_zero.contains(&id)
            || Self::continuous_live_orders(&state, &evidence) >= config.max_live_orders as usize
        {
            return Err("duplicate, stopped or excessive continuous command".into());
        }
        self.check_continuous_exposure(&state, &evidence, &intent)?;
        self.check_continuous_minimum(
            &intent,
            evidence.quotes.get(&intent.instrument_id).unwrap_or(&quote),
            minimum,
            get_atomic_clock_realtime().get_time_ns().as_u64(),
        )?;
        state.contexts.insert(id, (body, quote, minimum));
        note_activity(&mut state);
        Ok(())
    }

    fn continuous_live_orders(state: &ProductionState, evidence: &ProductionEvidence) -> usize {
        state.contexts.len()
            + state
                .creates
                .keys()
                .filter(|id| {
                    !state.known_zero.contains(*id)
                        && !evidence
                            .orders
                            .get(*id)
                            .is_some_and(|(_, settled, _)| *settled)
                })
                .count()
    }

    fn continuous_own_net(
        state: &ProductionState,
        evidence: &ProductionEvidence,
    ) -> Result<BTreeMap<InstrumentId, Decimal>, String> {
        let mut net = BTreeMap::new();
        for (id, intent) in &state.continuous.intents {
            let filled = if state.known_zero.contains(id) {
                Decimal::ZERO
            } else {
                evidence
                    .orders
                    .get(id)
                    .ok_or("owned continuous execution quantity is unknown")?
                    .0
            };
            if filled < Decimal::ZERO || filled > intent.quantity {
                return Err("owned continuous execution quantity is inconsistent".into());
            }
            let signed = if intent.side == "buy" {
                filled
            } else {
                -filled
            };
            let value: &mut Decimal = net.entry(intent.instrument_id).or_default();
            *value = value
                .checked_add(signed)
                .ok_or("continuous quantity overflow")?;
        }
        net.retain(|_, value| *value != Decimal::ZERO);
        Ok(net)
    }

    fn continuous_position_net(
        reading: &crate::reconciliation::AccountReading,
    ) -> Result<BTreeMap<InstrumentId, Decimal>, String> {
        let mut net = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for position in &reading.positions {
            use crate::reconciliation::PositionDirection;
            if matches!(position.direction, PositionDirection::Unknown(_))
                || (position.direction == PositionDirection::Long
                    && position.signed < Decimal::ZERO)
                || (position.direction == PositionDirection::Short
                    && position.signed > Decimal::ZERO)
                || (position.direction == PositionDirection::Neutral
                    && position.net_quantity != Decimal::ZERO)
                || position.signed
                    != crate::reconciliation::signed_position_quantity(
                        &position.direction,
                        position.net_quantity,
                    )
            {
                return Err("continuous account position direction or quantity is invalid".into());
            }
            let instrument = position
                .instrument_id
                .ok_or("unmapped continuous account position")?;
            if !seen.insert(instrument) {
                return Err("multiple position rows conflict with one-way net semantics".into());
            }
            if position.signed != Decimal::ZERO {
                net.insert(instrument, position.signed);
            }
        }
        Ok(net)
    }

    fn check_continuous_exposure(
        &self,
        state: &ProductionState,
        evidence: &ProductionEvidence,
        candidate: &ContinuousIntent,
    ) -> Result<(), String> {
        let config = self.continuous_config();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let candidate_mark = self
            .continuous_quote_mark(state, evidence, candidate.instrument_id, now)?
            .max(candidate.price.unwrap_or_default());
        let notional = candidate
            .quantity
            .checked_mul(candidate_mark)
            .ok_or("continuous order notional overflow")?;
        if notional > config.max_notional_per_order_usd {
            return Err("continuous per-order notional ceiling exceeded".into());
        }
        if candidate.reduce_only {
            return Ok(());
        }
        let net = Self::continuous_own_net(state, evidence)?;
        let mut marks = BTreeMap::<InstrumentId, Decimal>::new();
        for instrument in net.keys() {
            marks.insert(
                *instrument,
                self.continuous_quote_mark(state, evidence, *instrument, now)?,
            );
        }
        let mut pending = Vec::new();
        for (id, intent) in &state.continuous.intents {
            let mark = marks.entry(intent.instrument_id).or_default();
            *mark = (*mark).max(intent.price.unwrap_or_default());
            if state.known_zero.contains(id) || intent.reduce_only {
                continue;
            }
            match evidence.orders.get(id) {
                Some((_, true, _)) => {}
                Some((filled, false, _)) => pending.push((intent, intent.quantity - *filled)),
                None => pending.push((intent, intent.quantity)),
            }
        }
        let mut prepared = Vec::new();
        for (id, (body, quote, _)) in &state.contexts {
            let mark = marks.entry(quote.instrument_id).or_default();
            *mark = (*mark).max(quote.ask_price.as_decimal());
            if id != &candidate.client_order_id {
                prepared.push(Self::continuous_intent(body)?.1);
            }
        }
        for intent in &prepared {
            let mark = marks.entry(intent.instrument_id).or_default();
            *mark = (*mark).max(intent.price.unwrap_or_default());
            if !intent.reduce_only {
                pending.push((intent, intent.quantity));
            }
        }
        let mark = marks.entry(candidate.instrument_id).or_default();
        *mark = (*mark).max(candidate.price.unwrap_or_default());
        let mut gross = notional;
        for (instrument, quantity) in net {
            let mark = marks
                .get(&instrument)
                .ok_or("continuous position mark unavailable")?;
            gross = gross
                .checked_add(
                    quantity
                        .abs()
                        .checked_mul(*mark)
                        .ok_or("continuous position notional overflow")?,
                )
                .ok_or("continuous aggregate notional overflow")?;
        }
        for (intent, quantity) in pending {
            let price = self
                .continuous_quote_mark(state, evidence, intent.instrument_id, now)?
                .max(intent.price.unwrap_or_default());
            gross = gross
                .checked_add(
                    quantity
                        .checked_mul(price)
                        .ok_or("continuous reservation notional overflow")?,
                )
                .ok_or("continuous aggregate notional overflow")?;
        }
        if gross > config.max_gross_exposure_usd {
            return Err("continuous aggregate exposure ceiling exceeded".into());
        }
        Ok(())
    }

    fn continuous_quote_mark(
        &self,
        state: &ProductionState,
        evidence: &ProductionEvidence,
        instrument: InstrumentId,
        now: u64,
    ) -> Result<Decimal, String> {
        let quote = evidence
            .quotes
            .get(&instrument)
            .or_else(|| {
                state
                    .contexts
                    .values()
                    .map(|(_, quote, _)| quote)
                    .find(|quote| quote.instrument_id == instrument)
            })
            .ok_or("continuous position has no authoritative native quote")?;
        if quote.instrument_id != instrument
            || quote.ts_init.as_u64() < self.created_at
            || quote.ts_init.as_u64() > now
            || now.saturating_sub(quote.ts_init.as_u64()) > 2_000_000_000
            || quote.bid_price.as_decimal() <= Decimal::ZERO
            || quote.ask_price < quote.bid_price
            || quote.bid_size.as_decimal() <= Decimal::ZERO
            || quote.ask_size.as_decimal() <= Decimal::ZERO
        {
            return Err("continuous position quote is stale or invalid".into());
        }
        Ok(quote.ask_price.as_decimal())
    }

    fn check_continuous_minimum(
        &self,
        intent: &ContinuousIntent,
        quote: &QuoteTick,
        minimum: Option<Decimal>,
        now: u64,
    ) -> Result<(), String> {
        if quote.instrument_id != intent.instrument_id
            || quote.ts_init.as_u64() < self.created_at
            || quote.ts_init.as_u64() > now
            || now.saturating_sub(quote.ts_init.as_u64()) > 2_000_000_000
            || quote.bid_price.as_decimal() <= Decimal::ZERO
            || quote.ask_price < quote.bid_price
            || quote.bid_size.as_decimal() <= Decimal::ZERO
            || quote.ask_size.as_decimal() <= Decimal::ZERO
        {
            return Err("continuous native quote is stale or invalid".into());
        }
        let price = intent.price.unwrap_or_else(|| {
            if intent.side == "buy" {
                quote.ask_price.as_decimal()
            } else {
                quote.bid_price.as_decimal()
            }
        });
        if minimum.is_some_and(|minimum| {
            minimum <= Decimal::ZERO
                || intent
                    .quantity
                    .checked_mul(price)
                    .is_none_or(|value| value < minimum)
        }) {
            return Err("continuous native minimum is invalid or unmet".into());
        }
        Ok(())
    }

    fn continuous_market_ready(
        &self,
        state: &ProductionState,
        instrument: InstrumentId,
        now: u64,
    ) -> Result<(Decimal, Decimal), String> {
        let (info, closed, at) = state
            .continuous
            .metadata
            .get(&instrument)
            .ok_or("missing continuous instrument metadata")?;
        if *at < self.created_at
            || *at > now
            || now.saturating_sub(*at) > 65_000_000_000
            || !info.is_tradable()
            || *closed != Some(false)
        {
            return Err(
                "continuous metadata is stale, disabled or market hours are unknown".into(),
            );
        }
        let (tick, step) = info
            .try_increments()
            .map_err(|_| "invalid continuous instrument increments")?;
        Ok((tick.as_decimal(), step.as_decimal()))
    }

    fn check_continuous_rate(&self, state: &mut ProductionState, now: u64) -> Result<(), String> {
        while state
            .continuous
            .request_times
            .front()
            .is_some_and(|at| now.saturating_sub(*at) >= 1_000_000_000)
        {
            state.continuous.request_times.pop_front();
        }
        if state.continuous.request_times.len()
            >= self.continuous_config().max_requests_per_second as usize
        {
            return Err("continuous application request rate exhausted".into());
        }
        Ok(())
    }

    fn check_continuous_account(
        &self,
        state: &ProductionState,
        evidence: &ProductionEvidence,
        now: u64,
        new_risk: bool,
    ) -> Result<BTreeMap<InstrumentId, Decimal>, String> {
        let (reading, complete, at) = state
            .continuous
            .reading
            .as_ref()
            .ok_or("continuous account has not been reconciled")?;
        if (new_risk && !complete)
            || (!new_risk && !evidence.reduction_ready)
            || *at > now
            || now.saturating_sub(*at)
                > crate::reconciliation::ONDO_PRODUCTION_READING_FRESHNESS_NANOS
            || !state.identity_verified
            || !evidence.identity_matched
            || !evidence.journal_healthy
            || state
                .dms_confirmed_deadline
                .is_none_or(|deadline| now >= deadline)
            || !evidence.dms_verified
        {
            return Err(
                "continuous account, identity, journal or genuine DMS proof is unavailable".into(),
            );
        }
        let own = Self::continuous_own_net(state, evidence)?;
        if Self::continuous_position_net(reading)? != own {
            return Err(
                "continuous account net position differs from owned applied executions".into(),
            );
        }
        if new_risk
            && (!evidence.ready
                || evidence.unknown != 0
                || reading.orders.iter().any(|order| {
                    !order.status.is_terminal()
                        && !Self::continuous_order_owned(state, evidence, order)
                })
                || evidence
                    .available_margin_usdc
                    .is_none_or(|v| v < self.continuous_config().min_available_margin_usdc))
        {
            return Err(
                "continuous new risk is suspended by readiness, unresolved or external risk".into(),
            );
        }
        Ok(own)
    }

    fn continuous_order_owned(
        state: &ProductionState,
        evidence: &ProductionEvidence,
        order: &crate::reconciliation::OrderReading,
    ) -> bool {
        let Some(id) = &order.client_order_id else {
            return false;
        };
        let Some(intent) = state.continuous.intents.get(id) else {
            return false;
        };
        !state.known_zero.contains(id)
            && crate::common::parse::market_to_instrument_id(&order.market).ok()
                == Some(intent.instrument_id)
            && evidence
                .orders
                .get(id)
                .and_then(|(_, _, venue)| venue.as_ref())
                == Some(&order.venue_order_id)
    }

    fn check_continuous_reduction(
        state: &ProductionState,
        evidence: &ProductionEvidence,
        intent: &ContinuousIntent,
        own: &BTreeMap<InstrumentId, Decimal>,
    ) -> Result<(), String> {
        let net = own.get(&intent.instrument_id).copied().unwrap_or_default();
        if net == Decimal::ZERO
            || (net > Decimal::ZERO && intent.side != "sell")
            || (net < Decimal::ZERO && intent.side != "buy")
        {
            return Err("continuous reduce-only order would create opposite net risk".into());
        }
        let mut reserved = Decimal::ZERO;
        for (id, other) in &state.continuous.intents {
            if other.instrument_id != intent.instrument_id
                || !other.reduce_only
                || state.known_zero.contains(id)
            {
                continue;
            }
            let remaining = match evidence.orders.get(id) {
                Some((_, true, _)) => Decimal::ZERO,
                Some((filled, false, _)) => other.quantity - *filled,
                None => other.quantity,
            };
            reserved = reserved
                .checked_add(remaining)
                .ok_or("continuous reduction overflow")?;
        }
        for (id, (body, _, _)) in &state.contexts {
            if id == &intent.client_order_id {
                continue;
            }
            let (_, other) = Self::continuous_intent(body)?;
            if other.instrument_id == intent.instrument_id && other.reduce_only {
                reserved = reserved
                    .checked_add(other.quantity)
                    .ok_or("continuous reduction overflow")?;
            }
        }
        if reserved
            .checked_add(intent.quantity)
            .ok_or("continuous reduction overflow")?
            > net.abs()
        {
            return Err("continuous reduction exceeds verified owned net residual".into());
        }
        Ok(())
    }

    pub(super) fn authorize_continuous_post(
        &self,
        target: &str,
        body: &[u8],
    ) -> Result<(), String> {
        if target != "/v1/perps/orders" {
            return Err("continuous authority accepts only native single-order creates".into());
        }
        let (_, intent) = Self::continuous_intent(body)?;
        let mut state = self.state.lock();
        let (expected, prepared_quote, minimum) = state
            .contexts
            .get(&intent.client_order_id)
            .ok_or("continuous create has no native prepared context")?;
        let prepared_quote = *prepared_quote;
        let minimum = *minimum;
        let evidence = self.evidence(false)?;
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let current_quote = |evidence: &ProductionEvidence| {
            evidence
                .quotes
                .get(&intent.instrument_id)
                .copied()
                .or_else(|| intent.price.map(|_| prepared_quote))
        };
        let quote =
            current_quote(&evidence).ok_or("native continuous market quote is unavailable")?;
        let (tick, step) = self.continuous_market_ready(&state, intent.instrument_id, now)?;
        if expected.as_slice() != body
            || state.frozen
            || state.stopped
            || !state.started
            || state.creates.contains_key(&intent.client_order_id)
            || !self
                .continuous_config()
                .instrument_ids
                .contains(&intent.instrument_id)
            || intent.quantity % step != Decimal::ZERO
            || intent
                .price
                .is_some_and(|price| price % tick != Decimal::ZERO)
            || self
                .check_continuous_minimum(&intent, &quote, minimum, now)
                .is_err()
        {
            return Err("continuous native body, quote, increments or minimum is invalid".into());
        }
        self.check_continuous_rate(&mut state, now)?;
        if evidence.unknown > self.continuous_config().max_unresolved_orders as usize
            || Self::continuous_live_orders(&state, &evidence)
                > self.continuous_config().max_live_orders as usize
        {
            return Err("continuous live-order or unresolved risk limit exceeded".into());
        }
        let own = self.check_continuous_account(&state, &evidence, now, !intent.reduce_only)?;
        self.check_continuous_exposure(&state, &evidence, &intent)?;
        if intent.reduce_only {
            Self::check_continuous_reduction(&state, &evidence, &intent, &own)?;
        }
        let persisted = self.evidence(true)?;
        if !persisted.journal_healthy {
            return Err("continuous pre-send journal checkpoint failed".into());
        }
        if let Err(e) = self.append_continuous_record(&ContinuousJournalRecord::Intent {
            intent: intent.clone(),
        }) {
            state.frozen = true;
            state.snapshot = None;
            return Err(e);
        }
        let latest = self.evidence(false);
        let send_now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let reading_fresh = state
            .continuous
            .reading
            .as_ref()
            .is_some_and(|(_, complete, at)| {
                (*complete || intent.reduce_only)
                    && *at <= send_now
                    && send_now.saturating_sub(*at)
                        <= crate::reconciliation::ONDO_PRODUCTION_READING_FRESHNESS_NANOS
            });
        let valid = state
            .dms_confirmed_deadline
            .is_some_and(|deadline| send_now < deadline)
            && reading_fresh
            && self
                .continuous_market_ready(&state, intent.instrument_id, send_now)
                .is_ok()
            && latest.is_ok_and(|latest| {
                current_quote(&latest).is_some_and(|quote| {
                    self.check_continuous_minimum(&intent, &quote, minimum, send_now)
                        .is_ok()
                }) && latest.unknown <= self.continuous_config().max_unresolved_orders as usize
                    && self
                        .check_continuous_account(&state, &latest, send_now, !intent.reduce_only)
                        .is_ok_and(|own| {
                            !intent.reduce_only
                                || Self::check_continuous_reduction(&state, &latest, &intent, &own)
                                    .is_ok()
                        })
                    && self
                        .check_continuous_exposure(&state, &latest, &intent)
                        .is_ok()
            });
        // A persisted intent remains owned even if the last instant's admission fails
        state.creates.insert(
            intent.client_order_id.clone(),
            (intent.quantity, intent.reduce_only),
        );
        state
            .continuous
            .intents
            .insert(intent.client_order_id.clone(), intent.clone());
        state.contexts.remove(&intent.client_order_id);
        if !valid {
            if self
                .persist_continuous_zero(&intent.client_order_id)
                .is_ok()
            {
                state.known_zero.insert(intent.client_order_id);
            } else {
                state.frozen = true;
            }
            return Err("continuous admission expired during durable reservation".into());
        }
        state.continuous.request_times.push_back(send_now);
        state.requests = state.requests.saturating_add(1);
        note_activity(&mut state);
        Ok(())
    }

    pub(super) fn authorize_continuous_delete(&self, target: &str) -> Result<(), String> {
        let mut state = self.state.lock();
        if state.continuous.released {
            return Err("continuous journal ownership has been released".into());
        }
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let evidence = self.evidence(false)?;
        if !state.identity_verified || !evidence.identity_matched || !evidence.journal_healthy {
            return Err("continuous cancellation identity or journal is unavailable".into());
        }
        let owned = state.creates.keys().any(|id| {
            if state.known_zero.contains(id) {
                return false;
            }
            match evidence
                .orders
                .get(id)
                .and_then(|(_, _, venue)| venue.as_ref())
            {
                Some(venue) => crate::http::orders::order_lookup_target(venue).as_str() == target,
                None => client_order_lookup_target(id).as_str() == target,
            }
        });
        if !owned {
            return Err("continuous cancellation target is not an owned order".into());
        }
        self.check_continuous_rate(&mut state, now)?;
        if !self.evidence(true)?.journal_healthy {
            return Err("continuous cancellation journal checkpoint failed".into());
        }
        state.continuous.request_times.push_back(now);
        state.requests = state.requests.saturating_add(1);
        note_activity(&mut state);
        Ok(())
    }

    pub(super) fn verify_continuous_dms_reading(
        &self,
        reading: &crate::reconciliation::AccountReading,
    ) -> Result<(), String> {
        let state = self.state.lock();
        if state.continuous.released {
            return Err("continuous journal ownership has been released".into());
        }
        let evidence = self.evidence(false)?;
        if reading
            .orders
            .iter()
            .any(|o| !o.status.is_terminal() && !Self::continuous_order_owned(&state, &evidence, o))
        {
            return Err("continuous DMS found external open orders".into());
        }
        if Self::continuous_position_net(reading)? != Self::continuous_own_net(&state, &evidence)? {
            return Err(
                "continuous DMS account position differs from original owned executions".into(),
            );
        }
        Ok(())
    }

    pub(super) fn reconcile_continuous(
        &self,
        reading: &crate::reconciliation::AccountReading,
        clean: bool,
        generation: u64,
    ) {
        let mut state = self.state.lock();
        if state.continuous.released {
            return;
        }
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let Ok(evidence) = self.evidence(false) else {
            state.continuous.reading = None;
            state.snapshot = None;
            return;
        };
        state.continuous.reading = Some((reading.clone(), clean, now));
        let positions = Self::continuous_position_net(reading);
        let own = Self::continuous_own_net(&state, &evidence);
        let owned = positions
            .as_ref()
            .ok()
            .zip(own.as_ref().ok())
            .is_some_and(|(v, o)| v == o);
        let foreign = reading
            .orders
            .iter()
            .filter(|o| {
                !o.status.is_terminal() && !Self::continuous_order_owned(&state, &evidence, o)
            })
            .count();
        let open = reading
            .orders
            .iter()
            .filter(|o| {
                !o.status.is_terminal() && Self::continuous_order_owned(&state, &evidence, o)
            })
            .count();
        let metadata_fresh = self
            .instrument_ids()
            .iter()
            .all(|id| self.continuous_market_ready(&state, *id, now).is_ok());
        let protected_owned_account = owned
            && foreign == 0
            && metadata_fresh
            && state.identity_verified
            && evidence.identity_matched
            && evidence.journal_healthy
            && evidence.dms_verified
            && evidence.unknown == 0
            && state
                .dms_confirmed_deadline
                .is_some_and(|deadline| now < deadline);
        let new_risk_start = clean
            && evidence.ready
            && evidence
                .available_margin_usdc
                .is_some_and(|v| v >= self.minimum_available_margin_usdc());
        let recovered_reduction_start = !state.continuous.intents.is_empty()
            && evidence.reduction_ready
            && (open > 0 || own.as_ref().is_ok_and(|net| !net.is_empty()));
        if !state.started
            && protected_owned_account
            && (new_risk_start || recovered_reduction_start)
        {
            if state.cashflow_baseline.is_none() && state.continuous.intents.is_empty() {
                if let Some(baseline) =
                    CashflowBaseline::from_reading(reading, evidence.funding_accounted_usdc)
                {
                    if self
                        .append_continuous_record(&ContinuousJournalRecord::Baseline {
                            baseline: baseline.clone(),
                        })
                        .is_err()
                    {
                        state.frozen = true;
                        state.snapshot = None;
                        return;
                    }
                    state.cashflow_baseline = Some(baseline);
                }
            }
            state.started = true;
        }
        let flat = positions.as_ref().is_ok_and(BTreeMap::is_empty);
        let settled = state.creates.keys().all(|id| {
            state.known_zero.contains(id)
                || evidence
                    .orders
                    .get(id)
                    .is_some_and(|(_, settled, _)| *settled)
        });
        let complete = state.stopped
            && clean
            && owned
            && flat
            && foreign == 0
            && open == 0
            && evidence.unknown == 0
            && settled
            && state.contexts.is_empty()
            && generation == state.activity;
        if complete {
            state.frozen = true;
        }
        let native_ready = state.started
            && !state.stopped
            && !state.frozen
            && clean
            && owned
            && foreign == 0
            && metadata_fresh
            && evidence.ready
            && evidence.unknown == 0
            && state.identity_verified
            && evidence.identity_matched
            && evidence.journal_healthy
            && evidence.dms_verified
            && evidence
                .available_margin_usdc
                .is_some_and(|v| v >= self.minimum_available_margin_usdc())
            && state
                .dms_confirmed_deadline
                .is_some_and(|deadline| now < deadline);
        let reduction_ready = state.started
            && !state.stopped
            && !state.frozen
            && protected_owned_account
            && evidence.reduction_ready;
        state.generation = state.generation.saturating_add(1);
        let position_quantities: BTreeMap<String, String> = positions
            .unwrap_or_default()
            .into_iter()
            .map(|(id, value)| (id.to_string(), value.to_string()))
            .collect();
        state.snapshot = Some(serde_json::json!({
            "run_id": self.run_id, "mode": "continuous",
            "phase": if complete { "reconciled" } else { "continuous" },
            "generation": state.generation, "snapshot_unix_nanos": now,
            "latest_activity_generation": state.activity,
            "latest_activity_unix_nanos": state.activity_at,
            "reconciled_activity_generation": generation,
            "reconciliation_unix_nanos": now, "complete": complete,
            "coverage_complete": clean, "snapshot_fresh": true,
            "reconciled_flat": flat && owned, "account_positions_owned": owned,
            "position_quantities": position_quantities,
            "foreign_open_orders": foreign, "own_open_orders": open,
            "unknown_submissions": evidence.unknown, "late_fills": 0,
            "metadata_fresh": metadata_fresh, "native_ready": native_ready,
            "reduction_ready": reduction_ready,
            "connected_ready": native_ready || reduction_ready,
            "execution_cost": null,
        }));
        let cashflows = state
            .cashflow_baseline
            .as_ref()
            .and_then(|baseline| baseline.project(reading, evidence.funding_accounted_usdc));
        if let Some(snapshot) = state.snapshot.as_mut() {
            snapshot["account_cashflows"] = cashflows.unwrap_or(serde_json::Value::Null);
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::types::{Price, Quantity};
    use rstest::rstest;

    use super::*;

    fn config() -> OndoContinuousExecutionConfig {
        serde_json::from_value(serde_json::json!({
            "instrument_ids": ["NVDA-USD-PERP.ONDO", "TSLA-USD-PERP.ONDO"],
            "max_notional_per_order_usd": "100", "max_gross_exposure_usd": "500",
            "min_available_margin_usdc": "25", "max_live_orders": 4,
            "max_requests_per_second": 1000, "max_unresolved_orders": 2,
        }))
        .unwrap()
    }

    fn quote(instrument: InstrumentId, price: &str) -> QuoteTick {
        let now = get_atomic_clock_realtime().get_time_ns();
        QuoteTick::new(
            instrument,
            Price::from(price),
            Price::from(price),
            Quantity::from("1.00"),
            Quantity::from("1.00"),
            now,
            now,
        )
    }

    fn authority(
        config: OndoContinuousExecutionConfig,
    ) -> (
        Arc<ProductionAuthority>,
        Arc<Mutex<ProductionEvidence>>,
        PathBuf,
    ) {
        let path =
            std::env::temp_dir().join(format!("ondo-continuous-{}", nautilus_core::UUID4::new()));
        let guard = ProductionAuthority::new_continuous(
            config,
            "run".into(),
            "account",
            path.to_str().unwrap(),
            120,
        )
        .unwrap();
        let now = get_atomic_clock_realtime().get_time_ns();
        let infos = crate::http::models::parse_markets(include_str!(
            "../test_data/rest/markets_synthetic.json"
        ))
        .unwrap()
        .market_infos(now)
        .unwrap();
        for info in infos {
            guard.metadata_for(info.instrument_id(), Some(info), Some(false), now.as_u64());
        }
        let quotes = guard
            .instrument_ids()
            .into_iter()
            .map(|id| (id, quote(id, "230.00")))
            .collect();
        let evidence = Arc::new(Mutex::new(ProductionEvidence {
            ready: true,
            identity_matched: true,
            journal_healthy: true,
            dms_verified: true,
            reduction_ready: true,
            available_margin_usdc: Some(Decimal::from(1000)),
            funding_accounted_usdc: Some(Decimal::ZERO),
            quotes,
            orders: BTreeMap::new(),
            unknown: 0,
        }));
        let reader = Arc::clone(&evidence);
        guard
            .bind(Arc::new(move |_| Ok(reader.lock().clone())))
            .unwrap();
        guard.verify_identity(Some("account".into())).unwrap();
        assert!(guard.begin_dms_send(now.as_u64(), false));
        assert!(guard.confirm_dms(now.as_u64()));
        sync_reading(&guard, &evidence, true);
        assert!(guard.state.lock().started);
        (guard, evidence, path)
    }

    fn sync_reading(
        guard: &ProductionAuthority,
        evidence: &Mutex<ProductionEvidence>,
        clean: bool,
    ) {
        let net =
            ProductionAuthority::continuous_own_net(&guard.state.lock(), &evidence.lock()).unwrap();
        let positions = net
            .into_iter()
            .map(|(id, signed)| {
                crate::reconciliation::PositionReading::new(
                    &crate::common::parse::instrument_id_to_market(&id).unwrap(),
                    if signed > Decimal::ZERO {
                        "long"
                    } else {
                        "short"
                    },
                    signed.abs(),
                    None,
                )
            })
            .collect();
        let reading = crate::reconciliation::AccountReading {
            positions,
            balance: Some(crate::reconciliation::BalanceReading {
                wallet_balance: Some(Decimal::from(1000)),
                margin_balance: Some(Decimal::from(1000)),
                used_margin: Some(Decimal::ZERO),
                available_margin: Some(Decimal::from(1000)),
                withdrawable_margin: Some(Decimal::from(1000)),
                maintenance_margin_requirement: None,
                under_liquidation: Some(false),
                total_funding_payments: Some(Decimal::ZERO),
                unmapped: vec![],
                raw: "synthetic-unit-account".into(),
            }),
            read_at: get_atomic_clock_realtime().get_time_ns(),
            ..Default::default()
        };
        guard.reconcile(&reading, clean, guard.activity_generation());
    }

    fn body(
        id: &str,
        market: &str,
        side: &str,
        quantity: &str,
        price: &str,
        reduce: bool,
        tif: &str,
        post: bool,
    ) -> Vec<u8> {
        serde_json::to_vec(
            &serde_json::json!({"market":market,"side":side,"type":"limit",
            "size":quantity,"price":price,"timeInForce":tif,"postOnly":post,
            "reduceOnly":reduce,"clientOrderId":id}),
        )
        .unwrap()
    }

    fn prepare(guard: &ProductionAuthority, bytes: &[u8]) -> Result<(), String> {
        let (_, intent) = ProductionAuthority::continuous_intent(bytes)?;
        guard.prepare(
            intent.client_order_id,
            bytes.to_vec(),
            Some(quote(intent.instrument_id, "230.00")),
            None,
        )
    }

    fn market_body(id: &str, side: &str, quantity: &str, reduce: bool) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "market": "NVDA-USD.P", "side": side, "type": "market", "size": quantity,
            "postOnly": false, "reduceOnly": reduce, "clientOrderId": id,
        }))
        .unwrap()
    }

    #[rstest]
    #[case("buy", "sell")]
    #[case("sell", "buy")]
    fn test_continuous_market_opens_and_reduces_only_owned_net(
        #[case] open: &str,
        #[case] close: &str,
    ) {
        let (guard, evidence, path) = authority(config());
        fill(
            &guard,
            &evidence,
            &market_body("market-entry", open, "0.10", false),
        );
        let wrong = market_body("wrong-direction", open, "0.10", true);
        prepare(&guard, &wrong).unwrap();
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &wrong)
                .unwrap_err()
                .contains("opposite net risk")
        );
        assert!(guard.claim_cancelled_prepared("wrong-direction"));
        fill(
            &guard,
            &evidence,
            &market_body("market-exit", close, "0.10", true),
        );
        assert!(
            ProductionAuthority::continuous_own_net(&guard.state.lock(), &evidence.lock())
                .unwrap()
                .is_empty()
        );
        remove_journal(guard, path);
    }

    #[rstest]
    #[case("price", serde_json::json!("230.00"))]
    #[case("price", serde_json::Value::Null)]
    #[case("timeInForce", serde_json::json!("IOC"))]
    #[case("timeInForce", serde_json::Value::Null)]
    #[case("postOnly", serde_json::json!(true))]
    #[case("type", serde_json::json!("stop"))]
    fn test_continuous_market_refuses_malformed_fields(
        #[case] field: &str,
        #[case] value: serde_json::Value,
    ) {
        let mut body: serde_json::Value =
            serde_json::from_slice(&market_body("malformed", "buy", "0.10", false)).unwrap();
        body[field] = value;
        assert!(
            ProductionAuthority::continuous_intent(&serde_json::to_vec(&body).unwrap()).is_err()
        );
    }

    #[rstest]
    fn test_continuous_market_caps_and_minimum_recheck_current_native_quote() {
        let (guard, evidence, path) = authority(config());
        let instrument = "NVDA-USD-PERP.ONDO".parse().unwrap();
        let bytes = market_body("market-cap", "buy", "0.40", false);
        prepare(&guard, &bytes).unwrap();
        evidence
            .lock()
            .quotes
            .insert(instrument, quote(instrument, "251.00"));
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &bytes)
                .unwrap_err()
                .contains("ceiling")
        );
        assert!(!guard.owns("market-cap"));
        let under_minimum = market_body("market-minimum", "sell", "0.10", false);
        guard
            .prepare(
                "market-minimum".into(),
                under_minimum.clone(),
                Some(quote(instrument, "251.00")),
                Some(Decimal::from(25)),
            )
            .unwrap();
        evidence
            .lock()
            .quotes
            .insert(instrument, quote(instrument, "249.00"));
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &under_minimum)
                .unwrap_err()
                .contains("minimum")
        );
        assert!(!guard.owns("market-minimum"));
        assert!(
            guard
                .prepare(
                    "prepare-minimum".into(),
                    market_body("prepare-minimum", "buy", "0.01", false),
                    Some(quote(instrument, "249.00")),
                    Some(Decimal::from(25))
                )
                .unwrap_err()
                .contains("minimum")
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_market_requires_fresh_current_native_quote_even_when_prepared_is_fresh() {
        let (guard, evidence, path) = authority(config());
        let instrument = "NVDA-USD-PERP.ONDO".parse().unwrap();
        let bytes = market_body("market-freshness", "buy", "0.10", false);
        prepare(&guard, &bytes).unwrap();
        evidence.lock().quotes.remove(&instrument);
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &bytes)
                .unwrap_err()
                .contains("quote")
        );
        assert!(prepare(&guard, &market_body("missing-native", "buy", "0.10", false)).is_err());
        let mut stale = quote(instrument, "230.00");
        stale.ts_init = (stale.ts_init.as_u64() - 3_000_000_000).into();
        evidence.lock().quotes.insert(instrument, stale);
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &bytes)
                .unwrap_err()
                .contains("quote")
        );
        assert!(!guard.owns("market-freshness"));
        remove_journal(guard, path);
    }

    #[rstest]
    #[case("251.00", None)]
    #[case("100.00", Some(Decimal::from(50)))]
    fn test_continuous_market_durable_recheck_refuses_quote_cap_or_minimum_drift(
        #[case] latest_price: &'static str,
        #[case] minimum: Option<Decimal>,
    ) {
        let (mut guard, evidence, path) = authority(config());
        let instrument = "NVDA-USD-PERP.ONDO".parse().unwrap();
        let bytes = market_body("market-durable", "buy", "0.40", false);
        guard
            .prepare(
                "market-durable".into(),
                bytes.clone(),
                Some(quote(instrument, "230.00")),
                minimum,
            )
            .unwrap();
        Arc::get_mut(&mut guard).unwrap().evidence.take();
        let reader = Arc::clone(&evidence);
        guard
            .bind(Arc::new(move |checkpoint| {
                let mut proof = reader.lock();
                if checkpoint {
                    proof
                        .quotes
                        .insert(instrument, quote(instrument, latest_price));
                }
                Ok(proof.clone())
            }))
            .unwrap();
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &bytes)
                .unwrap_err()
                .contains("durable")
        );
        assert!(guard.owns("market-durable"));
        assert!(guard.state.lock().known_zero.contains("market-durable"));
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_market_recovery_retains_unknown_identity_and_exclusive_ownership() {
        let limits = config();
        let (guard, _, path) = authority(limits.clone());
        let bytes = market_body("market-lost", "sell", "0.10", false);
        prepare(&guard, &bytes).unwrap();
        guard.authorize_post("/v1/perps/orders", &bytes).unwrap();
        assert!(
            ProductionAuthority::new_continuous(
                limits.clone(),
                "run".into(),
                "account",
                path.to_str().unwrap(),
                120
            )
            .is_err()
        );
        drop(guard);
        let restored = ProductionAuthority::new_continuous(
            limits,
            "run".into(),
            "account",
            path.to_str().unwrap(),
            120,
        )
        .unwrap();
        assert!(restored.owns("market-lost"));
        assert!(!restored.claim_cancelled_prepared("market-lost"));
        assert_eq!(
            restored.state.lock().continuous.intents["market-lost"].body,
            bytes
        );
        assert!(
            restored
                .verify_continuous_dms_reading(&Default::default())
                .is_err()
        );
        remove_journal(restored, path);
    }

    fn fill(guard: &ProductionAuthority, evidence: &Mutex<ProductionEvidence>, bytes: &[u8]) {
        prepare(guard, bytes).unwrap();
        guard.authorize_post("/v1/perps/orders", bytes).unwrap();
        let (_, intent) = ProductionAuthority::continuous_intent(bytes).unwrap();
        evidence.lock().orders.insert(
            intent.client_order_id,
            (intent.quantity, true, Some("owned".into())),
        );
        sync_reading(guard, evidence, true);
    }

    fn remove_journal(guard: Arc<ProductionAuthority>, path: PathBuf) {
        drop(guard);
        std::fs::remove_file(format!("{}.production-run", path.display())).unwrap();
        std::fs::remove_file(format!("{}.production-run.lock", path.display())).unwrap();
    }

    #[rstest]
    fn test_continuous_repeated_round_trips_do_not_inherit_probe_limits() {
        let (guard, evidence, path) = authority(config());
        for cycle in 0..3 {
            for market in ["NVDA-USD.P", "TSLA-USD.P"] {
                fill(
                    &guard,
                    &evidence,
                    &body(
                        &format!("entry-{market}-{cycle}"),
                        market,
                        "buy",
                        "0.10",
                        "230.00",
                        false,
                        "IOC",
                        false,
                    ),
                );
                fill(
                    &guard,
                    &evidence,
                    &body(
                        &format!("exit-{market}-{cycle}"),
                        market,
                        "sell",
                        "0.10",
                        "230.00",
                        true,
                        "IOC",
                        false,
                    ),
                );
            }
        }
        assert_eq!(guard.state.lock().creates.len(), 12);
        assert!(!guard.state.lock().frozen);
        guard.stop_creates();
        sync_reading(&guard, &evidence, true);
        assert_eq!(guard.snapshot().unwrap()["phase"], "reconciled");
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_reduction_uses_shared_one_way_net_for_opposite_virtual_legs() {
        let (guard, evidence, path) = authority(config());
        fill(
            &guard,
            &evidence,
            &body(
                "long",
                "NVDA-USD.P",
                "buy",
                "0.10",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        fill(
            &guard,
            &evidence,
            &body(
                "virtual-short",
                "NVDA-USD.P",
                "sell",
                "0.05",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        let wrong = body(
            "wrong-direction",
            "NVDA-USD.P",
            "buy",
            "0.05",
            "230.00",
            true,
            "IOC",
            false,
        );
        prepare(&guard, &wrong).unwrap();
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &wrong)
                .unwrap_err()
                .contains("opposite net risk")
        );
        assert!(guard.claim_cancelled_prepared("wrong-direction"));
        let too_large = body(
            "too-large",
            "NVDA-USD.P",
            "sell",
            "0.10",
            "230.00",
            true,
            "IOC",
            false,
        );
        prepare(&guard, &too_large).unwrap();
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &too_large)
                .unwrap_err()
                .contains("net residual")
        );
        assert!(guard.claim_cancelled_prepared("too-large"));
        evidence.lock().ready = false;
        sync_reading(&guard, &evidence, false);
        fill(
            &guard,
            &evidence,
            &body(
                "verified-reduction",
                "NVDA-USD.P",
                "sell",
                "0.05",
                "230.00",
                true,
                "IOC",
                false,
            ),
        );
        assert!(
            ProductionAuthority::continuous_own_net(&guard.state.lock(), &evidence.lock())
                .unwrap()
                .is_empty()
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_pending_reductions_reserve_remaining_quantity() {
        let (guard, evidence, path) = authority(config());
        fill(
            &guard,
            &evidence,
            &body(
                "open",
                "NVDA-USD.P",
                "buy",
                "0.10",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        let first = body(
            "reduce-1",
            "NVDA-USD.P",
            "sell",
            "0.07",
            "230.00",
            true,
            "GTC",
            false,
        );
        prepare(&guard, &first).unwrap();
        guard.authorize_post("/v1/perps/orders", &first).unwrap();
        evidence.lock().orders.insert(
            "reduce-1".into(),
            (Decimal::ZERO, false, Some("reduce-venue".into())),
        );
        let second = body(
            "reduce-2",
            "NVDA-USD.P",
            "sell",
            "0.04",
            "230.00",
            true,
            "IOC",
            false,
        );
        prepare(&guard, &second).unwrap();
        assert!(
            guard
                .authorize_post("/v1/perps/orders", &second)
                .unwrap_err()
                .contains("net residual")
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_pending_commands_reserve_live_and_aggregate_limits() {
        let mut limits = config();
        limits.max_gross_exposure_usd = Decimal::from(100);
        let (guard, _, path) = authority(limits);
        prepare(
            &guard,
            &body(
                "first",
                "NVDA-USD.P",
                "buy",
                "0.30",
                "230.00",
                false,
                "GTC",
                true,
            ),
        )
        .unwrap();
        assert!(
            prepare(
                &guard,
                &body(
                    "excess",
                    "TSLA-USD.P",
                    "buy",
                    "0.20",
                    "230.00",
                    false,
                    "IOC",
                    false
                )
            )
            .unwrap_err()
            .contains("aggregate")
        );
        for id in ["second", "third", "fourth"] {
            prepare(
                &guard,
                &body(
                    id,
                    "TSLA-USD.P",
                    "buy",
                    "0.01",
                    "230.00",
                    false,
                    "GTC",
                    true,
                ),
            )
            .unwrap();
        }
        assert!(
            prepare(
                &guard,
                &body(
                    "fifth",
                    "TSLA-USD.P",
                    "buy",
                    "0.01",
                    "230.00",
                    false,
                    "IOC",
                    false
                )
            )
            .unwrap_err()
            .contains("excessive")
        );
        assert!(guard.claim_cancelled_prepared("first"));
        prepare(
            &guard,
            &body(
                "next",
                "TSLA-USD.P",
                "buy",
                "0.01",
                "230.00",
                false,
                "IOC",
                false,
            ),
        )
        .unwrap();
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_gross_marks_all_positions_with_current_native_quotes() {
        let mut limits = config();
        limits.max_gross_exposure_usd = Decimal::from(250);
        let (guard, evidence, path) = authority(limits);
        fill(
            &guard,
            &evidence,
            &body(
                "held",
                "NVDA-USD.P",
                "buy",
                "0.30",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        let instrument = "NVDA-USD-PERP.ONDO".parse().unwrap();
        evidence
            .lock()
            .quotes
            .insert(instrument, quote(instrument, "1000.00"));
        let new = body(
            "second-symbol",
            "TSLA-USD.P",
            "buy",
            "0.10",
            "230.00",
            false,
            "IOC",
            false,
        );
        assert!(prepare(&guard, &new).unwrap_err().contains("aggregate"));
        evidence.lock().quotes.remove(&instrument);
        assert!(
            prepare(&guard, &new)
                .unwrap_err()
                .contains("no authoritative")
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_recovery_retains_unknown_original_identity_and_refuses_concurrent_owner() {
        let limits = config();
        let (guard, _, path) = authority(limits.clone());
        let bytes = body(
            "lost-original",
            "NVDA-USD.P",
            "buy",
            "0.10",
            "230.00",
            false,
            "IOC",
            false,
        );
        prepare(&guard, &bytes).unwrap();
        guard.authorize_post("/v1/perps/orders", &bytes).unwrap();
        assert!(
            ProductionAuthority::new_continuous(
                limits.clone(),
                "run".into(),
                "account",
                path.to_str().unwrap(),
                120
            )
            .is_err()
        );
        drop(guard);
        let restored = ProductionAuthority::new_continuous(
            limits,
            "run".into(),
            "account",
            path.to_str().unwrap(),
            120,
        )
        .unwrap();
        assert!(restored.owns("lost-original"));
        assert!(restored.state.lock().cashflow_baseline.is_some());
        assert!(!restored.claim_cancelled_prepared("lost-original"));
        assert!(
            restored
                .state
                .lock()
                .continuous
                .intents
                .get("lost-original")
                .is_some_and(|intent| intent.body == bytes)
        );
        assert!(
            restored
                .verify_continuous_dms_reading(&crate::reconciliation::AccountReading::default())
                .is_err()
        );
        remove_journal(restored, path);
    }

    #[rstest]
    fn test_continuous_stop_preserves_residual_and_does_not_prove_clean_shutdown() {
        let (guard, evidence, path) = authority(config());
        fill(
            &guard,
            &evidence,
            &body(
                "residual",
                "NVDA-USD.P",
                "buy",
                "0.10",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        guard.stop_creates();
        sync_reading(&guard, &evidence, true);
        let snapshot = guard.snapshot().unwrap();
        assert_eq!(
            snapshot["position_quantities"]["NVDA-USD-PERP.ONDO"],
            "0.10"
        );
        assert_eq!(snapshot["reconciled_flat"], false);
        assert_eq!(snapshot["complete"], false);
        assert!(!guard.finish(true));
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_cancel_is_owned_and_shares_application_rate_budget() {
        let mut limits = config();
        limits.max_requests_per_second = 1;
        let (guard, evidence, path) = authority(limits);
        let bytes = body(
            "resting",
            "NVDA-USD.P",
            "buy",
            "0.10",
            "230.00",
            false,
            "GTC",
            true,
        );
        prepare(&guard, &bytes).unwrap();
        guard.authorize_post("/v1/perps/orders", &bytes).unwrap();
        evidence.lock().orders.insert(
            "resting".into(),
            (Decimal::ZERO, false, Some("own-venue".into())),
        );
        assert!(
            guard
                .authorize_delete("/v1/perps/orders/external")
                .unwrap_err()
                .contains("not an owned")
        );
        let target = crate::http::orders::order_lookup_target("own-venue");
        assert!(
            guard
                .authorize_delete(target.as_str())
                .unwrap_err()
                .contains("rate")
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_exact_limits_and_supported_time_in_force_are_explicit() {
        let mut limits = config();
        limits.max_notional_per_order_usd =
            Decimal::from_str_exact("100.00000000000000000001").unwrap();
        assert!(limits.validate().is_err());
        limits = config();
        limits.max_gross_exposure_usd =
            Decimal::from_str_exact("500.00000000000000000001").unwrap();
        assert!(limits.validate().is_err());
        let (guard, _, path) = authority(config());
        assert!(
            prepare(
                &guard,
                &body(
                    "ioc-post",
                    "NVDA-USD.P",
                    "buy",
                    "0.10",
                    "230.00",
                    false,
                    "IOC",
                    true
                )
            )
            .is_err()
        );
        assert!(
            prepare(
                &guard,
                &body(
                    "fok",
                    "NVDA-USD.P",
                    "buy",
                    "0.10",
                    "230.00",
                    false,
                    "FOK",
                    false
                )
            )
            .is_err()
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_released_diagnostics_cannot_mutate_the_recovered_run() {
        let limits = config();
        let (guard, _, path) = authority(limits.clone());
        guard.release_continuous_ownership();
        assert!(guard.continuous_ownership_released());
        assert!(!guard.permits_dms());
        assert!(!guard.begin_dms_send(get_atomic_clock_realtime().get_time_ns().as_u64(), false));
        assert!(
            guard
                .authorize_delete("/v1/perps/orders/client%3Aoriginal")
                .is_err()
        );
        assert!(
            prepare(
                &guard,
                &body(
                    "revoked",
                    "NVDA-USD.P",
                    "buy",
                    "0.10",
                    "230.00",
                    false,
                    "IOC",
                    false
                )
            )
            .is_err()
        );
        let restored = ProductionAuthority::new_continuous(
            limits,
            "run".into(),
            "account",
            path.to_str().unwrap(),
            120,
        )
        .unwrap();
        let before = std::fs::read(&restored.journal_binding).unwrap();
        guard.definite_zero("old-stale-command");
        assert!(!guard.claim_cancelled_prepared("old-stale-command"));
        assert_eq!(std::fs::read(&restored.journal_binding).unwrap(), before);
        drop(guard);
        remove_journal(restored, path);
    }

    #[rstest]
    fn test_continuous_external_reuse_of_original_client_identity_is_not_owned() {
        let (guard, evidence, path) = authority(config());
        fill(
            &guard,
            &evidence,
            &body(
                "original",
                "NVDA-USD.P",
                "buy",
                "0.10",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        let external = crate::reconciliation::OrderReading {
            venue_order_id: "external-reused-id".into(),
            client_order_id: Some("original".into()),
            market: "NVDA-USD.P".into(),
            status: crate::http::orders::OndoOrderStatus::Open,
            venue_filled: Some(Decimal::ZERO),
            applied_filled: None,
            tracked: false,
        };
        assert!(!ProductionAuthority::continuous_order_owned(
            &guard.state.lock(),
            &evidence.lock(),
            &external
        ));
        assert!(
            guard
                .authorize_delete(
                    crate::http::orders::order_lookup_target("external-reused-id").as_str()
                )
                .is_err()
        );
        assert!(
            guard
                .authorize_delete(client_order_lookup_target("original").as_str())
                .is_err()
        );
        remove_journal(guard, path);
    }

    #[rstest]
    fn test_continuous_recovery_starts_owned_reduction_when_new_risk_margin_is_unavailable() {
        let limits = config();
        let (guard, evidence, path) = authority(limits.clone());
        fill(
            &guard,
            &evidence,
            &body(
                "before-restart",
                "NVDA-USD.P",
                "buy",
                "0.10",
                "230.00",
                false,
                "IOC",
                false,
            ),
        );
        guard.release_continuous_ownership();
        let restored = ProductionAuthority::new_continuous(
            limits,
            "run".into(),
            "account",
            path.to_str().unwrap(),
            120,
        )
        .unwrap();
        let now = get_atomic_clock_realtime().get_time_ns();
        for info in crate::http::models::parse_markets(include_str!(
            "../test_data/rest/markets_synthetic.json"
        ))
        .unwrap()
        .market_infos(now)
        .unwrap()
        {
            restored.metadata_for(info.instrument_id(), Some(info), Some(false), now.as_u64());
        }
        {
            let mut proof = evidence.lock();
            proof.ready = false;
            proof.available_margin_usdc = Some(Decimal::ZERO);
            proof.quotes = restored
                .instrument_ids()
                .into_iter()
                .map(|id| (id, quote(id, "230.00")))
                .collect();
        }
        let reader = Arc::clone(&evidence);
        restored
            .bind(Arc::new(move |_| Ok(reader.lock().clone())))
            .unwrap();
        restored.verify_identity(Some("account".into())).unwrap();
        assert!(restored.begin_dms_send(now.as_u64(), false));
        assert!(restored.confirm_dms(now.as_u64()));
        sync_reading(&restored, &evidence, false);
        let snapshot = restored.snapshot().unwrap();
        assert_eq!(snapshot["native_ready"], false);
        assert_eq!(snapshot["reduction_ready"], true);
        assert_eq!(snapshot["connected_ready"], true);
        assert!(!restored.continuous_new_risk_ready());
        fill(
            &restored,
            &evidence,
            &body(
                "recovered-exit",
                "NVDA-USD.P",
                "sell",
                "0.10",
                "230.00",
                true,
                "IOC",
                false,
            ),
        );
        assert!(
            ProductionAuthority::continuous_own_net(&restored.state.lock(), &evidence.lock())
                .unwrap()
                .is_empty()
        );
        drop(guard);
        remove_journal(restored, path);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn test_continuous_send_uses_latest_native_quote_after_budget_wait(
        #[case] native_updated: bool,
    ) {
        let (mut guard, evidence, path) = authority(config());
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        Arc::get_mut(&mut guard).unwrap().created_at = now - 5_000_000_000;
        let instrument = "NVDA-USD-PERP.ONDO".parse().unwrap();
        let bytes = body(
            "queued-original",
            "NVDA-USD.P",
            "buy",
            "0.10",
            "230.00",
            false,
            "IOC",
            false,
        );
        let mut queued_quote = quote(instrument, "230.00");
        queued_quote.ts_init = (now - 3_000_000_000).into();
        queued_quote.ts_event = queued_quote.ts_init;
        guard
            .prepare(
                "queued-original".into(),
                bytes.clone(),
                Some(queued_quote),
                None,
            )
            .unwrap();
        if native_updated {
            evidence
                .lock()
                .quotes
                .insert(instrument, quote(instrument, "231.00"));
        } else {
            evidence.lock().quotes.remove(&instrument);
        }
        let result = guard.authorize_post("/v1/perps/orders", &bytes);
        if native_updated {
            result.unwrap();
            let state = guard.state.lock();
            let intent = state.continuous.intents.get("queued-original").unwrap();
            assert_eq!(intent.body, bytes);
            assert_eq!(intent.price, Some(Decimal::from(230)));
        } else {
            assert!(result.unwrap_err().contains("quote"));
            assert!(!guard.owns("queued-original"));
        }
        remove_journal(guard, path);
    }
}
