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

//! Explicit io standard-account proof, independent of the default perpetual and spot accounts.

use std::{collections::BTreeSet, sync::Arc};

use anyhow::Context;
use nautilus_core::{Params, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::types::{AccountBalance, Currency, Money};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;

use crate::{
    common::{
        enums::HyperliquidInfoRequestType,
        parse::{serialize_decimal_as_str, serialize_optional_decimal_as_str},
    },
    http::{client::HyperliquidHttpClient, query::InfoRequest},
    websocket::{
        client::HyperliquidWebSocketClient,
        messages::{SubscriptionRequest, SubscriptionResponseData},
    },
};

const USDC_TOKEN_ID: &str = "0x6d1e7cde53ba9467b783cb7c530ce054";
const ALL_PRIVATE_ACKS: u8 = 7;

/// A position from a complete io clearinghouse snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HyperliquidAccountScopePosition {
    pub coin: String,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub signed_size: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub margin_used: Decimal,
}

/// A detached read-only account proof. Decimal JSON values are exact strings.
///
/// `balance` is raw collateral, `equity` includes position valuation, `free` is
/// equity less used margin, and `withdrawable` remains a separate venue fact.
/// An untrusted proof never asserts that the account is flat.
#[derive(Clone, Debug, Serialize)]
pub struct HyperliquidAccountScopeSnapshot {
    pub dex: String,
    pub address: String,
    pub account_mode: String,
    pub collateral_token_id: String,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub balance: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub equity: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub withdrawable: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub used: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_str")]
    pub free: Decimal,
    /// The API does not supply total maintenance margin for isolated positions.
    #[serde(serialize_with = "serialize_optional_decimal_as_str")]
    pub total_maintenance: Option<Decimal>,
    pub positions: Vec<HyperliquidAccountScopePosition>,
    pub http_source_time_ms: u64,
    pub http_received_time_ms: u64,
    pub http_verification_started_time_ms: u64,
    pub private_stream_epoch: u64,
    pub ws_received_time_ms: Option<u64>,
    pub ws_source_time_ms: Option<u64>,
    pub trusted: bool,
    pub flat: Option<bool>,
    pub diagnostic: String,
    pub provenance: String,
}

#[derive(Clone, Debug)]
pub(crate) struct AccountScopeDiagnostics {
    pub(crate) state: Arc<Mutex<AccountScopeState>>,
    pub(crate) ws: HyperliquidWebSocketClient,
}

impl AccountScopeDiagnostics {
    pub(crate) fn snapshot(&self) -> Option<HyperliquidAccountScopeSnapshot> {
        self.state
            .lock()
            .snapshot(now_ms(), self.ws.is_active(), self.ws.connection_epoch())
    }
}

#[derive(Debug)]
pub(crate) struct AccountScopeState {
    address: String,
    max_age_ms: u64,
    version: u64,
    facts: Option<HyperliquidAccountScopeSnapshot>,
    diagnostic: Option<String>,
    acknowledgements: u8,
    ws_received_time_ms: Option<u64>,
    ws_source_time_ms: Option<u64>,
    ws_facts: Option<ScopedClearinghouse>,
    universe: BTreeSet<String>,
    refresh_pending: bool,
    stream_epoch: Option<u64>,
}

impl AccountScopeState {
    pub(crate) fn new(address: String, max_age_ms: u64) -> Self {
        Self {
            address,
            max_age_ms,
            version: 0,
            facts: None,
            diagnostic: Some("No complete io HTTP account proof".to_string()),
            acknowledgements: 0,
            ws_received_time_ms: None,
            ws_source_time_ms: None,
            ws_facts: None,
            universe: BTreeSet::new(),
            refresh_pending: false,
            stream_epoch: None,
        }
    }

    pub(crate) fn invalidate(&mut self, reason: &str, reset_stream: bool) {
        self.version = self.version.wrapping_add(1);
        self.diagnostic = Some(reason.to_string());
        self.refresh_pending = false;
        if reset_stream {
            self.acknowledgements = 0;
            self.ws_received_time_ms = None;
            self.ws_source_time_ms = None;
            self.ws_facts = None;
        }
    }

    pub(crate) fn bind_stream(&mut self, epoch: u64) {
        self.invalidate(
            "io private stream epoch changed; fresh HTTP proof required",
            true,
        );
        self.stream_epoch = Some(epoch);
    }

    pub(crate) fn snapshot(
        &self,
        now: u64,
        ws_active: bool,
        epoch: u64,
    ) -> Option<HyperliquidAccountScopeSnapshot> {
        let mut snapshot = self.facts.clone()?;
        snapshot.ws_received_time_ms = self.ws_received_time_ms;
        snapshot.ws_source_time_ms = self.ws_source_time_ms;
        let reason = self.diagnostic.clone().or_else(|| {
            if !ws_active {
                Some("Private WebSocket is not connected".to_string())
            } else if self.stream_epoch != Some(epoch) || snapshot.private_stream_epoch != epoch {
                Some("io account proof belongs to an earlier private stream epoch".to_string())
            } else if self.acknowledgements != ALL_PRIVATE_ACKS {
                Some("Private subscriptions are not fully acknowledged".to_string())
            } else if !fresh(snapshot.http_source_time_ms, now, self.max_age_ms)
                || !fresh(snapshot.http_received_time_ms, now, self.max_age_ms)
                || !fresh(
                    snapshot.http_verification_started_time_ms,
                    now,
                    self.max_age_ms,
                )
            {
                Some("io HTTP source or account-mode proof is stale".to_string())
            } else if self
                .ws_received_time_ms
                .is_none_or(|time| !fresh(time, now, self.max_age_ms))
            {
                Some("Complete io private stream evidence is missing or stale".to_string())
            } else if self
                .ws_source_time_ms
                .is_some_and(|time| !fresh(time, now, self.max_age_ms))
            {
                Some("io private stream source time is stale".to_string())
            } else {
                None
            }
        });
        snapshot.trusted = reason.is_none();
        snapshot.flat = snapshot.trusted.then(|| {
            snapshot
                .positions
                .iter()
                .all(|position| position.signed_size.is_zero())
        });
        snapshot.diagnostic = reason.unwrap_or_else(|| "Complete bounded io HTTP proof and matching private stream; WS source time may be unknown".to_string());
        Some(snapshot)
    }

    pub(crate) fn acknowledge(&mut self, data: &SubscriptionResponseData) -> anyhow::Result<()> {
        let (user, bit, dex_matches) = match &data.subscription {
            SubscriptionRequest::OrderUpdates { user } => (user, 1, true),
            SubscriptionRequest::UserEvents { user } => (user, 2, true),
            SubscriptionRequest::ClearinghouseState { user, dex } => (user, 4, dex == "io"),
            _ => return Ok(()),
        };
        if user != &self.address || !dex_matches || data.method != "subscribe" {
            self.invalidate("Private subscription identity or scope mismatch", true);
            anyhow::bail!("Private subscription identity or scope mismatch");
        }
        self.acknowledgements |= bit;
        Ok(())
    }

    pub(crate) fn observe_ws(&mut self, data: &Value, received: u64) -> anyhow::Result<()> {
        let result = (|| {
            anyhow::ensure!(
                data.get("user").and_then(Value::as_str) == Some(&self.address),
                "io private message user mismatch"
            );
            anyhow::ensure!(
                data.get("dex").and_then(Value::as_str) == Some("io"),
                "io private message DEX mismatch"
            );
            let raw = data
                .get("clearinghouseState")
                .context("Missing io private clearinghouse state")?;
            let facts = parse_clearinghouse(raw, None, &self.universe)?;
            if let Some(time) = raw.get("time").and_then(Value::as_u64) {
                anyhow::ensure!(
                    fresh(time, received, self.max_age_ms),
                    "io private source time is stale"
                );
            }
            Ok::<_, anyhow::Error>(facts)
        })();
        match result {
            Ok(facts) => {
                self.ws_received_time_ms = Some(received);
                self.ws_source_time_ms = data
                    .get("clearinghouseState")
                    .and_then(|raw| raw.get("time"))
                    .and_then(Value::as_u64);
                let changed_during_refresh = self.refresh_pending
                    && self
                        .ws_facts
                        .as_ref()
                        .is_some_and(|previous| !facts.same_position_sizes(previous));
                if changed_during_refresh {
                    self.invalidate(
                        "io private position sizes changed during HTTP proof refresh",
                        false,
                    );
                } else if self.diagnostic.is_none()
                    && let Some(http) = &self.facts
                    && !facts.matches_positions(http)
                {
                    self.invalidate(
                        "io private position sizes changed; a fresh HTTP proof is required",
                        false,
                    );
                }
                self.ws_facts = Some(facts);
                Ok(())
            }
            Err(e) => {
                self.ws_received_time_ms = None;
                self.ws_source_time_ms = None;
                self.ws_facts = None;
                self.invalidate(&format!("Invalid io private account snapshot: {e}"), false);
                Err(e)
            }
        }
    }
}

/// Refreshes all io facts without consulting default perpetual or spot balances.
pub(crate) async fn refresh_account_scope(
    state: &Arc<Mutex<AccountScopeState>>,
    http: &HyperliquidHttpClient,
    ws: &HyperliquidWebSocketClient,
    emitter: &ExecutionEventEmitter,
) -> anyhow::Result<()> {
    let started = now_ms();
    let epoch = ws.connection_epoch();
    let (address, version, max_age) = {
        let mut guard = state.lock();
        guard.invalidate("io account proof refresh is pending", false);
        guard.refresh_pending = true;
        (guard.address.clone(), guard.version, guard.max_age_ms)
    };
    let result = async {
        let role = http.account_scope_info(&InfoRequest::account_mode(&address, HyperliquidInfoRequestType::UserRole)).await?;
        validate_role(&role)?;
        validate_account_mode(http, &address).await?;
        let meta = http.account_scope_info(&InfoRequest::meta_for_dex("io")).await?;
        let spot = http.account_scope_info(&InfoRequest::spot_meta()).await?;
        let universe = validate_collateral(&meta, &spot)?;
        let raw = http.account_scope_info(&InfoRequest::clearinghouse_state_for_dex(&address, Some("io"))).await?;
        let received = now_ms();
        let source = raw.get("time").and_then(Value::as_u64).context("io HTTP clearinghouse source time is missing")?;
        let facts = parse_clearinghouse(&raw, Some(source), &universe)?;
        // The API has no atomic mode/state snapshot; repeat both mode facts to
        // catch an observed transition, while retaining the bounded limitation.
        validate_account_mode(http, &address).await?;
        let finished = now_ms();
        anyhow::ensure!(fresh(source, finished, max_age) && fresh(started, finished, max_age), "io account proof source or verification interval is stale");
        let snapshot = HyperliquidAccountScopeSnapshot {
            dex: "io".to_string(), address: address.clone(), account_mode: "standard_disabled_inferred".to_string(),
            collateral_token_id: USDC_TOKEN_ID.to_string(), balance: facts.balance, equity: facts.equity,
            withdrawable: facts.withdrawable, used: facts.used, free: facts.free,
            total_maintenance: None,
            positions: facts.positions.clone(), http_source_time_ms: source, http_received_time_ms: received,
            http_verification_started_time_ms: started, private_stream_epoch: epoch, ws_received_time_ms: None, ws_source_time_ms: None,
            trusted: false, flat: None, diagnostic: "Private stream proof is required".to_string(),
            provenance: "Explicit io REST request scope; marginSummary includes isolated positions; disabled-to-standard is an SDK-supported inference; mode/role/collateral requests are not atomic; WS source time is not synthesized".to_string(),
        };
        // Never clamp free or raise equity to fit withdrawable, which is a
        // distinct venue fact rather than the account-balance free component.
        let balance = scoped_balance(facts.equity, facts.used)?;
        let mut info = Params::new();
        info.insert("account_dex".to_string(), Value::String("io".to_string()));
        info.insert("balance_basis".to_string(), Value::String("marginSummary.accountValue; locked=totalMarginUsed; free=equity-used".to_string()));
        info.insert("raw_collateral".to_string(), Value::String(facts.balance.to_string()));
        info.insert("withdrawable".to_string(), Value::String(facts.withdrawable.to_string()));
        info.insert("used_margin".to_string(), Value::String(facts.used.to_string()));
        info.insert("cross_maintenance_margin_used".to_string(), Value::String(facts.cross_maintenance.to_string()));
        info.insert("total_maintenance_margin".to_string(), Value::String("unknown; isolated total is not supplied".to_string()));
        info.insert("money_precision_policy".to_string(), Value::String("USDC Money precision for equity and used; free derived by checked fixed-point subtraction; exact raw facts retained in scope snapshot".to_string()));
        let mut guard = state.lock();
        anyhow::ensure!(guard.version == version && guard.stream_epoch == Some(epoch) && ws.connection_epoch() == epoch && ws.is_active(), "io proof was invalidated while HTTP refresh was pending");
        guard.universe = universe;
        guard.facts = Some(snapshot.clone());
        if let Some(ws) = &guard.ws_facts {
            anyhow::ensure!(ws.positions.iter().all(|position| guard.universe.contains(&position.coin)), "io private position coin is outside the verified universe");
            anyhow::ensure!(ws.matches_positions(&snapshot), "io HTTP/private position-size conflict; flatness is unknown");
        }
        guard.diagnostic = None;
        guard.refresh_pending = false;
        drop(guard);
        emitter.emit_account_state(vec![balance], vec![], true, (source * 1_000_000).into(), Some(info));
        Ok::<_, anyhow::Error>(())
    }.await;
    if let Err(e) = &result {
        state
            .lock()
            .invalidate(&format!("io HTTP account proof failed: {e}"), false);
    }
    result
}

pub(crate) fn now_ms() -> u64 {
    get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000
}

fn fresh(time: u64, now: u64, maximum_age: u64) -> bool {
    time <= now && now - time <= maximum_age
}

fn scoped_balance(equity: Decimal, used: Decimal) -> anyhow::Result<AccountBalance> {
    let currency = Currency::USDC();
    let total = Money::from_decimal(equity, currency)?;
    let locked = Money::from_decimal(used, currency)?;
    let free_raw = total
        .raw
        .checked_sub(locked.raw)
        .context("io free Money arithmetic overflow")?;
    let free = Money::from_raw_checked(free_raw, currency)?;
    AccountBalance::new_checked(total, locked, free).map_err(Into::into)
}

async fn validate_account_mode(http: &HyperliquidHttpClient, address: &str) -> anyhow::Result<()> {
    let mode = http
        .account_scope_info(&InfoRequest::account_mode(
            address,
            HyperliquidInfoRequestType::UserAbstraction,
        ))
        .await?;
    anyhow::ensure!(
        mode.as_str() == Some("disabled"),
        "Unsupported or unknown io account abstraction mode"
    );
    let legacy = http
        .account_scope_info(&InfoRequest::account_mode(
            address,
            HyperliquidInfoRequestType::UserDexAbstraction,
        ))
        .await?;
    anyhow::ensure!(
        legacy.as_bool() == Some(false),
        "Unsupported or inconsistent io DEX abstraction mode"
    );
    Ok(())
}

fn validate_role(role: &Value) -> anyhow::Result<()> {
    match role.get("role").and_then(Value::as_str) {
        Some("user") => Ok(()),
        _ => anyhow::bail!(
            "Unsupported or unknown io account identity; agents, vaults and subaccounts are not supported"
        ),
    }
}

fn validate_collateral(meta: &Value, spot: &Value) -> anyhow::Result<BTreeSet<String>> {
    let index = meta
        .get("collateralToken")
        .and_then(Value::as_u64)
        .context("Missing io collateral token index")?;
    let tokens = spot
        .get("tokens")
        .and_then(Value::as_array)
        .context("Missing collateral token metadata")?;
    let selected = tokens
        .iter()
        .filter(|token| token.get("index").and_then(Value::as_u64) == Some(index))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        selected.len() == 1,
        "Missing or ambiguous io collateral token identity"
    );
    let token = selected[0];
    anyhow::ensure!(
        token.get("name").and_then(Value::as_str) == Some("USDC")
            && token.get("tokenId").and_then(Value::as_str) == Some(USDC_TOKEN_ID)
            && token.get("isCanonical").and_then(Value::as_bool) == Some(true),
        "Unsupported io collateral token identity"
    );
    let rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .context("Missing io asset universe")?;
    let mut universe = BTreeSet::new();
    for row in rows {
        let coin = row
            .get("name")
            .and_then(Value::as_str)
            .context("Missing io asset identity")?;
        anyhow::ensure!(
            valid_io_coin(coin) && universe.insert(coin.to_string()),
            "Invalid or duplicate io asset identity"
        );
    }
    anyhow::ensure!(
        !universe.is_empty(),
        "Empty io asset universe cannot prove account scope"
    );
    Ok(universe)
}

#[derive(Clone, Debug)]
struct ScopedClearinghouse {
    balance: Decimal,
    equity: Decimal,
    withdrawable: Decimal,
    used: Decimal,
    free: Decimal,
    cross_maintenance: Decimal,
    positions: Vec<HyperliquidAccountScopePosition>,
}

impl ScopedClearinghouse {
    fn same_position_sizes(&self, other: &Self) -> bool {
        self.positions
            .iter()
            .filter(|position| !position.signed_size.is_zero())
            .map(|position| (&position.coin, position.signed_size))
            .eq(other
                .positions
                .iter()
                .filter(|position| !position.signed_size.is_zero())
                .map(|position| (&position.coin, position.signed_size)))
    }
    fn matches_positions(&self, snapshot: &HyperliquidAccountScopeSnapshot) -> bool {
        self.positions
            .iter()
            .filter(|position| !position.signed_size.is_zero())
            .map(|position| (&position.coin, position.signed_size))
            .eq(snapshot
                .positions
                .iter()
                .filter(|position| !position.signed_size.is_zero())
                .map(|position| (&position.coin, position.signed_size)))
    }
}

fn parse_clearinghouse(
    raw: &Value,
    source: Option<u64>,
    universe: &BTreeSet<String>,
) -> anyhow::Result<ScopedClearinghouse> {
    let margin = raw
        .get("marginSummary")
        .context("Missing io marginSummary including isolated positions")?;
    let cross = raw
        .get("crossMarginSummary")
        .context("Missing io crossMarginSummary")?;
    for summary in [margin, cross] {
        for field in [
            "accountValue",
            "totalNtlPos",
            "totalRawUsd",
            "totalMarginUsed",
        ] {
            exact_decimal(summary, field)?;
        }
    }
    if source.is_some() {
        anyhow::ensure!(
            raw.get("time").and_then(Value::as_u64) == source,
            "Invalid io HTTP source time"
        );
    } else if let Some(time) = raw.get("time") {
        anyhow::ensure!(time.as_u64().is_some(), "Invalid io private source time");
    }
    let used = exact_decimal(margin, "totalMarginUsed")?;
    let equity = exact_decimal(margin, "accountValue")?;
    let withdrawable = exact_decimal(raw, "withdrawable")?;
    let cross_maintenance = exact_decimal(raw, "crossMaintenanceMarginUsed")?;
    anyhow::ensure!(
        used >= Decimal::ZERO
            && withdrawable >= Decimal::ZERO
            && cross_maintenance >= Decimal::ZERO,
        "Negative io margin or withdrawable fact"
    );
    let rows = raw
        .get("assetPositions")
        .and_then(Value::as_array)
        .context("Missing complete io assetPositions")?;
    let mut coins = BTreeSet::new();
    let mut positions = Vec::new();
    for row in rows {
        anyhow::ensure!(
            row.get("type").and_then(Value::as_str) == Some("oneWay"),
            "Unknown io position type"
        );
        let position = row.get("position").context("Missing io position payload")?;
        let coin = position
            .get("coin")
            .and_then(Value::as_str)
            .context("Missing io position coin")?;
        anyhow::ensure!(
            valid_io_coin(coin)
                && coins.insert(coin.to_string())
                && (universe.is_empty() || universe.contains(coin)),
            "Wrong, unknown or duplicate io position coin"
        );
        let leverage = position
            .get("leverage")
            .context("Missing io position leverage")?;
        anyhow::ensure!(
            leverage.get("type").and_then(Value::as_str) == Some("isolated")
                && leverage
                    .get("value")
                    .and_then(Value::as_u64)
                    .is_some_and(|value| value > 0),
            "Unsupported io position margin mode"
        );
        let margin_used = exact_decimal(position, "marginUsed")?;
        anyhow::ensure!(margin_used >= Decimal::ZERO, "Negative io position margin");
        positions.push(HyperliquidAccountScopePosition {
            coin: coin.to_string(),
            signed_size: exact_decimal(position, "szi")?,
            margin_used,
        });
    }
    positions.sort_by(|left, right| left.coin.cmp(&right.coin));
    let free = equity
        .checked_sub(used)
        .context("io free-equity arithmetic overflow")?;
    Ok(ScopedClearinghouse {
        balance: exact_decimal(margin, "totalRawUsd")?,
        equity,
        withdrawable,
        used,
        free,
        cross_maintenance,
        positions,
    })
}

fn exact_decimal(raw: &Value, field: &str) -> anyhow::Result<Decimal> {
    let value = raw
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("Missing exact io decimal {field}"))?;
    Decimal::from_str_exact(value).with_context(|| format!("Invalid exact io decimal {field}"))
}

fn valid_io_coin(coin: &str) -> bool {
    coin.strip_prefix("io:").is_some_and(|name| {
        !name.is_empty()
            && name.len() <= 40
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn raw() -> Value {
        let summary = json!({"accountValue":"5.125001", "totalNtlPos":"0", "totalRawUsd":"4.75", "totalMarginUsed":"9.25"});
        json!({"marginSummary":summary, "crossMarginSummary":summary,
            "crossMaintenanceMarginUsed":"0", "withdrawable":"2", "assetPositions":[], "time":1000})
    }

    fn proof() -> HyperliquidAccountScopeSnapshot {
        let facts = parse_clearinghouse(&raw(), Some(1000), &BTreeSet::new()).unwrap();
        HyperliquidAccountScopeSnapshot {
            dex: "io".into(),
            address: "user".into(),
            account_mode: "standard_disabled_inferred".into(),
            collateral_token_id: USDC_TOKEN_ID.into(),
            balance: facts.balance,
            equity: facts.equity,
            withdrawable: facts.withdrawable,
            used: facts.used,
            free: facts.free,
            total_maintenance: None,
            positions: facts.positions,
            http_source_time_ms: 1000,
            http_received_time_ms: 1000,
            http_verification_started_time_ms: 1000,
            private_stream_epoch: 7,
            ws_received_time_ms: None,
            ws_source_time_ms: None,
            trusted: false,
            flat: None,
            diagnostic: String::new(),
            provenance: String::new(),
        }
    }

    fn ready_state() -> AccountScopeState {
        let mut state = AccountScopeState::new("user".into(), 300);
        state.bind_stream(7);
        state.facts = Some(proof());
        state.diagnostic = None;
        state.acknowledgements = ALL_PRIVATE_ACKS;
        state.ws_received_time_ms = Some(1000);
        state
    }

    #[rstest]
    #[case("marginSummary")]
    #[case("crossMarginSummary")]
    #[case("crossMaintenanceMarginUsed")]
    #[case("withdrawable")]
    #[case("assetPositions")]
    #[case("time")]
    fn incomplete_http_is_rejected(#[case] field: &str) {
        let mut value = raw();
        value.as_object_mut().unwrap().remove(field);
        assert!(parse_clearinghouse(&value, Some(1000), &BTreeSet::new()).is_err());
    }

    #[rstest]
    fn exact_negative_free_and_equity_are_retained_without_money_clamping() {
        let facts = parse_clearinghouse(&raw(), Some(1000), &BTreeSet::new()).unwrap();
        assert_eq!(facts.free.to_string(), "-4.124999");
        let balance = scoped_balance(facts.equity, facts.used).unwrap();
        assert_eq!(balance.free.as_decimal(), facts.free);
        assert_eq!(balance.locked.as_decimal(), facts.used);
        let balance = scoped_balance(Decimal::from(-5), Decimal::from(9)).unwrap();
        assert_eq!(balance.total.as_decimal(), Decimal::from(-5));
        assert_eq!(balance.free.as_decimal(), Decimal::from(-14));
    }

    #[rstest]
    fn missing_facts_never_mean_flat() {
        assert!(
            AccountScopeState::new("user".into(), 300)
                .snapshot(1000, true, 7)
                .is_none()
        );
    }

    #[rstest]
    fn epoch_ack_transport_and_age_are_independent_trust_requirements() {
        let mut state = ready_state();
        assert_eq!(state.snapshot(1000, true, 7).unwrap().flat, Some(true));
        for (time, active, epoch) in [
            (1000, false, 7),
            (1000, true, 8),
            (1301, true, 7),
            (999, true, 7),
        ] {
            let snapshot = state.snapshot(time, active, epoch).unwrap();
            assert!(!snapshot.trusted);
            assert_eq!(snapshot.flat, None);
        }
        state.acknowledgements = 3;
        assert!(!state.snapshot(1000, true, 7).unwrap().trusted);
        state.bind_stream(8);
        assert_eq!(state.acknowledgements, 0);
        assert_eq!(state.snapshot(1000, true, 8).unwrap().flat, None);
    }

    #[rstest]
    fn explicit_ws_source_expires_even_when_receive_is_fresh() {
        let mut state = ready_state();
        state.ws_source_time_ms = Some(701);
        assert!(state.snapshot(1000, true, 7).unwrap().trusted);
        let snapshot = state.snapshot(1002, true, 7).unwrap();
        assert!(!snapshot.trusted);
        assert_eq!(snapshot.flat, None);
        assert_eq!(snapshot.ws_source_time_ms, Some(701));
        assert_eq!(snapshot.ws_received_time_ms, Some(1000));
        state.ws_source_time_ms = None;
        assert!(state.snapshot(1002, true, 7).unwrap().trusted);
    }

    #[rstest]
    fn ws_source_time_stays_unknown_and_pnl_changes_do_not_invalidate_rest() {
        let mut state = ready_state();
        let mut value = raw();
        value.as_object_mut().unwrap().remove("time");
        value["marginSummary"]["accountValue"] = json!("10.75");
        state
            .observe_ws(
                &json!({"user":"user","dex":"io","clearinghouseState":value}),
                1001,
            )
            .unwrap();
        let snapshot = state.snapshot(1001, true, 7).unwrap();
        assert!(snapshot.trusted);
        assert_eq!(snapshot.ws_source_time_ms, None);
        assert_eq!(snapshot.equity.to_string(), "5.125001");
    }

    #[rstest]
    #[case("other", "io")]
    #[case("user", "")]
    #[case("user", "xyz")]
    fn incorrect_ws_identity_or_dex_invalidates(#[case] user: &str, #[case] dex: &str) {
        let mut state = ready_state();
        assert!(
            state
                .observe_ws(
                    &json!({"user":user,"dex":dex,"clearinghouseState":raw()}),
                    1000
                )
                .is_err()
        );
        assert_eq!(state.snapshot(1000, true, 7).unwrap().flat, None);
    }

    #[rstest]
    fn changed_position_size_invalidates_and_malformed_position_cannot_be_flat() {
        let mut state = ready_state();
        let mut value = raw();
        value["assetPositions"] = json!([{"type":"oneWay","position":{"coin":"io:SNDK","szi":"1","marginUsed":"2","leverage":{"type":"isolated","value":2}}}]);
        state
            .observe_ws(
                &json!({"user":"user","dex":"io","clearinghouseState":value}),
                1000,
            )
            .unwrap();
        assert_eq!(state.snapshot(1000, true, 7).unwrap().flat, None);
        value["assetPositions"][0]["position"]["coin"] = json!("BTC");
        assert!(parse_clearinghouse(&value, Some(1000), &BTreeSet::new()).is_err());
        value["assetPositions"][0]["position"]["coin"] = json!("io:UNKNOWN");
        assert!(
            parse_clearinghouse(&value, Some(1000), &BTreeSet::from(["io:SNDK".to_string()]))
                .is_err()
        );
    }

    #[rstest]
    #[case("agent")]
    #[case("vault")]
    #[case("subAccount")]
    #[case("unknown")]
    fn only_direct_user_identity_is_supported(#[case] role: &str) {
        assert!(validate_role(&json!({"role":role,"data":{"master":"user"}})).is_err());
        assert!(validate_role(&json!({"role":"user"})).is_ok());
    }

    #[rstest]
    fn collateral_uses_explicit_token_index_and_exact_identity() {
        let meta = json!({"collateralToken":42,"universe":[{"name":"io:SNDK"}]});
        let mut spot = json!({"tokens":[{"index":9,"name":"OTHER"},{"index":42,"name":"USDC","tokenId":USDC_TOKEN_ID,"isCanonical":true}]});
        assert!(validate_collateral(&meta, &spot).is_ok());
        spot["tokens"][1]["tokenId"] = json!("fake");
        assert!(validate_collateral(&meta, &spot).is_err());
        spot["tokens"][1]["tokenId"] = json!(USDC_TOKEN_ID);
        let duplicate = spot["tokens"][1].clone();
        spot["tokens"].as_array_mut().unwrap().push(duplicate);
        assert!(validate_collateral(&meta, &spot).is_err());
    }

    #[rstest]
    fn snapshot_json_decimals_are_exact_strings_and_no_source_time_is_invented() {
        let state = ready_state();
        let snapshot = state.snapshot(1000, true, 7).unwrap();
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["equity"], "5.125001");
        assert_eq!(value["free"], "-4.124999");
        assert!(value["ws_source_time_ms"].is_null());
        assert!(value["total_maintenance"].is_null());
    }
}
