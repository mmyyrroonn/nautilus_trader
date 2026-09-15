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

//! The `GET /v1/markets` response schema and the single boundary that turns it into domain types.
//!
//! The data transfer objects here model the wire payload as it is described in
//! `crates/adapters/ondo/test_data/README.md`: every price, quantity and fee arrives as a
//! decimal string and stays one until it is converted. The same per-market fields describe a
//! WebSocket market record, so precision is derived once, here, and shared by both transports.
//!
//! # Fail-closed rules
//!
//! Market metadata decides tick size, lot size and tradability, so a missing or unusable value
//! fails the load instead of being skipped or guessed:
//!
//! - an absent `result` member or an empty market list is an error, never an empty success;
//! - a requested instrument that the payload does not carry is an error;
//! - a requested instrument whose price or quantity step is missing, zero or negative is an
//!   error;
//! - a requested instrument whose raw status cannot be classified is an error. A *known*
//!   `disabled` market is returned with its status and fee provenance, so callers can gate new
//!   orders on [`MarketInfo::is_tradable`]. A market whose payload carries neither a `status`
//!   string nor a `disabled` flag is **not** unclassifiable: on this venue that shape is how an
//!   enabled market is expressed (61 of the 81 markets in the 2026-09-14 capture, both P1 targets
//!   included), and it resolves to Active with the absence recorded in the status source. See
//!   [`crate::common::enums::MarketStatusInfo::resolve`].
//!
//! Which check runs for which market matters, because [`MarketsResponse::market_infos`] normalises
//! every trading pair while [`parse_instruments`] builds only the markets it was asked for:
//!
//! - fatal for *any* pair, requested or not: a market string that cannot be mapped to an
//!   instrument, and a present increment or fee that is not a decimal. These mean the payload is
//!   not the schema this module reads, so no market in it can be trusted;
//! - fatal only for a *selected* market: a step that is zero, negative or not representable as a
//!   [`Price`]/[`Quantity`], and a status that cannot be classified. An unselected market
//!   contributes nothing to the returned instruments, so it does not fail the load.

use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Price, Quantity, currency::Currency},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    common::{
        consts::ONDO_CONTRACT_MULTIPLIER,
        enums::{FeeRate, MarketFees, MarketStatus, MarketStatusInfo, MarketStatusSource},
        parse::{
            market_to_instrument_id, parse_decimal, parse_price_increment,
            parse_quantity_increment, split_market,
        },
    },
    http::{
        error::{OndoHttpError, OndoHttpResult},
        query::MARKETS_PATH,
    },
};

/// A `GET /v1/markets` response envelope (`GenericResponse`).
///
/// Unknown members are ignored, which is what lets the P0 synthetic fixture carry its
/// `_fixture` provenance block in the same document.
#[derive(Clone, Debug, Deserialize)]
pub struct MarketsResponse {
    /// The envelope's success flag.
    #[serde(default)]
    pub success: Option<bool>,
    /// The response payload. Absent means the request did not return market data.
    #[serde(default)]
    pub result: Option<MarketsResult>,
}

/// The `result` member of a `GET /v1/markets` response (`MarketsResult`).
///
/// The 2026-09-14 production capture carries `result.spot`, `result.perps` and `result.tokenConfig`
/// (`reports/ondo-acceptance/20260914T142730Z-p1-build/rest-capture/markets.json`, 81 trading pairs;
/// the tree's `test_data/rest/markets_observed_20260914.json` is the 4-pair excerpt of the same
/// response and carries `result.perps` alone). `spot` is not modelled at all: it is neither perps
/// metadata nor spot-token configuration, so it is ignored like any other member this schema does
/// not read.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketsResult {
    /// Perpetuals market metadata.
    #[serde(default)]
    pub perps: Option<PerpsMarkets>,
    /// Spot token configuration.
    ///
    /// This is spot-token configuration and must never be read as perps metadata
    /// (`test_data/README.md`). It is modelled only so the response shape is complete and the
    /// suppression of that misreading is visible.
    #[serde(default)]
    pub token_config: Vec<TokenConfig>,
}

/// The `perps` member of a `MarketsResult`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpsMarkets {
    /// The perps trading pairs.
    #[serde(default)]
    pub trading_pairs: Vec<PerpsTradingPair>,
}

/// One perps trading pair (`PerpsTradingPair`).
///
/// Every discrete value stays a decimal string as the venue sent it. A member that is absent is
/// [`None`] rather than a default, because absence and zero carry different meanings here.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpsTradingPair {
    /// The venue's market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The quantity step in base units. Never the price step.
    #[serde(default)]
    pub base_increment: Option<String>,
    /// The price step in quote units. Never the quantity step.
    #[serde(default)]
    pub quote_increment: Option<String>,
    /// The raw market status string.
    ///
    /// The official `PerpsTradingPair` schema declares no status field, and the 2026-09-14
    /// production capture confirms the venue sends none (0 of 81 markets,
    /// `test_data/rest/markets_observed_20260914.json`). The member stays modelled so the
    /// allow-list path is exercised and so a status string appearing on the wire is classified
    /// rather than ignored; its value set is otherwise unverified.
    ///
    /// A member that is present but is not a string (including an explicit JSON `null`) fails the
    /// decode closed instead of being read as an absent member; see
    /// [`deserialize_present_string`].
    #[serde(default, deserialize_with = "deserialize_present_string")]
    pub status: Option<String>,
    /// The `disabled` flag, documented on `Contract`.
    ///
    /// Observed on `/v1/markets` in the 2026-09-14 capture: present and `true` on the 20 disabled
    /// markets, absent on the other 61 (including both P1 targets). Absence is the venue's way of
    /// saying enabled; see [`MarketStatusInfo::resolve`]. A member that is present but is not a
    /// boolean (including an explicit JSON `null`) fails the decode closed; see
    /// [`deserialize_present_bool`].
    #[serde(default, deserialize_with = "deserialize_present_bool")]
    pub disabled: Option<bool>,
    /// Whether the *underlying* market is closed, documented on `Contract`.
    ///
    /// This describes underlying trading hours, not the perp's tradability, and is never merged
    /// with the market status.
    #[serde(default)]
    pub is_closed: Option<bool>,
    /// The maker fee rate as a decimal fraction of filled notional.
    #[serde(default)]
    pub maker_fee: Option<String>,
    /// The taker fee rate as a decimal fraction of filled notional.
    #[serde(default)]
    pub taker_fee: Option<String>,
}

/// Spot token configuration (`TokenConfig`).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenConfig {
    /// The token symbol.
    pub id: String,
    /// The token name.
    #[serde(default)]
    pub name: Option<String>,
    /// The token's display decimals.
    #[serde(default)]
    pub decimals: Option<u32>,
    /// The token's on-chain decimals.
    #[serde(default)]
    pub token_decimals: Option<u32>,
}

/// Deserialises a *present* `status` member as a string, or fails the decode.
///
/// `#[serde(default)]` covers a member that is missing from the payload, so this function only runs
/// for a member the payload actually carried: a JSON `null` or a number is then a marker this adapter
/// cannot classify, and it fails the whole load closed (as `OndoHttpError::Decode`) instead of being
/// silently read as an absent member -- absence is a *classified* state on this venue
/// ([`MarketStatusInfo::resolve`], rule 4 of the 2026-09-14 status ruling). The member is read as a
/// [`serde_json::Value`] first so the failure names the offending field and shows what arrived.
fn deserialize_present_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(value) => Ok(Some(value)),
        other => Err(serde::de::Error::custom(format!(
            "`status` was present but is not a string, was `{other}`"
        ))),
    }
}

/// Deserialises a *present* `disabled` member as a boolean, or fails the decode.
///
/// Same rule as [`deserialize_present_string`]: a missing member keeps its meaning (this venue omits
/// `disabled` on enabled markets), while a member that is present but not a boolean -- including an
/// explicit JSON `null` -- cannot be classified and fails the load closed rather than being read as
/// "enabled".
fn deserialize_present_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Bool(value) => Ok(Some(value)),
        other => Err(serde::de::Error::custom(format!(
            "`disabled` was present but is not a boolean, was `{other}`"
        ))),
    }
}

/// Parses a `GET /v1/markets` body.
///
/// The trading pairs are read from `result.perps.tradingPairs`. The plan writes the path as
/// `perps.tradingPairs` while the official spec example nests it under `result`
/// (`test_data/conflicts.md` gap 6); the 2026-09-14 production capture settled this in favour of the
/// spec's nesting (`success` + `result.perps.tradingPairs`, `rest/markets_observed_20260914.json`).
/// This function implements the spec's path and fails closed when `result` is absent, so a payload
/// in the other shape is reported rather than silently read as empty metadata.
///
/// # Errors
///
/// Returns an error if the body is not valid JSON for the envelope, if the envelope reports
/// failure, if `result` is absent, or if the response carries no perps trading pairs at all.
/// An empty market list is a start-up failure, never an empty success.
pub fn parse_markets(payload: &str) -> OndoHttpResult<MarketsResponse> {
    let response = serde_json::from_str::<MarketsResponse>(payload)
        .map_err(|e| OndoHttpError::Decode(e.to_string()))?;

    if response.success == Some(false) {
        return Err(OndoHttpError::Unsuccessful);
    }

    if response.result.is_none() {
        return Err(OndoHttpError::MissingField {
            context: format!("{MARKETS_PATH} response"),
            field: "result",
        });
    }

    if response.trading_pairs().is_empty() {
        return Err(OndoHttpError::EmptyResult);
    }

    Ok(response)
}

impl MarketsResponse {
    /// Returns the perps trading pairs carried by the response.
    ///
    /// [`parse_markets`] has already rejected a response without perps market data, so this is
    /// empty only for a response value a caller built itself.
    #[must_use]
    pub fn trading_pairs(&self) -> &[PerpsTradingPair] {
        self.result
            .as_ref()
            .and_then(|result| result.perps.as_ref())
            .map_or(&[], |perps| perps.trading_pairs.as_slice())
    }

    /// Returns the spot token configuration carried by the response.
    ///
    /// This is spot configuration; it is never a source of perps market metadata.
    #[must_use]
    pub fn token_config(&self) -> &[TokenConfig] {
        self.result
            .as_ref()
            .map_or(&[], |result| result.token_config.as_slice())
    }

    /// Normalises every perps trading pair in the response.
    ///
    /// `fetched_at` is the local time the response was received. It is stamped onto each fee
    /// rate's provenance, so a metadata fee can always be told apart from an account-specific
    /// rate and from an undated documentation assumption.
    ///
    /// # Errors
    ///
    /// Returns an error if a market string cannot be mapped to an instrument id, or if an
    /// increment or fee is present but not a decimal.
    pub fn market_infos(&self, fetched_at: UnixNanos) -> OndoHttpResult<Vec<MarketInfo>> {
        self.trading_pairs()
            .iter()
            .map(|pair| market_info_from_pair(pair, fetched_at))
            .collect()
    }
}

/// Normalised metadata for one Ondo Perps market.
///
/// The fields a later layer needs to act on are exposed as typed values with their provenance,
/// so nothing has to re-read the payload or re-derive precision.
#[derive(Clone, Debug)]
pub struct MarketInfo {
    market: String,
    instrument_id: InstrumentId,
    base_token: String,
    status: MarketStatusInfo,
    fees: MarketFees,
    base_increment: Option<Decimal>,
    quote_increment: Option<Decimal>,
    underlying_closed: Option<bool>,
}

impl MarketInfo {
    /// Returns the venue's market string, as in `NVDA-USD.P`.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the Nautilus instrument id this market maps to.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the base token the market is denominated in.
    #[must_use]
    pub fn base_token(&self) -> &str {
        &self.base_token
    }

    /// Returns the classified market status.
    #[must_use]
    pub const fn status(&self) -> MarketStatus {
        self.status.status()
    }

    /// Returns the market status with the evidence it was classified from.
    #[must_use]
    pub const fn status_info(&self) -> &MarketStatusInfo {
        &self.status
    }

    /// Returns the maker and taker fee rates with their provenance.
    #[must_use]
    pub const fn fees(&self) -> &MarketFees {
        &self.fees
    }

    /// Returns the quantity step in base units, when the payload carried one.
    #[must_use]
    pub const fn base_increment(&self) -> Option<Decimal> {
        self.base_increment
    }

    /// Returns the price step in quote units, when the payload carried one.
    #[must_use]
    pub const fn quote_increment(&self) -> Option<Decimal> {
        self.quote_increment
    }

    /// Returns whether the underlying market is closed, when the payload reported it.
    ///
    /// This is not the perp's tradability; see [`Self::status`].
    #[must_use]
    pub const fn underlying_closed(&self) -> Option<bool> {
        self.underlying_closed
    }

    /// Returns whether new orders may be sent for this market.
    #[must_use]
    pub const fn is_tradable(&self) -> bool {
        self.status.is_tradable()
    }

    /// Converts the venue's increments into the price step and quantity step.
    ///
    /// # Errors
    ///
    /// Returns an error when either step is absent, zero, or negative, or when it cannot be
    /// represented as a Nautilus [`Price`] or [`Quantity`].
    pub fn try_increments(&self) -> OndoHttpResult<(Price, Quantity)> {
        let context = format!("market `{}`", self.market);

        let quote = self
            .quote_increment
            .ok_or_else(|| OndoHttpError::MissingField {
                context: context.clone(),
                field: "quoteIncrement",
            })?;
        let base = self
            .base_increment
            .ok_or_else(|| OndoHttpError::MissingField {
                context: context.clone(),
                field: "baseIncrement",
            })?;

        let price_increment =
            parse_price_increment(&quote.to_string(), "quoteIncrement").map_err(|e| {
                OndoHttpError::InvalidField {
                    context: context.clone(),
                    field: "quoteIncrement",
                    value: quote.to_string(),
                    reason: e.to_string(),
                }
            })?;
        let size_increment =
            parse_quantity_increment(&base.to_string(), "baseIncrement").map_err(|e| {
                OndoHttpError::InvalidField {
                    context,
                    field: "baseIncrement",
                    value: base.to_string(),
                    reason: e.to_string(),
                }
            })?;

        Ok((price_increment, size_increment))
    }

    /// Builds the Nautilus instrument for this market.
    ///
    /// The products are linear, quoted in USD, settled in USDC, with quantity denominated in the
    /// base token and a contract multiplier of [`ONDO_CONTRACT_MULTIPLIER`]. Ondo's market
    /// metadata supplies no venue timestamp, so `ts_event` falls back to `ts_init`, the local
    /// time the payload was received.
    ///
    /// A market whose status is [`MarketStatus::Unknown`] is never built: with no tradability to
    /// gate on it is not an instrument, it is an unresolved reading of the payload. A *known*
    /// `disabled` market is built normally, and callers gate it on [`Self::is_tradable`]. This is
    /// the same rule [`parse_instruments`] applies for a whole load, enforced here as well because
    /// this is the per-market entry point later layers call.
    ///
    /// Maker and taker fees are attached only when the payload carried them. Both the domain
    /// type's default and this rule mean an absent fee must never be read as a zero fee; use
    /// [`Self::fees`] to tell the two apart.
    ///
    /// # Errors
    ///
    /// Returns an error if the market's status cannot be classified
    /// (the same [`OndoHttpError::UnknownMarketStatus`] that [`parse_instruments`] raises), if the
    /// increments are missing or unusable, or if the instrument fails construction.
    pub fn instrument(&self, ts_init: UnixNanos) -> OndoHttpResult<InstrumentAny> {
        if self.status.status() == MarketStatus::Unknown {
            return Err(unknown_status_error(self));
        }

        let (price_increment, size_increment) = self.try_increments()?;

        // The venue's own market string stays as the raw symbol, so a round trip back to a
        // subscription does not go through the Nautilus product marker.
        let raw_symbol =
            Symbol::new_checked(&self.market).map_err(|e| OndoHttpError::UnsupportedMarket {
                market: self.market.clone(),
                reason: e.to_string(),
            })?;

        let instrument = CryptoPerpetual::builder()
            .instrument_id(self.instrument_id)
            .raw_symbol(raw_symbol)
            .base_currency(Currency::get_or_create_crypto(&self.base_token))
            .quote_currency(Currency::USD())
            .settlement_currency(Currency::USDC())
            .is_inverse(false)
            .price_precision(price_increment.precision)
            .size_precision(size_increment.precision)
            .price_increment(price_increment)
            .size_increment(size_increment)
            .multiplier(Quantity::from(ONDO_CONTRACT_MULTIPLIER))
            .maybe_maker_fee(self.fees.maker().map(FeeRate::rate))
            .maybe_taker_fee(self.fees.taker().map(FeeRate::rate))
            .ts_event(ts_init)
            .ts_init(ts_init)
            .build()
            .map_err(|e| OndoHttpError::InvalidField {
                context: format!("market `{}`", self.market),
                field: "instrument",
                value: self.instrument_id.to_string(),
                reason: e.to_string(),
            })?;

        Ok(InstrumentAny::CryptoPerpetual(instrument))
    }
}

/// Parses a `GET /v1/markets` body into Nautilus instruments.
///
/// The returned instruments follow the order of the response. When `load_ids` is empty every
/// market in the response is loaded, because a non-empty `load_ids` is what narrows the
/// published set.
///
/// The maker and taker fees attached to those instruments are the *public metadata* reading of
/// the payload (`FeeSource::PublicMetadata`, see [`MarketFees`]), stamped with the endpoint and
/// the local time they were read. They are never the authenticated account's actual rate, and an
/// absent rate is never a zero; read the account-specific rate from its own endpoint.
///
/// # Errors
///
/// Fails closed, as a start-up failure rather than a silent skip, when the payload cannot be
/// decoded, carries no market data, does not carry a requested instrument, cannot map a market
/// string, carries a missing or non-positive increment for a requested instrument, or reports a
/// status for a requested instrument that cannot be classified.
pub fn parse_instruments(
    payload: &str,
    load_ids: &[InstrumentId],
    ts_init: UnixNanos,
) -> OndoHttpResult<Vec<InstrumentAny>> {
    let infos = parse_markets(payload)?.market_infos(ts_init)?;

    for load_id in load_ids {
        if !infos.iter().any(|info| info.instrument_id() == *load_id) {
            return Err(OndoHttpError::MissingInstrument {
                instrument_id: *load_id,
            });
        }
    }

    let selected = infos
        .iter()
        .filter(|info| load_ids.is_empty() || load_ids.contains(&info.instrument_id()));

    let mut instruments = Vec::new();
    for info in selected {
        // An unclassifiable status cannot gate new orders, so a requested market reporting one
        // is a start-up failure. A known `disabled` market is still published, with its status
        // and fee provenance attached, for callers to gate on `is_tradable`.
        if info.status() == MarketStatus::Unknown {
            return Err(unknown_status_error(info));
        }
        instruments.push(info.instrument(ts_init)?);
    }

    Ok(instruments)
}

/// Builds the fail-closed error for a market whose status cannot be classified.
///
/// Since the 2026-09-14 observation the only status evidence that can leave a status `Unknown` is a
/// `status` *string* the allow-list does not know (`MarketStatusInfo::resolve`), so the error's `raw`
/// member is that verbatim string. The match stays total so a future classification source cannot
/// panic here; an absent pair of fields is no longer unknown -- it is this venue's "enabled".
fn unknown_status_error(info: &MarketInfo) -> OndoHttpError {
    let status = info.status_info();
    let raw = match status.source() {
        MarketStatusSource::StatusString => status.raw().unwrap_or_default().to_string(),
        other => format!("<no verbatim status string; resolved from {other:?}>"),
    };

    OndoHttpError::UnknownMarketStatus {
        market: info.market().to_string(),
        raw,
    }
}

fn market_info_from_pair(
    pair: &PerpsTradingPair,
    fetched_at: UnixNanos,
) -> OndoHttpResult<MarketInfo> {
    let market = pair.market.as_str();
    let (base_token, _quote_token) =
        split_market(market).map_err(|e| OndoHttpError::UnsupportedMarket {
            market: market.to_string(),
            reason: e.to_string(),
        })?;

    let instrument_id =
        market_to_instrument_id(market).map_err(|e| OndoHttpError::UnsupportedMarket {
            market: market.to_string(),
            reason: e.to_string(),
        })?;

    let base_increment = parse_increment(pair.base_increment.as_deref(), market, "baseIncrement")?;
    let quote_increment =
        parse_increment(pair.quote_increment.as_deref(), market, "quoteIncrement")?;

    Ok(MarketInfo {
        market: market.to_string(),
        instrument_id,
        base_token: base_token.to_string(),
        status: MarketStatusInfo::resolve(pair.status.as_deref(), pair.disabled),
        fees: MarketFees::new(
            fee_rate(pair, pair.maker_fee.as_deref(), "makerFee", fetched_at)?,
            fee_rate(pair, pair.taker_fee.as_deref(), "takerFee", fetched_at)?,
        ),
        base_increment,
        quote_increment,
        underlying_closed: pair.is_closed,
    })
}

fn parse_increment(
    raw: Option<&str>,
    market: &str,
    field: &'static str,
) -> OndoHttpResult<Option<Decimal>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    parse_decimal(raw, field)
        .map(Some)
        .map_err(|e| OndoHttpError::InvalidField {
            context: format!("market `{market}`"),
            field,
            value: raw.to_string(),
            reason: e.to_string(),
        })
}

/// Reads one fee rate from a trading pair.
///
/// `makerFee` and `takerFee` are also declared on `Contract` (`GET /v1/perps/contracts`), so where
/// the venue means the rates to live was open when this schema was authored: the P0 synthetic
/// fixture carries them on the pair, and the endpoint that would have settled it answered HTTP 403
/// during the freeze. **RESOLVED BY OBSERVATION (2026-09-14)**: the observed `GET /v1/markets`
/// excerpt (`test_data/rest/markets_observed_20260914.json`) carries both members on **4 of 4**
/// trading pairs, at the same two rates, so reading them from the pair is what the venue serves;
/// the `Contract` declaration stays the documented alternative rather than the source used here.
/// Whatever rate is read is recorded with the field, the endpoint and the local time it was read,
/// so no later task can mistake it for an account-specific rate or for a zero.
fn fee_rate(
    pair: &PerpsTradingPair,
    raw: Option<&str>,
    field: &'static str,
    fetched_at: UnixNanos,
) -> OndoHttpResult<Option<FeeRate>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let rate = parse_decimal(raw, field).map_err(|e| OndoHttpError::InvalidField {
        context: format!("market `{}`", pair.market),
        field,
        value: raw.to_string(),
        reason: e.to_string(),
    })?;

    Ok(Some(FeeRate::public_metadata(
        rate,
        field,
        MARKETS_PATH,
        fetched_at,
    )))
}
