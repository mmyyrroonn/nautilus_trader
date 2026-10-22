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

//! Ed25519 typed-payload signing described by <https://docs.arcus.xyz/api-reference/authentication>.

use std::{collections::BTreeMap, fmt::Debug};

use anyhow::Context;
use ed25519_dalek::{Signer, SigningKey};
use nautilus_core::{UnixNanos, datetime::add_n_months_nanos, hex};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::models::{
    ArcusCancelRequest, ArcusMarket, ArcusModifyRequest, ArcusOrderRequest, ArcusOrderSide,
    ArcusOrderType, ArcusTimeInForce,
};

/// Holds a reusable signing key without exposing secret material in Debug output.
pub struct ArcusSigner {
    key: SigningKey,
    api_key: String,
}

impl Debug for ArcusSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ArcusSigner))
            .field("api_key", &self.api_key)
            .finish_non_exhaustive()
    }
}

impl ArcusSigner {
    /// Constructs a signer from a 32-byte hexadecimal Ed25519 seed.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed hexadecimal input or a seed of a different length.
    pub fn from_seed_hex(seed: &str) -> anyhow::Result<Self> {
        let bytes = Zeroizing::new(hex::decode(seed).context("Invalid Arcus signing seed hex")?);
        let seed: &[u8; 32] = bytes
            .as_slice()
            .try_into()
            .context("Arcus signing seed must be 32 bytes")?;
        let key = SigningKey::from_bytes(seed);
        let api_key = hex::encode(key.verifying_key().to_bytes());
        Ok(Self { key, api_key })
    }

    /// Returns the lowercase hexadecimal Ed25519 public key.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Signs a validated typed order payload without an action prefix.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, expiry, grid alignment, or market limits.
    pub fn sign_order(
        &self,
        order: &ArcusOrderRequest,
        market: &ArcusMarket,
        timestamp: u64,
    ) -> anyhow::Result<String> {
        let payload = order_signing_payload(order, market, timestamp)?;
        Ok(hex::encode(self.key.sign(payload.as_bytes()).to_bytes()))
    }

    /// Signs a typed cancel payload identifying exactly one target.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, timestamp, or cancellation target.
    pub fn sign_cancel(
        &self,
        order: &ArcusCancelRequest,
        timestamp: u64,
    ) -> anyhow::Result<String> {
        let payload = cancel_signing_payload(order, timestamp)?;
        Ok(hex::encode(self.key.sign(payload.as_bytes()).to_bytes()))
    }

    /// Signs an exact operation-three modify payload for a confirmed server order ID.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid immutable attributes, identity, expiry, or increments.
    pub fn sign_modify(
        &self,
        order: &ArcusModifyRequest,
        market: &ArcusMarket,
        timestamp: u64,
    ) -> anyhow::Result<String> {
        let payload = modify_signing_payload(order, market, timestamp)?;
        Ok(hex::encode(self.key.sign(payload.as_bytes()).to_bytes()))
    }

    /// Signs the concatenated timestamp, action, and recursively sorted JSON body.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied body cannot be serialized.
    pub fn sign_legacy<T: Serialize>(
        &self,
        action: &str,
        body: &T,
        timestamp: u64,
    ) -> anyhow::Result<String> {
        let value = serde_json::to_value(body)?;
        let canonical = canonical_json(value)?;
        let message = format!("{timestamp}{action}{canonical}");
        Ok(hex::encode(self.key.sign(message.as_bytes()).to_bytes()))
    }
}

fn canonical_json(value: Value) -> anyhow::Result<String> {
    fn sorted(value: Value) -> Value {
        match value {
            Value::Object(obj) => Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, sorted(v)))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
            other => other,
        }
    }
    Ok(serde_json::to_string(&sorted(value))?)
}

/// Validates the master Ethereum address and subaccount index.
///
/// # Errors
///
/// Returns an error for invalid hexadecimal syntax or a subaccount outside zero through nine.
pub fn validate_address(address: &str, account_index: u8) -> anyhow::Result<()> {
    anyhow::ensure!(
        account_index <= 9,
        "Arcus account index must be 0 through 9"
    );
    anyhow::ensure!(
        address.len() == 42
            && address.as_bytes()[0] == b'0'
            && matches!(address.as_bytes()[1], b'x' | b'X')
            && address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit),
        "Invalid Arcus Ethereum address"
    );
    Ok(())
}

/// Validates the case-sensitive client identifier accepted by Arcus.
///
/// # Errors
///
/// Returns an error for invalid length or characters outside the documented ASCII alphabet.
pub fn validate_client_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !id.is_empty()
            && id.len() <= 36
            && id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "Arcus client ID must contain 1 through 36 ASCII letters, digits, hyphens or underscores"
    );
    Ok(())
}

fn units(value: Decimal, unit: Decimal) -> anyhow::Result<u64> {
    anyhow::ensure!(
        unit > Decimal::ZERO && value > Decimal::ZERO,
        "Arcus price and quantity increments must be positive"
    );
    anyhow::ensure!(
        value % unit == Decimal::ZERO,
        "Arcus price or quantity is not an exact increment"
    );
    value
        .checked_div(unit)
        .context("Arcus integer conversion overflow")?
        .to_u64()
        .context("Arcus integer price or quantity exceeds u64")
}

/// Builds the exact sorted integer payload; no action or timestamp prefix is added.
///
/// # Errors
///
/// Returns an error for invalid order attributes, inexact integer conversion, or overflow.
pub fn order_signing_payload(
    order: &ArcusOrderRequest,
    market: &ArcusMarket,
    timestamp: u64,
) -> anyhow::Result<String> {
    let minimum = timestamp / 1_000 + 32 * 86_400 * 1_000_000;
    let mut payload = typed_order_fields(order, market, timestamp, minimum)?;

    if let Some(id) = &order.client_id {
        validate_client_id(id)?;
        payload.insert("c", json!(id));
    }
    Ok(serde_json::to_string(&payload)?)
}

fn typed_order_fields(
    order: &ArcusOrderRequest,
    market: &ArcusMarket,
    timestamp: u64,
    minimum_expiry_us: u64,
) -> anyhow::Result<BTreeMap<&'static str, Value>> {
    validate_address(&order.address, order.account_index)?;
    anyhow::ensure!(
        order.timestamp == timestamp,
        "Arcus order timestamp must match signing timestamp"
    );
    anyhow::ensure!(
        order.market_id == market.market_id,
        "Arcus order market metadata mismatch"
    );
    anyhow::ensure!(
        order.quantity >= market.min_order_size && order.quantity <= market.max_order_size,
        "Arcus order quantity exceeds market limits"
    );
    anyhow::ensure!(
        order.reduce_only
            || order
                .quantity
                .checked_mul(order.price)
                .context("Arcus notional overflow")?
                >= market.min_order_notional,
        "Arcus order notional below market minimum"
    );
    let price_ticks = units(order.price, market.tick_size)?;
    let quantity_units = units(order.quantity, market.step_size)?;

    if let Some(tier) = market
        .tick_tiers
        .iter()
        .find(|tier| tier.up_to_price.is_none_or(|upper| order.price < upper))
    {
        units(order.price, tier.tick)?;
    }
    anyhow::ensure!(
        order.time_in_force != ArcusTimeInForce::Gtc,
        "Arcus submissions use GTT rather than GTC"
    );

    if order.order_type == ArcusOrderType::Market {
        anyhow::ensure!(
            order.time_in_force == ArcusTimeInForce::Ioc,
            "Arcus MARKET orders require IOC"
        );
        anyhow::ensure!(
            market.mark_price > Decimal::ZERO
                && (order.price - market.mark_price).abs() <= market.mark_price / Decimal::TEN,
            "Arcus MARKET protective price exceeds ten percent of mark"
        );
    }
    let expiry: u64 = order
        .good_til_time
        .parse()
        .context("Invalid Arcus goodTilTime")?;
    anyhow::ensure!(
        expiry >= minimum_expiry_us,
        "Arcus goodTilTime is earlier than the required future expiry"
    );
    let expiry_ns = expiry
        .checked_mul(1_000)
        .context("Arcus goodTilTime overflows nanoseconds")?;
    let payload = BTreeMap::from([
        ("ad", json!(order.address.to_ascii_lowercase())),
        ("ai", json!(order.account_index)),
        ("ct", json!(timestamp)),
        ("g", json!(expiry_ns)),
        ("m", json!(order.market_id)),
        ("op", json!(1)),
        ("p", json!(price_ticks)),
        ("q", json!(quantity_units)),
        ("r", json!(u8::from(order.reduce_only))),
        (
            "s",
            json!(u8::from(order.order_side == ArcusOrderSide::Sell)),
        ),
        (
            "t",
            json!(match order.time_in_force {
                ArcusTimeInForce::Gtt => 0,
                ArcusTimeInForce::Fok => 1,
                ArcusTimeInForce::Ioc => 2,
                ArcusTimeInForce::Alo => 3,
                ArcusTimeInForce::Gtc => unreachable!(),
            }),
        ),
        ("v", json!(1)),
    ]);

    Ok(payload)
}

/// Builds the canonical operation-three payload without a client-ID alias or action prefix.
///
/// # Errors
///
/// Returns an error for a non-hex server order ID, stale expiry, invalid nonce, or market limits.
pub fn modify_signing_payload(
    order: &ArcusModifyRequest,
    market: &ArcusMarket,
    timestamp: u64,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        !order.order_id.is_empty() && order.order_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Arcus modify requires a hexadecimal server order ID"
    );
    anyhow::ensure!(
        order.client_time.parse::<u64>()? == timestamp,
        "Arcus modify nonce must match signing timestamp"
    );
    let minimum = add_n_months_nanos(UnixNanos::from(timestamp), 1)?
        .as_u64()
        .div_ceil(1_000);
    let base = ArcusOrderRequest {
        address: order.address.clone(),
        account_index: order.account_index,
        market_id: order.market_id,
        order_side: order.side,
        order_type: ArcusOrderType::Limit,
        time_in_force: order.time_in_force,
        quantity: order.quantity,
        price: order.price,
        good_til_time: order.good_til_time.clone(),
        reduce_only: order.reduce_only,
        client_id: None,
        timestamp,
    };
    let mut payload = typed_order_fields(&base, market, timestamp, minimum)?;
    payload.insert("op", json!(3));
    payload.insert("id", json!(order.order_id));
    Ok(serde_json::to_string(&payload)?)
}

/// Builds a typed cancel payload, preserving the target identifier's exact spelling.
///
/// # Errors
///
/// Returns an error for invalid account identity, timestamp, or target selection.
pub fn cancel_signing_payload(
    order: &ArcusCancelRequest,
    timestamp: u64,
) -> anyhow::Result<String> {
    validate_address(&order.address, order.account_index)?;
    anyhow::ensure!(
        order.timestamp == timestamp,
        "Arcus cancel timestamp must match signing timestamp"
    );
    let mut payload = BTreeMap::from([
        ("ad", json!(order.address.to_ascii_lowercase())),
        ("ai", json!(order.account_index)),
        ("ct", json!(timestamp)),
        ("m", json!(order.market_id)),
        ("op", json!(2)),
        ("v", json!(1)),
    ]);

    match (order.kind.as_str(), &order.order_id, &order.client_id) {
        ("orderId", Some(id), None) if !id.is_empty() => {
            payload.insert("id", json!(id));
        }
        ("clientId", None, Some(id)) => {
            validate_client_id(id)?;
            payload.insert("c", json!(id));
        }
        _ => anyhow::bail!("Arcus cancel requires exactly one target matching kind"),
    }
    Ok(serde_json::to_string(&payload)?)
}
