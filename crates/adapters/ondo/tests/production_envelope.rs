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

use nautilus_ondo::production::OndoExecutionEnvelopeConfig;
use rstest::rstest;

fn envelope() -> OndoExecutionEnvelopeConfig {
    serde_json::from_value(serde_json::json!({
        "instrument_id":"NVDA-USD-PERP.ONDO", "entry_side":"buy",
        "entry_max_quantity":"0.1", "entry_worst_price":"150", "entry_max_notional_usd":"15",
        "close_side":"sell", "close_max_quantity":"0.1", "close_worst_price":"149",
        "max_close_attempts":2, "max_notional_per_order_usd":"50", "max_gross_exposure_usd":"100","min_available_margin_usdc":"25",
        "max_orders":3, "max_new_risk_requests":1, "max_app_requests":6,
        "entry_deadline_unix_nanos":200000000000_u64, "cleanup_deadline_unix_nanos":300000000000_u64,
        "require_flat_start":true
    })).unwrap()
}

#[rstest]
fn exact_approved_envelope_validates() {
    assert!(envelope().validate(100000000000).is_ok());
}

fn btc_envelope(side: &str) -> OndoExecutionEnvelopeConfig {
    let mut raw = serde_json::to_value(envelope()).unwrap();
    raw["instrument_id"] = serde_json::json!("BTC-USD-PERP.ONDO");
    raw["entry_side"] = serde_json::json!(side);
    raw["close_side"] = serde_json::json!(if side == "buy" { "sell" } else { "buy" });
    raw["entry_max_quantity"] = serde_json::json!("0.0002");
    raw["close_max_quantity"] = serde_json::json!("0.0002");
    raw["entry_worst_price"] = serde_json::json!(if side == "buy" { "50100" } else { "49900" });
    raw["close_worst_price"] = serde_json::json!(if side == "buy" { "49900" } else { "50100" });
    raw["max_notional_per_order_usd"] = serde_json::json!("20");
    raw["max_gross_exposure_usd"] = serde_json::json!("20");
    serde_json::from_value(raw).unwrap()
}

#[rstest]
#[case("buy")]
#[case("sell")]
fn bounded_btc_envelope_validates(#[case] side: &str) {
    assert!(btc_envelope(side).validate(100000000000).is_ok());
}

#[rstest]
#[case("ETH-USD-PERP.ONDO")]
#[case("BTC-USD-PERP.ASTER")]
#[case("BTC-USDC-PERP.ONDO")]
#[case("BTC-USD.P.ONDO")]
fn btc_permission_does_not_allow_other_instruments(#[case] instrument: &str) {
    let mut raw = serde_json::to_value(btc_envelope("buy")).unwrap();
    raw["instrument_id"] = serde_json::json!(instrument);
    let config: OndoExecutionEnvelopeConfig = serde_json::from_value(raw).unwrap();
    assert!(config.validate(100000000000).is_err());
}

#[rstest]
#[case("entry_max_notional_usd", serde_json::json!("21"))]
#[case("max_notional_per_order_usd", serde_json::json!("51"))]
#[case("max_gross_exposure_usd", serde_json::json!("101"))]
#[case("min_available_margin_usdc", serde_json::json!("24"))]
#[case("close_max_quantity", serde_json::json!("0.0003"))]
#[case("max_new_risk_requests", serde_json::json!(2))]
#[case("max_app_requests", serde_json::json!(7))]
#[case("require_flat_start", serde_json::json!(false))]
#[case("entry_deadline_unix_nanos", serde_json::json!(100000000000_u64))]
#[case("cleanup_deadline_unix_nanos", serde_json::json!(800000000000_u64))]
fn btc_envelope_retains_hard_limits(#[case] field: &str, #[case] value: serde_json::Value) {
    let mut raw = serde_json::to_value(btc_envelope("buy")).unwrap();
    raw[field] = value;
    let config: OndoExecutionEnvelopeConfig = serde_json::from_value(raw).unwrap();
    assert!(config.validate(100000000000).is_err());
}

#[rstest]
#[case("entry_max_notional_usd", "21")]
#[case("max_notional_per_order_usd", "50.000000000000000001")]
#[case("max_gross_exposure_usd", "100.000000000000000001")]
#[case("entry_max_quantity", "0")]
#[case("entry_worst_price", "0")]
#[case("close_max_quantity", "0.2")]
#[case("close_side", "buy")]
#[case("entry_side", "BUY")]
fn invalid_envelope_is_not_clamped(#[case] field: &str, #[case] value: &str) {
    let mut raw = serde_json::to_value(envelope()).unwrap();
    raw[field] = serde_json::json!(value);
    let parsed = serde_json::from_value::<OndoExecutionEnvelopeConfig>(raw);
    assert!(parsed.is_err() || parsed.unwrap().validate(100000000000).is_err());
}

#[rstest]
fn expired_or_unbounded_envelope_is_refused() {
    assert!(envelope().validate(200000000000).is_err());
    let mut e = envelope();
    e.cleanup_deadline_unix_nanos = 800000000000;
    assert!(e.validate(100000000000).is_err());
}

#[rstest]
fn raw_production_transport_cannot_manufacture_a_run_authority() {
    use std::sync::Arc;

    use nautilus_ondo::{
        common::{
            credential::OndoCredential,
            enums::{OndoAuthenticationScope, OndoEnvironment},
        },
        http::client::OndoHttpClient,
    };
    let credential = OndoCredential::new(
        OndoEnvironment::Production,
        "ondoKeyId_UNIT_TEST_ONLY".into(),
        "ondoApiSecret_UNIT_TEST_ONLY".into(),
    )
    .unwrap();
    let result = OndoHttpClient::builder()
        .base_url("http://127.0.0.1:1".into())
        .credential(Arc::new(credential))
        .authentication_scope(OndoAuthenticationScope::ProductionTrading)
        .build();
    assert!(result.is_err());
}

#[rstest]
fn precision_beyond_exact_decimal_capacity_is_refused_not_rounded() {
    let mut raw = serde_json::to_value(envelope()).unwrap();
    raw["entry_worst_price"] = serde_json::json!("150.0000000000000000000000000000000001");
    assert!(serde_json::from_value::<OndoExecutionEnvelopeConfig>(raw).is_err());
}

#[rstest]
#[case(6, true)]
#[case(7, false)]
#[case(12, false)]
fn app_request_ceiling_cannot_exceed_six(#[case] requests: u32, #[case] valid: bool) {
    let mut config = envelope();
    config.max_app_requests = requests;
    assert_eq!(config.validate(100000000000).is_ok(), valid);
}

#[rstest]
#[case("1", false)]
#[case("24.999999999999999999", false)]
#[case("25", true)]
#[case("25.000000000000000001", true)]
fn usdc_margin_floor_is_exact_and_cannot_be_lowered(#[case] minimum: &str, #[case] valid: bool) {
    let mut config = envelope();
    config.min_available_margin_usdc = rust_decimal::Decimal::from_str_exact(minimum).unwrap();
    assert_eq!(config.validate(100000000000).is_ok(), valid);
}

/// An opening quantity the approved closing attempts cannot cover in full leaves a
/// position this envelope cannot legally clean up, so it is refused before any order.
#[rstest]
fn close_capacity_shortfall_is_refused_before_entry() {
    let mut config = envelope();
    config.close_max_quantity = rust_decimal::Decimal::from_str_exact("0.01").unwrap();
    assert_eq!(config.max_close_attempts, 2);
    assert_eq!(config.max_orders, 3);
    assert!(config.validate(100000000000).is_err());
}

/// The closing quantity is only sufficient across the attempts one opening order
/// actually leaves: three orders with two attempts leave two, and two orders leave one.
#[rstest]
fn close_capacity_counts_only_attempts_the_envelope_can_send() {
    let mut two = envelope();
    two.close_max_quantity = rust_decimal::Decimal::from_str_exact("0.05").unwrap();
    assert_eq!(two.validate(100000000000), Ok(()));

    let mut one = two.clone();
    one.max_orders = 2;
    assert!(one.validate(100000000000).is_err());

    let mut single_attempt = envelope();
    single_attempt.close_max_quantity = rust_decimal::Decimal::from_str_exact("0.1").unwrap();
    single_attempt.max_close_attempts = 1;
    single_attempt.max_orders = 2;
    assert_eq!(single_attempt.validate(100000000000), Ok(()));
}

/// A closing order that cannot even be sent at the approved directional bound fits no
/// ceiling, whatever the venue price does later.
#[rstest]
fn close_notional_at_the_directional_bound_must_fit_the_order_ceiling() {
    let mut fitting = envelope();
    fitting.close_worst_price = rust_decimal::Decimal::from_str_exact("149").unwrap();
    assert_eq!(fitting.validate(100000000000), Ok(()));

    let mut oversized = envelope();
    oversized.close_worst_price = rust_decimal::Decimal::from_str_exact("600").unwrap();
    assert!(oversized.validate(100000000000).is_err());
}

/// The gross ceiling is its own refusal: a close that fits the per-order ceiling but not
/// the gross one is still structurally unsendable.
#[rstest]
fn close_notional_must_fit_the_gross_ceiling_too() {
    let mut config = envelope();
    config.entry_worst_price = rust_decimal::Decimal::from_str_exact("100").unwrap();
    config.entry_max_notional_usd = rust_decimal::Decimal::from_str_exact("10").unwrap();
    config.max_gross_exposure_usd = rust_decimal::Decimal::from_str_exact("10").unwrap();
    config.close_worst_price = rust_decimal::Decimal::from_str_exact("110").unwrap();

    // close 0.1 x 110 = 11, which is above the gross ceiling and below the per-order one.
    assert!(
        config
            .close_max_quantity
            .checked_mul(config.close_worst_price)
            .unwrap()
            > config.max_gross_exposure_usd
    );
    assert!(
        config
            .close_max_quantity
            .checked_mul(config.close_worst_price)
            .unwrap()
            <= config.max_notional_per_order_usd
    );
    assert!(config.validate(100000000000).is_err());
}

/// The capacity and ceiling checks hold for a short entry that closes by buying.
#[rstest]
fn short_entry_envelopes_are_checked_in_the_close_direction_too() {
    let mut config = envelope();
    config.entry_side = "sell".into();
    config.close_side = "buy".into();
    config.entry_worst_price = rust_decimal::Decimal::from_str_exact("149").unwrap();
    config.close_worst_price = rust_decimal::Decimal::from_str_exact("150").unwrap();
    assert_eq!(config.validate(100000000000), Ok(()));

    let mut shortfall = config.clone();
    shortfall.close_max_quantity = rust_decimal::Decimal::from_str_exact("0.01").unwrap();
    assert!(shortfall.validate(100000000000).is_err());
}

/// Coverage that overflows exact decimal arithmetic is refused, not clamped into a
/// quantity the envelope never approved.
#[rstest]
fn close_capacity_overflow_is_refused_rather_than_clamped() {
    let mut config = envelope();
    config.entry_max_quantity =
        rust_decimal::Decimal::from_str_exact("40000000000000000000000000000").unwrap();
    config.entry_worst_price =
        rust_decimal::Decimal::from_str_exact("0.0000000000000000000000000003").unwrap();
    config.close_max_quantity = config.entry_max_quantity;
    config.close_worst_price =
        rust_decimal::Decimal::from_str_exact("0.0000000000000000000000000001").unwrap();
    assert!(config.validate(100000000000).is_err());
}
