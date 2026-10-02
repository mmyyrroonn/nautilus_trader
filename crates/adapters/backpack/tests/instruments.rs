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

//! Public metadata observations and explicitly marked synthetic adverse inputs.

use std::str::FromStr;

use nautilus_backpack::{
    config::BackpackConfig,
    instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics},
    models::BackpackMarket,
    parsing::{
        BackpackInstrumentError, basis_points_to_ratio, parse_market, unix_microseconds_to_nanos,
    },
    provider::BackpackInstrumentProvider,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::Instrument,
    types::{Currency, fixed::FIXED_PRECISION},
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

const BTC: &str = include_str!("../test_data/btc_usdc_perp.json");
const SOL: &str = include_str!("../test_data/sol_usdc_perp.json");

fn decimal(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn config(symbols: &[&str]) -> BackpackConfig {
    BackpackConfig::new_checked(symbols.iter().map(|s| (*s).to_string()).collect()).unwrap()
}

fn fixture(body: &str) -> BackpackMarket {
    serde_json::from_str(body).unwrap()
}

fn synthetic_economics() -> BackpackInstrumentEconomics {
    BackpackInstrumentEconomics::new_checked(
        decimal("0.1"),
        decimal("0.05"),
        decimal("-0.00001"),
        decimal("0.0002"),
        BackpackEconomicsSource::Synthetic,
        "offline framework test; not venue or account facts".to_string(),
    )
    .unwrap()
}

#[rstest]
#[case(BTC, "BTC", "0.1", "0.00001", "1000000", 1, 5)]
#[case(SOL, "SOL", "0.01", "0.01", "1000", 2, 2)]
fn test_distinct_official_observations_and_every_instrument_field(
    #[case] body: &str,
    #[case] base: &str,
    #[case] tick: &str,
    #[case] step: &str,
    #[case] max_price: &str,
    #[case] price_precision: u8,
    #[case] size_precision: u8,
) {
    let market = fixture(body);
    let symbol = format!("{base}_USDC_PERP");
    let receipt = UnixNanos::from(1_800_000_000_123_456_789);
    let metadata = parse_market(&market, &config(&[&symbol]), receipt).unwrap();
    let instrument = metadata
        .to_instrument(Some(&synthetic_economics()))
        .unwrap();
    assert_eq!(
        metadata.instrument_id.to_string(),
        format!("{symbol}.BACKPACK")
    );
    assert_eq!(metadata.raw_symbol.as_str(), symbol);
    assert_eq!(metadata.base_currency.code.as_str(), base);
    assert_eq!(metadata.quote_currency, Currency::USDC());
    assert_eq!(metadata.price_increment.as_decimal(), decimal(tick));
    assert_eq!(metadata.size_increment.as_decimal(), decimal(step));
    assert_eq!(metadata.min_quantity.as_decimal(), decimal(step));
    assert_eq!(metadata.max_quantity, None);
    assert_eq!(metadata.min_price.as_decimal(), decimal(tick));
    assert_eq!(metadata.max_price.unwrap().as_decimal(), decimal(max_price));
    assert_eq!(metadata.funding_interval_ms, Some(3_600_000));
    assert_eq!(metadata.funding_rate_lower_bound_bps, Some(decimal("-100")));
    assert_eq!(metadata.funding_rate_upper_bound_bps, Some(decimal("100")));
    assert_eq!(metadata.received_at, receipt);
    assert_eq!(
        metadata.raw_metadata,
        serde_json::from_str::<Value>(body).unwrap()
    );
    assert_eq!(instrument.id, metadata.instrument_id);
    assert_eq!(instrument.raw_symbol, metadata.raw_symbol);
    assert_eq!(instrument.base_currency, metadata.base_currency);
    assert_eq!(instrument.quote_currency, Currency::USDC());
    assert_eq!(instrument.settlement_currency, Currency::USDC());
    assert!(!instrument.is_inverse);
    assert_eq!(instrument.price_precision, price_precision);
    assert_eq!(instrument.size_precision, size_precision);
    assert_eq!(instrument.price_increment, metadata.price_increment);
    assert_eq!(instrument.size_increment, metadata.size_increment);
    assert_eq!(instrument.multiplier.as_decimal(), Decimal::ONE);
    assert_eq!(instrument.lot_size, metadata.size_increment);
    assert_eq!(instrument.min_quantity, Some(metadata.min_quantity));
    assert_eq!(instrument.max_quantity, None);
    assert_eq!(instrument.min_price, Some(metadata.min_price));
    assert_eq!(instrument.max_price, metadata.max_price);
    assert_eq!(instrument.min_notional, None);
    assert_eq!(instrument.max_notional, None);
    assert_eq!(instrument.tick_scheme, None);
    assert_eq!(instrument.margin_init, decimal("0.1"));
    assert_eq!(instrument.margin_maint, decimal("0.05"));
    assert_eq!(instrument.maker_fee, decimal("-0.00001"));
    assert_eq!(instrument.taker_fee, decimal("0.0002"));
    assert_eq!(instrument.ts_event, UnixNanos::from(0));
    assert_eq!(instrument.ts_init, receipt);
    let info = instrument.info.as_ref().unwrap();
    assert_eq!(info["backpack_execution_ready"], false);
    assert_eq!(info["backpack_metadata"], metadata.raw_metadata);
    assert_eq!(info["backpack_economics"]["source"], "Synthetic");
    assert_eq!(
        info["backpack_economics"]["reference"],
        "offline framework test; not venue or account facts"
    );
    assert_eq!(info["backpack_economics"]["event_timestamp"], "unknown");
    assert_eq!(
        info["backpack_economics"]["initialization_timestamp"],
        "caller_receipt"
    );
    assert_eq!(instrument.into_any().id(), metadata.instrument_id);
}

#[rstest]
#[case(BTC)]
#[case(SOL)]
fn test_official_response_round_trip(#[case] body: &str) {
    let market = fixture(body);
    assert_eq!(
        serde_json::to_value(market).unwrap(),
        serde_json::from_str::<Value>(body).unwrap()
    );
}

#[rstest]
fn test_unknown_numeric_and_nested_metadata_are_preserved_exactly() {
    let mut value: Value = serde_json::from_str(BTC).unwrap();
    let numeric: Value = serde_json::from_str("0.12345678901234567890123456789").unwrap();
    value["futureNumericField"] = numeric;
    value["filters"]["futureFilter"] =
        json!({"opaque": [null, "future", 18446744073709551615_u64]});
    let market: BackpackMarket = serde_json::from_value(value.clone()).unwrap();
    let metadata = parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(7)).unwrap();
    assert_eq!(metadata.raw_metadata, value);
    assert_eq!(
        metadata.raw_metadata["futureNumericField"].to_string(),
        "0.12345678901234567890123456789"
    );
}

#[rstest]
#[case("SPOT")]
#[case("IPERP")]
#[case("DATED")]
#[case("PREDICTION")]
#[case("RFQ")]
#[case("FUTURE_UNKNOWN")]
fn test_synthetic_unsupported_market_types_are_not_inferred_from_suffix(#[case] market_type: &str) {
    let mut market = fixture(BTC);
    market.market_type = market_type.to_string();
    assert!(matches!(
        parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)),
        Err(BackpackInstrumentError::Config(_))
    ));
}

#[rstest]
#[case("Closed")]
#[case("CancelOnly")]
#[case("LimitOnly")]
#[case("PostOnly")]
#[case("UNKNOWN")]
fn test_synthetic_non_open_markets_are_refused(#[case] state: &str) {
    let mut market = fixture(BTC);
    market.order_book_state = state.to_string();
    assert!(matches!(
        parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)),
        Err(BackpackInstrumentError::InactiveMarket { .. })
    ));
}

#[rstest]
fn test_synthetic_visibility_identity_quote_and_allowlist_failures() {
    let mut market = fixture(BTC);
    let selected = config(&["BTC_USDC_PERP"]);
    market.visible = false;
    assert!(matches!(
        parse_market(&market, &selected, UnixNanos::from(0)),
        Err(BackpackInstrumentError::InactiveMarket { .. })
    ));
    market.visible = true;
    market.base_symbol = "SOL".to_string();
    assert!(matches!(
        parse_market(&market, &selected, UnixNanos::from(0)),
        Err(BackpackInstrumentError::InconsistentIdentity)
    ));
    market.base_symbol = "BTC".to_string();
    market.quote_symbol = "USDT".to_string();
    assert!(matches!(
        parse_market(&market, &selected, UnixNanos::from(0)),
        Err(BackpackInstrumentError::Config(_))
    ));
    market.quote_symbol = "USDC".to_string();
    assert!(matches!(
        parse_market(&market, &config(&["SOL_USDC_PERP"]), UnixNanos::from(0)),
        Err(BackpackInstrumentError::Config(_))
    ));
}

#[rstest]
#[case("/symbol")]
#[case("/baseSymbol")]
#[case("/quoteSymbol")]
#[case("/marketType")]
#[case("/orderBookState")]
#[case("/visible")]
#[case("/createdAt")]
#[case("/filters/price/tickSize")]
#[case("/filters/price/minPrice")]
#[case("/filters/quantity/stepSize")]
#[case("/filters/quantity/minQuantity")]
fn test_synthetic_missing_required_fields_are_not_defaulted(#[case] path: &str) {
    let mut value: Value = serde_json::from_str(BTC).unwrap();
    let (parent, key) = path.rsplit_once('/').unwrap();
    value
        .pointer_mut(parent)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove(key);
    assert!(serde_json::from_value::<BackpackMarket>(value).is_err());
}

#[rstest]
#[case("0")]
#[case("-0.1")]
#[case("NaN")]
#[case("1e-5")]
#[case(" 0.1")]
#[case("0.1 ")]
#[case("+0.1")]
#[case(".1")]
#[case("1.")]
#[case("79228162514264337593543950336")]
#[case("0.00000000000000000000000000001")]
fn test_synthetic_invalid_price_increment(#[case] value: &str) {
    let mut market = fixture(BTC);
    market.filters.price.tick_size = value.to_string();
    assert!(parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).is_err());
}

#[rstest]
fn test_synthetic_numeric_decimal_field_is_refused() {
    let mut value: Value = serde_json::from_str(BTC).unwrap();
    value["filters"]["price"]["tickSize"] = json!(0.1);
    assert!(serde_json::from_value::<BackpackMarket>(value).is_err());
}

#[rstest]
#[case("0", None)]
#[case("-1", None)]
#[case("0.000015", None)]
#[case("0.00002", Some("0.00001"))]
#[case("0.00001", Some("0.000015"))]
#[case("0.00001", Some("0"))]
fn test_synthetic_invalid_quantity_bounds(#[case] minimum: &str, #[case] maximum: Option<&str>) {
    let mut market = fixture(BTC);
    market.filters.quantity.min_quantity = minimum.to_string();
    market.filters.quantity.max_quantity = maximum.map(str::to_string);
    assert!(parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).is_err());
}

#[rstest]
fn test_synthetic_nullable_bounds_and_unknown_funding_remain_unknown() {
    let mut market = fixture(BTC);
    market.filters.price.max_price = None;
    market.funding_interval = None;
    market.funding_rate_lower_bound = None;
    market.funding_rate_upper_bound = None;
    let metadata = parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).unwrap();
    assert_eq!(metadata.max_quantity, None);
    assert_eq!(metadata.max_price, None);
    assert_eq!(metadata.funding_interval_ms, None);
    assert_eq!(metadata.funding_rate_lower_bound_bps, None);
    assert_eq!(metadata.funding_rate_upper_bound_bps, None);
    assert!(matches!(
        metadata.to_instrument(None),
        Err(BackpackInstrumentError::MissingEconomics)
    ));
}

#[rstest]
fn test_synthetic_finite_max_quantity_and_exact_increment_precision() {
    let mut market = fixture(BTC);
    market.filters.quantity.max_quantity = Some("12345.67890".to_string());
    market.filters.price.tick_size = "0.1000".to_string();
    let metadata = parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).unwrap();
    assert_eq!(metadata.price_increment.precision, 1);
    assert_eq!(
        metadata.max_quantity.unwrap().as_decimal(),
        decimal("12345.67890")
    );
    assert_eq!(metadata.max_quantity.unwrap().precision, 5);
}

#[rstest]
fn test_synthetic_precision_boundary() {
    let mut market = fixture(BTC);
    let smallest = format!("0.{}1", "0".repeat(usize::from(FIXED_PRECISION) - 1));
    market.filters.quantity.step_size = smallest.clone();
    market.filters.quantity.min_quantity = smallest;
    let metadata = parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).unwrap();
    assert_eq!(metadata.size_increment.precision, FIXED_PRECISION);
    assert_ne!(metadata.size_increment.as_decimal(), Decimal::ZERO);
    market.filters.quantity.step_size = format!("0.{}1", "0".repeat(usize::from(FIXED_PRECISION)));
    market.filters.quantity.min_quantity = market.filters.quantity.step_size.clone();
    assert!(parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).is_err());
}

#[rstest]
fn test_synthetic_funding_units_signs_and_validation() {
    assert_eq!(
        basis_points_to_ratio(decimal("-100")).unwrap(),
        decimal("-0.01")
    );
    assert_eq!(
        basis_points_to_ratio(decimal("12.3456")).unwrap(),
        decimal("0.00123456")
    );
    assert!(basis_points_to_ratio(decimal("0.0000000000000000000000000001")).is_err());
    let mut market = fixture(BTC);
    market.funding_interval = Some(0);
    assert!(parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).is_err());
    market.funding_interval = Some(3_600_000);
    market.funding_rate_lower_bound = Some("101".to_string());
    assert!(parse_market(&market, &config(&["BTC_USDC_PERP"]), UnixNanos::from(0)).is_err());
}

#[rstest]
fn test_documented_microsecond_conversion_and_overflow() {
    assert_eq!(
        unix_microseconds_to_nanos(1_800_000_000_123_456).unwrap(),
        UnixNanos::from(1_800_000_000_123_456_000)
    );
    assert_eq!(unix_microseconds_to_nanos(0).unwrap(), UnixNanos::from(0));
    assert!(unix_microseconds_to_nanos(u64::MAX).is_err());
}

#[rstest]
#[case(BackpackEconomicsSource::Configured, "Configured")]
#[case(BackpackEconomicsSource::VenueObserved, "VenueObserved")]
#[case(BackpackEconomicsSource::Synthetic, "Synthetic")]
fn test_all_economic_provenance_classes_remain_not_execution_ready(
    #[case] source: BackpackEconomicsSource,
    #[case] name: &str,
) {
    let metadata = parse_market(
        &fixture(BTC),
        &config(&["BTC_USDC_PERP"]),
        UnixNanos::from(0),
    )
    .unwrap();
    let economics = BackpackInstrumentEconomics::new_checked(
        decimal("0.1"),
        decimal("0.05"),
        Decimal::ZERO,
        decimal("0.0002"),
        source,
        "explicit source record".to_string(),
    )
    .unwrap();
    let instrument = metadata.to_instrument(Some(&economics)).unwrap();
    let info = instrument.info.unwrap();
    assert_eq!(info["backpack_execution_ready"], false);
    assert_eq!(info["backpack_economics"]["source"], name);
}

#[rstest]
fn test_invalid_economic_inputs_and_unknown_currency() {
    assert!(
        BackpackInstrumentEconomics::new_checked(
            decimal("0.01"),
            decimal("0.1"),
            Decimal::ZERO,
            Decimal::ZERO,
            BackpackEconomicsSource::Synthetic,
            "test".to_string()
        )
        .is_err()
    );
    assert!(
        BackpackInstrumentEconomics::new_checked(
            decimal("0.1"),
            decimal("0.05"),
            Decimal::ZERO,
            Decimal::ZERO,
            BackpackEconomicsSource::Synthetic,
            String::new()
        )
        .is_err()
    );
    let mut market = fixture(BTC);
    market.symbol = "UNREGISTEREDB1_USDC_PERP".to_string();
    market.base_symbol = "UNREGISTEREDB1".to_string();
    assert!(matches!(
        parse_market(
            &market,
            &config(&["UNREGISTEREDB1_USDC_PERP"]),
            UnixNanos::from(0)
        ),
        Err(BackpackInstrumentError::UnknownCurrency(_))
    ));
}

#[rstest]
fn test_provider_filters_unlisted_markets_and_refreshes_latest_metadata() {
    let mut provider = BackpackInstrumentProvider::new(config(&["BTC_USDC_PERP", "SOL_USDC_PERP"]));
    let mut unlisted = fixture(BTC);
    unlisted.symbol = "UNLISTED_USDC".to_string();
    unlisted.market_type = "SPOT".to_string();
    provider
        .replace_markets(&[fixture(BTC), fixture(SOL), unlisted], UnixNanos::from(12))
        .unwrap();
    assert_eq!(provider.all().count(), 2);
    let id = InstrumentId::from_str("BTC_USDC_PERP.BACKPACK").unwrap();
    assert_eq!(provider.get(&id).unwrap().received_at, UnixNanos::from(12));
    let mut fresh = fixture(BTC);
    fresh.filters.price.tick_size = "0.01".to_string();
    provider
        .replace_markets(&[fresh, fixture(SOL)], UnixNanos::from(13))
        .unwrap();
    assert_eq!(provider.get(&id).unwrap().received_at, UnixNanos::from(13));
    assert_eq!(
        provider.get(&id).unwrap().price_increment.as_decimal(),
        decimal("0.01")
    );
    let other_venue = InstrumentId::from_str("BTC_USDC_PERP.OTHER").unwrap();
    assert!(provider.get(&other_venue).is_none());
}

#[rstest]
#[case("missing")]
#[case("duplicate")]
#[case("invalid")]
fn test_synthetic_failed_refresh_invalidates_old_snapshot(#[case] failure: &str) {
    let mut provider = BackpackInstrumentProvider::new(config(&["BTC_USDC_PERP", "SOL_USDC_PERP"]));
    provider
        .replace_markets(&[fixture(BTC), fixture(SOL)], UnixNanos::from(1))
        .unwrap();
    let markets = match failure {
        "missing" => vec![fixture(BTC)],
        "duplicate" => vec![fixture(BTC), fixture(SOL), fixture(BTC)],
        "invalid" => {
            let mut market = fixture(SOL);
            market.order_book_state = "Closed".to_string();
            vec![fixture(BTC), market]
        }
        _ => unreachable!(),
    };
    assert!(
        provider
            .replace_markets(&markets, UnixNanos::from(2))
            .is_err()
    );
    assert_eq!(provider.all().count(), 0);
    let id = InstrumentId::from_str("BTC_USDC_PERP.BACKPACK").unwrap();
    assert!(provider.get(&id).is_none());
}

#[rstest]
fn test_provider_explicit_invalidation_after_transport_or_decode_failure() {
    let mut provider = BackpackInstrumentProvider::new(config(&["BTC_USDC_PERP"]));
    provider
        .replace_markets(&[fixture(BTC)], UnixNanos::from(1))
        .unwrap();
    assert_eq!(provider.all().count(), 1);
    provider.invalidate();
    assert_eq!(provider.all().count(), 0);
}
