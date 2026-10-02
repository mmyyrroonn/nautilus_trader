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

//! Official public captures and clearly synthetic continuity/fault replays.

use nautilus_backpack::{
    config::BackpackConfig,
    depth::{BackpackBookState, BackpackDepthSynchronizer},
    instruments::BackpackInstrumentMetadata,
    models::BackpackMarket,
    parsing::parse_market,
    public::{
        BackpackDepthUpdate, BackpackPublicEvent, BackpackPublicStreamParser,
        MAX_DEPTH_FRAME_LEVELS, MAX_PUBLIC_FRAME_BYTES,
    },
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AggressorSide, BookAction, BookType, RecordFlag},
    orderbook::OrderBook,
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

const QUOTE: &str = include_str!("../test_data/public_bookticker_btc.json");
const TRADE: &str = include_str!("../test_data/public_trade_btc.json");
const MARK: &str = include_str!("../test_data/public_markprice_btc.json");
const DEPTH: &str = include_str!("../test_data/public_depth_btc.json");
const SNAPSHOT: &str = include_str!("../test_data/btc_depth_limit5.json");

fn metadata() -> BackpackInstrumentMetadata {
    let market: BackpackMarket =
        serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
    let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()]).unwrap();
    parse_market(&market, &config, UnixNanos::from(5)).unwrap()
}

fn parser(generation: u64) -> BackpackPublicStreamParser {
    BackpackPublicStreamParser::new(metadata(), generation)
}

fn book(max_buffer: usize, max_levels: usize) -> BackpackDepthSynchronizer {
    BackpackDepthSynchronizer::new_checked(metadata(), 1, 5, max_buffer, max_levels).unwrap()
}

fn d(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn synthetic_snapshot(sequence: &str, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Vec<u8> {
    serde_json::to_vec(&json!({"lastUpdateId":sequence,"timestamp":1_694_687_965_940_999_u64,"bids":bids,"asks":asks})).unwrap()
}

fn snapshot(sequence: &str) -> Vec<u8> {
    synthetic_snapshot(
        sequence,
        &[("99.0", "1.00000"), ("98.0", "2.00000")],
        &[("101.0", "3.00000"), ("102.0", "4.00000")],
    )
}

fn update(
    generation: u64,
    first: u64,
    last: u64,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
) -> BackpackDepthUpdate {
    let value = json!({"stream":"depth.BTC_USDC_PERP","data":{"e":"depth","s":"BTC_USDC_PERP","E":1_694_687_965_941_000_u64,"T":1_694_687_965_940_999_u64,"U":first,"u":last,"b":bids,"a":asks}});
    match parser(generation)
        .decode(
            generation,
            &serde_json::to_vec(&value).unwrap(),
            UnixNanos::from(77),
        )
        .unwrap()
    {
        BackpackPublicEvent::Depth(update) => update,
        _ => unreachable!(),
    }
}

#[rstest]
fn test_real_quote_capture_and_exact_integer_sequence_compatibility() {
    let mut decoder = parser(1);
    let tick = match decoder
        .decode(1, QUOTE.as_bytes(), UnixNanos::from(7))
        .unwrap()
    {
        BackpackPublicEvent::Quote(tick) => tick,
        _ => unreachable!(),
    };
    assert_eq!(tick.instrument_id, metadata().instrument_id);
    assert_eq!(tick.bid_price.as_decimal(), d("86645.2"));
    assert_eq!(tick.ask_price.as_decimal(), d("86645.3"));
    assert_eq!(tick.bid_size.as_decimal(), d("1.60813"));
    assert_eq!(tick.ask_size.as_decimal(), d("7.76096"));
    assert_eq!(tick.ts_event, UnixNanos::from(1_790_915_525_118_046_000));
    assert_eq!(tick.ts_init, UnixNanos::from(7));
    assert!(matches!(
        decoder
            .decode(1, QUOTE.as_bytes(), UnixNanos::from(8))
            .unwrap(),
        BackpackPublicEvent::Duplicate
    ));
    let mut quoted: Value = serde_json::from_str(QUOTE).unwrap();
    quoted["data"]["u"] = json!("6904947655");
    assert!(matches!(
        decoder
            .decode(1, &serde_json::to_vec(&quoted).unwrap(), UnixNanos::from(9))
            .unwrap(),
        BackpackPublicEvent::Quote(_)
    ));
}

#[rstest]
fn test_real_trade_capture_and_synthetic_buyer_maker_mapping() {
    let mut decoder = parser(1);
    let tick = match decoder
        .decode(1, TRADE.as_bytes(), UnixNanos::from(10))
        .unwrap()
    {
        BackpackPublicEvent::Trade(tick) => tick,
        _ => unreachable!(),
    };
    assert_eq!(tick.instrument_id, metadata().instrument_id);
    assert_eq!(tick.trade_id.as_str(), "99419870");
    assert_eq!(tick.price.as_decimal(), d("86642.9"));
    assert_eq!(tick.size.as_decimal(), d("0.07206"));
    assert_eq!(tick.aggressor_side, AggressorSide::Buy);
    assert_eq!(tick.ts_event, UnixNanos::from(1_790_915_531_025_000_000));
    assert_eq!(tick.ts_init, UnixNanos::from(10));
    assert!(matches!(
        decoder
            .decode(1, TRADE.as_bytes(), UnixNanos::from(11))
            .unwrap(),
        BackpackPublicEvent::Duplicate
    ));
    let mut value: Value = serde_json::from_str(TRADE).unwrap();
    value["data"]["m"] = json!(true);
    value["data"]["t"] = json!(99419871);
    let event = decoder
        .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(12))
        .unwrap();
    assert!(
        matches!(event, BackpackPublicEvent::Trade(tick) if tick.aggressor_side == AggressorSide::Sell)
    );
}

#[rstest]
fn test_real_mark_capture_preserves_raw_rate_and_millisecond_next_funding() {
    let event = parser(1)
        .decode(1, MARK.as_bytes(), UnixNanos::from(13))
        .unwrap();
    let BackpackPublicEvent::Mark { price, funding } = event else {
        unreachable!()
    };
    assert_eq!(price.instrument_id, metadata().instrument_id);
    assert_eq!(price.value.as_decimal(), d("86661.9"));
    assert_eq!(price.ts_event, UnixNanos::from(1_790_915_525_640_379_000));
    assert_eq!(price.ts_init, UnixNanos::from(13));
    assert_eq!(funding.raw_rate, d("0.0000125"));
    assert_eq!(
        funding.next_funding_ns,
        UnixNanos::from(1_790_917_200_000_000_000)
    );
    assert_eq!(funding.interval_ms, Some(3_600_000));
}

#[rstest]
fn test_synthetic_theoretical_mark_is_exact_without_executable_tick_rounding() {
    let mut value: Value = serde_json::from_str(MARK).unwrap();
    value["data"]["p"] = json!("86661.912345678");
    let event = parser(1)
        .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
        .unwrap();
    assert!(
        matches!(event, BackpackPublicEvent::Mark { price, .. } if price.value.as_decimal() == d("86661.912345678"))
    );
}

#[rstest]
#[case("b", "B")]
#[case("a", "A")]
fn test_synthetic_empty_side_is_unusable_without_zero_quote(
    #[case] price: &str,
    #[case] size: &str,
) {
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["data"][price] = Value::Null;
    value["data"][size] = Value::Null;
    let event = parser(1)
        .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
        .unwrap();
    assert!(matches!(
        event,
        BackpackPublicEvent::QuoteUnavailable { .. }
    ));
    value["data"][size] = json!("1.0");
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
#[case("a")]
#[case("A")]
#[case("b")]
#[case("B")]
fn test_synthetic_missing_side_field_is_not_assumed_empty(#[case] key: &str) {
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["data"].as_object_mut().unwrap().remove(key);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
#[case("bookTicker.OTHER")]
#[case("depth.BTC_USDC_PERP")]
#[case("account.bookTicker.BTC_USDC_PERP")]
fn test_synthetic_topic_payload_mismatch(#[case] topic: &str) {
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["stream"] = json!(topic);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
#[case(include_str!("../test_data/official_book_ticker.json"), "bookTicker")]
#[case(include_str!("../test_data/official_trade.json"), "trade")]
#[case(include_str!("../test_data/official_depth.json"), "depth")]
#[case(include_str!("../test_data/official_mark_price.json"), "markPrice")]
fn test_official_documentation_spot_examples_are_not_relabelled_as_perpetuals(
    #[case] body: &str,
    #[case] topic: &str,
) {
    let payload: Value = serde_json::from_str(body).unwrap();
    let envelope = json!({"stream":format!("{topic}.SOL_USDC"),"data":payload});
    assert!(
        parser(1)
            .decode(
                1,
                &serde_json::to_vec(&envelope).unwrap(),
                UnixNanos::from(0)
            )
            .is_err()
    );
}

#[rstest]
fn test_generation_frame_bound_timestamp_overflow_and_duplicate_json_fields() {
    assert!(
        parser(2)
            .decode(1, QUOTE.as_bytes(), UnixNanos::from(0))
            .is_err()
    );
    assert!(
        parser(1)
            .decode(
                1,
                &vec![b' '; MAX_PUBLIC_FRAME_BYTES + 1],
                UnixNanos::from(0)
            )
            .is_err()
    );
    for key in ["E", "T"] {
        let mut value: Value = serde_json::from_str(QUOTE).unwrap();
        value["data"][key] = json!(u64::MAX);
        assert!(
            parser(1)
                .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
                .is_err()
        );
    }
    let mut value: Value = serde_json::from_str(MARK).unwrap();
    value["data"]["n"] = json!(u64::MAX);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
    let duplicate = QUOTE.replace(
        "\"e\":\"bookTicker\"",
        "\"e\":\"trade\",\"e\":\"bookTicker\"",
    );
    assert!(
        parser(1)
            .decode(1, duplicate.as_bytes(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
fn test_synthetic_non_power_of_ten_price_and_size_grids() {
    let mut info = metadata();
    info.price_increment = nautilus_model::types::Price::from_decimal(d("0.5")).unwrap();
    info.size_increment = nautilus_model::types::Quantity::from_decimal(d("0.00005")).unwrap();
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["data"]["b"] = json!("100.0");
    value["data"]["a"] = json!("100.5");
    value["data"]["B"] = json!("0.00010");
    value["data"]["A"] = json!("0.00015");
    assert!(matches!(
        BackpackPublicStreamParser::new(info.clone(), 1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .unwrap(),
        BackpackPublicEvent::Quote(_)
    ));
    value["data"]["b"] = json!("100.1");
    assert!(
        BackpackPublicStreamParser::new(info.clone(), 1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
    value["data"]["b"] = json!("100.0");
    value["data"]["B"] = json!("0.00011");
    assert!(
        BackpackPublicStreamParser::new(info, 1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
fn test_real_snapshot_is_sorted_clear_snapshot_flagged_and_truncated() {
    let mut sync = book(4, 10);
    let batch = sync
        .install_snapshot(1, SNAPSHOT.as_bytes(), UnixNanos::from(20))
        .unwrap();
    assert!(sync.is_continuous());
    assert_eq!(sync.coverage().unwrap().snapshot_limit, 5);
    assert_eq!(batch.sequence, 6904864764);
    assert_eq!(batch.ts_event, UnixNanos::from(1_790_915_412_979_224_000));
    assert_eq!(batch.ts_init, UnixNanos::from(20));
    assert_eq!(batch.deltas.len(), 11);
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    assert_eq!(batch.deltas[1].order.price.as_decimal(), d("86781.4"));
    assert_eq!(batch.deltas[5].order.price.as_decimal(), d("86777.7"));
    assert_eq!(batch.deltas[6].order.price.as_decimal(), d("86781.5"));
    assert!(
        batch
            .deltas
            .iter()
            .all(|delta| delta.flags & RecordFlag::F_SNAPSHOT as u8 != 0)
    );
    assert_eq!(
        batch
            .deltas
            .iter()
            .filter(|delta| delta.flags & RecordFlag::F_LAST as u8 != 0)
            .count(),
        1
    );
    assert_ne!(
        batch.deltas.last().unwrap().flags & RecordFlag::F_LAST as u8,
        0
    );
    let mut native = OrderBook::new(metadata().instrument_id, BookType::L2_MBP);
    native.apply_deltas(&batch).unwrap();
    assert_eq!(native.best_bid_price().unwrap().as_decimal(), d("86781.4"));
    assert_eq!(native.best_ask_price().unwrap().as_decimal(), d("86781.5"));
}

#[rstest]
fn test_real_depth_capture_is_decoded_but_cannot_bridge_unrelated_snapshot() {
    let BackpackPublicEvent::Depth(change) = parser(1)
        .decode(1, DEPTH.as_bytes(), UnixNanos::from(0))
        .unwrap()
    else {
        unreachable!()
    };
    let mut sync = book(4, 10);
    sync.apply(change).unwrap();
    assert!(
        sync.install_snapshot(1, SNAPSHOT.as_bytes(), UnixNanos::from(0))
            .is_err()
    );
    assert_eq!(sync.state(), BackpackBookState::Stale);
    assert!(sync.coverage().is_none());
}

#[rstest]
fn test_synthetic_buffer_overlap_bridge_absolute_updates_zero_delete_and_native_batch() {
    let mut sync = book(4, 10);
    assert!(
        sync.apply(update(1, 98, 99, &[("97.0", "1")], &[]))
            .unwrap()
            .is_none()
    );
    sync.apply(update(1, 100, 102, &[("99.0", "5")], &[("101.0", "0")]))
        .unwrap();
    sync.apply(update(1, 103, 104, &[("99.0", "7")], &[("101.5", "1")]))
        .unwrap();
    let initial = sync
        .install_snapshot(1, &snapshot("100"), UnixNanos::from(3))
        .unwrap();
    assert_eq!(initial.sequence, 104);
    assert_eq!(initial.ts_init, UnixNanos::from(77));
    assert_eq!(sync.best_covered_bid().unwrap().1.as_decimal(), d("7"));
    assert_eq!(sync.best_covered_ask().unwrap().0.as_decimal(), d("101.5"));
    let mut native = OrderBook::new(metadata().instrument_id, BookType::L2_MBP);
    native.apply_deltas(&initial).unwrap();
    assert_eq!(native.best_bid_size().unwrap().as_decimal(), d("7"));
    let live = sync
        .apply(update(1, 105, 105, &[("99.0", "0"), ("98.5", "6")], &[]))
        .unwrap()
        .unwrap();
    assert_eq!(live.deltas[0].action, BookAction::Delete);
    assert_eq!(live.deltas[1].action, BookAction::Update);
    assert_eq!(live.deltas[0].flags, 0);
    assert_eq!(live.deltas[1].flags, RecordFlag::F_LAST as u8);
    native.apply_deltas(&live).unwrap();
    assert_eq!(native.best_bid_price().unwrap().as_decimal(), d("98.5"));
    assert_eq!(native.best_bid_size().unwrap().as_decimal(), d("6"));
    assert!(
        sync.apply(update(1, 105, 105, &[("99.0", "1")], &[]))
            .unwrap()
            .is_none()
    );
    assert_eq!(sync.best_covered_bid().unwrap().0.as_decimal(), d("98.5"));
}

#[rstest]
#[case(102, 102)]
#[case(100, 102)]
fn test_synthetic_live_gap_or_partial_overlap_invalidates_before_return(
    #[case] first: u64,
    #[case] last: u64,
) {
    let mut sync = book(4, 10);
    sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    assert!(sync.apply(update(1, first, last, &[], &[])).is_err());
    assert_eq!(sync.state(), BackpackBookState::Stale);
    assert!(!sync.is_continuous());
    assert!(sync.best_covered_bid().is_none());
}

#[rstest]
fn test_synthetic_buffer_overflow_generation_failure_and_explicit_restart() {
    let mut sync = book(1, 10);
    sync.apply(update(1, 101, 101, &[], &[])).unwrap();
    assert!(sync.apply(update(1, 102, 102, &[], &[])).is_err());
    assert_eq!(sync.state(), BackpackBookState::Stale);
    assert!(
        sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
            .is_err()
    );
    assert!(sync.restart(1).is_err());
    sync.restart(2).unwrap();
    assert_eq!(sync.state(), BackpackBookState::Buffering);
    assert!(
        sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
            .is_err()
    );
    assert_eq!(sync.state(), BackpackBookState::Stale);
    sync.restart(3).unwrap();
    sync.install_snapshot(3, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    assert!(sync.apply(update(2, 101, 101, &[], &[])).is_err());
    assert_eq!(sync.state(), BackpackBookState::Stale);
}

#[rstest]
fn test_synthetic_deleting_snapshot_edge_does_not_invent_top_n_coverage() {
    let mut sync = book(4, 10);
    sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    sync.apply(update(
        1,
        101,
        101,
        &[("97.0", "1"), ("99.0", "0"), ("98.0", "0")],
        &[],
    ))
    .unwrap();
    assert!(sync.is_continuous());
    assert_eq!(
        sync.coverage()
            .unwrap()
            .initial_bid_floor
            .unwrap()
            .as_decimal(),
        d("98.0")
    );
    assert!(sync.best_covered_bid().is_none());
    sync.apply(update(1, 102, 102, &[("98.5", "2")], &[]))
        .unwrap();
    assert_eq!(sync.best_covered_bid().unwrap().0.as_decimal(), d("98.5"));
}

#[rstest]
fn test_synthetic_storage_overflow_crossed_book_invalid_snapshot_and_bad_frame_stale() {
    let mut sync = book(4, 5);
    sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    assert!(
        sync.apply(update(
            1,
            101,
            101,
            &[("97", "1"), ("96", "1"), ("95", "1"), ("94", "1")],
            &[]
        ))
        .is_err()
    );
    assert_eq!(sync.state(), BackpackBookState::Stale);
    sync.restart(2).unwrap();
    sync.install_snapshot(2, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    assert!(
        sync.apply(update(2, 101, 101, &[("101.5", "1")], &[]))
            .is_err()
    );
    sync.restart(3).unwrap();
    assert!(
        sync.install_snapshot(3, b"invalid JSON", UnixNanos::from(0))
            .is_err()
    );
    assert_eq!(sync.state(), BackpackBookState::Stale);
    sync.restart(4).unwrap();
    sync.install_snapshot(4, &snapshot("100"), UnixNanos::from(0))
        .unwrap();
    assert!(
        parser(4)
            .decode(4, b"invalid JSON", UnixNanos::from(0))
            .is_err()
    );
    sync.invalidate(); // Explicit transport-owner responsibility after a failed frame decode.
    assert_eq!(sync.state(), BackpackBookState::Stale);
}

#[rstest]
fn test_synthetic_empty_snapshot_has_clear_last_and_no_bbo_coverage() {
    let mut sync = book(4, 5);
    let batch = sync
        .install_snapshot(1, &synthetic_snapshot("100", &[], &[]), UnixNanos::from(0))
        .unwrap();
    assert_eq!(batch.deltas.len(), 1);
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    assert_eq!(
        batch.deltas[0].flags,
        RecordFlag::F_LAST as u8 | RecordFlag::F_SNAPSHOT as u8
    );
    assert!(sync.is_continuous());
    assert!(sync.best_covered_bid().is_none());
    assert!(sync.best_covered_ask().is_none());
    assert!(sync.apply(update(1, 101, 101, &[], &[])).unwrap().is_none());
    assert!(sync.apply(update(1, 102, 102, &[], &[])).unwrap().is_none());
}

#[rstest]
#[case(json!(-1))]
#[case(json!(1.5))]
#[case(json!("-1"))]
#[case(json!("18446744073709551616"))]
#[case(json!("1e2"))]
fn test_synthetic_invalid_quote_sequence_is_rejected(#[case] sequence: Value) {
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["data"]["u"] = sequence;
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
fn test_synthetic_malformed_quote_cannot_hide_behind_duplicate_watermark() {
    let mut decoder = parser(1);
    decoder
        .decode(1, QUOTE.as_bytes(), UnixNanos::from(0))
        .unwrap();
    let mut value: Value = serde_json::from_str(QUOTE).unwrap();
    value["data"]["b"] = value["data"]["a"].clone();
    assert!(
        decoder
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
#[case("depth.BTC_USDC_PERP")]
#[case("depth.200ms.BTC_USDC_PERP")]
#[case("depth.600ms.BTC_USDC_PERP")]
#[case("depth.1000ms.BTC_USDC_PERP")]
fn test_documented_depth_topics_are_accepted(#[case] topic: &str) {
    let mut value: Value = serde_json::from_str(DEPTH).unwrap();
    value["stream"] = json!(topic);
    assert!(matches!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .unwrap(),
        BackpackPublicEvent::Depth(_)
    ));
}

#[rstest]
fn test_synthetic_invalid_depth_ranges_duplicate_prices_and_level_bound() {
    let mut value: Value = serde_json::from_str(DEPTH).unwrap();
    value["data"]["U"] = json!(u64::MAX);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
    value["data"]["U"] = value["data"]["u"].clone();
    value["data"]["b"] = json!([["99.0", "1"], ["99.00", "2"]]);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
    value["data"]["b"] = json!(vec![["99", "1"]; MAX_DEPTH_FRAME_LEVELS + 1]);
    assert!(
        parser(1)
            .decode(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
            .is_err()
    );
}

#[rstest]
#[case(6, 4, 10)]
#[case(5, 0, 10)]
#[case(5, 4, 4)]
fn test_snapshot_and_storage_bounds_are_explicit(
    #[case] limit: usize,
    #[case] buffered: usize,
    #[case] stored: usize,
) {
    assert!(
        BackpackDepthSynchronizer::new_checked(metadata(), 1, limit, buffered, stored).is_err()
    );
}

#[rstest]
fn test_synthetic_snapshot_invalid_fields_fail_closed_and_publish_nothing() {
    let mut too_many: Value = serde_json::from_slice(&snapshot("100")).unwrap();
    too_many["bids"] = json!([
        ["99", "1"],
        ["98", "1"],
        ["97", "1"],
        ["96", "1"],
        ["95", "1"],
        ["94", "1"]
    ]);
    let mut zero: Value = serde_json::from_slice(&snapshot("100")).unwrap();
    zero["bids"][0][1] = json!("0");
    let mut bad_time: Value = serde_json::from_slice(&snapshot("100")).unwrap();
    bad_time["timestamp"] = json!(u64::MAX);
    let mut bad_id: Value = serde_json::from_slice(&snapshot("100")).unwrap();
    bad_id["lastUpdateId"] = json!("18446744073709551616");
    for value in [too_many, zero, bad_time, bad_id] {
        let mut sync = book(4, 10);
        assert!(
            sync.install_snapshot(1, &serde_json::to_vec(&value).unwrap(), UnixNanos::from(0))
                .is_err()
        );
        assert_eq!(sync.state(), BackpackBookState::Stale);
        assert!(sync.coverage().is_none());
    }
}

#[rstest]
fn test_synthetic_buffered_later_gap_does_not_publish_partial_snapshot() {
    let mut sync = book(4, 10);
    sync.apply(update(1, 100, 101, &[("99", "7")], &[]))
        .unwrap();
    sync.apply(update(1, 103, 103, &[("99", "8")], &[]))
        .unwrap();
    assert!(
        sync.install_snapshot(1, &snapshot("100"), UnixNanos::from(0))
            .is_err()
    );
    assert_eq!(sync.state(), BackpackBookState::Stale);
    assert!(sync.best_covered_bid().is_none());
}
