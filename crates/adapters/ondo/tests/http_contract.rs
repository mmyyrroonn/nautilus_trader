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

//! Contract tests for the Ondo Perps market metadata schema and its parsing boundary.
//!
//! Payloads used here come from three places:
//!
//! - `test_data/rest/markets_observed_20260914.json` is an **observed** excerpt of a real
//!   production `GET /v1/markets` response (2026-09-14T15:15:02Z, fetched with the adapter's own
//!   public client). Its 4 trading pairs are byte-faithful; `test_data/manifest.json` records the
//!   source and the hash, and is the authority on the kind.
//! - `test_data/rest/markets_synthetic.json` is hand-authored from the official REST schema because
//!   the first attempt at the live host answered HTTP 403 during the protocol freeze. Its
//!   `_fixture` block marks it, and `test_data/manifest.json` is the authority on that kind.
//! - Bodies built inline below carry the same explicit `_fixture.kind` marker, so an inline body
//!   can never be mistaken for a captured response.

use std::{fs, path::PathBuf, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_ondo::{
    common::{
        enums::{FeeRate, FeeSource, MarketStatus, MarketStatusSource},
        parse::{market_to_instrument_id, parse_timestamp},
    },
    http::{
        error::OndoHttpError,
        models::{MarketInfo, MarketsResponse, parse_instruments, parse_markets},
        private::{ACCOUNT_PATH, OndoPrivateReadQuery, OndoPrivateResponse},
        query::{FILLS_PATH, MARKETS_PATH},
    },
};
use rstest::rstest;
use rust_decimal::Decimal;

const NVDA_MARKET: &str = "NVDA-USD.P";
const TSLA_MARKET: &str = "TSLA-USD.P";
const ENA_MARKET: &str = "ENA-USD.P";
const BTC_MARKET: &str = "BTC-USD.P";
const SYNTH_MARKET: &str = "SYNTH-USD.P";
const TEST_MARKET: &str = "TEST-USD.P";

/// sha256 of `test_data/rest/markets_observed_20260914.json`.
///
/// Computed out of band when the fixture was written (this adapter has no hashing crate as a
/// dependency, and adding one for a test would be more than this fix warrants) and recorded in
/// `test_data/manifest.json`. Pinned here so an edit of the observed payload cannot pass unnoticed.
const OBSERVED_MARKETS_SHA256: &str =
    "3fc411115f1d9ba2ca664adcddb87805d087331a803e4e8511d55f691e4d5af7";

fn ts_init() -> UnixNanos {
    UnixNanos::from(1_789_384_200_000_000_000)
}

fn test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

fn markets_fixture() -> String {
    fs::read_to_string(test_data_dir().join("rest").join("markets_synthetic.json"))
        .expect("read markets_synthetic.json fixture")
}

/// The observed excerpt of the 2026-09-14 production `GET /v1/markets` response.
///
/// Unlike the synthetic fixture this file is not hand-authored: it is 4 of the 81 trading pairs the
/// venue served, copied verbatim (see `test_data/manifest.json` and `test_data/README.md`). It is
/// the payload that settled `test_data/conflicts.md` conflict 5.
fn markets_observed_fixture() -> String {
    fs::read_to_string(
        test_data_dir()
            .join("rest")
            .join("markets_observed_20260914.json"),
    )
    .expect("read markets_observed_20260914.json fixture")
}

fn manifest() -> serde_json::Value {
    serde_json::from_str(
        &fs::read_to_string(test_data_dir().join("manifest.json")).expect("read manifest.json"),
    )
    .expect("parse manifest.json")
}

fn instrument_id(market: &str) -> InstrumentId {
    market_to_instrument_id(market).expect("market maps to an instrument id")
}

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal literal")
}

fn info_for<'a>(infos: &'a [MarketInfo], market: &str) -> &'a MarketInfo {
    infos
        .iter()
        .find(|info| info.market() == market)
        .expect("market metadata is present")
}

fn crypto_perpetual(instrument: &InstrumentAny) -> &CryptoPerpetual {
    match instrument {
        InstrumentAny::CryptoPerpetual(perp) => perp,
        other => panic!("expected a CryptoPerpetual instrument, was {other:?}"),
    }
}

/// A synthetic single-market `GET /v1/markets` body.
///
/// The fixture market `TEST-USD.P` is not a venue market; it exists only to drive boundary cases
/// that `test_data/rest/markets_synthetic.json` does not carry, and the `_fixture` block states
/// that the values are invented.
fn synthetic_markets_body(base_increment: &str, quote_increment: &str) -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body for GET /v1/markets. It is NOT an observed server response."
  }},
  "success": true,
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{
          "market": "{TEST_MARKET}",
          "baseIncrement": "{base_increment}",
          "quoteIncrement": "{quote_increment}",
          "status": "active",
          "disabled": false,
          "makerFee": "0.0001",
          "takerFee": "0.00025"
        }}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

/// A synthetic `GET /v1/markets` body whose only market carries no `quoteIncrement` field.
fn synthetic_markets_body_without_quote_increment() -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body whose trading pair omits quoteIncrement. It is NOT an observed server response."
  }},
  "success": true,
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{"market": "{TEST_MARKET}", "baseIncrement": "0.01", "status": "active"}}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

/// A synthetic `GET /v1/markets` body whose only market carries no status evidence at all.
///
/// Since the 2026-09-14 observation this is not a hypothetical shape: it is one of the two shapes
/// the venue actually serves (`disabled` absent, and `disabled: true`), and it is the shape both P1
/// targets have. `test_data/conflicts.md` conflict 5 records the counts.
fn synthetic_markets_body_without_status_evidence() -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body whose trading pair omits both the status string and the disabled flag. It is NOT an observed server response."
  }},
  "success": true,
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{"market": "{TEST_MARKET}", "baseIncrement": "0.01", "quoteIncrement": "0.01"}}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

/// A synthetic `GET /v1/markets` body whose only market carries `"disabled": false` and no status
/// string: the *explicit* enabled shape, which the observed venue payload never uses for an enabled
/// market and which must stay distinguishable from an absent flag.
fn synthetic_markets_body_with_explicit_false_disabled() -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body whose trading pair carries an explicit disabled:false and no status string. It is NOT an observed server response."
  }},
  "success": true,
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{"market": "{TEST_MARKET}", "baseIncrement": "0.01", "quoteIncrement": "0.01", "disabled": false}}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

/// A synthetic `GET /v1/markets` body whose only market carries the given status-evidence members
/// verbatim, so a mistyped or `null` marker can be driven through the decode boundary.
fn synthetic_markets_body_with_status_member(member: &str) -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body whose trading pair carries an unclassifiable status marker. It is NOT an observed server response."
  }},
  "success": true,
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{"market": "{TEST_MARKET}", "baseIncrement": "0.01", "quoteIncrement": "0.01", {member}}}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

/// The synthetic `TEST-USD.P` body with its `success` member replaced by `success_member`.
///
/// An empty `success_member` omits the member entirely; the rest of the body stays valid.
fn synthetic_markets_body_with_success_member(success_member: &str) -> String {
    format!(
        r#"{{
  "_fixture": {{
    "kind": "synthetic",
    "authored_at_utc": "inline in tests/http_contract.rs",
    "what_this_is": "A hand-authored boundary body for GET /v1/markets whose success member is varied. It is NOT an observed server response."
  }},
  {success_member}
  "result": {{
    "perps": {{
      "tradingPairs": [
        {{
          "market": "{TEST_MARKET}",
          "baseIncrement": "0.01",
          "quoteIncrement": "0.01",
          "status": "active"
        }}
      ]
    }},
    "tokenConfig": []
  }}
}}"#
    )
}

// ------------------------------------------------------------------------------------------------
// Fixture provenance
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_fixture_provenance_is_explicit() {
    let manifest = manifest();
    let fixtures = manifest["fixtures"].as_array().expect("fixtures array");

    let fixture_for = |path: &str| {
        fixtures
            .iter()
            .find(|fixture| fixture["path"] == path)
            .unwrap_or_else(|| panic!("fixture {path} is indexed"))
            .clone()
    };
    let kind_for = |path: &str| {
        fixture_for(path)["kind"]
            .as_str()
            .expect("kind string")
            .to_string()
    };

    // `ws/markprices_observed.json` is named `_observed` but is an official spec example, so the
    // manifest kind, not the file name, is what the tests trust.
    assert_eq!(kind_for("rest/markets_synthetic.json"), "synthetic");
    assert_eq!(kind_for("ws/markprices_observed.json"), "official-example");

    // The live-observation fixture added 2026-09-14 is observed, and its recorded source, hash and
    // capture time must be present and well-formed rather than merely asserted by a file name.
    let observed = fixture_for("rest/markets_observed_20260914.json");
    assert_eq!(observed["kind"], "observed");
    assert_eq!(
        observed["captured_at_utc"],
        "2026-09-14T15:15:02.434934+00:00"
    );
    assert_eq!(
        observed["source"]["url"],
        "https://api.ondoperps.xyz/v1/markets"
    );
    let recorded_sha = observed["sha256"].as_str().expect("sha256 string");
    assert_eq!(recorded_sha.len(), 64);
    assert_eq!(
        recorded_sha, OBSERVED_MARKETS_SHA256,
        "the indexed hash is the hash of the observed excerpt"
    );

    let body: serde_json::Value = serde_json::from_str(&markets_fixture()).expect("fixture json");
    assert_eq!(body["_fixture"]["kind"], "synthetic");

    // A captured response carries no `_fixture` block: its envelope is the venue's own, and the
    // manifest is the only statement of its provenance.
    let payload = markets_observed_fixture();
    assert!(!payload.contains("_fixture"));
    let observed_body: serde_json::Value = serde_json::from_str(&payload).expect("observed json");
    assert_eq!(observed_body["success"], true);

    // The recorded byte count is the fixture as written. (`sha256` above is pinned as a literal
    // rather than recomputed: no hashing crate is a dependency of this adapter, and adding one for a
    // test would be more than this fix warrants. The value was computed out of band and is recorded
    // with the fixture in the manifest and in the task-2 fix report.)
    assert_eq!(
        observed["bytes"].as_u64().expect("bytes"),
        payload.len() as u64,
        "manifest byte count must describe the fixture as written"
    );
}

// ------------------------------------------------------------------------------------------------
// Instrument identity
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_market_to_instrument_id_maps_canonical_markets() {
    assert_eq!(
        market_to_instrument_id(NVDA_MARKET).unwrap().to_string(),
        "NVDA-USD-PERP.ONDO",
    );
    assert_eq!(
        market_to_instrument_id(TSLA_MARKET).unwrap().to_string(),
        "TSLA-USD-PERP.ONDO",
    );
}

#[rstest]
#[case::missing_product_marker("NVDA-USD")]
#[case::unknown_product_marker("NVDA-USD.S")]
#[case::empty("")]
#[case::unknown_quote("NVDA-USDT.P")]
#[case::missing_base("-USD.P")]
#[case::missing_quote("NVDA-.P")]
#[case::ambiguous_base_dot("BRK.B-USD.P")]
#[case::whitespace_base("NVDA USD.P")]
fn test_market_to_instrument_id_rejects_unknown_forms(#[case] market: &str) {
    assert!(
        market_to_instrument_id(market).is_err(),
        "market `{market}` must not be renamed into an instrument"
    );
}

// ------------------------------------------------------------------------------------------------
// Timestamps
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_parse_timestamp_preserves_nanosecond_digits() {
    assert_eq!(
        parse_timestamp("1970-01-01T00:00:01.123456789Z")
            .unwrap()
            .as_u64(),
        1_123_456_789,
    );

    // Observed production frame time (2026-09-14T11:09:59.570112122Z on depthBooksPerps): the
    // last three digits must survive a parse, which microsecond-truncating implementations lose.
    let observed = parse_timestamp("2026-09-14T11:09:59.570112122Z").unwrap();
    assert_eq!(observed.as_u64(), 1_789_384_199_570_112_122);
    assert_eq!(observed.as_u64() % 1_000, 122);
}

#[rstest]
#[case::empty("")]
#[case::not_a_timestamp("not-a-timestamp")]
#[case::date_only("2026-09-14")]
#[case::missing_offset("2026-09-14T11:09:59")]
#[case::before_epoch("1960-01-01T00:00:00Z")]
#[case::numeric_seconds("1789384199.5")]
fn test_parse_timestamp_rejects_non_rfc3339_input(#[case] value: &str) {
    assert!(
        parse_timestamp(value).is_err(),
        "`{value}` must not parse as an RFC 3339 nanosecond timestamp"
    );
}

// ------------------------------------------------------------------------------------------------
// Market metadata
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_parse_instruments_builds_requested_markets() {
    let payload = markets_fixture();
    let instruments = parse_instruments(
        &payload,
        &[instrument_id(NVDA_MARKET), instrument_id(TSLA_MARKET)],
        ts_init(),
    )
    .unwrap();

    assert_eq!(instruments.len(), 2);

    let perp = crypto_perpetual(&instruments[0]);
    assert_eq!(perp.id.to_string(), "NVDA-USD-PERP.ONDO");
    assert_eq!(perp.raw_symbol.as_str(), NVDA_MARKET);
    assert_eq!(perp.base_currency.code.as_str(), "NVDA");
    assert_eq!(perp.quote_currency.code.as_str(), "USD");
    assert_eq!(perp.settlement_currency.code.as_str(), "USDC");
    assert!(!perp.is_inverse);
    assert_eq!(perp.price_precision, 2);
    assert_eq!(perp.size_precision, 2);
    assert_eq!(perp.price_increment, Price::from("0.01"));
    assert_eq!(perp.size_increment, Quantity::from("0.01"));
    assert_eq!(perp.multiplier, Quantity::from(1));
    assert_eq!(perp.ts_event, ts_init());
    assert_eq!(perp.ts_init, ts_init());
    assert_eq!(perp.maker_fee, decimal("0.0001"));
    assert_eq!(perp.taker_fee, decimal("0.00025"));

    let tsla = crypto_perpetual(&instruments[1]);
    assert_eq!(tsla.id.to_string(), "TSLA-USD-PERP.ONDO");
    assert_eq!(tsla.raw_symbol.as_str(), TSLA_MARKET);
    assert_eq!(tsla.base_currency.code.as_str(), "TSLA");
}

// ------------------------------------------------------------------------------------------------
// The observed production payload (2026-09-14)
// ------------------------------------------------------------------------------------------------

/// The P1 acceptance case: the two target pairs as the live venue serves them, with `disabled`
/// ABSENT. Before this fix that shape mapped to `Unknown` and failed the load closed, so no real
/// market could be loaded at all; the load is now the acceptance check.
#[rstest]
fn test_observed_fixture_loads_the_two_target_pairs() {
    let payload = markets_observed_fixture();

    let instruments = parse_instruments(
        &payload,
        &[
            InstrumentId::from("NVDA-USD-PERP.ONDO"),
            InstrumentId::from("TSLA-USD-PERP.ONDO"),
        ],
        ts_init(),
    )
    .unwrap();

    assert_eq!(instruments.len(), 2);
    assert_eq!(
        crypto_perpetual(&instruments[0]).id.to_string(),
        "NVDA-USD-PERP.ONDO"
    );
    assert_eq!(
        crypto_perpetual(&instruments[1]).id.to_string(),
        "TSLA-USD-PERP.ONDO"
    );

    let nvda = crypto_perpetual(&instruments[0]);
    assert_eq!(nvda.raw_symbol.as_str(), NVDA_MARKET);
    assert_eq!(nvda.base_currency.code.as_str(), "NVDA");
    assert_eq!(nvda.quote_currency.code.as_str(), "USD");
    assert_eq!(nvda.settlement_currency.code.as_str(), "USDC");
    assert!(!nvda.is_inverse);
    assert_eq!(nvda.price_precision, 2);
    assert_eq!(nvda.size_precision, 2);
    assert_eq!(nvda.price_increment, Price::from("0.01"));
    assert_eq!(nvda.size_increment, Quantity::from("0.01"));
    assert_eq!(nvda.multiplier, Quantity::from(1));
    assert_eq!(nvda.maker_fee, decimal("0.0001"));
    assert_eq!(nvda.taker_fee, decimal("0.00025"));
    assert_eq!(nvda.ts_event, ts_init());
    assert_eq!(nvda.ts_init, ts_init());

    // Both targets classify as tradable *and* record which shape of evidence said so: an absent
    // `disabled` flag, never an explicit `false`.
    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    // 4 trading pairs in the excerpt; the observed `result` also carries `tokenConfig`, which this
    // excerpt does not include, and `spot`, which is not perps metadata.
    assert_eq!(infos.len(), 4);
    assert!(parse_markets(&payload).unwrap().token_config().is_empty());

    for market in [NVDA_MARKET, TSLA_MARKET] {
        let info = info_for(&infos, market);
        assert_eq!(info.status(), MarketStatus::Active, "{market}");
        assert_eq!(
            info.status_info().source(),
            MarketStatusSource::Absent,
            "{market}"
        );
        assert_eq!(info.status_info().raw(), None, "{market}");
        assert!(info.is_tradable(), "{market}");
        assert_eq!(info.base_increment(), Some(decimal("0.01")), "{market}");
        assert_eq!(info.quote_increment(), Some(decimal("0.01")), "{market}");
    }

    // The key facts of the captured pair, read from the JSON itself: `disabled` is absent and
    // `tags` says Stock. If the fixture were ever re-cut, this pins what it must still say.
    let body: serde_json::Value = serde_json::from_str(&payload).expect("observed json");
    let pairs = body["result"]["perps"]["tradingPairs"]
        .as_array()
        .expect("tradingPairs array");
    let nvda_json = pairs
        .iter()
        .find(|pair| pair["market"] == NVDA_MARKET)
        .expect("NVDA-USD.P is in the excerpt");
    assert!(nvda_json.get("disabled").is_none());
    assert_eq!(nvda_json["tags"][0], "Stock");
    assert!(
        nvda_json.get("schedule").is_some(),
        "the venue expresses market hours through `schedule`, which this adapter does not interpret"
    );
}

/// A `disabled: true` market requested explicitly is still built, with `Disabled` status, and is
/// gated as not tradable. This is the behaviour the existing synthetic-fixture tests already pinned
/// (`test_market_infos_expose_increments_status_and_fee_provenance`); it is re-pinned against the
/// real payload rather than changed to an exclusion.
#[rstest]
fn test_observed_disabled_market_is_built_but_not_tradable() {
    let payload = markets_observed_fixture();

    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    let ena = info_for(&infos, ENA_MARKET);
    assert_eq!(ena.status(), MarketStatus::Disabled);
    assert_eq!(ena.status_info().source(), MarketStatusSource::DisabledFlag);
    assert_eq!(ena.status_info().raw(), None);
    assert!(!ena.is_tradable());
    assert!(ena.instrument(ts_init()).is_ok());

    let instruments = parse_instruments(&payload, &[instrument_id(ENA_MARKET)], ts_init()).unwrap();

    assert_eq!(instruments.len(), 1);
    let perp = crypto_perpetual(&instruments[0]);
    assert_eq!(perp.id.to_string(), "ENA-USD-PERP.ONDO");
    // ENA is the asymmetric increment case in the real payload: quantity step 1, price step 0.00001.
    // They must never be swapped.
    assert_eq!(perp.size_increment, Quantity::from(1));
    assert_eq!(perp.size_precision, 0);
    assert_eq!(perp.price_increment, Price::from("0.00001"));
    assert_eq!(perp.price_precision, 5);
}

/// The excerpt's non-stock pair: a crypto market with no `schedule` object at all. It must load on
/// the same absent-`disabled` rule as the equity pairs.
#[rstest]
fn test_observed_non_stock_market_loads_without_a_schedule() {
    let payload = markets_observed_fixture();

    let instruments = parse_instruments(&payload, &[instrument_id(BTC_MARKET)], ts_init()).unwrap();

    assert_eq!(instruments.len(), 1);
    let perp = crypto_perpetual(&instruments[0]);
    assert_eq!(perp.id.to_string(), "BTC-USD-PERP.ONDO");
    assert_eq!(perp.size_increment, Quantity::from("0.0001"));
    assert_eq!(perp.size_precision, 4);
    assert_eq!(perp.price_increment, Price::from("1"));
    assert_eq!(perp.price_precision, 0);

    let body: serde_json::Value = serde_json::from_str(&payload).expect("observed json");
    let pairs = body["result"]["perps"]["tradingPairs"]
        .as_array()
        .expect("tradingPairs array");
    let btc_json = pairs
        .iter()
        .find(|pair| pair["market"] == BTC_MARKET)
        .expect("BTC-USD.P is in the excerpt");
    assert_eq!(btc_json["tags"][0], "Crypto");
    assert!(btc_json.get("disabled").is_none());
    assert!(
        btc_json.get("schedule").is_none(),
        "the crypto pair carries no schedule object"
    );
}

/// An explicit `"disabled": false` and an absent `disabled` both classify as Active, and stay
/// distinguishable through the recorded status source -- which is why the source is part of the type.
#[rstest]
fn test_explicit_false_and_absent_disabled_are_both_active_but_distinguishable() {
    let explicit_payload = synthetic_markets_body_with_explicit_false_disabled();
    let explicit_infos = parse_markets(&explicit_payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    let explicit = info_for(&explicit_infos, TEST_MARKET);
    assert_eq!(explicit.status(), MarketStatus::Active);
    assert_eq!(
        explicit.status_info().source(),
        MarketStatusSource::DisabledFlag
    );

    let observed_payload = markets_observed_fixture();
    let observed_infos = parse_markets(&observed_payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    let absent = info_for(&observed_infos, NVDA_MARKET);
    assert_eq!(absent.status(), MarketStatus::Active);
    assert_eq!(absent.status_info().source(), MarketStatusSource::Absent);

    assert_eq!(explicit.status(), absent.status());
    assert_ne!(
        explicit.status_info().source(),
        absent.status_info().source(),
        "an explicit `false` and an absent flag must not collapse into one another"
    );
    assert!(explicit.is_tradable() && absent.is_tradable());
}

#[rstest]
fn test_parse_instruments_fails_when_a_requested_id_is_absent() {
    let payload = markets_fixture();
    let requested = [
        instrument_id(NVDA_MARKET),
        InstrumentId::from("ABC-USD-PERP.ONDO"),
    ];

    let error = parse_instruments(&payload, &requested, ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::MissingInstrument { .. }));
    assert!(
        error.to_string().contains("ABC-USD-PERP.ONDO"),
        "error names the missing instrument: {error}"
    );
}

#[rstest]
fn test_parse_instruments_fails_on_an_unclassifiable_status() {
    let payload = markets_fixture();

    let error = parse_instruments(&payload, &[instrument_id(SYNTH_MARKET)], ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::UnknownMarketStatus { .. }));
    assert!(
        error.to_string().contains("halted"),
        "error preserves the raw status: {error}"
    );
}

/// CHANGED 2026-09-14 (spec change backed by the live observation, not a weakening).
///
/// BEFORE: this test drove a payload whose trading pair omitted both `status` and `disabled` and
/// asserted the load failed as `UnknownMarketStatus` with a diagnostic containing "absent".
/// AFTER: that shape is the venue's way of expressing an *enabled* market (61 of 81 captured
/// markets, both P1 targets), so it loads as `Active` with the absence recorded in the status
/// source. Only a `status` *string* outside the allow-list stays Unknown and fails closed, and that
/// is what the diagnostic half of the test now covers.
#[rstest]
fn test_only_an_unrecognised_status_string_is_unknown_and_absence_loads_as_active() {
    let payload = synthetic_markets_body_without_status_evidence();

    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    assert_eq!(infos[0].status(), MarketStatus::Active);
    assert_eq!(infos[0].status_info().source(), MarketStatusSource::Absent);
    assert_eq!(infos[0].status_info().raw(), None);
    assert!(infos[0].is_tradable());

    let instruments =
        parse_instruments(&payload, &[instrument_id(TEST_MARKET)], ts_init()).unwrap();
    assert_eq!(instruments.len(), 1);

    // The unrecognised-value case is the one that stays verbatim and fails closed.
    let halted = parse_instruments(
        &markets_fixture(),
        &[instrument_id(SYNTH_MARKET)],
        ts_init(),
    )
    .unwrap_err();
    assert!(matches!(halted, OndoHttpError::UnknownMarketStatus { .. }));
    assert!(
        halted.to_string().contains("halted"),
        "the diagnostic preserves the raw status: {halted}"
    );
    assert!(
        halted.to_string().contains(SYNTH_MARKET),
        "the diagnostic names the market: {halted}"
    );
    assert!(!halted.to_string().contains("absent"));
}

/// Rule 4 of the 2026-09-14 status ruling: a record carrying a status *marker* the schema cannot
/// classify must not be read as "enabled". It fails the whole load closed as a decode failure, so no
/// market in that payload is trusted -- including one whose status looks classifiable.
#[rstest]
#[case::disabled_as_a_string(r#""disabled": "true""#, "disabled")]
#[case::disabled_as_a_number(r#""disabled": 1"#, "disabled")]
#[case::disabled_as_null(r#""disabled": null"#, "disabled")]
#[case::status_as_a_number(r#""status": 3"#, "status")]
#[case::status_as_null(r#""status": null"#, "status")]
fn test_a_mistyped_or_null_status_marker_fails_closed(#[case] member: &str, #[case] field: &str) {
    let payload = synthetic_markets_body_with_status_member(member);

    let error = parse_instruments(&payload, &[instrument_id(TEST_MARKET)], ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::Decode(_)), "{error}");
    assert!(
        error.to_string().contains(field),
        "the decode error names the offending member: {error}"
    );
}

#[rstest]
fn test_market_infos_expose_increments_status_and_fee_provenance() {
    let payload = markets_fixture();
    let fetched_at = ts_init();
    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(fetched_at)
        .unwrap();

    // Four trading pairs; tokenConfig is spot token configuration and contributes no perps market.
    assert_eq!(infos.len(), 4);
    assert!(
        infos.iter().all(|info| info.market() != "USDC"),
        "tokenConfig must never be read as perps metadata"
    );

    let nvda = info_for(&infos, NVDA_MARKET);
    assert_eq!(nvda.status(), MarketStatus::Active);
    assert_eq!(nvda.status_info().raw(), Some("active"));
    assert_eq!(
        nvda.status_info().source(),
        MarketStatusSource::StatusString
    );
    assert!(nvda.is_tradable());

    let maker = nvda.fees().maker().expect("maker fee present");
    assert_eq!(maker.rate(), decimal("0.0001"));
    assert_eq!(maker.source(), FeeSource::PublicMetadata);
    assert_eq!(maker.field(), "makerFee");
    assert_eq!(maker.origin(), MARKETS_PATH);
    assert_eq!(maker.fetched_at(), Some(fetched_at));
    let taker = nvda.fees().taker().expect("taker fee present");
    assert_eq!(taker.rate(), decimal("0.00025"));
    assert_eq!(taker.field(), "takerFee");

    // A known `disabled` market is classified, keeps its raw string, and is not tradable; it is
    // still a complete instrument, because only an *unclassifiable* status is a hard failure.
    let ena = info_for(&infos, ENA_MARKET);
    assert_eq!(ena.status(), MarketStatus::Disabled);
    assert_eq!(ena.status_info().raw(), Some("disabled"));
    assert!(!ena.is_tradable());
    assert!(ena.instrument(ts_init()).is_ok());
}

#[rstest]
fn test_market_infos_map_quantity_and_price_increments_without_swapping() {
    let payload = markets_fixture();
    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();

    // SYNTH-USD.P is the fixture's deliberately fake market; its raw status `halted` is not an
    // official value, and its increments are the only ones with a three-digit quantity step.
    let synth = info_for(&infos, SYNTH_MARKET);
    assert_eq!(synth.base_increment(), Some(decimal("0.001")));
    assert_eq!(synth.quote_increment(), Some(decimal("0.05")));
    assert_eq!(synth.status(), MarketStatus::Unknown);
    assert_eq!(synth.status_info().raw(), Some("halted"));
    assert!(!synth.is_tradable());
    assert_eq!(synth.underlying_closed(), None);

    // ENA-USD.P is asymmetric the other way round (`baseIncrement` 0.1 vs `quoteIncrement`
    // 0.0001) and is a *known* `disabled` market, so it still builds: quantity comes from the base
    // step and price from the quote step, never the other way round.
    let ena = info_for(&infos, ENA_MARKET);
    let instrument = ena.instrument(ts_init()).unwrap();
    let perp = crypto_perpetual(&instrument);
    assert_eq!(perp.price_increment, Price::from("0.0001"));
    assert_eq!(perp.price_precision, 4);
    assert_eq!(perp.size_increment, Quantity::from("0.1"));
    assert_eq!(perp.size_precision, 1);

    // The unknown-status market is never built, whatever its increments are: there is no
    // tradability to gate on, so it is a fail-closed error and not an instrument.
    let error = synth.instrument(ts_init()).unwrap_err();
    assert!(matches!(error, OndoHttpError::UnknownMarketStatus { .. }));
    assert!(
        error.to_string().contains("halted"),
        "error preserves the raw status: {error}"
    );
}

#[rstest]
fn test_fee_source_separates_account_metadata_and_documentation_assumptions() {
    let fetched_at = ts_init();
    // No account-scoped fee endpoint exists to name here. Across the frozen REST spec's 72
    // operations only one schema declares `makerFee`/`takerFee` - `Contract` - and only one
    // endpoint returns it, the *public* `GET /v1/perps/contracts`. So an account-reported rate has
    // no documented source yet and this origin stays an explicit placeholder rather than a path
    // that would read as real.
    let accounted = FeeRate::account_reported(
        decimal("0.0002"),
        "makerFee",
        "<account-scoped source: none declared by the spec>",
        fetched_at,
    );
    let documented = FeeRate::documentation_assumption(
        decimal("0.00025"),
        "ONDO_TAKER_FEE_BPS",
        "https://docs.ondoperps.xyz/fees.md",
    );

    assert_eq!(accounted.source(), FeeSource::AccountReported);
    assert_eq!(accounted.fetched_at(), Some(fetched_at));
    assert_eq!(documented.source(), FeeSource::DocumentationAssumption);
    assert_eq!(
        documented.fetched_at(),
        None,
        "a dated documentation assumption carries no observation time"
    );
    assert_ne!(accounted.source(), documented.source());
    assert_ne!(accounted.source(), FeeSource::PublicMetadata);
}

// ------------------------------------------------------------------------------------------------
// Fail-closed boundaries
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_parse_instruments_fails_when_a_requested_increment_is_missing() {
    let payload = synthetic_markets_body_without_quote_increment();

    let infos = parse_markets(&payload)
        .unwrap()
        .market_infos(ts_init())
        .unwrap();
    assert_eq!(infos[0].quote_increment(), None);
    assert_eq!(infos[0].base_increment(), Some(decimal("0.01")));

    let error = parse_instruments(&payload, &[instrument_id(TEST_MARKET)], ts_init()).unwrap_err();

    assert!(matches!(
        error,
        OndoHttpError::MissingField {
            field: "quoteIncrement",
            ..
        }
    ));
    assert!(error.to_string().contains(TEST_MARKET));
}

#[rstest]
#[case::zero_quantity_step("0", "0.01")]
#[case::zero_price_step("0.01", "0")]
#[case::negative_quantity_step("-0.01", "0.01")]
#[case::negative_price_step("0.01", "-0.01")]
fn test_parse_instruments_fails_on_non_positive_increments(
    #[case] base_increment: &str,
    #[case] quote_increment: &str,
) {
    let payload = synthetic_markets_body(base_increment, quote_increment);

    let error = parse_instruments(&payload, &[instrument_id(TEST_MARKET)], ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::InvalidField { .. }));
    assert!(
        error.to_string().contains("increment"),
        "error names the offending increment: {error}"
    );
}

#[rstest]
fn test_parse_instruments_with_no_load_ids_loads_every_market_in_the_response() {
    let payload = synthetic_markets_body("0.01", "0.01");

    let instruments = parse_instruments(&payload, &[], ts_init()).unwrap();

    assert_eq!(instruments.len(), 1);
    assert_eq!(
        crypto_perpetual(&instruments[0]).id.to_string(),
        "TEST-USD-PERP.ONDO",
    );
}

#[rstest]
fn test_parse_instruments_with_no_load_ids_fails_on_an_unknown_status_market() {
    // With no `load_ids` every market in the response is selected, so the fixture's fake `halted`
    // market is part of the load and fails it closed rather than being skipped.
    let payload = markets_fixture();

    let error = parse_instruments(&payload, &[], ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::UnknownMarketStatus { .. }));
    assert!(
        error.to_string().contains(SYNTH_MARKET),
        "error names the offending market: {error}"
    );
    assert!(
        error.to_string().contains("halted"),
        "error preserves the raw status: {error}"
    );

    // The very same body is fine when the request names what it wants: an unrequested
    // unknown-status market is not fatal to a specific load.
    assert!(parse_instruments(&payload, &[instrument_id(NVDA_MARKET)], ts_init()).is_ok());
}

#[rstest]
#[case::empty_trading_pairs(
    r#"{"success": true, "result": {"perps": {"tradingPairs": []}, "tokenConfig": []}}"#
)]
#[case::absent_trading_pairs(r#"{"success": true, "result": {"perps": {}}}"#)]
fn test_parse_markets_fails_on_an_empty_result(#[case] payload: &str) {
    let error = parse_markets(payload).unwrap_err();

    assert!(matches!(error, OndoHttpError::EmptyResult));
}

#[rstest]
#[case::absent_result(r#"{"success": true}"#)]
#[case::null_result(r#"{"success": true, "result": null}"#)]
fn test_parse_markets_fails_when_result_is_absent(#[case] payload: &str) {
    let error = parse_markets(payload).unwrap_err();

    assert!(matches!(
        error,
        OndoHttpError::MissingField {
            field: "result",
            ..
        }
    ));
}

#[rstest]
fn test_parse_markets_fails_when_the_envelope_reports_failure() {
    let payload = r#"{"success": false, "result": null}"#;

    let error = parse_markets(payload).unwrap_err();

    assert!(matches!(error, OndoHttpError::Unsuccessful));
}

#[rstest]
#[case::success_omitted("")]
#[case::success_null(r#""success": null,"#)]
fn test_parse_markets_accepts_a_body_that_omits_the_success_flag(#[case] success_member: &str) {
    let payload = synthetic_markets_body_with_success_member(success_member);

    // `success` is an `Option<bool>` with `#[serde(default)]`, so only an explicit `false` is a
    // failure: a body that leaves the member out, or sends `null`, alongside a valid `result` is
    // accepted. Pinned here so the leniency is deliberate rather than accidental.
    let response = parse_markets(&payload).unwrap();
    assert_eq!(response.success, None);
    assert_eq!(response.trading_pairs().len(), 1);

    let instruments =
        parse_instruments(&payload, &[instrument_id(TEST_MARKET)], ts_init()).unwrap();
    assert_eq!(instruments.len(), 1);
}

#[rstest]
fn test_parse_markets_fails_on_a_malformed_body() {
    let error = parse_markets("not json").unwrap_err();

    assert!(matches!(error, OndoHttpError::Decode(_)));
}

#[rstest]
fn test_parse_instruments_fails_when_the_response_carries_no_market_data() {
    let payload = r#"{"success": true}"#;

    let error = parse_instruments(payload, &[instrument_id(NVDA_MARKET)], ts_init()).unwrap_err();

    assert!(matches!(error, OndoHttpError::MissingField { .. }));
}

#[rstest]
fn test_token_config_is_parsed_but_never_used_as_perps_metadata() {
    let payload = markets_fixture();
    let response: MarketsResponse = parse_markets(&payload).unwrap();

    let token_config = response.token_config();
    assert_eq!(token_config.len(), 1);
    assert_eq!(token_config[0].id, "USDC");
    assert_eq!(token_config[0].decimals, Some(2));
    assert_eq!(token_config[0].token_decimals, Some(6));

    let infos = response.market_infos(ts_init()).unwrap();
    assert_eq!(infos.len(), 4);
}

// ------------------------------------------------------------------------------------------------
// The private (authenticated) read schemas
//
// No authenticated response has ever been observed - no sandbox key existed during the protocol
// freeze - so every payload here is authored from the documented shape and marked as such. What is
// asserted is the contract a later task depends on: the envelope, the preserved cursor, the raw
// HTTP status, and the decimal strings that stay the venue's own lexemes.
// ------------------------------------------------------------------------------------------------

/// A synthetic `GET /v1/perps/fills` body: the `GenericResponse` envelope the other endpoints use,
/// a cursor, and one `ApiFill` whose members are the protocol table's.
fn synthetic_fills_body(direction: &str) -> String {
    format!(
        r#"{{"_fixture":{{"kind":"synthetic","note":"authored from test_data/README.md, not observed"}},"success":true,"cursor":"opaque-page-2","result":[{{"id":"fill-1","orderId":"order-1","clientOrderId":"ondo_probe_example_1","parentOrderID":null,"market":"NVDA-USD.P","price":"212.2299496152638348004712442969639146","size":"0.01","side":"buy","direction":"{direction}","filledCost":"2.122299496152638348004712442969639146","fee":"0.00025","feeRebate":"0","pnl":"-0.4","time":"2026-09-14T11:09:59.570112122Z","isMaker":true,"isADL":false}}]}}"#,
    )
}

#[rstest]
fn test_a_private_read_keeps_the_http_status_the_cursor_and_the_decimal_lexemes() {
    let body = synthetic_fills_body("openLong");

    let response = OndoPrivateResponse::decode(200, body.as_bytes()).unwrap();

    assert_eq!(response.http_status(), 200);
    assert_eq!(response.success(), Some(true));
    assert_eq!(response.cursor(), Some("opaque-page-2"));
    assert_eq!(response.cursor_field(), Some("cursor"));

    // The item text is the venue's own: a 37-significant-digit lexeme is never rounded, and the
    // `_fixture` block survives inside the envelope without being read as schema data.
    let items = response.items().unwrap();
    assert_eq!(items.len(), 1);
    assert!(
        items[0]
            .get()
            .contains(r#""price":"212.2299496152638348004712442969639146""#),
        "{}",
        items[0].get(),
    );

    let fills = response.fills().unwrap();
    assert_eq!(fills.len(), 1);
    let fill = &fills[0];
    assert_eq!(fill.id(), "fill-1");
    assert_eq!(fill.order_id(), "order-1");
    assert_eq!(fill.client_order_id(), Some("ondo_probe_example_1"));
    assert_eq!(fill.parent_order_id(), None);
    assert_eq!(fill.market(), "NVDA-USD.P");
    assert_eq!(
        fill.price(),
        Some("212.2299496152638348004712442969639146"),
        "a decimal string is returned exactly as the venue sent it",
    );
    assert_eq!(
        fill.filled_cost(),
        Some("2.122299496152638348004712442969639146")
    );
    assert_eq!(fill.fee(), Some("0.00025"));
    assert_eq!(fill.fee_rebate(), Some("0"));
    assert_eq!(fill.pnl(), Some("-0.4"));
    assert_eq!(fill.time(), Some("2026-09-14T11:09:59.570112122Z"));
    assert_eq!(fill.is_maker(), Some(true));
    assert_eq!(fill.is_adl(), Some(false));
    assert!(fill.direction().is_open());
}

/// `conflicts.md` conflict 4: the REST schema names camelCase directions in its `enum` and spaced
/// ones in its own `description`, and the WS schema uses the spaced form. Both are read, the
/// normalisation is one whitespace/underscore boundary, and anything else fails closed with the raw
/// value attached.
#[rstest]
#[case::camel_case("openLong", "open_long")]
#[case::spaced("open long", "open_long")]
#[case::camel_short("flipLongToShort", "flip_long_to_short")]
#[case::spaced_short("flip short to long", "flip_short_to_long")]
#[case::close_spaced("close short", "close_short")]
fn test_both_documented_direction_spellings_normalise_to_one(
    #[case] raw: &str,
    #[case] expected: &str,
) {
    let fills = OndoPrivateResponse::decode(200, synthetic_fills_body(raw).as_bytes())
        .unwrap()
        .fills()
        .unwrap();

    assert_eq!(fills[0].direction().as_str(), expected, "raw `{raw}`");
}

#[rstest]
#[case::invented("openLonger")]
#[case::open("open")]
#[case::empty("")]
#[case::numeric("1")]
fn test_an_unrecognised_direction_fails_closed_with_the_raw_value(#[case] raw: &str) {
    let error = OndoPrivateResponse::decode(200, synthetic_fills_body(raw).as_bytes())
        .unwrap()
        .fills()
        .expect_err("an unrecognised direction is never mapped to a default");

    match &error {
        OndoHttpError::InvalidField { field, value, .. } => {
            assert_eq!(*field, "direction");
            assert_eq!(value, raw, "the raw value is attached");
        }
        other => panic!("expected a named invalid-field error, was {other:?}"),
    }
}

#[rstest]
#[case::missing_id(
    r#"{"success":true,"result":[{"orderId":"o","market":"NVDA-USD.P","direction":"openLong"}]}"#
)]
#[case::missing_order_id(
    r#"{"success":true,"result":[{"id":"f","market":"NVDA-USD.P","direction":"openLong"}]}"#
)]
#[case::missing_direction(
    r#"{"success":true,"result":[{"id":"f","orderId":"o","market":"NVDA-USD.P"}]}"#
)]
#[case::wrong_typed_direction(
    r#"{"success":true,"result":[{"id":"f","orderId":"o","market":"NVDA-USD.P","direction":7}]}"#
)]
fn test_a_fill_that_is_not_the_documented_shape_fails_closed(#[case] body: &str) {
    let error = OndoPrivateResponse::decode(200, body.as_bytes())
        .unwrap()
        .fills()
        .expect_err("an incomplete ApiFill is not a fill");

    assert!(
        matches!(
            error,
            OndoHttpError::Decode(_) | OndoHttpError::MissingField { .. }
        ),
        "was {error:?}",
    );
}

/// The cursor member is read from the documented `PageInfo` member name (`nextCursor`) and from the
/// other spellings the read surface accepts, records which one it used, and fails closed on one it
/// cannot read.
#[rstest]
#[case::next_cursor(r#"{"success":true,"next_cursor":"c2","result":[]}"#, "next_cursor")]
#[case::next(r#"{"success":true,"next":"c2","result":[]}"#, "next")]
#[case::nested_inside_result(
    r#"{"success":true,"result":{"nextCursor":"c2","items":[]}}"#,
    "nextCursor"
)]
fn test_the_cursor_member_is_read_and_recorded(#[case] body: &str, #[case] field: &str) {
    let response = OndoPrivateResponse::decode(200, body.as_bytes()).unwrap();

    assert_eq!(response.cursor(), Some("c2"));
    assert_eq!(response.cursor_field(), Some(field));
}

/// The **documented** paged-list shape, in the composition the frozen spec gives it.
///
/// `GET /v1/perps/orders` and `GET /v1/perps/fills` are both
/// `allOf: [GenericResponse, {properties: {result: array<...>, pageInfo: PageInfo}}]`, and
/// `PageInfo` is `{prevCursor, nextCursor}`. So `result` is the array and `pageInfo` is its
/// **sibling at the envelope root** - not a member inside `result`.
///
/// This is the shape that made a paginated read stop after one page: a cursor searched for only at
/// the root and inside an *object* `result` never saw the documented `pageInfo.nextCursor`, so the
/// walk read `(None, None)` as "no more pages" and the rest of the history was dropped without an
/// error. The other cursor cases above use shapes the spec does not document, which is why none of
/// them could catch it.
#[rstest]
#[case::the_documented_next_cursor(
    r#"{"success":true,"result":[{"id":"fill-1"}],"pageInfo":{"prevCursor":"back","nextCursor":"forward"}}"#,
    Some(("forward", "nextCursor")),
)]
#[case::an_empty_page_still_carries_the_cursor(
    r#"{"success":true,"result":[],"pageInfo":{"nextCursor":"forward"}}"#,
    Some(("forward", "nextCursor")),
)]
#[case::a_last_page_carries_only_the_previous_cursor(
    r#"{"success":true,"result":[{"id":"fill-1"}],"pageInfo":{"prevCursor":"back"}}"#,
    None
)]
#[case::a_null_page_info_is_the_end_of_the_history(
    r#"{"success":true,"result":[{"id":"fill-1"}],"pageInfo":null}"#,
    None
)]
#[case::the_root_still_wins_over_page_info(
    r#"{"success":true,"cursor":"root","result":[],"pageInfo":{"nextCursor":"forward"}}"#,
    Some(("root", "cursor")),
)]
fn test_the_documented_page_info_carries_the_cursor(
    #[case] body: &str,
    #[case] expected: Option<(&str, &str)>,
) {
    let response = OndoPrivateResponse::decode(200, body.as_bytes()).unwrap();

    assert_eq!(
        response.cursor(),
        expected.map(|(cursor, _field)| cursor),
        "body: {body}",
    );
    assert_eq!(
        response.cursor_field(),
        expected.map(|(_cursor, field)| field),
        "the recorded member is the one the cursor was read from (body: {body})",
    );
}

/// A `pageInfo` that is present but cannot be read is not the end of the history either.
///
/// Same rule as an unreadable cursor member: a truncated walk must never look like a completed one,
/// so a `pageInfo` carrying a non-string `nextCursor` fails closed with that member named.
#[rstest]
#[case::numeric(r#"{"success":true,"result":[],"pageInfo":{"nextCursor":7}}"#)]
#[case::blank(r#"{"success":true,"result":[],"pageInfo":{"nextCursor":" "}}"#)]
#[case::null_member(r#"{"success":true,"result":[],"pageInfo":{"nextCursor":null}}"#)]
fn test_an_unreadable_page_info_cursor_fails_closed(#[case] body: &str) {
    let error = OndoPrivateResponse::decode(200, body.as_bytes())
        .expect_err("an unreadable cursor is not the end of the history");

    assert!(
        matches!(
            error,
            OndoHttpError::InvalidField {
                field: "cursor",
                ..
            }
        ),
        "was {error:?}",
    );
}

#[rstest]
fn test_an_empty_page_and_an_absent_result_are_different_answers() {
    // An empty list is a legitimate page: an account with no fills is not a failure, and no cursor
    // member means there is no next page.
    let empty = OndoPrivateResponse::decode(200, br#"{"success":true,"result":[]}"#)
        .expect("an empty page is a legitimate answer");
    assert_eq!(empty.cursor(), None);
    assert_eq!(empty.cursor_field(), None);
    assert!(empty.items().unwrap().is_empty());

    // A cursor member that *is* present but unusable is reported, never read as "no more pages":
    // that would silently look like the end of the history.
    let unusable = OndoPrivateResponse::decode(200, br#"{"success":true,"cursor":"","result":[]}"#)
        .expect_err("an empty cursor member is not a cursor");
    assert!(
        matches!(
            unusable,
            OndoHttpError::InvalidField {
                field: "cursor",
                ..
            }
        ),
        "was {unusable:?}",
    );

    let absent = OndoPrivateResponse::decode(200, br#"{"success":true}"#)
        .expect_err("a successful private read always carries `result`");
    assert!(
        matches!(
            absent,
            OndoHttpError::MissingField {
                field: "result",
                ..
            }
        ),
        "was {absent:?}",
    );

    let unsuccessful = OndoPrivateResponse::decode(
        200,
        br#"{"success":false,"result":null,"code":"api_key_not_found"}"#,
    )
    .expect_err("an unsuccessful envelope is a failure even on HTTP 200");
    assert!(
        matches!(unsuccessful, OndoHttpError::Unsuccessful),
        "was {unsuccessful:?}",
    );
}

/// The request target of a private read is built once, in a fixed order, so the signature and the
/// wire bytes are the same string.
#[rstest]
fn test_the_private_read_target_is_built_once_in_a_fixed_order() {
    let query = OndoPrivateReadQuery::new()
        .with_cursor("opaque-token")
        .with_limit(100)
        .with_market("NVDA-USD.P");

    assert_eq!(
        query.target(FILLS_PATH).as_str(),
        "/v1/perps/fills?market=NVDA-USD.P&limit=100&cursor=opaque-token",
    );
    assert_eq!(query.market(), Some("NVDA-USD.P"));
    assert_eq!(query.limit(), Some(100));
    assert_eq!(query.cursor(), Some("opaque-token"));

    // An account read takes no filters, and its path is declared once in `http::private` and
    // asserted here so a change to it is deliberate. It is `/v1/account`: the frozen REST spec
    // (`docs/api-reference/rest-spec.json`, sha256 `860a96ca…`) declares that path and declares no
    // `/v1/perps/account` at all. Documented, and still unverified - no host has answered it.
    assert_eq!(
        OndoPrivateReadQuery::default()
            .target(ACCOUNT_PATH)
            .as_str(),
        ACCOUNT_PATH
    );
    assert_eq!(ACCOUNT_PATH, "/v1/account");
}
