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

//! Decimal, timestamp and symbol parsing shared by the HTTP and WebSocket layers.
//!
//! Every discrete value the venue sends is a decimal string. These helpers convert such a
//! string exactly, without a floating-point round trip, and reject anything that is not the
//! exact form the protocol table describes.

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::common::consts::{
    ONDO_PERP_MARKET_SUFFIX, ONDO_PERP_SYMBOL_MARKER, ONDO_QUOTE_SYMBOL, ONDO_VENUE,
};

/// Parses an RFC 3339 timestamp into [`UnixNanos`], preserving all nine fractional digits.
///
/// The subsecond field is parsed as an exact count of nanoseconds, so
/// `2026-09-14T11:09:59.570112122Z` keeps its trailing `122` ns. An implementation that routes
/// this string through a microsecond-resolution datetime (Python's `datetime`, for example)
/// silently loses those digits, which is not acceptable for `ts_event`.
///
/// # Errors
///
/// Returns an error if the value is not a valid RFC 3339 datetime, if it is before the Unix
/// epoch, or if it cannot be represented as [`UnixNanos`].
pub fn parse_timestamp(value: &str) -> anyhow::Result<UnixNanos> {
    let timestamp = value
        .parse::<Timestamp>()
        .with_context(|| format!("invalid RFC 3339 timestamp `{value}`"))?;
    let nanos = timestamp.as_nanosecond();
    ensure!(nanos >= 0, "timestamp `{value}` is before the Unix epoch");
    let nanos = u64::try_from(nanos)
        .with_context(|| format!("timestamp `{value}` is outside the UnixNanos range"))?;

    Ok(UnixNanos::from(nanos))
}

/// Splits an Ondo Perps market string into its base and quote tokens.
///
/// The venue's scheme is `<BASE>-<QUOTE>.P`, as in `NVDA-USD.P`.
///
/// # Errors
///
/// Returns an error when the string does not carry exactly one perps product marker, when the
/// base or quote token is empty, when the base token contains characters that are not ASCII
/// alphanumeric or `-`, or when the base token contains `.` (which would make the mapping to an
/// `InstrumentId` ambiguous).
pub fn split_market(market: &str) -> anyhow::Result<(&str, &str)> {
    let pair = market
        .strip_suffix(ONDO_PERP_MARKET_SUFFIX)
        .with_context(|| {
            format!("market `{market}` does not carry the perps marker `{ONDO_PERP_MARKET_SUFFIX}`")
        })?;

    let (base, quote) = pair.split_once('-').with_context(|| {
        format!("market `{market}` is not in the form `<BASE>-<QUOTE>{ONDO_PERP_MARKET_SUFFIX}`")
    })?;

    ensure!(
        !base.is_empty() && !quote.is_empty(),
        "market `{market}` has an empty base or quote token"
    );
    ensure!(
        !base.contains('-'),
        "market `{market}` base token `{base}` is ambiguous"
    );
    ensure!(
        base.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "market `{market}` base token `{base}` contains unsupported characters"
    );
    ensure!(
        !quote.contains('.'),
        "market `{market}` quote token `{quote}` is ambiguous"
    );
    ensure!(
        quote == ONDO_QUOTE_SYMBOL,
        "market `{market}` is quoted in `{quote}`; only {ONDO_QUOTE_SYMBOL}-quoted perps are supported"
    );

    Ok((base, quote))
}

/// Maps an Ondo Perps market string onto a Nautilus [`InstrumentId`].
///
/// Nautilus needs a product marker because the same base token can appear in more than one
/// product family, so `NVDA-USD.P` becomes `NVDA-USD-PERP` at the [`ONDO_VENUE`]. The venue's
/// own string stays available through the instrument's `raw_symbol`.
///
/// # Errors
///
/// Returns an error for any market string that is not exactly the supported form; an unknown
/// market or product type is never renamed into an existing instrument.
pub fn market_to_instrument_id(market: &str) -> anyhow::Result<InstrumentId> {
    let (base, quote) = split_market(market)?;
    let symbol = Symbol::new_checked(format!("{base}-{quote}-{ONDO_PERP_SYMBOL_MARKER}"))
        .with_context(|| format!("market `{market}` does not map to a valid Nautilus symbol"))?;

    Ok(InstrumentId::new(symbol, *ONDO_VENUE))
}

/// Maps a Nautilus [`InstrumentId`] back onto the venue's market string.
///
/// The inverse of [`market_to_instrument_id`], and the only place an order's `market` member is
/// built: an instrument that is not an Ondo Perps instrument of the form the adapter itself created
/// is refused, never renamed into a market the venue might have (plan §4.1: an unknown market is not
/// mapped onto an existing instrument).
///
/// # Errors
///
/// Returns an error when the venue is not [`ONDO_VENUE`], when the symbol does not end with
/// `-{ONDO_QUOTE_SYMBOL}-{ONDO_PERP_SYMBOL_MARKER}`, when the base token is empty or contains `.` or
/// `-`, or when the base token contains characters outside `A-Z a-z 0-9 -`.
pub fn instrument_id_to_market(instrument_id: &InstrumentId) -> anyhow::Result<String> {
    ensure!(
        instrument_id.venue == *ONDO_VENUE,
        "instrument `{instrument_id}` is not at the {} venue",
        *ONDO_VENUE
    );

    let symbol = instrument_id.symbol.as_str();
    let suffix = format!("-{ONDO_QUOTE_SYMBOL}-{ONDO_PERP_SYMBOL_MARKER}");
    let base = symbol.strip_suffix(&suffix).with_context(|| {
        format!(
            "instrument symbol `{symbol}` does not carry the `{ONDO_QUOTE_SYMBOL}-{ONDO_PERP_SYMBOL_MARKER}` marker"
        )
    })?;

    ensure!(
        !base.is_empty(),
        "instrument `{instrument_id}` has an empty base token"
    );
    ensure!(
        !base.contains('.') && !base.contains('-'),
        "instrument `{instrument_id}` base token `{base}` is ambiguous"
    );
    ensure!(
        base.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "instrument `{instrument_id}` base token `{base}` contains unsupported characters"
    );

    Ok(format!(
        "{base}-{ONDO_QUOTE_SYMBOL}{ONDO_PERP_MARKET_SUFFIX}"
    ))
}

/// Parses a wire decimal string exactly.
///
/// [`Decimal::from_str_exact`] is the constructor used, never `Decimal::from_str`: a lexeme
/// carrying more significant digits than [`Decimal`] holds fails here instead of being silently
/// rounded. Exponent notation is consequently not accepted either, because `Decimal::from_str`
/// reaches it only through a lossy fallback.
///
/// # Errors
///
/// Returns an error if `value` is not an exact decimal, including a decimal whose significant
/// digits do not fit in [`Decimal`].
pub fn parse_decimal(value: &str, field: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str_exact(value)
        .with_context(|| format!("`{field}` value `{value}` is not an exact decimal"))
}

/// Parses a price step (tick size) from a wire decimal string.
///
/// The precision of the returned [`Price`] is the scale the venue declared for the increment
/// itself, never the scale of an observed price.
///
/// # Errors
///
/// Returns an error if the value is not a decimal, is zero or negative, or cannot be
/// represented as a [`Price`].
pub fn parse_price_increment(value: &str, field: &str) -> anyhow::Result<Price> {
    let decimal = parse_decimal(value, field)?;
    ensure!(
        decimal.is_sign_positive() && !decimal.is_zero(),
        "`{field}` value `{value}` is not a positive price increment"
    );
    Price::from_decimal(decimal)
        .with_context(|| format!("`{field}` value `{value}` is not a representable Price step"))
}

/// Parses a quantity step (lot size) from a wire decimal string.
///
/// The precision of the returned [`Quantity`] is the scale the venue declared for the increment
/// itself.
///
/// # Errors
///
/// Returns an error if the value is not a decimal, is zero or negative, or cannot be
/// represented as a [`Quantity`].
pub fn parse_quantity_increment(value: &str, field: &str) -> anyhow::Result<Quantity> {
    let decimal = parse_decimal(value, field)?;
    ensure!(
        decimal.is_sign_positive() && !decimal.is_zero(),
        "`{field}` value `{value}` is not a positive quantity increment"
    );
    Quantity::from_decimal(decimal)
        .with_context(|| format!("`{field}` value `{value}` is not a representable Quantity step"))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_parse_timestamp_preserves_nanoseconds() {
        let ts = parse_timestamp("2026-09-14T11:09:59.570112122Z").unwrap();

        assert_eq!(ts.as_u64(), 1_789_384_199_570_112_122);
        assert_eq!(ts.as_u64() % 1_000, 122);
    }

    #[rstest]
    #[case::empty("")]
    #[case::plain_text("not-a-timestamp")]
    #[case::date_only("2026-09-14")]
    #[case::missing_offset("2026-09-14T11:09:59")]
    #[case::before_epoch("1960-01-01T00:00:00Z")]
    #[case::numeric_seconds("1789384199.5")]
    fn test_parse_timestamp_rejects_non_rfc3339_values(#[case] value: &str) {
        assert!(parse_timestamp(value).is_err());
    }

    #[rstest]
    #[case::canonical("NVDA-USD.P", "NVDA-USD-PERP.ONDO")]
    #[case::tokenized_equity("TSLA-USD.P", "TSLA-USD-PERP.ONDO")]
    fn test_market_to_instrument_id(#[case] market: &str, #[case] expected: &str) {
        assert_eq!(
            market_to_instrument_id(market).unwrap().to_string(),
            expected
        );
    }

    #[rstest]
    #[case::unknown_market_type("NVDA-USD")]
    #[case::unknown_quote("NVDA-USDT.P")]
    #[case::ambiguous_base("BRK.B-USD.P")]
    #[case::two_dashes("NVDA-USD-P.P")]
    #[case::path_like("../../etc/passwd.P")]
    fn test_market_to_instrument_id_rejects_unmappable_markets(#[case] market: &str) {
        assert!(market_to_instrument_id(market).is_err());
    }

    #[rstest]
    fn test_split_market_returns_base_and_quote() {
        assert_eq!(split_market("NVDA-USD.P").unwrap(), ("NVDA", "USD"));
    }

    #[rstest]
    fn test_parse_decimal_is_exact() {
        assert_eq!(
            parse_decimal("0.05", "quoteIncrement").unwrap().to_string(),
            "0.05"
        );
        assert_eq!(
            parse_decimal("0.001", "baseIncrement").unwrap().to_string(),
            "0.001"
        );
        assert_eq!(
            parse_decimal("0.0001", "makerFee").unwrap().to_string(),
            "0.0001"
        );
        // Exponent notation is not a lexeme form the protocol table describes, and `from_str`
        // reached it only through its lossy fallback, so the exact constructor rejects it.
        assert!(parse_decimal("1e-3", "baseIncrement").is_err());
        assert!(parse_decimal("0x10", "baseIncrement").is_err());
    }

    #[rstest]
    #[case::premium_index("-0.000023702373955322580120738856244065276621")]
    #[case::bid("212.22994961526383480047124429696391461")]
    fn test_parse_decimal_rejects_lexemes_wider_than_decimal_holds(#[case] value: &str) {
        // Both lexemes are archived in `test_data/ws/funding_observed.json`; they carry 38
        // significant digits, so a rounding parser would report a value the venue never sent.
        let error = parse_decimal(value, "premiumIndex").unwrap_err();
        let message = error.to_string();

        assert!(
            message.contains("premiumIndex"),
            "the error names the field: {message}"
        );
        assert!(
            message.contains(value),
            "the error carries the offending lexeme: {message}"
        );
    }

    #[rstest]
    fn test_parse_price_increment_uses_the_declared_scale() {
        let increment = parse_price_increment("0.05", "quoteIncrement").unwrap();

        assert_eq!(increment, Price::from("0.05"));
        assert_eq!(increment.precision, 2);
    }

    #[rstest]
    fn test_parse_quantity_increment_uses_the_declared_scale() {
        let increment = parse_quantity_increment("0.001", "baseIncrement").unwrap();

        assert_eq!(increment, Quantity::from("0.001"));
        assert_eq!(increment.precision, 3);
    }

    #[rstest]
    #[case::zero("0")]
    #[case::negative("-0.01")]
    #[case::zero_scale("0.000")]
    fn test_parse_increments_reject_non_positive_values(#[case] value: &str) {
        assert!(parse_price_increment(value, "quoteIncrement").is_err());
        assert!(parse_quantity_increment(value, "baseIncrement").is_err());
    }
}
