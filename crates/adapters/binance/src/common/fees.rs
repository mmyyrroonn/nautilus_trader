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

//! Binance instrument fee fallbacks and per-venue account fee overrides.

use std::sync::{LazyLock, RwLock};

use ahash::AHashMap;
use nautilus_model::identifiers::Venue;
use rust_decimal::Decimal;
use ustr::Ustr;

/// Default Spot maker and taker fee when account rates are unavailable.
pub const BINANCE_SPOT_FEE_DEFAULT: Decimal = Decimal::from_parts(1, 0, 0, false, 3);

/// Returns the documented USD-M VIP maker and taker rates used by legacy parity.
///
/// Tiers above 9 use tier 0 so an unknown venue value cannot silently grant a
/// lower commission estimate.
#[must_use]
pub fn futures_fee_tier_rates(tier: u8) -> (Decimal, Decimal) {
    match tier {
        1 => (Decimal::new(16, 5), Decimal::new(4, 4)),
        2 => (Decimal::new(14, 5), Decimal::new(35, 5)),
        3 => (Decimal::new(12, 5), Decimal::new(32, 5)),
        4 => (Decimal::new(1, 4), Decimal::new(3, 4)),
        5 => (Decimal::new(8, 5), Decimal::new(27, 5)),
        6 => (Decimal::new(6, 5), Decimal::new(25, 5)),
        7 => (Decimal::new(4, 5), Decimal::new(22, 5)),
        8 => (Decimal::new(2, 5), Decimal::new(2, 4)),
        9 => (Decimal::ZERO, Decimal::new(17, 5)),
        _ => (Decimal::new(2, 4), Decimal::new(5, 4)),
    }
}

/// One venue symbol an account fee override applies to.
type FeeOverrideKey = (Venue, Ustr);
/// The registered maker and taker rates.
type FeeOverrideRates = (Decimal, Decimal);
type FeeOverrideMap = AHashMap<FeeOverrideKey, FeeOverrideRates>;

/// Account maker/taker rates registered for one venue's symbols.
///
/// Instrument metadata comes from `exchangeInfo`, which carries no commission data, so the
/// parser fills the fee fields from the account's fee tier — or, with no credentials, from the
/// VIP-0 defaults. A Binance-API-compatible venue whose real rates can only be obtained through
/// its *own* authenticated endpoint (Aster signs with EIP-712, not HMAC, so this client cannot
/// query it) has no way to get those rates into an instrument that this parser rebuilds on every
/// refresh — and the periodic refresh would silently restore the placeholder.
///
/// A venue's execution client registers the rates it verified; the instrument parser then
/// applies them wherever it would otherwise use a fallback. The map is empty unless something
/// registers into it, so Binance itself is unaffected.
static FEE_OVERRIDES: LazyLock<RwLock<FeeOverrideMap>> =
    LazyLock::new(|| RwLock::new(AHashMap::new()));

/// Registers the account's real maker/taker rates for one venue symbol.
///
/// Applies to every instrument load and refresh for that venue from this point on. Call it once
/// the rates have actually been confirmed with the venue: an unverified value registered here is
/// indistinguishable downstream from a measured one.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn register_instrument_fees(
    venue: Venue,
    symbol: Ustr,
    maker_fee: Decimal,
    taker_fee: Decimal,
) {
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .insert((venue, symbol), (maker_fee, taker_fee));
}

/// Removes the registered rates for one venue symbol.
///
/// Used when a rate can no longer be confirmed: leaving an earlier value in place would keep
/// applying it as though it were still verified.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn clear_instrument_fee(venue: Venue, symbol: Ustr) {
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .remove(&(venue, symbol));
}

/// Removes every registered fee override for a venue.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn clear_instrument_fees(venue: Venue) {
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .retain(|(registered, _), _| *registered != venue);
}

/// Returns the registered rates for one venue symbol, if any.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
#[must_use]
pub fn instrument_fees(venue: Venue, symbol: &str) -> Option<FeeOverrideRates> {
    FEE_OVERRIDES
        .read()
        .expect("fee override registry is poisoned")
        .get(&(venue, Ustr::from(symbol)))
        .copied()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    #[rstest]
    #[case(0, dec!(0.0002), dec!(0.0005))]
    #[case(4, dec!(0.0001), dec!(0.0003))]
    #[case(9, dec!(0), dec!(0.00017))]
    #[case(10, dec!(0.0002), dec!(0.0005))]
    fn test_futures_fee_tier_rates(
        #[case] tier: u8,
        #[case] expected_maker: Decimal,
        #[case] expected_taker: Decimal,
    ) {
        assert_eq!(
            futures_fee_tier_rates(tier),
            (expected_maker, expected_taker)
        );
    }

    #[rstest]
    fn test_no_override_is_registered_by_default() {
        // Binance never registers anything, so its instrument fees are untouched.
        assert_eq!(
            instrument_fees(Venue::new(Ustr::from("BINANCE")), "BTCUSDT"),
            None
        );
    }

    #[rstest]
    fn test_override_is_scoped_to_its_venue_and_symbol() {
        let venue = Venue::new(Ustr::from("FEETEST"));
        let other = Venue::new(Ustr::from("FEETESTOTHER"));

        register_instrument_fees(venue, Ustr::from("BTCUSDT"), dec!(0.00005), dec!(0.0004));

        assert_eq!(
            instrument_fees(venue, "BTCUSDT"),
            Some((dec!(0.00005), dec!(0.0004)))
        );
        assert_eq!(instrument_fees(venue, "ETHUSDT"), None);
        assert_eq!(instrument_fees(other, "BTCUSDT"), None);

        clear_instrument_fees(venue);
        assert_eq!(instrument_fees(venue, "BTCUSDT"), None);
    }

    #[rstest]
    fn test_clearing_one_symbol_leaves_the_others_registered() {
        let venue = Venue::new(Ustr::from("FEETESTSYMBOL"));
        register_instrument_fees(venue, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));
        register_instrument_fees(venue, Ustr::from("ETHUSDT"), dec!(0.0003), dec!(0.0004));

        clear_instrument_fee(venue, Ustr::from("BTCUSDT"));

        assert_eq!(instrument_fees(venue, "BTCUSDT"), None);
        assert_eq!(
            instrument_fees(venue, "ETHUSDT"),
            Some((dec!(0.0003), dec!(0.0004)))
        );
        clear_instrument_fees(venue);
    }

    #[rstest]
    fn test_clearing_one_venue_leaves_another_registered() {
        let kept = Venue::new(Ustr::from("FEETESTKEPT"));
        let cleared = Venue::new(Ustr::from("FEETESTCLEARED"));
        register_instrument_fees(kept, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));
        register_instrument_fees(cleared, Ustr::from("BTCUSDT"), dec!(0.0003), dec!(0.0004));

        clear_instrument_fees(cleared);

        assert_eq!(
            instrument_fees(kept, "BTCUSDT"),
            Some((dec!(0.0001), dec!(0.0002)))
        );
        assert_eq!(instrument_fees(cleared, "BTCUSDT"), None);
        clear_instrument_fees(kept);
    }
}
