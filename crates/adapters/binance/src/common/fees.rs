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

/// Identifies the account whose fees are registered, and the endpoint they apply to.
///
/// The endpoint is what separates one account's rates from another's: two clients for different
/// accounts on the same venue reach it through different base URLs (mainnet and testnet, or two
/// deployments), and the instrument parser that has to apply the rates knows only the URL it is
/// itself talking to. The account is carried alongside so a second account registering against
/// the *same* endpoint is rejected rather than silently overwriting the first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeScope {
    endpoint: Ustr,
    account: Ustr,
}

impl FeeScope {
    /// Creates a fee scope for one account reached at one HTTP base URL.
    #[must_use]
    pub fn new(base_url: &str, account_id: &str) -> Self {
        Self {
            endpoint: normalize_endpoint(base_url),
            account: Ustr::from(account_id),
        }
    }

    /// Returns the normalised endpoint these fees apply to.
    #[must_use]
    pub const fn endpoint(&self) -> Ustr {
        self.endpoint
    }

    /// Returns the account that owns them.
    #[must_use]
    pub const fn account(&self) -> Ustr {
        self.account
    }
}

fn normalize_endpoint(base_url: &str) -> Ustr {
    Ustr::from(base_url.trim_end_matches('/'))
}

/// One endpoint symbol an account fee override applies to.
type FeeOverrideKey = (Ustr, Ustr);
/// The registered maker and taker rates.
type FeeOverrideRates = (Decimal, Decimal);
/// The owning account and its rates.
type FeeOverrideEntry = (Ustr, FeeOverrideRates);
type FeeOverrideMap = AHashMap<FeeOverrideKey, FeeOverrideEntry>;

/// Account maker/taker rates registered per endpoint symbol.
///
/// Instrument metadata comes from `exchangeInfo`, which carries no commission data, so the
/// parser fills the fee fields from the account's fee tier — or, with no credentials, from the
/// VIP-0 defaults. A Binance-API-compatible venue whose real rates can only be obtained through
/// its *own* authenticated endpoint (Aster signs with EIP-712, not HMAC, so this client cannot
/// query it) has no way to get those rates into an instrument that this parser rebuilds on every
/// refresh — and the periodic refresh would silently restore the placeholder.
///
/// A venue's execution client registers the rates it verified; the instrument parser then
/// applies them wherever it would otherwise use a fallback, matching on the base URL it is
/// talking to. Keying on the endpoint rather than the venue is what keeps two accounts on the
/// same venue — mainnet and testnet, or two deployments — from overwriting each other. The map
/// is empty unless something registers into it, so Binance itself is unaffected.
static FEE_OVERRIDES: LazyLock<RwLock<FeeOverrideMap>> =
    LazyLock::new(|| RwLock::new(AHashMap::new()));

/// Registers the account's real maker/taker rates for one symbol at one endpoint.
///
/// Applies to every instrument load and refresh against that endpoint from this point on. Call
/// it once the rates have actually been confirmed with the venue: an unverified value registered
/// here is indistinguishable downstream from a measured one.
///
/// Returns `false` without changing anything when a *different* account already owns the
/// symbol at that endpoint. Two accounts sharing one endpoint cannot both have their rates
/// applied, because the parser has nothing to tell them apart; the conflict is surfaced rather
/// than resolved by last-writer-wins.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn register_instrument_fees(
    scope: &FeeScope,
    symbol: Ustr,
    maker_fee: Decimal,
    taker_fee: Decimal,
) -> bool {
    let mut overrides = FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned");

    let key = (scope.endpoint, symbol);
    if let Some((owner, _)) = overrides.get(&key)
        && *owner != scope.account
    {
        log::error!(
            "Refusing to register {symbol} fees for account {} at {}: account {owner} already \
             owns them, and instrument metadata cannot distinguish the two",
            scope.account,
            scope.endpoint,
        );
        return false;
    }

    overrides.insert(key, (scope.account, (maker_fee, taker_fee)));
    true
}

/// Removes the registered rates for one symbol at one endpoint.
///
/// Used when a rate can no longer be confirmed: leaving an earlier value in place would keep
/// applying it as though it were still verified. Only the owning account can remove them.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn clear_instrument_fee(scope: &FeeScope, symbol: Ustr) {
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .retain(|(endpoint, registered), (owner, _)| {
            !(*endpoint == scope.endpoint && *registered == symbol && *owner == scope.account)
        });
}

/// Removes every fee override an account registered at its endpoint.
///
/// Called when the owning client stops, so a later client for a different account can take the
/// endpoint over.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn clear_scope_fees(scope: &FeeScope) {
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .retain(|(endpoint, _), (owner, _)| {
            !(*endpoint == scope.endpoint && *owner == scope.account)
        });
}

/// Removes every fee override registered against an endpoint, whoever owns it.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
pub fn clear_endpoint_fees(base_url: &str) {
    let endpoint = normalize_endpoint(base_url);
    FEE_OVERRIDES
        .write()
        .expect("fee override registry is poisoned")
        .retain(|(registered, _), _| *registered != endpoint);
}

/// Returns the rates registered for one symbol at one endpoint, if any.
///
/// # Panics
///
/// Panics if the registry lock is poisoned.
#[must_use]
pub fn instrument_fees(base_url: &str, symbol: &str) -> Option<FeeOverrideRates> {
    FEE_OVERRIDES
        .read()
        .expect("fee override registry is poisoned")
        .get(&(normalize_endpoint(base_url), Ustr::from(symbol)))
        .map(|(_, rates)| *rates)
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
        assert_eq!(instrument_fees("https://fapi.binance.com", "BTCUSDT"), None);
    }

    #[rstest]
    fn test_override_is_scoped_to_its_endpoint_and_symbol() {
        let scope = FeeScope::new("https://one.example", "ACCOUNT-A");
        let other = "https://two.example";

        assert!(register_instrument_fees(
            &scope,
            Ustr::from("BTCUSDT"),
            dec!(0.00005),
            dec!(0.0004)
        ));

        assert_eq!(
            instrument_fees("https://one.example", "BTCUSDT"),
            Some((dec!(0.00005), dec!(0.0004)))
        );
        assert_eq!(instrument_fees("https://one.example", "ETHUSDT"), None);
        assert_eq!(instrument_fees(other, "BTCUSDT"), None);

        clear_scope_fees(&scope);
        assert_eq!(instrument_fees("https://one.example", "BTCUSDT"), None);
    }

    #[rstest]
    fn test_trailing_slash_does_not_split_an_endpoint() {
        let scope = FeeScope::new("https://slash.example/", "ACCOUNT-A");
        register_instrument_fees(&scope, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));

        assert_eq!(
            instrument_fees("https://slash.example", "BTCUSDT"),
            Some((dec!(0.0001), dec!(0.0002)))
        );
        clear_scope_fees(&scope);
    }

    #[rstest]
    fn test_two_accounts_on_one_endpoint_do_not_overwrite_each_other() {
        // Nothing downstream could tell the two apart, so the second registration is refused
        // rather than silently replacing the first account's verified rates.
        let first = FeeScope::new("https://shared.example", "ACCOUNT-A");
        let second = FeeScope::new("https://shared.example", "ACCOUNT-B");

        assert!(register_instrument_fees(
            &first,
            Ustr::from("BTCUSDT"),
            dec!(0.0001),
            dec!(0.0002)
        ));
        assert!(!register_instrument_fees(
            &second,
            Ustr::from("BTCUSDT"),
            dec!(0.0009),
            dec!(0.001)
        ));

        assert_eq!(
            instrument_fees("https://shared.example", "BTCUSDT"),
            Some((dec!(0.0001), dec!(0.0002))),
            "the first account keeps its rates",
        );

        // Once the owner releases the endpoint the other account can take it.
        clear_scope_fees(&first);
        assert!(register_instrument_fees(
            &second,
            Ustr::from("BTCUSDT"),
            dec!(0.0009),
            dec!(0.001)
        ));
        assert_eq!(
            instrument_fees("https://shared.example", "BTCUSDT"),
            Some((dec!(0.0009), dec!(0.001)))
        );
        clear_scope_fees(&second);
    }

    #[rstest]
    fn test_a_foreign_account_cannot_clear_an_entry() {
        let owner = FeeScope::new("https://owned.example", "ACCOUNT-A");
        let stranger = FeeScope::new("https://owned.example", "ACCOUNT-B");
        register_instrument_fees(&owner, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));

        clear_instrument_fee(&stranger, Ustr::from("BTCUSDT"));

        assert_eq!(
            instrument_fees("https://owned.example", "BTCUSDT"),
            Some((dec!(0.0001), dec!(0.0002))),
            "only the owning account may drop its own registration",
        );
        clear_scope_fees(&owner);
    }

    #[rstest]
    fn test_clearing_one_symbol_leaves_the_others_registered() {
        let scope = FeeScope::new("https://symbols.example", "ACCOUNT-A");
        register_instrument_fees(&scope, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));
        register_instrument_fees(&scope, Ustr::from("ETHUSDT"), dec!(0.0003), dec!(0.0004));

        clear_instrument_fee(&scope, Ustr::from("BTCUSDT"));

        assert_eq!(instrument_fees("https://symbols.example", "BTCUSDT"), None);
        assert_eq!(
            instrument_fees("https://symbols.example", "ETHUSDT"),
            Some((dec!(0.0003), dec!(0.0004)))
        );
        clear_scope_fees(&scope);
    }

    #[rstest]
    fn test_clearing_one_endpoint_leaves_another_registered() {
        let kept = FeeScope::new("https://kept.example", "ACCOUNT-A");
        let cleared = FeeScope::new("https://cleared.example", "ACCOUNT-B");
        register_instrument_fees(&kept, Ustr::from("BTCUSDT"), dec!(0.0001), dec!(0.0002));
        register_instrument_fees(&cleared, Ustr::from("BTCUSDT"), dec!(0.0003), dec!(0.0004));

        clear_endpoint_fees("https://cleared.example");

        assert_eq!(
            instrument_fees("https://kept.example", "BTCUSDT"),
            Some((dec!(0.0001), dec!(0.0002)))
        );
        assert_eq!(instrument_fees("https://cleared.example", "BTCUSDT"), None);
        clear_scope_fees(&kept);
    }
}
