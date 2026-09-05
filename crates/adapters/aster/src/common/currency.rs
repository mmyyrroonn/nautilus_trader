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

//! Currency resolution for venue-sourced asset codes.
//!
//! Aster lists assets the Nautilus currency map has never heard of: a live testnet
//! `GET /fapi/v3/balance` answers with `USDT`, `BTC`, `ASTER` and `AFEE`. `Currency::from`
//! panics on an unknown code, which would take the whole trading node down over an airdrop
//! or a fee-credit asset the account never trades, so every currency built from a venue
//! string goes through [`resolve_currency`] instead.

use nautilus_model::{enums::CurrencyType, types::Currency};

/// Decimal precision assigned to dynamically registered Aster assets.
///
/// Aster reports balances with 8 decimals; the value is also what the core
/// `Currency::get_or_create_crypto` helper uses for unknown crypto assets.
const DYNAMIC_CURRENCY_PRECISION: u8 = 8;

/// Returns the [`Currency`] for a venue-reported asset `code`, registering it when unknown.
///
/// A code already in the global currency map is returned as-is, preserving its registered
/// precision. An unknown code is registered as an 8-decimal [`CurrencyType::Crypto`] currency
/// and logged at debug level; the registration is a no-op on every later call, so the log line
/// appears once per process for each new asset.
///
/// This never panics. A code that cannot form a valid currency at all (empty, or invalid
/// UTF-8 after trimming) falls back to `USDT`, Aster's settlement asset, with a warning.
#[must_use]
pub fn resolve_currency(code: &str) -> Currency {
    let code = code.trim();

    if let Some(currency) = Currency::try_from_str(code) {
        return currency;
    }

    match Currency::new_checked(
        code,
        DYNAMIC_CURRENCY_PRECISION,
        0,
        code,
        CurrencyType::Crypto,
    ) {
        Ok(currency) => {
            if let Err(e) = Currency::register(currency, false) {
                log::warn!("Failed to register Aster asset '{code}': {e}");
            } else {
                log::debug!(
                    "Registered unknown Aster asset '{code}' as crypto with precision \
                     {DYNAMIC_CURRENCY_PRECISION}"
                );
            }
            currency
        }
        Err(e) => {
            log::warn!("Unusable Aster asset code '{code}', falling back to USDT: {e}");
            Currency::USDT()
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::CurrencyType, types::Currency};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_known_code_keeps_registered_definition() {
        let usdt = resolve_currency("USDT");

        assert_eq!(usdt, Currency::USDT());
        assert_eq!(usdt.precision, Currency::USDT().precision);
    }

    #[rstest]
    fn test_code_is_trimmed() {
        assert_eq!(resolve_currency("  USDT "), Currency::USDT());
    }

    #[rstest]
    #[case("ASTER")]
    #[case("AFEE")]
    fn test_unknown_code_is_registered_as_crypto(#[case] code: &str) {
        let currency = resolve_currency(code);

        assert_eq!(currency.code.as_str(), code);
        assert_eq!(currency.precision, DYNAMIC_CURRENCY_PRECISION);
        assert_eq!(currency.currency_type, CurrencyType::Crypto);
        assert_eq!(currency.iso4217, 0);

        // Registered, so the second call resolves through the map rather than re-creating.
        assert_eq!(Currency::try_from_str(code), Some(currency));
        assert_eq!(resolve_currency(code), currency);
    }

    #[rstest]
    #[case("")]
    #[case("   ")]
    fn test_unusable_code_falls_back_to_usdt(#[case] code: &str) {
        assert_eq!(resolve_currency(code), Currency::USDT());
    }
}
