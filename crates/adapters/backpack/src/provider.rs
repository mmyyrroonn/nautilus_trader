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

//! Transport-independent complete refreshes for an explicit instrument allowlist.

use std::collections::BTreeMap;

use nautilus_core::UnixNanos;
use nautilus_model::identifiers::InstrumentId;

use crate::{
    config::BackpackConfig,
    instruments::BackpackInstrumentMetadata,
    models::BackpackMarket,
    parsing::{BackpackInstrumentError, parse_market},
};

/// A fail-closed metadata provider with no network or credential ownership.
#[derive(Debug)]
pub struct BackpackInstrumentProvider {
    config: BackpackConfig,
    markets: BTreeMap<InstrumentId, BackpackInstrumentMetadata>,
}

impl BackpackInstrumentProvider {
    /// Creates an empty provider; configuration has already been validated.
    #[must_use]
    pub fn new(config: BackpackConfig) -> Self {
        Self {
            config,
            markets: BTreeMap::new(),
        }
    }

    /// Replaces the complete allowlisted market snapshot from a fresh public response.
    ///
    /// Unlisted markets are filtered before product parsing. The complete allowlist
    /// must be covered exactly once. A failed refresh invalidates previous metadata;
    /// no partial or stale snapshot remains available through the provider.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid selected market, duplicate, or missing market.
    pub fn replace_markets(
        &mut self,
        markets: &[BackpackMarket],
        received_at: UnixNanos,
    ) -> Result<(), BackpackInstrumentError> {
        self.invalidate();
        let mut replacement = BTreeMap::new();

        for market in markets {
            if !self.config.symbols().contains(&market.symbol) {
                continue;
            }

            let metadata = parse_market(market, &self.config, received_at)?;

            if replacement
                .insert(metadata.instrument_id, metadata)
                .is_some()
            {
                return Err(BackpackInstrumentError::DuplicateMarket(
                    market.symbol.clone(),
                ));
            }
        }

        for symbol in self.config.symbols() {
            if !replacement
                .values()
                .any(|market| market.raw_symbol.as_str() == symbol)
            {
                return Err(BackpackInstrumentError::MissingMarket(symbol.clone()));
            }
        }

        self.markets = replacement;
        Ok(())
    }

    /// Invalidates the current snapshot after a transport or response-decoding failure.
    pub fn invalidate(&mut self) {
        self.markets.clear();
    }

    /// Returns metadata only from the latest successfully validated complete refresh.
    #[must_use]
    pub fn get(&self, instrument_id: &InstrumentId) -> Option<&BackpackInstrumentMetadata> {
        self.markets.get(instrument_id)
    }

    /// Iterates over the latest complete validated snapshot.
    pub fn all(&self) -> impl Iterator<Item = &BackpackInstrumentMetadata> {
        self.markets.values()
    }
}
