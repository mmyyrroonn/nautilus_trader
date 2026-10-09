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

//! Finite selected-instrument account witnesses for optional Aster admission.

use std::{collections::BTreeSet, str::FromStr};

use nautilus_model::{
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use serde::{Deserialize, Serialize};

/// Parses the supported bounded ordinary decimal grammar without rounding or underflow.
pub(crate) fn exact_decimal(raw: &str, field: &str) -> anyhow::Result<rust_decimal::Decimal> {
    anyhow::ensure!(
        !raw.is_empty() && raw.len() <= 128,
        "Invalid selected {field} decimal length"
    );
    let bytes = raw.as_bytes();
    let mut cursor = usize::from(bytes[0] == b'-');
    let start = cursor;
    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        cursor += 1;
    }
    anyhow::ensure!(
        cursor > start,
        "Unsupported selected {field} decimal grammar"
    );
    if cursor < bytes.len() && bytes[cursor] == b'.' {
        cursor += 1;
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        anyhow::ensure!(
            cursor > start && cursor - start <= 28,
            "Unsupported selected {field} decimal fraction"
        );
    }
    anyhow::ensure!(
        cursor == bytes.len(),
        "Unsupported selected {field} decimal grammar"
    );
    rust_decimal::Decimal::from_str_exact(raw)
        .map_err(|_| anyhow::anyhow!("Selected {field} decimal is not exactly representable"))
}

/// The optional finite selected-scope proof policy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectedScopePolicy {
    pub instrument_ids: Vec<String>,
    pub balance_asset: String,
    pub max_age_ms: u64,
    pub max_refresh_ms: u64,
}

impl SelectedScopePolicy {
    pub(crate) fn parse(
        raw: &str,
        venue: Venue,
        loaded: Option<&[String]>,
        load_all: bool,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            raw.len() <= 4096,
            "Aster selected scope policy exceeds 4096 bytes"
        );
        let policy: Self = serde_json::from_str(raw)?;
        anyhow::ensure!(
            (1..=8).contains(&policy.instrument_ids.len()),
            "Aster selected scope requires 1 to 8 instrument IDs"
        );
        anyhow::ensure!(
            (1..=60_000).contains(&policy.max_age_ms),
            "Invalid Aster selected scope age bound"
        );
        anyhow::ensure!(
            (1..=60_000).contains(&policy.max_refresh_ms),
            "Invalid Aster selected scope refresh bound"
        );
        anyhow::ensure!(
            !policy.balance_asset.is_empty()
                && policy.balance_asset.len() <= 16
                && policy
                    .balance_asset
                    .bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
            "Invalid Aster selected scope balance asset"
        );
        anyhow::ensure!(
            !load_all,
            "Aster selected scope requires finite instrument loading"
        );
        let loaded = loaded
            .ok_or_else(|| anyhow::anyhow!("Aster selected scope requires explicit load IDs"))?;
        let mut selected = BTreeSet::new();
        for raw_id in &policy.instrument_ids {
            let id = InstrumentId::from_str(raw_id)?;
            anyhow::ensure!(
                id.venue == venue && id.to_string() == *raw_id,
                "Aster selected scope ID must use the exact configured venue"
            );
            anyhow::ensure!(
                selected.insert(raw_id.as_str()),
                "Duplicate Aster selected scope instrument ID"
            );
        }
        let configured: BTreeSet<&str> = loaded.iter().map(String::as_str).collect();
        anyhow::ensure!(
            configured.len() == loaded.len() && selected == configured,
            "Aster selected scope IDs must equal finite configured load IDs"
        );
        Ok(policy)
    }

    pub(crate) fn select(
        &self,
        instruments: &[InstrumentAny],
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        self.instrument_ids
            .iter()
            .map(|id| {
                let instrument = instruments
                    .iter()
                    .find(|i| i.id().to_string() == *id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Aster selected scope instrument {id} is not loaded")
                    })?;
                anyhow::ensure!(
                    instrument.quote_currency().code.as_str() == self.balance_asset
                        && instrument.settlement_currency().code.as_str() == self.balance_asset,
                    "Aster selected scope settlement does not match the balance asset"
                );
                Ok(instrument.clone())
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SelectedPosition {
    pub instrument_id: String,
    pub signed_quantity: String,
    pub source_update_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SelectedBalance {
    pub asset: String,
    pub total: String,
    pub free: String,
    pub source_update_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SelectedOpenOrder {
    pub client_order_id: String,
    pub venue_order_id: String,
    pub instrument_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SelectedWitness {
    pub generation: u64,
    pub received_time_ns: u64,
    pub positions: Vec<SelectedPosition>,
    pub balance: SelectedBalance,
    pub open_orders: Vec<SelectedOpenOrder>,
    pub metadata: Vec<InstrumentAny>,
}

impl SelectedWitness {
    pub(crate) fn fresh(&self, generation: u64, now_ns: u64, max_age_ms: u64) -> bool {
        self.generation == generation
            && now_ns >= self.received_time_ns
            && now_ns - self.received_time_ns <= max_age_ms * 1_000_000
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("0.00000000000000000000000000001")]
    #[case("0.000000000000000000000000000001")]
    #[case("1e-29")]
    #[case("1e-100")]
    #[case("79228162514264337593543950336")]
    #[case("1.00000000000000000000000000000")]
    #[case("1_000")]
    #[case(" 1")]
    #[case("1 ")]
    #[case("+1")]
    #[case(".1")]
    #[case("1.")]
    fn selected_decimal_rejects_rounding_underflow_and_unsupported_grammar(#[case] raw: &str) {
        assert!(exact_decimal(raw, "quantity").is_err());
    }

    #[rstest]
    #[case("0")]
    #[case("-0.125")]
    #[case("100.01000000")]
    #[case("0.0000000000000000000000000001")]
    #[case("79228162514264337593543950335")]
    fn selected_decimal_accepts_exact_original_values(#[case] raw: &str) {
        assert!(exact_decimal(raw, "quantity").is_ok());
    }

    fn parse(raw: &str) -> anyhow::Result<SelectedScopePolicy> {
        SelectedScopePolicy::parse(
            raw,
            Venue::from("ASTER"),
            Some(&["SNDKUSD1-PERP.ASTER".to_owned()]),
            false,
        )
    }

    #[rstest]
    fn strict_policy_accepts_exact_finite_scope() {
        assert!(parse(r#"{"instrument_ids":["SNDKUSD1-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":2000,"max_refresh_ms":15000}"#).is_ok());
    }

    #[rstest]
    #[case("true")]
    #[case("2.0")]
    #[case("0")]
    #[case("60001")]
    #[case("-1")]
    fn strict_policy_rejects_invalid_numeric_age(#[case] age: &str) {
        assert!(parse(&format!(r#"{{"instrument_ids":["SNDKUSD1-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":{age},"max_refresh_ms":15000}}"#)).is_err());
    }

    #[rstest]
    #[case(r#"{"instrument_ids":["SNDKUSD1-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":2,"max_refresh_ms":15,"ready":true}"#)]
    #[case(r#"{"instrument_ids":["SNDKUSD1-PERP.ASTER","SNDKUSD1-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":2,"max_refresh_ms":15}"#)]
    #[case(r#"{"instrument_ids":["BTCUSDT-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":2,"max_refresh_ms":15}"#)]
    #[case(r#"{"instrument_ids":["SNDKUSD1-PERP.ASTER"],"balance_asset":"USD1","max_age_ms":2,"max_age_ms":3,"max_refresh_ms":15}"#)]
    fn strict_policy_rejects_unknown_or_duplicate_scope(#[case] raw: &str) {
        assert!(parse(raw).is_err());
    }
}
