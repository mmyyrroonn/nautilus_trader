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

//! Exact scalar canonicalization for Ed25519 authentication.
//!
//! Protocol: <https://docs.backpack.exchange/#section/Authentication>.
//! Unestablished escaping, arrays, nulls and empty values are rejected.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde_json::{Map, Value};

use crate::http::error::{BackpackHttpError, BackpackHttpErrorKind};

/// A validated receive window in milliseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackReceiveWindow(u64);
impl BackpackReceiveWindow {
    /// Validates the inclusive range 1..=60000 milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error outside the adapter range, capped at the documented maximum.
    pub fn new(milliseconds: u64) -> Result<Self, BackpackHttpError> {
        if !(1..=60_000).contains(&milliseconds) {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
        }
        Ok(Self(milliseconds))
    }
    /// Returns the window in milliseconds.
    #[must_use]
    pub const fn milliseconds(self) -> u64 {
        self.0
    }
}
impl Default for BackpackReceiveWindow {
    fn default() -> Self {
        Self(5_000)
    }
}

/// An exactly representable scalar parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackpackScalar {
    Token(String),
    Unsigned(u64),
    Signed(i64),
    Decimal(Decimal),
    Boolean(bool),
}
impl BackpackScalar {
    fn text(&self) -> String {
        match self {
            Self::Token(value) => value.clone(),
            Self::Unsigned(value) => value.to_string(),
            Self::Signed(value) => value.to_string(),
            Self::Decimal(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
        }
    }
    fn json(&self) -> Value {
        match self {
            Self::Token(value) => Value::String(value.clone()),
            Self::Unsigned(value) => Value::from(*value),
            Self::Signed(value) => Value::from(*value),
            Self::Decimal(value) => Value::String(value.to_string()),
            Self::Boolean(value) => Value::Bool(*value),
        }
    }
}

/// Sorted parameters shared by canonical and wire representations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackpackParameters(BTreeMap<String, BackpackScalar>);
impl BackpackParameters {
    /// Inserts one parameter or omits an absent optional value.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate/reserved keys or ambiguous tokens.
    pub fn insert(
        &mut self,
        key: &str,
        value: Option<BackpackScalar>,
    ) -> Result<(), BackpackHttpError> {
        if key.is_empty()
            || key.len() > 64
            || !key.bytes().all(|b| b.is_ascii_alphanumeric())
            || ["instruction", "timestamp", "window"].contains(&key)
            || self.0.contains_key(key)
        {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
        }
        if let Some(value) = value {
            if let BackpackScalar::Token(token) = &value
                && (token.is_empty()
                    || token.len() > 256
                    || !token
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b)))
            {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
            }
            self.0.insert(key.to_string(), value);
        }
        Ok(())
    }
    /// Returns the sorted query string, excluding authentication fields.
    #[must_use]
    pub fn query_string(&self) -> String {
        self.0
            .iter()
            .map(|(key, value)| format!("{key}={}", value.text()))
            .collect::<Vec<_>>()
            .join("&")
    }
    /// Returns a scalar JSON body from the same parameter values.
    #[must_use]
    pub fn json_body(&self) -> Value {
        Value::Object(
            self.0
                .iter()
                .map(|(key, value)| (key.clone(), value.json()))
                .collect::<Map<_, _>>(),
        )
    }
    pub(crate) fn get(&self, key: &str) -> Option<&BackpackScalar> {
        self.0.get(key)
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

pub(crate) fn canonical_rest(
    instruction: &str,
    parameters: &BackpackParameters,
    timestamp: u64,
    window: BackpackReceiveWindow,
) -> String {
    let query = parameters.query_string();
    let separator = if query.is_empty() { "" } else { "&" };
    format!(
        "instruction={instruction}{separator}{query}&timestamp={timestamp}&window={}",
        window.milliseconds()
    )
}
pub(crate) fn canonical_websocket(timestamp: u64, window: BackpackReceiveWindow) -> String {
    format!(
        "instruction=subscribe&timestamp={timestamp}&window={}",
        window.milliseconds()
    )
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    #[rstest]
    fn test_official_cancel_canonical_example() {
        // Official example: https://docs.backpack.exchange/#section/Authentication
        let mut params = BackpackParameters::default();
        params
            .insert("symbol", Some(BackpackScalar::Token("BTC_USDT".into())))
            .unwrap();
        params
            .insert("orderId", Some(BackpackScalar::Unsigned(28)))
            .unwrap();
        assert_eq!(
            canonical_rest(
                "orderCancel",
                &params,
                1_614_550_000_000,
                BackpackReceiveWindow::default()
            ),
            "instruction=orderCancel&orderId=28&symbol=BTC_USDT&timestamp=1614550000000&window=5000"
        );
    }
    #[rstest]
    fn test_scalar_wire_and_canonical_share_values() {
        // Synthetic scalar coverage, not a captured venue payload
        let mut params = BackpackParameters::default();
        params
            .insert(
                "price",
                Some(BackpackScalar::Decimal(Decimal::new(12300, 3))),
            )
            .unwrap();
        params
            .insert("postOnly", Some(BackpackScalar::Boolean(false)))
            .unwrap();
        params
            .insert("clientId", Some(BackpackScalar::Unsigned(u32::MAX.into())))
            .unwrap();
        params.insert("quantity", None).unwrap();
        assert_eq!(
            params.query_string(),
            "clientId=4294967295&postOnly=false&price=12.300"
        );
        assert_eq!(
            params.json_body(),
            serde_json::json!({"clientId": 4294967295_u64,
            "postOnly": false, "price": "12.300"})
        );
    }
    #[rstest]
    #[case(0)]
    #[case(60_001)]
    #[case(u64::MAX)]
    fn test_invalid_window(#[case] value: u64) {
        assert!(BackpackReceiveWindow::new(value).is_err());
    }
    #[rstest]
    #[case(1)]
    #[case(5_000)]
    #[case(60_000)]
    fn test_window_boundaries(#[case] value: u64) {
        assert_eq!(
            BackpackReceiveWindow::new(value).unwrap().milliseconds(),
            value
        );
    }
    #[rstest]
    #[case("")]
    #[case("a&b")]
    #[case("a=b")]
    #[case("a b")]
    #[case("a%20")]
    #[case("a+b")]
    #[case("a/b")]
    #[case("\n")]
    fn test_ambiguous_tokens_refused(#[case] token: &str) {
        let mut params = BackpackParameters::default();
        assert!(
            params
                .insert("symbol", Some(BackpackScalar::Token(token.into())))
                .is_err()
        );
    }
    #[rstest]
    fn test_duplicate_and_reserved_keys_refused() {
        let mut params = BackpackParameters::default();
        params
            .insert("limit", Some(BackpackScalar::Unsigned(0)))
            .unwrap();
        assert!(
            params
                .insert("limit", Some(BackpackScalar::Unsigned(1)))
                .is_err()
        );
        for key in ["instruction", "timestamp", "window", "bad-key"] {
            assert!(params.insert(key, None).is_err());
        }
    }
}
