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

//! Ordered request parameters for signed Aster V3 requests.
//!
//! Aster verifies the EIP-712 signature over the exact parameter string it receives, so the
//! order in which parameters are appended is part of the protocol. [`AsterParams`] is an
//! insertion-ordered list rather than a map for that reason.

/// Insertion-ordered request parameters.
///
/// The authentication triple (`nonce`, `user`, `signer`) is prepended by the HTTP client just
/// before signing, matching the ordering used by CCXT and by Aster's own examples.
#[derive(Debug, Clone, Default)]
pub struct AsterParams {
    entries: Vec<(String, String)>,
}

impl AsterParams {
    /// Creates an empty parameter list.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Appends a parameter, preserving insertion order.
    ///
    /// Takes the value by value so call sites can pass literals and temporaries directly;
    /// only its string rendering is retained.
    #[must_use]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "builder ergonomics: callers pass literals and temporaries"
    )]
    pub fn with(mut self, key: &str, value: impl ToString) -> Self {
        self.entries.push((key.to_string(), value.to_string()));
        self
    }

    /// Appends a parameter when `value` is `Some`.
    #[must_use]
    pub fn with_opt(self, key: &str, value: Option<impl ToString>) -> Self {
        match value {
            Some(value) => self.with(key, value),
            None => self,
        }
    }

    /// Returns whether any parameters have been added.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of parameters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the ordered key/value pairs.
    #[must_use]
    pub fn entries(&self) -> &[(String, String)] {
        &self.entries
    }

    /// Consumes the list and returns the ordered key/value pairs.
    #[must_use]
    pub fn into_entries(self) -> Vec<(String, String)> {
        self.entries
    }

    /// Returns the value recorded for `key`, if any.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

impl FromIterator<(String, String)> for AsterParams {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_new_is_empty() {
        let params = AsterParams::new();

        assert!(params.is_empty());
        assert_eq!(params.len(), 0);
        assert_eq!(params.get("symbol"), None);
    }

    #[rstest]
    fn test_builder_preserves_insertion_order() {
        let params = AsterParams::new()
            .with("symbol", "BTCUSDT")
            .with("side", "BUY")
            .with("type", "LIMIT")
            .with("quantity", 10)
            .with("price", "0.7");

        let keys: Vec<&str> = params
            .entries()
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>();

        assert_eq!(keys, ["symbol", "side", "type", "quantity", "price"]);
        assert_eq!(params.get("quantity"), Some("10"));
    }

    #[rstest]
    fn test_optional_parameters_are_skipped_when_none() {
        let params = AsterParams::new()
            .with("symbol", "BTCUSDT")
            .with_opt("orderId", None::<i64>)
            .with_opt("origClientOrderId", Some("O-1"));

        assert_eq!(params.len(), 2);
        assert_eq!(params.get("orderId"), None);
        assert_eq!(params.get("origClientOrderId"), Some("O-1"));
    }

    #[rstest]
    fn test_into_entries_returns_the_ordered_pairs() {
        let params = AsterParams::new()
            .with("symbol", "BTCUSDT")
            .with_opt("limit", Some(50u32))
            .with_opt("startTime", None::<u64>);

        assert_eq!(
            params.into_entries(),
            vec![
                ("symbol".to_string(), "BTCUSDT".to_string()),
                ("limit".to_string(), "50".to_string()),
            ]
        );
    }

    #[rstest]
    fn test_duplicate_keys_are_preserved_in_order() {
        let params = AsterParams::new().with("a", "1").with("a", "2");

        assert_eq!(params.len(), 2);
        assert_eq!(params.get("a"), Some("1"));
    }
}
