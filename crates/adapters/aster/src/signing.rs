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

//! EIP-712 request signing for the Aster Futures V3 API.
//!
//! Every `TRADE`, `USER_DATA`, and `USER_STREAM` endpoint of Aster's V3 API is authenticated
//! with an EIP-712 typed-data signature rather than an HMAC digest. The signed payload is the
//! *parameter string* of the request:
//!
//! ```text
//! nonce=<micros>&user=<address>&signer=<address>&<business params in insertion order>
//! ```
//!
//! Values are percent-encoded with `encodeURIComponent` semantics. The string is wrapped in the
//! `Message { string msg }` struct and signed against the domain
//! `AsterSignTransaction / 1 / <chain id> / 0x0`. The resulting 65-byte `r || s || v` signature
//! (with `v` normalised to 27/28) is appended as `&signature=0x...` and sent as the query string
//! (GET) or as an `application/x-www-form-urlencoded` body (POST/PUT/DELETE).
//!
//! # References
//!
//! - <https://asterdex.github.io/aster-api-website/futures-v3/general-info/>

use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::{
    signers::{SignerSync, local::PrivateKeySigner},
    sol_types::{Eip712Domain, SolStruct, eip712_domain},
};
use alloy_primitives::Address;

use crate::common::enums::AsterEnvironment;

alloy::sol! {
    /// The single EIP-712 struct Aster V3 signs: the request parameter string.
    struct Message {
        string msg;
    }
}

/// Characters that `encodeURIComponent` leaves unescaped.
///
/// Per the ECMAScript specification the unreserved set is
/// `A-Z a-z 0-9 - _ . ! ~ * ' ( )`. Everything else (including `&`, `=`, `,`, `/`, `:` and
/// space) is percent-encoded from its UTF-8 bytes. CCXT's `encodeValuesWithJson` uses exactly
/// this function, so matching it byte-for-byte is what makes the signature vectors line up.
const fn is_uri_component_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
        )
}

/// Percent-encodes `value` with JavaScript `encodeURIComponent` semantics.
#[must_use]
pub fn encode_uri_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());

    for byte in value.as_bytes() {
        if is_uri_component_unreserved(*byte) {
            out.push(*byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }

    out
}

/// Builds the canonical Aster V3 parameter string from ordered key/value pairs.
///
/// Keys are emitted verbatim; values are percent-encoded. Insertion order is preserved
/// because Aster verifies the signature over the exact string it receives.
#[must_use]
pub fn build_param_string(params: &[(String, String)]) -> String {
    let mut out = String::new();

    for (key, value) in params {
        if !out.is_empty() {
            out.push('&');
        }
        out.push_str(key);
        out.push('=');
        out.push_str(&encode_uri_component(value));
    }

    out
}

/// EIP-712 signer for Aster Futures V3 requests.
///
/// The private key is held inside an [`alloy`] `PrivateKeySigner`; [`Debug`] never renders it.
#[derive(Clone)]
pub struct AsterEip712Signer {
    signer: PrivateKeySigner,
    domain: Eip712Domain,
    chain_id: u64,
}

impl std::fmt::Debug for AsterEip712Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AsterEip712Signer))
            .field("address", &self.address())
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

impl AsterEip712Signer {
    /// Creates a new [`AsterEip712Signer`] from a hex private key (with or without `0x`).
    ///
    /// # Errors
    ///
    /// Returns an error if the private key cannot be parsed as a secp256k1 scalar.
    pub fn new(private_key_hex: &str, chain_id: u64) -> anyhow::Result<Self> {
        let trimmed = private_key_hex.trim();
        let key_hex = trimmed.strip_prefix("0x").unwrap_or(trimmed);

        let signer = PrivateKeySigner::from_str(key_hex)
            .map_err(|e| anyhow::anyhow!("Invalid Aster signer private key: {e}"))?;

        let domain = eip712_domain! {
            name: "AsterSignTransaction",
            version: "1",
            chain_id: chain_id,
            verifying_contract: Address::ZERO,
        };

        Ok(Self {
            signer,
            domain,
            chain_id,
        })
    }

    /// Creates a new [`AsterEip712Signer`] for the given Aster environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the private key cannot be parsed.
    pub fn for_environment(
        private_key_hex: &str,
        environment: AsterEnvironment,
    ) -> anyhow::Result<Self> {
        Self::new(private_key_hex, environment.chain_id())
    }

    /// Returns the signer's Ethereum address.
    #[must_use]
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    /// Returns the signer's Ethereum address as a lowercase `0x`-prefixed string.
    #[must_use]
    pub fn address_hex(&self) -> String {
        format!("{:#x}", self.signer.address())
    }

    /// Returns the EIP-712 chain id this signer is bound to.
    #[must_use]
    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Signs an Aster V3 parameter string.
    ///
    /// Returns the 65-byte signature as `0x` + hex(`r` || `s` || `v`) with `v` in {27, 28}.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying secp256k1 signing operation fails.
    pub fn sign_param_string(&self, param_string: &str) -> anyhow::Result<String> {
        let message = Message {
            msg: param_string.to_string(),
        };
        let hash = message.eip712_signing_hash(&self.domain);

        let signature = self
            .signer
            .sign_hash_sync(&hash)
            .map_err(|e| anyhow::anyhow!("Failed to sign Aster request: {e}"))?;

        let r = signature.r();
        let s = signature.s();
        let v: u8 = if signature.v() { 28 } else { 27 };

        Ok(format!("0x{r:064x}{s:064x}{v:02x}"))
    }

    /// Builds the signed query/body string for a request.
    ///
    /// Returns `param_string + "&signature=0x..."`.
    ///
    /// # Errors
    ///
    /// Returns an error if signing fails.
    pub fn sign_params(&self, params: &[(String, String)]) -> anyhow::Result<String> {
        let param_string = build_param_string(params);
        let signature = self.sign_param_string(&param_string)?;
        Ok(format!("{param_string}&signature={signature}"))
    }
}

/// Monotonic microsecond nonce generator.
///
/// Aster requires the `nonce` to be within +/- 60 s of server time and strictly increasing per
/// signer. The generator clamps to `previous + 1` whenever the wall clock does not advance
/// (equal or backwards), so back-to-back requests never collide.
#[derive(Debug)]
pub struct NonceGenerator {
    last: AtomicU64,
}

impl Default for NonceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceGenerator {
    /// Creates a new [`NonceGenerator`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
        }
    }

    /// Returns the next strictly increasing nonce in microseconds since the UNIX epoch.
    #[must_use]
    pub fn next(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);

        self.next_from(now)
    }

    /// Returns the next nonce given an externally supplied microsecond timestamp.
    ///
    /// Exposed for deterministic testing of the monotonicity guarantee.
    #[must_use]
    pub fn next_from(&self, now_micros: u64) -> u64 {
        let mut previous = self.last.load(Ordering::Relaxed);

        loop {
            let candidate = if now_micros > previous {
                now_micros
            } else {
                previous.saturating_add(1)
            };

            match self.last.compare_exchange_weak(
                previous,
                candidate,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return candidate,
                Err(current) => previous = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Test-only key published in CCXT's static request fixtures
    /// (`ts/src/test/static/request/aster.json`). It holds no funds anywhere.
    const CCXT_TEST_PRIVATE_KEY: &str =
        "0xff3bdd43534543d421f05aec535965b5050ad6ac15345435345435453495e771";
    const CCXT_TEST_WALLET_ADDRESS: &str = "0xb67f9a782d3678a0bac50c22eacbb4924fe9d4cf";

    /// Aster mainnet EIP-712 chain id (CCXT `options.v3ChainId`).
    const MAINNET_CHAIN_ID: u64 = 1666;

    fn signer() -> AsterEip712Signer {
        AsterEip712Signer::new(CCXT_TEST_PRIVATE_KEY, MAINNET_CHAIN_ID).unwrap()
    }

    #[rstest]
    fn test_address_derived_from_private_key() {
        assert_eq!(signer().address_hex(), CCXT_TEST_WALLET_ADDRESS);
    }

    #[rstest]
    fn test_signer_accepts_key_without_0x_prefix() {
        let stripped = CCXT_TEST_PRIVATE_KEY.strip_prefix("0x").unwrap();
        let signer = AsterEip712Signer::new(stripped, MAINNET_CHAIN_ID).unwrap();

        assert_eq!(signer.address_hex(), CCXT_TEST_WALLET_ADDRESS);
    }

    #[rstest]
    fn test_signer_rejects_invalid_key() {
        assert!(AsterEip712Signer::new("not-a-key", MAINNET_CHAIN_ID).is_err());
    }

    #[rstest]
    fn test_debug_does_not_leak_private_key() {
        let rendered = format!("{:?}", signer());

        assert!(!rendered.contains("ff3bdd43"), "{rendered}");
        assert!(rendered.contains("chain_id"), "{rendered}");
    }

    #[rstest]
    fn test_chain_ids_follow_environment() {
        let mainnet =
            AsterEip712Signer::for_environment(CCXT_TEST_PRIVATE_KEY, AsterEnvironment::Mainnet)
                .unwrap();
        let testnet =
            AsterEip712Signer::for_environment(CCXT_TEST_PRIVATE_KEY, AsterEnvironment::Testnet)
                .unwrap();

        assert_eq!(mainnet.chain_id(), 1666);
        assert_eq!(testnet.chain_id(), 714);
    }

    // --------------------------------------------------------------------------------------------
    // Signature vectors.
    //
    // `test_data/signing_vectors.json` carries the parameter strings copied verbatim from CCXT's
    // static request fixtures (`ts/src/test/static/request/aster.json`) plus the expected
    // signatures regenerated with `eth-account` -- the library Aster's own signing example uses.
    // CCXT lists `signature` in its `skipKeys`, so the signatures recorded there are never
    // checked by CCXT's own tests and no longer match the recorded parameter strings; the file
    // header documents the provenance in full.
    // --------------------------------------------------------------------------------------------

    const SIGNING_VECTORS: &str = include_str!("../test_data/signing_vectors.json");

    #[derive(serde::Deserialize)]
    struct SigningVectorFile {
        private_key: String,
        wallet_address: String,
        cases: Vec<SigningVectorCase>,
    }

    #[derive(serde::Deserialize)]
    struct SigningVectorCase {
        label: String,
        endpoint: String,
        chain_id: u64,
        param_string: String,
        signature: String,
    }

    #[rstest]
    fn test_sign_param_string_matches_reference_vectors() {
        let vectors: SigningVectorFile = serde_json::from_str(SIGNING_VECTORS).unwrap();

        assert_eq!(vectors.private_key, CCXT_TEST_PRIVATE_KEY);
        assert_eq!(vectors.wallet_address, CCXT_TEST_WALLET_ADDRESS);
        assert!(vectors.cases.len() >= 3, "expected at least three vectors");

        let mut mainnet_cases = 0;
        let mut testnet_cases = 0;

        for case in &vectors.cases {
            let signer = AsterEip712Signer::new(&vectors.private_key, case.chain_id).unwrap();
            let signature = signer.sign_param_string(&case.param_string).unwrap();

            assert_eq!(
                signature, case.signature,
                "vector '{}' ({}) chain_id={}",
                case.label, case.endpoint, case.chain_id,
            );

            match case.chain_id {
                1666 => mainnet_cases += 1,
                714 => testnet_cases += 1,
                other => panic!("unexpected chain id {other} in vectors"),
            }
        }

        assert!(mainnet_cases >= 3, "expected several mainnet vectors");
        assert_eq!(testnet_cases, 1, "expected one testnet chain-id vector");
    }

    #[rstest]
    fn test_signature_depends_on_chain_id() {
        let mainnet = AsterEip712Signer::new(CCXT_TEST_PRIVATE_KEY, 1666).unwrap();
        let testnet = AsterEip712Signer::new(CCXT_TEST_PRIVATE_KEY, 714).unwrap();
        let param_string = "nonce=1&user=0xabc&signer=0xabc";

        assert_ne!(
            mainnet.sign_param_string(param_string).unwrap(),
            testnet.sign_param_string(param_string).unwrap(),
        );
    }

    #[rstest]
    fn test_signature_layout_is_65_bytes_with_recovery_id() {
        let signature = signer().sign_param_string("nonce=1&user=0x0&signer=0x0").unwrap();

        assert!(signature.starts_with("0x"));
        assert_eq!(signature.len(), 2 + 130);

        let v = u8::from_str_radix(&signature[signature.len() - 2..], 16).unwrap();
        assert!(v == 27 || v == 28, "unexpected v={v}");
    }

    #[rstest]
    fn test_sign_params_appends_signature_to_param_string() {
        let params = vec![
            ("nonce".to_string(), "1776835698495000".to_string()),
            ("user".to_string(), CCXT_TEST_WALLET_ADDRESS.to_string()),
            ("signer".to_string(), CCXT_TEST_WALLET_ADDRESS.to_string()),
            ("symbol".to_string(), "BTCUSDT".to_string()),
        ];

        let signed = signer().sign_params(&params).unwrap();

        assert!(signed.starts_with(
            "nonce=1776835698495000&user=0xb67f9a782d3678a0bac50c22eacbb4924fe9d4cf&signer=0xb67f9a782d3678a0bac50c22eacbb4924fe9d4cf&symbol=BTCUSDT&signature=0x"
        ));
    }

    #[rstest]
    #[case("BTCUSDT", "BTCUSDT")]
    #[case("0.7", "0.7")]
    #[case("O-20260101-000000-001-001-1", "O-20260101-000000-001-001-1")]
    #[case("a b", "a%20b")]
    #[case("[123]", "%5B123%5D")]
    #[case("a&b=c", "a%26b%3Dc")]
    #[case("a/b:c", "a%2Fb%3Ac")]
    #[case("-_.!~*'()", "-_.!~*'()")]
    fn test_encode_uri_component(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(encode_uri_component(input), expected);
    }

    #[rstest]
    fn test_encode_uri_component_matches_ccxt_order_id_list_fixture() {
        // CCXT `cancelOrders` fixture: orderIdList=%5B17338441758%5D
        assert_eq!(encode_uri_component("[17338441758]"), "%5B17338441758%5D");
    }

    #[rstest]
    fn test_build_param_string_preserves_insertion_order_and_encodes_values() {
        let params = vec![
            ("nonce".to_string(), "1".to_string()),
            ("user".to_string(), "0xabc".to_string()),
            ("signer".to_string(), "0xabc".to_string()),
            ("newClientOrderId".to_string(), "O-1 2".to_string()),
        ];

        assert_eq!(
            build_param_string(&params),
            "nonce=1&user=0xabc&signer=0xabc&newClientOrderId=O-1%202"
        );
    }

    #[rstest]
    fn test_build_param_string_empty() {
        assert_eq!(build_param_string(&[]), "");
    }

    #[rstest]
    fn test_nonce_generator_is_strictly_increasing_when_clock_stalls() {
        let generator = NonceGenerator::new();

        assert_eq!(generator.next_from(1_000), 1_000);
        assert_eq!(generator.next_from(1_000), 1_001);
        assert_eq!(generator.next_from(999), 1_002);
        assert_eq!(generator.next_from(5_000), 5_000);
    }

    #[rstest]
    fn test_nonce_generator_returns_microseconds() {
        let generator = NonceGenerator::new();
        let nonce = generator.next();

        // Microseconds since epoch: > year 2001 and < year 2286 in microsecond units.
        assert!(nonce > 1_000_000_000_000_000, "nonce={nonce}");
        assert!(nonce < 10_000_000_000_000_000, "nonce={nonce}");
        assert!(generator.next() > nonce);
    }
}
