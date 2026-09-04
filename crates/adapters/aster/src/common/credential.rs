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

//! Aster API credential resolution.
//!
//! Aster's V3 API authenticates with an EIP-712 signature produced by an *API wallet*
//! (also called an agent wallet). Three values are involved:
//!
//! - `signer_private_key` - the API wallet's secp256k1 key; the only secret.
//! - `signer_address` - the API wallet address; derived from the key when not configured.
//! - `user_address` - the master account wallet address; defaults to the signer address when
//!   trading directly from the master wallet rather than through a delegated API wallet.
//!
//! The private key is read from configuration or the `ASTER_SIGNER_PRIVATE_KEY` environment
//! variable. It is never logged: [`Debug`] renders it redacted, and it is zeroized on drop.

use std::fmt::Debug;

use nautilus_core::string::secret::REDACTED;
use zeroize::Zeroizing;

use crate::{common::enums::AsterEnvironment, signing::AsterEip712Signer};

/// Environment variable holding the Aster API wallet private key.
pub const ASTER_SIGNER_PRIVATE_KEY_ENV: &str = "ASTER_SIGNER_PRIVATE_KEY";

/// Environment variable holding the Aster API wallet (signer) address.
pub const ASTER_SIGNER_ADDRESS_ENV: &str = "ASTER_SIGNER_ADDRESS";

/// Environment variable holding the Aster master account (user) address.
pub const ASTER_USER_ADDRESS_ENV: &str = "ASTER_USER_ADDRESS";

/// Resolved Aster signing credentials.
///
/// Holds the signer alongside the `user` / `signer` addresses that every signed V3 request
/// carries. Cloning is cheap; the private key never leaves the contained signer.
#[derive(Clone)]
pub struct AsterCredential {
    signer: AsterEip712Signer,
    signer_address: String,
    user_address: String,
}

impl Debug for AsterCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AsterCredential))
            .field("user_address", &self.user_address)
            .field("signer_address", &self.signer_address)
            .field("signer_private_key", &REDACTED)
            .finish()
    }
}

impl AsterCredential {
    /// Resolves credentials from configuration values, falling back to environment variables.
    ///
    /// - The private key comes from `config_private_key` or [`ASTER_SIGNER_PRIVATE_KEY_ENV`].
    /// - `signer_address` defaults to the address derived from the key; when supplied
    ///   explicitly (config or [`ASTER_SIGNER_ADDRESS_ENV`]) it must match that derivation.
    /// - `user_address` defaults to the signer address.
    ///
    /// # Errors
    ///
    /// Returns an error if no private key can be found, if the key is malformed, or if a
    /// configured signer address does not match the key.
    pub fn resolve(
        config_private_key: Option<&str>,
        config_signer_address: Option<&str>,
        config_user_address: Option<&str>,
        environment: AsterEnvironment,
    ) -> anyhow::Result<Self> {
        Self::resolve_with_env(
            config_private_key,
            config_signer_address,
            config_user_address,
            environment,
            |key| std::env::var(key).ok(),
        )
    }

    /// Resolves credentials against a caller-supplied environment lookup.
    ///
    /// Behaves exactly like [`Self::resolve`] but reads environment variables through
    /// `env_lookup`, which keeps the resolution rules testable without mutating the
    /// process-wide environment.
    ///
    /// # Errors
    ///
    /// Returns an error if no private key can be found, if the key is malformed, or if a
    /// configured signer address does not match the key.
    pub fn resolve_with_env<F>(
        config_private_key: Option<&str>,
        config_signer_address: Option<&str>,
        config_user_address: Option<&str>,
        environment: AsterEnvironment,
        env_lookup: F,
    ) -> anyhow::Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let resolve_value = |configured: Option<&str>, env_key: &str| {
            configured
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty())
                .or_else(|| env_lookup(env_key).filter(|value| !value.trim().is_empty()))
        };

        let private_key = Zeroizing::new(
            resolve_value(config_private_key, ASTER_SIGNER_PRIVATE_KEY_ENV).ok_or_else(|| {
                anyhow::anyhow!(
                    "Aster signer private key not found: set `signer_private_key` on the \
                     execution client config or the `{ASTER_SIGNER_PRIVATE_KEY_ENV}` \
                     environment variable"
                )
            })?,
        );

        let signer = AsterEip712Signer::for_environment(private_key.trim(), environment)?;
        let derived_address = signer.address_hex();

        if let Some(configured) = resolve_value(config_signer_address, ASTER_SIGNER_ADDRESS_ENV)
            && !addresses_equal(&configured, &derived_address)
        {
            anyhow::bail!(
                "Aster signer address mismatch: configured {configured}, but the private key \
                 derives {derived_address}"
            );
        }

        let user_address = resolve_value(config_user_address, ASTER_USER_ADDRESS_ENV)
            .map_or_else(|| derived_address.clone(), |value| value.trim().to_lowercase());

        Ok(Self {
            signer,
            signer_address: derived_address,
            user_address,
        })
    }

    /// Returns the EIP-712 signer.
    #[must_use]
    pub const fn signer(&self) -> &AsterEip712Signer {
        &self.signer
    }

    /// Returns the API wallet (signer) address, lowercase and `0x`-prefixed.
    #[must_use]
    pub fn signer_address(&self) -> &str {
        &self.signer_address
    }

    /// Returns the master account (user) address, lowercase and `0x`-prefixed.
    #[must_use]
    pub fn user_address(&self) -> &str {
        &self.user_address
    }

    /// Returns whether the signer address and the user address are the same wallet.
    #[must_use]
    pub fn is_self_signed(&self) -> bool {
        addresses_equal(&self.signer_address, &self.user_address)
    }
}

fn addresses_equal(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use rstest::rstest;

    use super::*;

    /// Test-only key published in CCXT's static request fixtures; holds no funds.
    const TEST_PRIVATE_KEY: &str =
        "0xff3bdd43534543d421f05aec535965b5050ad6ac15345435345435453495e771";
    const TEST_ADDRESS: &str = "0xb67f9a782d3678a0bac50c22eacbb4924fe9d4cf";

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: AHashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    fn empty_env() -> impl Fn(&str) -> Option<String> {
        |_: &str| None
    }

    #[rstest]
    fn test_resolve_from_config_derives_addresses() {
        let credential = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            None,
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap();

        assert_eq!(credential.signer_address(), TEST_ADDRESS);
        assert_eq!(credential.user_address(), TEST_ADDRESS);
        assert!(credential.is_self_signed());
        assert_eq!(credential.signer().chain_id(), 1666);
    }

    #[rstest]
    fn test_resolve_from_environment_variable() {
        let credential = AsterCredential::resolve_with_env(
            None,
            None,
            None,
            AsterEnvironment::Testnet,
            env(&[(ASTER_SIGNER_PRIVATE_KEY_ENV, TEST_PRIVATE_KEY)]),
        )
        .unwrap();

        assert_eq!(credential.signer_address(), TEST_ADDRESS);
        assert_eq!(credential.signer().chain_id(), 714);
    }

    #[rstest]
    fn test_resolve_reads_addresses_from_environment() {
        let master = "0x1111111111111111111111111111111111111111";
        let credential = AsterCredential::resolve_with_env(
            None,
            None,
            None,
            AsterEnvironment::Mainnet,
            env(&[
                (ASTER_SIGNER_PRIVATE_KEY_ENV, TEST_PRIVATE_KEY),
                (ASTER_SIGNER_ADDRESS_ENV, TEST_ADDRESS),
                (ASTER_USER_ADDRESS_ENV, master),
            ]),
        )
        .unwrap();

        assert_eq!(credential.signer_address(), TEST_ADDRESS);
        assert_eq!(credential.user_address(), master);
        assert!(!credential.is_self_signed());
    }

    #[rstest]
    fn test_config_takes_precedence_over_environment() {
        let credential = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            None,
            Some("0x3333333333333333333333333333333333333333"),
            AsterEnvironment::Mainnet,
            env(&[
                (ASTER_SIGNER_PRIVATE_KEY_ENV, "0xdeadbeef"),
                (
                    ASTER_USER_ADDRESS_ENV,
                    "0x2222222222222222222222222222222222222222",
                ),
            ]),
        )
        .unwrap();

        assert_eq!(credential.signer_address(), TEST_ADDRESS);
        assert_eq!(
            credential.user_address(),
            "0x3333333333333333333333333333333333333333"
        );
    }

    #[rstest]
    fn test_resolve_accepts_checksummed_signer_address() {
        let credential = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            Some("0xb67f9a782D3678a0BAC50C22eacbb4924Fe9D4cF"),
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap();

        assert_eq!(credential.signer_address(), TEST_ADDRESS);
    }

    #[rstest]
    fn test_resolve_rejects_mismatched_signer_address() {
        let error = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            Some("0x4444444444444444444444444444444444444444"),
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("signer address mismatch"), "{error}");
    }

    #[rstest]
    fn test_resolve_without_key_errors_and_names_the_env_var() {
        let error = AsterCredential::resolve_with_env(
            None,
            None,
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains(ASTER_SIGNER_PRIVATE_KEY_ENV), "{error}");
    }

    #[rstest]
    #[case(Some("   "))]
    #[case(None)]
    fn test_resolve_treats_blank_values_as_absent(#[case] configured: Option<&str>) {
        let error = AsterCredential::resolve_with_env(
            configured,
            None,
            None,
            AsterEnvironment::Mainnet,
            env(&[(ASTER_SIGNER_PRIVATE_KEY_ENV, "  ")]),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains(ASTER_SIGNER_PRIVATE_KEY_ENV), "{error}");
    }

    #[rstest]
    fn test_resolve_rejects_malformed_key() {
        let error = AsterCredential::resolve_with_env(
            Some("0xnot-a-key"),
            None,
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("Invalid Aster signer private key"), "{error}");
    }

    #[rstest]
    fn test_debug_redacts_private_key() {
        let credential = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            None,
            None,
            AsterEnvironment::Mainnet,
            empty_env(),
        )
        .unwrap();
        let rendered = format!("{credential:?}");

        assert!(rendered.contains(REDACTED), "{rendered}");
        assert!(!rendered.contains("ff3bdd43"), "{rendered}");
    }
}
