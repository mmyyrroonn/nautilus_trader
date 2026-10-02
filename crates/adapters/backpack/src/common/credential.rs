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

//! Explicit audience-bound Ed25519 credentials, without environment lookup.

use std::{collections::HashMap, fmt, sync::Arc};

use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::SigningKey;
use nautilus_cryptography::signing::ed25519_signature;
use zeroize::Zeroizing;

use crate::{
    common::endpoints::BackpackEndpoints,
    http::error::{BackpackHttpError, BackpackHttpErrorKind},
    signing::{BackpackReceiveWindow, canonical_websocket},
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum CredentialAudience {
    Production,
    Loopback(BackpackEndpoints),
}

/// A zeroized seed bound to production or exactly one explicit local peer.
#[derive(Clone)]
pub struct BackpackCredential {
    seed: Arc<Zeroizing<[u8; 32]>>,
    verifying_key: String,
    audience: CredentialAudience,
}
impl fmt::Debug for BackpackCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackCredential")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}
impl BackpackCredential {
    /// Decodes caller-provided production credentials without environment lookup.
    ///
    /// # Errors
    ///
    /// Returns an error unless input is canonical base64 of exactly 32 bytes.
    pub fn production(seed: &str) -> Result<Self, BackpackHttpError> {
        Self::decode(seed, CredentialAudience::Production)
    }
    /// Decodes caller-provided credentials for one validated local peer.
    ///
    /// # Errors
    ///
    /// Returns an error for production endpoints or malformed seed material.
    pub fn loopback_peer(
        seed: &str,
        endpoints: &BackpackEndpoints,
    ) -> Result<Self, BackpackHttpError> {
        if !endpoints.is_loopback() {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Credentials));
        }
        Self::decode(seed, CredentialAudience::Loopback(endpoints.clone()))
    }
    fn decode(seed: &str, audience: CredentialAudience) -> Result<Self, BackpackHttpError> {
        let decoded = Zeroizing::new(
            STANDARD
                .decode(seed)
                .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Credentials))?,
        );
        if decoded.len() != 32 || seed.len() != 44 {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Credentials));
        }
        let mut bytes = Zeroizing::new([0_u8; 32]);
        bytes.copy_from_slice(&decoded);
        let verifying_key =
            STANDARD.encode(SigningKey::from_bytes(&bytes).verifying_key().as_bytes());
        Ok(Self {
            seed: Arc::new(bytes),
            verifying_key,
            audience,
        })
    }
    pub(crate) fn check_audience(
        &self,
        endpoints: &BackpackEndpoints,
    ) -> Result<(), BackpackHttpError> {
        let matches = match &self.audience {
            CredentialAudience::Production => !endpoints.is_loopback(),
            CredentialAudience::Loopback(expected) => expected == endpoints,
        };
        if !matches {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Credentials));
        }
        Ok(())
    }
    pub(crate) fn sign(&self, canonical: &str) -> Result<String, BackpackHttpError> {
        ed25519_signature(&self.seed[..], canonical)
            .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Credentials))
    }
    pub(crate) fn headers(
        &self,
        canonical: &str,
        timestamp: u64,
        window: BackpackReceiveWindow,
    ) -> Result<HashMap<String, String>, BackpackHttpError> {
        Ok(HashMap::from([
            ("X-API-Key".into(), self.verifying_key.clone()),
            ("X-Signature".into(), self.sign(canonical)?),
            ("X-Timestamp".into(), timestamp.to_string()),
            ("X-Window".into(), window.milliseconds().to_string()),
        ]))
    }
    /// Constructs an authenticated subscription without opening a connection.
    ///
    /// # Errors
    ///
    /// Returns an error for an audience mismatch or unsupported/ambiguous topics.
    pub fn subscription(
        &self,
        endpoints: &BackpackEndpoints,
        topics: Vec<String>,
        timestamp_ms: u64,
        window: BackpackReceiveWindow,
    ) -> Result<BackpackWebSocketSubscription, BackpackHttpError> {
        self.check_audience(endpoints)?;
        if topics.is_empty() || topics.len() > 100 || timestamp_ms > i64::MAX as u64 {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
        }
        let mut unique = std::collections::BTreeSet::new();
        for topic in &topics {
            let valid = topic == "account.balanceUpdate"
                || ["account.orderUpdate", "account.positionUpdate"]
                    .iter()
                    .any(|prefix| {
                        topic == prefix
                            || topic
                                .strip_prefix(&format!("{prefix}."))
                                .is_some_and(|symbol| {
                                    symbol.strip_suffix("_USDC_PERP").is_some_and(|base| {
                                        !base.is_empty()
                                            && base.bytes().all(|b| {
                                                b.is_ascii_uppercase() || b.is_ascii_digit()
                                            })
                                    })
                                })
                    });
            if !valid || !unique.insert(topic) {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
            }
        }
        let signature = self.sign(&canonical_websocket(timestamp_ms, window))?;
        Ok(BackpackWebSocketSubscription {
            topics,
            authentication: [
                self.verifying_key.clone(),
                signature,
                timestamp_ms.to_string(),
                window.milliseconds().to_string(),
            ],
        })
    }
}

/// A WS payload with redacted Debug; wire bytes remain explicitly accessible.
#[derive(Clone)]
pub struct BackpackWebSocketSubscription {
    topics: Vec<String>,
    authentication: [String; 4],
}
impl fmt::Debug for BackpackWebSocketSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackWebSocketSubscription")
            .field("authentication", &"[REDACTED]")
            .finish()
    }
}
impl BackpackWebSocketSubscription {
    /// Serializes the explicit wire payload; callers must never log the result.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"method": "SUBSCRIBE", "params": self.topics,
            "signature": self.authentication})
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    #[rstest]
    fn test_rfc8032_independent_signature_vector() {
        // Public RFC 8032 section 7.1 test 1 seed, never a venue credential
        // Source: https://www.rfc-editor.org/rfc/rfc8032#section-7.1
        let seed = STANDARD.encode([
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ]);
        let credential = BackpackCredential::production(&seed).unwrap();
        let signature = STANDARD.decode(credential.sign("").unwrap()).unwrap();
        let expected = [
            0xe5, 0x56, 0x43, 0x00, 0xc3, 0x60, 0xac, 0x72, 0x90, 0x86, 0xe2, 0xcc, 0x80, 0x6e,
            0x82, 0x8a, 0x84, 0x87, 0x7f, 0x1e, 0xb8, 0xe5, 0xd9, 0x74, 0xd8, 0x73, 0xe0, 0x65,
            0x22, 0x49, 0x01, 0x55, 0x5f, 0xb8, 0x82, 0x15, 0x90, 0xa3, 0x3b, 0xac, 0xc6, 0x1e,
            0x39, 0x70, 0x1c, 0xf9, 0xb4, 0x6b, 0xd2, 0x5b, 0xf5, 0xf0, 0x59, 0x5b, 0xbe, 0x24,
            0x65, 0x51, 0x41, 0x43, 0x8e, 0x7a, 0x10, 0x0b,
        ];
        assert_eq!(signature, expected);
        assert!(!format!("{credential:?}").contains(&seed));
        assert!(!format!("{credential:?}").contains(&credential.verifying_key));
    }
    #[rstest]
    fn test_audience_and_websocket_payload() {
        let seed = STANDARD.encode([7; 32]);
        let endpoints =
            BackpackEndpoints::loopback_override("http://127.0.0.1:8080", "ws://127.0.0.1:8081")
                .unwrap();
        assert!(
            BackpackCredential::production(&seed)
                .unwrap()
                .check_audience(&endpoints)
                .is_err()
        );
        let credential = BackpackCredential::loopback_peer(&seed, &endpoints).unwrap();
        assert!(
            credential
                .check_audience(&BackpackEndpoints::production())
                .is_err()
        );
        let changed =
            BackpackEndpoints::loopback_override("http://127.0.0.1:8082", "ws://127.0.0.1:8081")
                .unwrap();
        assert!(credential.check_audience(&changed).is_err());
        let payload = credential
            .subscription(
                &endpoints,
                vec!["account.balanceUpdate".into()],
                1_614_550_000_000,
                BackpackReceiveWindow::default(),
            )
            .unwrap();
        let json = payload.to_json();
        assert_eq!(json["signature"][2], "1614550000000");
        assert_eq!(json["signature"][3], "5000");
        assert_eq!(
            json["signature"][1],
            credential
                .sign("instruction=subscribe&timestamp=1614550000000&window=5000")
                .unwrap()
        );
        assert!(!format!("{payload:?}").contains(&credential.verifying_key));
        assert!(
            credential
                .subscription(
                    &endpoints,
                    vec!["account.rfq".into()],
                    0,
                    BackpackReceiveWindow::default()
                )
                .is_err()
        );
    }
    #[rstest]
    fn test_malformed_keys_never_appear_in_errors() {
        for seed in ["not-secret-key-material", "", "YWJj", "a\n"] {
            let e = BackpackCredential::production(seed).unwrap_err();
            if !seed.is_empty() {
                assert!(!format!("{e:?} {e}").contains(seed));
            }
        }
        assert!(BackpackCredential::production(&STANDARD.encode([1; 64])).is_err());
    }
}
