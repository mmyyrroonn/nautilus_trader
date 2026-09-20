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

//! Credential storage and the environment gate for the authenticated Ondo Perps endpoints.
//!
//! The public data client needs no credentials: it reads public market data and, per the
//! integration plan, never loads `.env` or reads a key. This module is the single home of the API
//! key id and the API secret, and of the one decision that has to happen *before* either is read:
//! which environment an authenticated session may belong to.
//!
//! # The environment gate runs first
//!
//! [`validate_authenticated_environment`] is called by [`resolve_credential`] before a single
//! environment variable is looked up, and again by the authenticated HTTP client's constructor
//! before the client exists. Its rules (plan §1: the execution client defaults to sandbox, must
//! error when credentials are absent, and must never fall back to another account or to
//! production; §6.1: no automatic protocol or environment switching; §R0.3: the endpoint is an
//! allowlist):
//!
//! 1. an authenticated session is opened under a
//!    [`crate::common::enums::OndoAuthenticationScope`], which pairs the environment with whether
//!    the session may write. Only [`OndoAuthenticationScope::SandboxTrading`] may write; the two
//!    read-only scopes refuse every signed write at the transport's dispatch. There is deliberately
//!    no production trading scope, so a production configuration is refused at the configuration
//!    before a credential is read;
//! 2. the base URL must be an endpoint this session may sign for, which is
//!    [`crate::common::endpoint::OndoEndpointPolicy`]'s decision: the official host of the session's
//!    own environment ([`OndoEndpoint::Official`]) or a loopback test service
//!    ([`OndoEndpoint::LoopbackTestService`]). Anything else - another remote host, a lookalike of
//!    the official host, the *other* environment's official host, userinfo, a non-TLS remote
//!    service, an unreadable URL - is refused, and a host inside the production domain that is not
//!    this session's own official host is refused *as production*
//!    ([`OndoEnvironmentError::ProductionHostForbidden`]) before any other rule is consulted.
//!
//! Neither refusal reads a credential, opens a socket, or has a fallback: there is one gate and one
//! answer. The URL is judged as the transport's own parser reads it, so what is admitted here is
//! what would be dialled.
//!
//! # The secret cannot leak
//!
//! [`OndoCredential`] is the only type that holds the secret. It is not `Clone` (the client shares
//! one instance through an [`std::sync::Arc`] instead of copying it), it is neither `Serialize` nor
//! `Deserialize`, its [`std::fmt::Debug`] masks the key id and redacts the secret, and its
//! [`std::fmt::Display`] renders the redaction placeholder and nothing else. Errors never carry a
//! credential, and [`OndoCredential::redact`] is what the authenticated transport applies to a
//! venue body before an error keeps it, so a body that echoed a credential cannot carry one into a
//! log. The secret is wiped when the last handle is dropped ([`zeroize::ZeroizeOnDrop`]).
//!
//! # Prefixes
//!
//! The API-key page is explicit that the HMAC key is the **full** secret, including its
//! `ondoApiSecret_` prefix, and that the `ONDO-KEY-ID` header carries the key id **including** its
//! `ondoKeyId_` prefix. Both values are stored and used verbatim: nothing here strips, trims (beyond
//! removing the surrounding whitespace a variable assignment can carry), or normalises them.

use std::fmt;

use nautilus_core::string::secret::{REDACTED, mask_api_key};
use zeroize::ZeroizeOnDrop;

// The gate's own types are the endpoint policy's: an authenticated session and the endpoint it
// signs for are one decision, and the URL rules live in `common::endpoint` so REST and the private
// WebSocket share them. Re-exported here because this is the path every caller already imports the
// gate's error from.
pub use crate::common::endpoint::{OndoEndpoint, OndoEnvironmentError};
use crate::common::{
    endpoint::{OndoEndpointPolicy, OndoSchemeFamily},
    enums::{OndoAuthenticationScope, OndoEnvironment},
};

/// The environment variable holding the sandbox API key id (plan §7 Task 9).
pub const ONDO_SANDBOX_API_KEY_VAR: &str = "ONDO_SANDBOX_API_KEY";

/// The environment variable holding the sandbox API secret (plan §7 Task 9).
pub const ONDO_SANDBOX_API_SECRET_VAR: &str = "ONDO_SANDBOX_API_SECRET";

/// The environment variable holding the mainnet API key id.
///
/// Read only for [`OndoAuthenticationScope::ProductionReadOnly`]. There is no fallback between
/// the mainnet and sandbox variable sets: a sandbox session never reads these, and a production
/// read-only session never reads the sandbox pair.
pub const ONDO_MAINNET_API_KEY_VAR: &str = "ONDO_MAINNET_API_KEY";

/// The environment variable holding the mainnet API secret.
///
/// Read only for [`OndoAuthenticationScope::ProductionReadOnly`], with the same no-fallback rule
/// as [`ONDO_MAINNET_API_KEY_VAR`].
pub const ONDO_MAINNET_API_SECRET_VAR: &str = "ONDO_MAINNET_API_SECRET";

/// Why a credential could not be resolved.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    /// The environment gate refused the session before anything was read.
    #[error(transparent)]
    Environment(#[from] OndoEnvironmentError),
    /// The environment variable is not set.
    #[error(
        "`{name}` is not set: the Ondo Perps adapter reads its credentials from the environment (plan §1)"
    )]
    MissingVariable {
        /// The variable's name. Never its value.
        name: &'static str,
    },
    /// The value is present but carries nothing.
    #[error("`{name}` is set but is empty")]
    EmptyValue {
        /// The variable's or field's name. Never its value.
        name: &'static str,
    },
}

/// Enforces the gate an authenticated REST session must pass.
///
/// This is the one place a scope and a base URL are judged for the REST surface, and it reads
/// nothing: it takes no credential, opens no socket, and has no fallback path. Call it - or let
/// [`resolve_credential`] call it - before reading a key. It is the REST policy
/// ([`OndoSchemeFamily::Http`]) of [`crate::common::endpoint::OndoEndpointPolicy`], which the
/// private WebSocket applies for its own schemes.
///
/// # Errors
///
/// Returns the policy's refusal:
/// [`OndoEnvironmentError::ProductionHostForbidden`],
/// [`OndoEnvironmentError::UserInfoForbidden`], [`OndoEnvironmentError::HostNotAllowed`],
/// [`OndoEnvironmentError::UnsupportedScheme`], [`OndoEnvironmentError::PortNotAllowed`] or
/// [`OndoEnvironmentError::MalformedUrl`]. A URL the gate cannot place is refused, not passed
/// through: the allowlist admits the session's own official host and a loopback test service, and
/// nothing else.
pub fn validate_authenticated_environment(
    scope: OndoAuthenticationScope,
    base_url: &str,
) -> Result<OndoEndpoint, OndoEnvironmentError> {
    OndoEndpointPolicy::authenticated(scope, OndoSchemeFamily::Http).classify(base_url)
}

/// Enforces the gate an authenticated private WebSocket session must pass.
///
/// The same decision as [`validate_authenticated_environment`], for the other scheme family: the
/// two surfaces of one authenticated session are held to one policy
/// ([`OndoEndpointPolicy`]), and the only thing that differs is which schemes the endpoint's own
/// authority uses. A private session dials `wss://` for the official host and `ws://` or `wss://`
/// for a loopback test service, and a `https://` URL is refused here exactly as a `wss://` one is
/// refused there.
///
/// # Errors
///
/// Returns the policy's refusal; see [`validate_authenticated_environment`].
pub fn validate_authenticated_websocket_environment(
    scope: OndoAuthenticationScope,
    base_url: &str,
) -> Result<OndoEndpoint, OndoEnvironmentError> {
    OndoEndpointPolicy::authenticated(scope, OndoSchemeFamily::WebSocket).classify(base_url)
}

/// Resolves the credential for `scope` from the process environment.
///
/// The environment gate ([`validate_authenticated_environment`]) runs first, so a base URL the
/// policy refuses is refused before a variable is looked at. A sandbox scope reads
/// `ONDO_SANDBOX_API_KEY` and `ONDO_SANDBOX_API_SECRET`; the production read-only scope reads
/// `ONDO_MAINNET_API_KEY` and `ONDO_MAINNET_API_SECRET`. Surrounding whitespace is removed from
/// each variable's value: an assignment is not part of a credential. Nothing else about the value
/// changes, so the documented prefixes survive. There is no fallback between the two variable sets.
///
/// # Errors
///
/// Returns [`CredentialError::Environment`] when the gate refuses, or
/// [`CredentialError::MissingVariable`] / [`CredentialError::EmptyValue`] naming the variable that
/// had no usable value.
pub fn resolve_credential(
    scope: OndoAuthenticationScope,
    base_url: &str,
) -> Result<OndoCredential, CredentialError> {
    resolve_credential_with(scope, base_url, |name| std::env::var(name).ok())
}

/// [`resolve_credential`] against an injected variable lookup.
///
/// Crate-private on purpose: the lookup is a seam for the ordering test (a lookup that panics
/// proves the gate ran before any variable was read), not a public API for reading credentials from
/// somewhere other than the process environment.
pub(crate) fn resolve_credential_with<F>(
    scope: OndoAuthenticationScope,
    base_url: &str,
    lookup: F,
) -> Result<OndoCredential, CredentialError>
where
    F: Fn(&str) -> Option<String>,
{
    validate_authenticated_environment(scope, base_url)?;

    let (key_var, secret_var) = credential_variables(scope);
    let key_id = required(&lookup, key_var)?;
    let api_secret = required(&lookup, secret_var)?;

    OndoCredential::new(scope.environment(), key_id, api_secret)
}

/// The environment variables a scope's credential is read from.
///
/// Every scope names a pair, and the two sets are disjoint: a sandbox scope reads the sandbox
/// variables and the production read-only scope reads the mainnet ones. There is no fallback.
const fn credential_variables(scope: OndoAuthenticationScope) -> (&'static str, &'static str) {
    match scope {
        OndoAuthenticationScope::SandboxTrading | OndoAuthenticationScope::SandboxReadOnly => {
            (ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR)
        }
        OndoAuthenticationScope::ProductionReadOnly
        | OndoAuthenticationScope::ProductionTrading => {
            (ONDO_MAINNET_API_KEY_VAR, ONDO_MAINNET_API_SECRET_VAR)
        }
    }
}

/// Reads one required variable through `lookup`.
fn required<F>(lookup: &F, name: &'static str) -> Result<String, CredentialError>
where
    F: Fn(&str) -> Option<String>,
{
    let value = lookup(name)
        .map(|value| value.trim().to_string())
        .ok_or(CredentialError::MissingVariable { name })?;

    if value.is_empty() {
        return Err(CredentialError::EmptyValue { name });
    }

    Ok(value)
}

/// The API key id and secret an authenticated Ondo Perps request is signed with.
///
/// See the module documentation for what this type deliberately does not implement. The secret is
/// held as bytes so that no `&str` view of it exists outside this module, and it is wiped on drop.
#[derive(ZeroizeOnDrop)]
pub struct OndoCredential {
    #[zeroize(skip)]
    environment: OndoEnvironment,
    key_id: Box<str>,
    api_secret: Box<[u8]>,
}

impl OndoCredential {
    /// Creates a credential for `environment`.
    ///
    /// Both members are stored verbatim: the API-key page's prefixes (`ondoKeyId_`,
    /// `ondoApiSecret_`) are part of the values the venue compares, so removing one would produce a
    /// signature the venue rejects.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialError::EmptyValue`] when either member carries nothing. Whether the
    /// environment may be authenticated against at all is [`validate_authenticated_environment`]'s
    /// decision, not this constructor's.
    pub fn new(
        environment: OndoEnvironment,
        key_id: String,
        api_secret: String,
    ) -> Result<Self, CredentialError> {
        if key_id.trim().is_empty() {
            return Err(CredentialError::EmptyValue { name: "key_id" });
        }

        if api_secret.trim().is_empty() {
            return Err(CredentialError::EmptyValue { name: "api_secret" });
        }

        Ok(Self {
            environment,
            key_id: key_id.into_boxed_str(),
            api_secret: api_secret.into_bytes().into_boxed_slice(),
        })
    }

    /// Returns the environment this credential belongs to.
    #[must_use]
    pub const fn environment(&self) -> OndoEnvironment {
        self.environment
    }

    /// Returns the API key id, prefixes intact, as the `ONDO-KEY-ID` header carries it.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Returns the HMAC key: the full API secret, `ondoApiSecret_` prefix included.
    ///
    /// Crate-private so no later layer can hold a plaintext copy of the secret.
    pub(crate) fn api_secret(&self) -> &[u8] {
        &self.api_secret
    }

    /// Returns `text` with this credential's secret and key id replaced by the redaction marker.
    ///
    /// The authenticated transport applies this to a venue body before an error keeps it. The venue
    /// echoes the key id it was sent (its own `ip_not_permitted` message does exactly that), and a
    /// body is the last place a credential could reach a log.
    #[must_use]
    pub fn redact(&self, text: &str) -> String {
        let mut redacted = text.to_string();

        if let Ok(secret) = std::str::from_utf8(&self.api_secret) {
            if !secret.is_empty() {
                redacted = redacted.replace(secret, REDACTED);
            }
        }

        redacted.replace(self.key_id.as_ref(), REDACTED)
    }
}

impl fmt::Debug for OndoCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(OndoCredential))
            .field("environment", &self.environment)
            .field("key_id", &mask_api_key(&self.key_id))
            .field("api_secret", &REDACTED)
            .finish()
    }
}

impl fmt::Display for OndoCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::common::consts::ONDO_HTTP_BASE_URL_SANDBOX;

    const SANDBOX_URL: &str = "https://api.ondoperps-sandbox.xyz";

    /// The gate this module exposes is the REST policy, not a second implementation of it: the
    /// endpoint rules live with the endpoint type, and what is judged here is the same pair.
    #[rstest]
    fn test_the_gate_is_the_rest_endpoint_policy() {
        assert_eq!(
            validate_authenticated_environment(
                OndoAuthenticationScope::SandboxTrading,
                ONDO_HTTP_BASE_URL_SANDBOX
            ),
            Ok(OndoEndpoint::Official),
        );
        assert_eq!(
            validate_authenticated_environment(
                OndoAuthenticationScope::SandboxTrading,
                "http://127.0.0.1:8080"
            ),
            Ok(OndoEndpoint::LoopbackTestService),
        );
        assert_eq!(
            validate_authenticated_environment(
                OndoAuthenticationScope::SandboxTrading,
                "wss://api.ondoperps-sandbox.xyz/ws"
            ),
            Err(OndoEnvironmentError::UnsupportedScheme {
                scheme: "wss".to_string(),
                expected: "https",
            }),
            "a WebSocket URL is not a REST endpoint: the families are separate",
        );
        assert_eq!(
            validate_authenticated_environment(
                OndoAuthenticationScope::ProductionReadOnly,
                crate::common::consts::ONDO_HTTP_BASE_URL_PRODUCTION
            ),
            Ok(OndoEndpoint::Official),
            "the production read-only scope admits its own authority",
        );
    }

    /// The gate runs before any variable is read: the lookup panics if it is ever called, so a
    /// credential-first implementation cannot pass this test.
    #[rstest]
    fn test_the_environment_gate_runs_before_any_credential_is_read() {
        let never_called = |name: &str| panic!("the gate must refuse `{name}` before it is read");

        let error = resolve_credential_with(
            OndoAuthenticationScope::SandboxTrading,
            "https://api.ondoperps.xyz",
            never_called,
        )
        .expect_err("the production host is refused for a sandbox session");

        assert_eq!(
            error,
            CredentialError::Environment(OndoEnvironmentError::ProductionHostForbidden {
                host: "api.ondoperps.xyz".to_string(),
            }),
        );

        // A production read-only session may not be pointed at the sandbox authority, and the
        // refusal is a host refusal rather than a production one.
        let error = resolve_credential_with(
            OndoAuthenticationScope::ProductionReadOnly,
            SANDBOX_URL,
            never_called,
        )
        .expect_err("the sandbox host is refused for a production session");

        assert_eq!(
            error,
            CredentialError::Environment(OndoEnvironmentError::HostNotAllowed {
                host: "api.ondoperps-sandbox.xyz".to_string(),
            }),
        );
    }

    #[rstest]
    fn test_resolution_names_a_variable_that_is_absent_or_empty() {
        let absent = |_name: &str| None;
        let error =
            resolve_credential_with(OndoAuthenticationScope::SandboxTrading, SANDBOX_URL, absent)
                .expect_err("nothing to read");

        assert_eq!(
            error,
            CredentialError::MissingVariable {
                name: ONDO_SANDBOX_API_KEY_VAR
            },
        );
        assert!(
            error.to_string().contains("ONDO_SANDBOX_API_KEY"),
            "{error}"
        );

        let empty = |_name: &str| Some("   ".to_string());
        let error =
            resolve_credential_with(OndoAuthenticationScope::SandboxTrading, SANDBOX_URL, empty)
                .expect_err("nothing usable to read");

        assert_eq!(
            error,
            CredentialError::EmptyValue {
                name: ONDO_SANDBOX_API_KEY_VAR
            },
        );

        // The key id is present, so the missing one is named: the secret.
        let values = HashMap::from([(
            ONDO_SANDBOX_API_KEY_VAR,
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
        )]);
        let error = resolve_credential_with(
            OndoAuthenticationScope::SandboxTrading,
            SANDBOX_URL,
            |name| values.get(name).cloned(),
        )
        .expect_err("the secret is missing");

        assert_eq!(
            error,
            CredentialError::MissingVariable {
                name: ONDO_SANDBOX_API_SECRET_VAR
            },
        );
    }

    #[rstest]
    fn test_resolution_keeps_the_prefixes_and_drops_surrounding_whitespace() {
        let values = HashMap::from([
            (
                ONDO_SANDBOX_API_KEY_VAR,
                "  ondoKeyId_UNIT_TEST_ONLY\n".to_string(),
            ),
            (
                ONDO_SANDBOX_API_SECRET_VAR,
                "\tondoApiSecret_UNIT_TEST_ONLY ".to_string(),
            ),
        ]);

        let credential = resolve_credential_with(
            OndoAuthenticationScope::SandboxTrading,
            SANDBOX_URL,
            |name| values.get(name).cloned(),
        )
        .expect("the sandbox credential resolves");

        assert_eq!(credential.environment(), OndoEnvironment::Sandbox);
        assert_eq!(credential.key_id(), "ondoKeyId_UNIT_TEST_ONLY");
        assert_eq!(credential.api_secret(), b"ondoApiSecret_UNIT_TEST_ONLY");
    }

    #[rstest]
    fn test_redaction_removes_the_secret_and_the_key_id() {
        let credential = OndoCredential::new(
            OndoEnvironment::Sandbox,
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            "ondoApiSecret_UNIT_TEST_ONLY".to_string(),
        )
        .expect("the fake credential builds");

        let body = "IP addr 1.2.3.4 is not allowed for key ondoKeyId_UNIT_TEST_ONLY (secret ondoApiSecret_UNIT_TEST_ONLY)";
        let redacted = credential.redact(body);

        assert!(
            !redacted.contains("ondoApiSecret_UNIT_TEST_ONLY"),
            "{redacted}"
        );
        assert!(!redacted.contains("ondoKeyId_UNIT_TEST_ONLY"), "{redacted}");
        assert_eq!(redacted.matches(REDACTED).count(), 2, "{redacted}");
        // The rest of the venue's message survives: redaction is not censoring.
        assert!(
            redacted.contains("IP addr 1.2.3.4 is not allowed"),
            "{redacted}"
        );
    }

    #[rstest]
    fn test_the_secret_is_the_full_prefixed_value_and_no_plaintext_view_exists() {
        let credential = OndoCredential::new(
            OndoEnvironment::Sandbox,
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            "ondoApiSecret_UNIT_TEST_ONLY".to_string(),
        )
        .expect("the fake credential builds");

        assert_eq!(credential.api_secret(), b"ondoApiSecret_UNIT_TEST_ONLY");

        let rendered = format!("{credential:?} {credential}");
        assert!(
            !rendered.contains("ondoApiSecret_UNIT_TEST_ONLY"),
            "{rendered}"
        );
        assert!(!rendered.contains("ondoKeyId_UNIT_TEST_ONLY"), "{rendered}");
        assert!(rendered.contains(REDACTED));
        assert!(rendered.contains("ondo...ONLY"), "{rendered}");
    }

    #[rstest]
    fn test_a_blank_credential_is_refused_by_name() {
        assert_eq!(
            OndoCredential::new(
                OndoEnvironment::Sandbox,
                " ".to_string(),
                "secret".to_string()
            )
            .expect_err("a blank key id is refused"),
            CredentialError::EmptyValue { name: "key_id" },
        );
        assert_eq!(
            OndoCredential::new(
                OndoEnvironment::Sandbox,
                "key".to_string(),
                "\n".to_string()
            )
            .expect_err("a blank secret is refused"),
            CredentialError::EmptyValue { name: "api_secret" },
        );
    }

    /// The two variable sets are disjoint and there is no fallback: a session reads only the pair
    /// its scope names, whatever the other pair is set to.
    #[rstest]
    fn test_the_two_scopes_read_disjoint_variables() {
        assert_eq!(
            credential_variables(OndoAuthenticationScope::SandboxTrading),
            (ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR),
        );
        assert_eq!(
            credential_variables(OndoAuthenticationScope::SandboxReadOnly),
            (ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR),
        );
        assert_eq!(
            credential_variables(OndoAuthenticationScope::ProductionReadOnly),
            (ONDO_MAINNET_API_KEY_VAR, ONDO_MAINNET_API_SECRET_VAR),
        );
    }

    /// The mainnet credential resolves only for the production read-only scope, and a present
    /// mainnet pair is invisible to a sandbox session.
    #[rstest]
    fn test_the_production_read_only_scope_resolves_the_mainnet_credential() {
        let values = HashMap::from([
            (
                ONDO_MAINNET_API_KEY_VAR,
                "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            ),
            (
                ONDO_MAINNET_API_SECRET_VAR,
                "ondoApiSecret_UNIT_TEST_ONLY".to_string(),
            ),
        ]);

        let credential = resolve_credential_with(
            OndoAuthenticationScope::ProductionReadOnly,
            crate::common::consts::ONDO_HTTP_BASE_URL_PRODUCTION,
            |name| values.get(name).cloned(),
        )
        .expect("the production read-only credential resolves");

        assert_eq!(credential.environment(), OndoEnvironment::Production);
        assert_eq!(credential.key_id(), "ondoKeyId_UNIT_TEST_ONLY");

        // A sandbox session with only the mainnet pair present names the sandbox variable it
        // wanted, so a cross-environment credential can never be substituted.
        let error = resolve_credential_with(
            OndoAuthenticationScope::SandboxTrading,
            SANDBOX_URL,
            |name| values.get(name).cloned(),
        )
        .expect_err("the sandbox pair is absent");

        assert_eq!(
            error,
            CredentialError::MissingVariable {
                name: ONDO_SANDBOX_API_KEY_VAR
            },
        );
    }
}
