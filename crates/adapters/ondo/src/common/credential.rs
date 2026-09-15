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
//! production; §6.1: no automatic protocol or environment switching):
//!
//! 1. an authenticated session may only be opened for [`OndoEnvironment::Sandbox`]; a production
//!    configuration is refused with [`OndoEnvironmentError::ProductionForbidden`], whatever URL it
//!    carries;
//! 2. the base URL host may not be the production host or a subdomain of it, so the configuration's
//!    own `base_url_http` override cannot aim a sandbox credential at production. That refusal is
//!    [`OndoEnvironmentError::ProductionHostForbidden`].
//!
//! Neither refusal reads a credential, opens a socket, or has a fallback: there is one gate and one
//! answer.
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

use crate::common::enums::OndoEnvironment;

/// The environment variable holding the sandbox API key id (plan §7 Task 9).
pub const ONDO_SANDBOX_API_KEY_VAR: &str = "ONDO_SANDBOX_API_KEY";

/// The environment variable holding the sandbox API secret (plan §7 Task 9).
pub const ONDO_SANDBOX_API_SECRET_VAR: &str = "ONDO_SANDBOX_API_SECRET";

/// The registrable domain every production host belongs to.
///
/// Derived from [`crate::common::consts::ONDO_HTTP_BASE_URL_PRODUCTION`] rather than written out
/// twice: the gate refuses this domain and any subdomain of it. The sandbox host is a different
/// registrable domain (`ondoperps-sandbox.xyz`), so it is not caught by this rule.
const ONDO_PRODUCTION_DOMAIN: &str = "ondoperps.xyz";

/// Why an environment cannot carry an authenticated session.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OndoEnvironmentError {
    /// Production is not an environment this adapter may authenticate against.
    #[error(
        "an authenticated Ondo Perps session may not be opened against production: this adapter \
         authenticates the sandbox environment only (plan §1, §6.1)"
    )]
    ProductionForbidden,
    /// The base URL points at the production venue.
    #[error(
        "the base URL host `{host}` belongs to the production Ondo Perps domain, so a sandbox \
         credential may not be sent to it"
    )]
    ProductionHostForbidden {
        /// The host the rule matched.
        host: String,
    },
}

/// Why a credential could not be resolved.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    /// The environment gate refused the session before anything was read.
    #[error(transparent)]
    Environment(#[from] OndoEnvironmentError),
    /// The environment variable is not set.
    #[error(
        "`{name}` is not set: the Ondo Perps sandbox adapter reads its credentials from the environment (plan §1)"
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

/// Enforces the gate an authenticated session must pass.
///
/// This is the only place an environment and a base URL are judged, and it reads nothing: it takes
/// no credential, opens no socket, and has no fallback path. Call it - or let
/// [`resolve_credential`] call it - before reading a key.
///
/// # Errors
///
/// Returns [`OndoEnvironmentError::ProductionForbidden`] when `environment` is
/// [`OndoEnvironment::Production`], and [`OndoEnvironmentError::ProductionHostForbidden`] when
/// `base_url`'s host is the production domain or a subdomain of it. A URL whose host cannot be read
/// is not the production host and is accepted: the rule's job is to make production unreachable,
/// not to allow-list hosts (a local test server is an explicit, documented use of
/// `base_url_http`).
pub fn validate_authenticated_environment(
    environment: OndoEnvironment,
    base_url: &str,
) -> Result<(), OndoEnvironmentError> {
    if environment != OndoEnvironment::Sandbox {
        return Err(OndoEnvironmentError::ProductionForbidden);
    }

    let Some(host) = host_of(base_url) else {
        return Ok(());
    };

    if host == ONDO_PRODUCTION_DOMAIN || host.ends_with(&format!(".{ONDO_PRODUCTION_DOMAIN}")) {
        return Err(OndoEnvironmentError::ProductionHostForbidden { host });
    }

    Ok(())
}

/// Returns the lowercased host of `base_url`, when it carries one.
fn host_of(base_url: &str) -> Option<String> {
    let after_scheme = base_url
        .split_once("://")
        .map_or(base_url, |(_scheme, rest)| rest);
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    // `user:password@host`
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_userinfo, host)| host);
    // An IPv6 literal is bracketed; anything else ends at its port.
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map(|(host, _port)| host)?,
        None => authority.split(':').next()?,
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();

    (!host.is_empty()).then_some(host)
}

/// Resolves the sandbox credential from the process environment.
///
/// The environment gate ([`validate_authenticated_environment`]) runs first, so a production
/// configuration or a production base URL is refused before `ONDO_SANDBOX_API_KEY` or
/// `ONDO_SANDBOX_API_SECRET` is looked at. Surrounding whitespace is removed from each variable's
/// value: an assignment is not part of a credential. Nothing else about the value changes, so the
/// documented prefixes survive.
///
/// # Errors
///
/// Returns [`CredentialError::Environment`] when the gate refuses, or
/// [`CredentialError::MissingVariable`] / [`CredentialError::EmptyValue`] naming the variable that
/// had no usable value.
pub fn resolve_credential(
    environment: OndoEnvironment,
    base_url: &str,
) -> Result<OndoCredential, CredentialError> {
    resolve_credential_with(environment, base_url, |name| std::env::var(name).ok())
}

/// [`resolve_credential`] against an injected variable lookup.
///
/// Crate-private on purpose: the lookup is a seam for the ordering test (a lookup that panics
/// proves the gate ran before any variable was read), not a public API for reading credentials from
/// somewhere other than the process environment.
pub(crate) fn resolve_credential_with<F>(
    environment: OndoEnvironment,
    base_url: &str,
    lookup: F,
) -> Result<OndoCredential, CredentialError>
where
    F: Fn(&str) -> Option<String>,
{
    validate_authenticated_environment(environment, base_url)?;

    // Production names no variable at all: the gate above already refused it, and returning early
    // keeps that true if the gate is ever reordered.
    let Some((key_var, secret_var)) = credential_variables(environment) else {
        return Err(CredentialError::Environment(
            OndoEnvironmentError::ProductionForbidden,
        ));
    };

    let key_id = required(&lookup, key_var)?;
    let api_secret = required(&lookup, secret_var)?;

    OndoCredential::new(environment, key_id, api_secret)
}

/// The environment variables a credential is read from, or [`None`] for an environment this adapter
/// does not authenticate against.
fn credential_variables(environment: OndoEnvironment) -> Option<(&'static str, &'static str)> {
    match environment {
        OndoEnvironment::Sandbox => Some((ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR)),
        OndoEnvironment::Production => None,
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
    use crate::common::consts::ONDO_HTTP_BASE_URL_PRODUCTION;

    const SANDBOX_URL: &str = "https://api.ondoperps-sandbox.xyz";

    #[rstest]
    fn test_the_production_domain_covers_the_documented_production_base_url() {
        let host = host_of(ONDO_HTTP_BASE_URL_PRODUCTION).expect("the production URL has a host");

        assert!(
            host == ONDO_PRODUCTION_DOMAIN || host.ends_with(&format!(".{ONDO_PRODUCTION_DOMAIN}")),
            "`{host}` must be inside the domain the gate refuses",
        );
        assert!(
            host_of(SANDBOX_URL)
                .is_some_and(|host| !host.ends_with(&format!(".{ONDO_PRODUCTION_DOMAIN}"))),
            "the sandbox host must not be inside the production domain",
        );
    }

    #[rstest]
    #[case::scheme_and_path("https://api.ondoperps.xyz/v1/markets", Some("api.ondoperps.xyz"))]
    #[case::no_scheme("api.ondoperps.xyz:8443", Some("api.ondoperps.xyz"))]
    #[case::trailing_root_dot("api.ondoperps.xyz.", Some("api.ondoperps.xyz"))]
    #[case::userinfo("https://key:secret@api.ondoperps.xyz", Some("api.ondoperps.xyz"))]
    #[case::upper_case("HTTPS://API.ONDOPERPS.XYZ", Some("api.ondoperps.xyz"))]
    #[case::ipv6("http://[::1]:8080/ws", Some("::1"))]
    #[case::loopback("http://127.0.0.1:8080", Some("127.0.0.1"))]
    #[case::empty("", None)]
    fn test_host_extraction(#[case] url: &str, #[case] expected: Option<&str>) {
        assert_eq!(host_of(url).as_deref(), expected);
    }

    /// The gate runs before any variable is read: the lookup panics if it is ever called, so a
    /// credential-first implementation cannot pass this test.
    #[rstest]
    fn test_the_environment_gate_runs_before_any_credential_is_read() {
        let never_called = |name: &str| panic!("the gate must refuse `{name}` before it is read");

        let error = resolve_credential_with(
            OndoEnvironment::Sandbox,
            "https://api.ondoperps.xyz",
            never_called,
        )
        .expect_err("the production host is refused");

        assert_eq!(
            error,
            CredentialError::Environment(OndoEnvironmentError::ProductionHostForbidden {
                host: "api.ondoperps.xyz".to_string(),
            }),
        );

        let error = resolve_credential_with(OndoEnvironment::Production, SANDBOX_URL, never_called)
            .expect_err("production is refused");

        assert_eq!(
            error,
            CredentialError::Environment(OndoEnvironmentError::ProductionForbidden),
        );
    }

    #[rstest]
    fn test_resolution_names_a_variable_that_is_absent_or_empty() {
        let absent = |_name: &str| None;
        let error = resolve_credential_with(OndoEnvironment::Sandbox, SANDBOX_URL, absent)
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
        let error = resolve_credential_with(OndoEnvironment::Sandbox, SANDBOX_URL, empty)
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
        let error = resolve_credential_with(OndoEnvironment::Sandbox, SANDBOX_URL, |name| {
            values.get(name).cloned()
        })
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

        let credential = resolve_credential_with(OndoEnvironment::Sandbox, SANDBOX_URL, |name| {
            values.get(name).cloned()
        })
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

    #[rstest]
    fn test_production_names_no_credential_variable() {
        assert!(credential_variables(OndoEnvironment::Production).is_none());
        assert_eq!(
            credential_variables(OndoEnvironment::Sandbox),
            Some((ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR)),
        );
    }
}
