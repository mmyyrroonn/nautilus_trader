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

//! Offline contract for the Ondo Perps request signing and the credential wrapper.
//!
//! Nothing here reaches the venue and no credential is read: every key in this file is fake key
//! material, and every expected signature comes from
//! `test_data/signing_rest_vectors.json`, which was produced by a **Python** standard-library
//! script (`hashlib` + `hmac`) rather than by this implementation. The Rust assertions therefore
//! compare against a foreign computation instead of restating their own expression.
//!
//! The authenticated transport (which header set goes on the wire, which bytes are signed, what a
//! 401/403 from the real path looks like) is exercised in `tests/http_client.rs`, next to the
//! scripted mock server that already captures request heads.

use nautilus_core::{hex, string::secret::REDACTED};
use nautilus_ondo::{
    common::{
        credential::{
            CredentialError, ONDO_SANDBOX_API_KEY_VAR, ONDO_SANDBOX_API_SECRET_VAR, OndoCredential,
            OndoEndpoint, OndoEnvironmentError, resolve_credential,
            validate_authenticated_environment,
        },
        enums::OndoEnvironment,
    },
    http::{
        client::OndoHttpClient,
        error::{OndoAuthFailure, OndoHttpError, classify_auth_rejection},
        query::ORDERS_PATH,
    },
    signing::{
        ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS, ONDO_WS_LOGIN_PHRASE, OndoSigningError,
        check_clock_skew, sign_rest, sign_ws,
    },
};
use rstest::rstest;

/// The Python-generated reference vectors. See the file's `_fixture` block for its provenance.
const VECTORS: &str = include_str!("../test_data/signing_rest_vectors.json");

/// sha256 of `test_data/signing_rest_vectors.json`, computed when the fixture was generated.
///
/// Pinned so an edit of the reference values cannot pass unnoticed. The digest is taken with the
/// crate's own hash dependency rather than with the signing code under test.
///
/// Re-pinned 2026-09-15: the first generation signed the path `/v1/perps/account`, which no spec
/// declares, in the `empty-get-body` case; it was corrected to the real `/v1/account` and every
/// other case was re-derived unchanged. The fixture's own `_fixture.correction` records this, and
/// the script that did it is archived at
/// `reports/ondo-acceptance/20260915T-signed-fixture-correction/gen_signing_vectors.py`.
const VECTORS_SHA256: &str = "a60d10f0c9e2ae85fb7227f2cce00e8b51af1a20e2ed1b3b283a329d4123de2f";

fn fixture() -> serde_json::Value {
    serde_json::from_str(VECTORS).expect("the reference fixture parses")
}

fn credential() -> OndoCredential {
    let fixture = fixture();

    OndoCredential::new(
        OndoEnvironment::Sandbox,
        fixture["key_id"].as_str().expect("key_id").to_string(),
        fixture["api_secret"]
            .as_str()
            .expect("api_secret")
            .to_string(),
    )
    .expect("the fake unit-test credential is well formed")
}

/// The fixture's secret, as the raw `&str` the tests sign with when they need one.
fn fake_secret() -> String {
    fixture()["api_secret"]
        .as_str()
        .expect("api_secret")
        .to_string()
}

fn rest_cases() -> Vec<serde_json::Value> {
    fixture()["rest_cases"]
        .as_array()
        .expect("rest_cases array")
        .clone()
}

fn rest_case(label: &str) -> serde_json::Value {
    rest_cases()
        .into_iter()
        .find(|case| case["label"] == label)
        .unwrap_or_else(|| panic!("reference case `{label}` is present"))
}

fn sign_case(case: &serde_json::Value) -> String {
    sign_rest(
        &credential(),
        case["timestamp_ms"]
            .as_str()
            .expect("timestamp_ms")
            .parse::<u64>()
            .expect("timestamp_ms is a millisecond count"),
        case["method_in"].as_str().expect("method_in"),
        case["path_query"].as_str().expect("path_query"),
        case["body"].as_str().expect("body").as_bytes(),
    )
}

// ------------------------------------------------------------------------------------------------
// The fixture is the reference; the hashes below are its provenance
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_reference_fixture_is_the_python_generated_one() {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, VECTORS.as_bytes());

    assert_eq!(
        hex::encode(digest.as_ref()),
        VECTORS_SHA256,
        "the reference vectors must be the file the Python generator produced",
    );

    let fixture = fixture();

    assert_eq!(fixture["_fixture"]["kind"], "synthetic");
    assert_eq!(fixture["_fixture"]["generator_language"], "Python 3.12.9");
    assert!(
        fixture["_fixture"]["key_material"]
            .as_str()
            .expect("key_material")
            .starts_with("FAKE."),
        "the fixture's key material must be labelled as fake",
    );
    assert_eq!(fixture["ws_login_phrase"], ONDO_WS_LOGIN_PHRASE);

    // The Task 6 brief's own vector, recomputed here by the Python interpreter the brief names:
    // `hmac.new(b"ondoApiSecret_UNIT_TEST_ONLY", b"1789384200000GET/v1/perps/orders?market=NVDA-USD.P&limit=2",
    // hashlib.sha256).hexdigest()`.
    assert_eq!(
        rest_case("task-6-brief-vector")["signature"],
        "df4ba6005e3f028f9928a80b0f8dd33ad6af488418dffed317a9543d2d9bb816",
    );
}

// ------------------------------------------------------------------------------------------------
// The Rust signature is exactly the reference signature
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_every_reference_vector_matches_the_rust_signature() {
    for case in rest_cases() {
        let label = case["label"].as_str().expect("label");
        let expected = case["signature"].as_str().expect("signature");
        let signature = sign_case(&case);

        assert_eq!(
            signature,
            expected,
            "case `{label}`: message `{}`",
            case["message"].as_str().expect("message"),
        );
        assert_eq!(
            signature.len(),
            64,
            "case `{label}` is lowercase hex sha256"
        );
    }
}

/// The method is upper-cased before it enters the message, and the reference vector proves it: the
/// case is called with `get` while the Python message carries `GET`.
#[rstest]
fn test_a_lowercase_method_is_signed_as_the_uppercase_form() {
    let case = rest_case("lowercase-method-is-signed-uppercase");

    assert_eq!(case["method_in"], "get");
    assert_eq!(case["method_in_message"], "GET");
    assert_eq!(
        sign_case(&case),
        case["signature"].as_str().expect("signature"),
    );
    assert_eq!(
        sign_case(&case),
        sign_rest(
            &credential(),
            1_789_384_200_000,
            "GET",
            "/v1/perps/contracts",
            b"",
        ),
    );
}

/// One byte of the query, or of the body, is the difference between two signatures. The mutation is
/// compared against the *reference* signature, so the assertion cannot be satisfied by a function
/// that hashes something other than the request.
#[rstest]
fn test_changing_one_byte_of_the_query_or_the_body_changes_the_signature() {
    let case = rest_case("post-limit-order-body-decimal-strings");
    let expected = case["signature"].as_str().expect("signature").to_string();
    let timestamp_ms = 1_789_384_200_456;
    let path_query = case["path_query"].as_str().expect("path_query");
    let body = case["body"].as_str().expect("body");
    let credential = credential();

    assert_eq!(
        sign_rest(
            &credential,
            timestamp_ms,
            "POST",
            path_query,
            body.as_bytes()
        ),
        expected,
    );

    let mutated_query = "/v1/perps/orderz".to_string();
    assert_ne!(
        sign_rest(
            &credential,
            timestamp_ms,
            "POST",
            &mutated_query,
            body.as_bytes()
        ),
        expected,
        "one byte of the path changes the signature",
    );

    let mutated_body = body.replace("\"0.01\"", "\"0.02\"");
    assert_ne!(mutated_body, body);
    assert_ne!(
        sign_rest(
            &credential,
            timestamp_ms,
            "POST",
            path_query,
            mutated_body.as_bytes()
        ),
        expected,
        "one byte of the body changes the signature",
    );

    // The query parameter order is part of the signed bytes, not something the signer re-sorts.
    let reference_case = rest_case("task-6-brief-vector");
    assert_ne!(
        signature_of(
            &reference_case,
            "/v1/perps/orders?market=NVDA-USD.P&limit=2"
        ),
        signature_of(
            &reference_case,
            "/v1/perps/orders?limit=2&market=NVDA-USD.P"
        ),
        "the signer covers the query in the order it was given, never re-ordered",
    );
}

fn signature_of(case: &serde_json::Value, path_query: &str) -> String {
    sign_rest(
        &credential(),
        case["timestamp_ms"]
            .as_str()
            .expect("timestamp_ms")
            .parse::<u64>()
            .expect("timestamp_ms"),
        case["method_in"].as_str().expect("method_in"),
        path_query,
        b"",
    )
}

/// An empty GET body and a DELETE body are signed over as the bytes they are.
#[rstest]
#[case::empty_get_body("empty-get-body")]
#[case::delete_with_body("delete-with-body")]
fn test_empty_and_delete_bodies_are_signed_over_exactly(#[case] label: &str) {
    let case = rest_case(label);

    assert_eq!(
        sign_case(&case),
        case["signature"].as_str().expect("signature"),
    );
}

/// A decimal string and a UTF-8 value are signed byte-for-byte, without a JSON round trip.
#[rstest]
#[case::decimal_strings("post-limit-order-body-decimal-strings")]
#[case::utf8_query_value("utf8-query-value")]
#[case::utf8_body("utf8-body")]
fn test_decimals_and_utf8_survive_the_signature(#[case] label: &str) {
    let case = rest_case(label);
    let path_query = case["path_query"].as_str().expect("path_query");
    let body = case["body"].as_str().expect("body");

    assert_eq!(
        sign_case(&case),
        case["signature"].as_str().expect("signature"),
    );

    match label {
        "post-limit-order-body-decimal-strings" => {
            assert!(body.contains(r#""size":"0.01""#), "{body}");
            assert!(
                body.contains(r#""price":"100.00""#),
                "the scale survives: {body}"
            );
        }
        "utf8-query-value" => {
            assert!(path_query.contains("caf%C3%A9"), "{path_query}");
            assert_eq!(
                percent_decode("caf%C3%A9"),
                "café",
                "the signed query carries the UTF-8 value",
            );
        }
        "utf8_body" | "utf8-body" => {
            assert!(body.contains("café"), "{body}");
            assert!(
                body.contains(r#""size":"0.01000000""#),
                "the decimal scale is signed as written: {body}",
            );
        }
        other => panic!("unexpected case `{other}`"),
    }
}

/// Decodes the percent-escapes of an ASCII query fragment, so a test can name the UTF-8 value a
/// signed query carries rather than trusting that the escape means what it says.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).expect("hex digits");
            out.push(u8::from_str_radix(hex, 16).expect("valid hex escape"));
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }

    String::from_utf8(out).expect("the escape sequence is UTF-8")
}

// ------------------------------------------------------------------------------------------------
// The WebSocket login digest: one order, and only one
// ------------------------------------------------------------------------------------------------

/// `conflicts.md` conflict 2 is UNRESOLVED offline, and the adapter implements reading A (`time`
/// first, the login-page prose) as the only order it can produce. The fixture records **both**
/// candidate digests, so this test proves the implementation is reading A *and* that reading B is a
/// different value - which is what makes a one-line flip in `signing::ws_login_message` the whole
/// change a sandbox retry needs.
#[rstest]
fn test_the_ws_login_digest_is_the_timestamp_first_order_and_not_the_other() {
    let fixture = fixture();
    let cases = fixture["ws_login_cases"]
        .as_array()
        .expect("ws_login_cases");
    let timestamp_ms = 1_789_384_200_000;

    let reading_a = cases
        .iter()
        .find(|case| case["label"] == "timestamp-first-reading-a-implemented")
        .expect("reading A is recorded");
    let reading_b = cases
        .iter()
        .find(|case| case["label"] == "phrase-first-reading-b-not-implemented")
        .expect("reading B is recorded");

    assert_eq!(
        reading_a["message"],
        format!("{timestamp_ms}{ONDO_WS_LOGIN_PHRASE}")
    );
    assert_eq!(
        reading_b["message"],
        format!("{ONDO_WS_LOGIN_PHRASE}{timestamp_ms}")
    );

    let signature = sign_ws(&credential(), timestamp_ms);

    assert_eq!(
        signature,
        reading_a["signature"]
            .as_str()
            .expect("reading A signature"),
        "the WS login digest must be reading A (`time` first)",
    );
    assert_ne!(
        signature,
        reading_b["signature"]
            .as_str()
            .expect("reading B signature"),
        "reading B is a different digest: nothing may fall back to it automatically",
    );
}

// ------------------------------------------------------------------------------------------------
// The credential keeps its documented prefixes
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_documented_key_prefixes_are_kept_intact() {
    let fixture = fixture();
    let key_id = fixture["key_id"].as_str().expect("key_id");
    let api_secret = fixture["api_secret"].as_str().expect("api_secret");

    assert!(key_id.starts_with("ondoKeyId_"), "{key_id}");
    assert!(api_secret.starts_with("ondoApiSecret_"), "{api_secret}");

    let credential = credential();
    assert_eq!(credential.key_id(), key_id);
    assert_eq!(credential.environment(), OndoEnvironment::Sandbox);

    // The reference digest is keyed with the *prefixed* secret. A stripped secret is a different
    // HMAC key, which is exactly the failure the prefix rule exists to prevent.
    let stripped = OndoCredential::new(
        OndoEnvironment::Sandbox,
        key_id.to_string(),
        api_secret.trim_start_matches("ondoApiSecret_").to_string(),
    )
    .expect("a stripped secret is still a credential value");

    let case = rest_case("task-6-brief-vector");
    let expected = case["signature"].as_str().expect("signature");
    let timestamp_ms = 1_789_384_200_000;
    let path_query = case["path_query"].as_str().expect("path_query");

    assert_eq!(
        sign_rest(&credential, timestamp_ms, "GET", path_query, b""),
        expected,
    );
    assert_ne!(
        sign_rest(&stripped, timestamp_ms, "GET", path_query, b""),
        expected,
        "the HMAC key is the full secret, prefix included",
    );
}

#[rstest]
fn test_an_empty_credential_member_is_refused_by_name() {
    let secret = fake_secret();

    let no_key = OndoCredential::new(OndoEnvironment::Sandbox, String::new(), secret.clone())
        .expect_err("an empty key id is refused");
    assert_eq!(no_key, CredentialError::EmptyValue { name: "key_id" });

    let no_secret = OndoCredential::new(
        OndoEnvironment::Sandbox,
        "ondoKeyId_UNIT".to_string(),
        String::new(),
    )
    .expect_err("an empty secret is refused");
    assert_eq!(
        no_secret,
        CredentialError::EmptyValue { name: "api_secret" }
    );
}

// ------------------------------------------------------------------------------------------------
// The venue's named auth failures
// ------------------------------------------------------------------------------------------------

/// Every error code `api_key_authentication.md` documents maps to its own named failure. The codes
/// arrive in the venue's own body, so the classification reads the body it was given - and never
/// switches accounts, retries, or falls back to another environment.
#[rstest]
#[case::wrong_key(
    401,
    r#"{"code":"api_key_not_found"}"#,
    OndoAuthFailure::ApiKeyNotFound
)]
#[case::expired_timestamp(
    401,
    r#"{"code":"timestamp_too_far"}"#,
    OndoAuthFailure::TimestampTooFar
)]
#[case::unparseable_timestamp(
    401,
    r#"{"errorCode":"failed_to_parse_timestamp"}"#,
    OndoAuthFailure::FailedToParseTimestamp
)]
#[case::undecodable_signature(
    401,
    r#"{"error_code":"failed_to_decode_hex_signature"}"#,
    OndoAuthFailure::FailedToDecodeHexSignature
)]
#[case::signature_mismatch(
    401,
    r#"{"code":"signature_mismatch"}"#,
    OndoAuthFailure::SignatureMismatch
)]
#[case::wrong_scope(
    403,
    r#"{"code":"key_doesnt_have_scope"}"#,
    OndoAuthFailure::KeyDoesntHaveScope
)]
#[case::ip_not_permitted(401, r#"{"code":"ip_not_permitted"}"#, OndoAuthFailure::IpNotPermitted)]
#[case::bare_401(401, "login required", OndoAuthFailure::Unauthorized)]
#[case::bare_403(403, "forbidden", OndoAuthFailure::Forbidden)]
fn test_the_documented_auth_failures_are_distinct_and_explicit(
    #[case] status: u16,
    #[case] body: &str,
    #[case] expected: OndoAuthFailure,
) {
    let error = classify_auth_rejection(status, body.to_string());

    match &error {
        OndoHttpError::AuthRejected {
            failure,
            status: seen,
            ..
        } => {
            assert_eq!(*failure, expected);
            assert_eq!(*seen, status);
        }
        other => panic!("expected a named auth rejection, was {other:?}"),
    }

    let rendered = error.to_string();
    assert!(
        rendered.contains(expected.name()),
        "`{rendered}` must name the failure `{}`",
        expected.name(),
    );
    assert!(
        !nautilus_ondo::http::error::should_retry_ondo_http_error(&error),
        "an auth rejection is terminal: never retried, never re-signed against another environment",
    );
}

/// The two boundaries the brief names separately: an expired timestamp (the ±30 s tolerance) and a
/// plain 401 are different failures, and neither is a 403.
#[rstest]
fn test_an_expired_timestamp_is_not_a_401_and_not_a_forbidden() {
    let expired = classify_auth_rejection(401, r#"{"code":"timestamp_too_far"}"#.to_string());
    let unauthorized = classify_auth_rejection(401, "login required".to_string());
    let forbidden = classify_auth_rejection(403, "forbidden".to_string());

    assert_ne!(expired.to_string(), unauthorized.to_string());
    assert_ne!(unauthorized.to_string(), forbidden.to_string());
    assert_ne!(expired.to_string(), forbidden.to_string());
}

// ------------------------------------------------------------------------------------------------
// The environment gate runs before any credential is read
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_sandbox_gate_refuses_production_in_every_form() {
    assert_eq!(
        validate_authenticated_environment(
            OndoEnvironment::Sandbox,
            "https://api.ondoperps-sandbox.xyz",
        ),
        Ok(OndoEndpoint::Official),
    );
    assert_eq!(
        validate_authenticated_environment(OndoEnvironment::Sandbox, "http://127.0.0.1:8080"),
        Ok(OndoEndpoint::LoopbackTestService),
    );

    // The config type's own override slot is what makes this reachable: `base_url_http` may be set
    // to anything, so the gate, not the configuration, is what keeps production unreachable.
    assert_eq!(
        validate_authenticated_environment(OndoEnvironment::Sandbox, "https://api.ondoperps.xyz"),
        Err(OndoEnvironmentError::ProductionHostForbidden {
            host: "api.ondoperps.xyz".to_string(),
        }),
    );
    assert_eq!(
        validate_authenticated_environment(OndoEnvironment::Sandbox, "https://ondoperps.xyz"),
        Err(OndoEnvironmentError::ProductionHostForbidden {
            host: "ondoperps.xyz".to_string(),
        }),
    );

    // A host that merely contains the production host is not the production host - and, under the
    // allowlist, it is not the official host either, so it is refused. This case used to be the
    // gate's *acceptance* case, which is exactly the hole the allowlist closes: the old rule
    // refused production and admitted every other host.
    assert_eq!(
        validate_authenticated_environment(
            OndoEnvironment::Sandbox,
            "https://api.ondoperps.xyz.evil.example",
        ),
        Err(OndoEnvironmentError::HostNotAllowed {
            host: "api.ondoperps.xyz.evil.example".to_string(),
        }),
    );

    // Production is refused whatever the URL says, so a production configuration can never be
    // authenticated even if it points at the sandbox host.
    assert_eq!(
        validate_authenticated_environment(
            OndoEnvironment::Production,
            "https://api.ondoperps-sandbox.xyz",
        ),
        Err(OndoEnvironmentError::ProductionForbidden),
    );
}

#[rstest]
fn test_a_production_base_url_cannot_bypass_the_sandbox_gate() {
    // No `ONDO_SANDBOX_*` variable is set in this test process, so a missing-variable error would be
    // the answer if the credential were read first. The answer is the *environment* error, which is
    // what makes the ordering observable from outside.
    let error = resolve_credential(OndoEnvironment::Sandbox, "https://api.ondoperps.xyz")
        .expect_err("the production host is refused before anything is read");

    assert_eq!(
        error,
        CredentialError::Environment(OndoEnvironmentError::ProductionHostForbidden {
            host: "api.ondoperps.xyz".to_string(),
        }),
    );

    let error = resolve_credential(OndoEnvironment::Production, "https://api.ondoperps.xyz")
        .expect_err("production is refused");
    assert_eq!(
        error,
        CredentialError::Environment(OndoEnvironmentError::ProductionForbidden),
    );

    // ... and the client refuses to be built at all, which is the last line of defence: an
    // authenticated client with a production base URL does not exist, so nothing can send from it.
    let error = OndoHttpClient::builder()
        .base_url("https://api.ondoperps.xyz".to_string())
        .credential(credential())
        .build()
        .expect_err("an authenticated client cannot be built on the production host");

    assert!(
        matches!(
            error,
            OndoHttpError::Environment(OndoEnvironmentError::ProductionHostForbidden { .. })
        ),
        "was {error:?}",
    );

    // The sandbox host, and the same credential, build: the gate is not merely refusing everything.
    let client = OndoHttpClient::builder()
        .base_url("https://api.ondoperps-sandbox.xyz".to_string())
        .credential(credential())
        .build()
        .expect("the sandbox client builds");

    assert!(client.is_authenticated());
    assert_eq!(
        client.base_url(),
        "https://api.ondoperps-sandbox.xyz",
        "the gate does not rewrite the URL it accepted",
    );
}

#[rstest]
fn test_the_sandbox_credential_variables_are_the_documented_names() {
    assert_eq!(ONDO_SANDBOX_API_KEY_VAR, "ONDO_SANDBOX_API_KEY");
    assert_eq!(ONDO_SANDBOX_API_SECRET_VAR, "ONDO_SANDBOX_API_SECRET");
}

// ------------------------------------------------------------------------------------------------
// The endpoint gate is an allowlist, not a production blacklist
// ------------------------------------------------------------------------------------------------

/// An authenticated session signs for exactly two authorities: the official host of its own
/// environment, and a loopback test service. Every other authority - however it is spelled - is
/// refused, with the reason it was refused, and the refusal happens before a credential is read.
#[rstest]
#[case::an_unrelated_remote_host(
    "https://evil.example",
    OndoEnvironmentError::HostNotAllowed { host: "evil.example".to_string() }
)]
#[case::a_host_that_merely_contains_the_sandbox_host(
    "https://api.ondoperps-sandbox.xyz.evil.example",
    OndoEnvironmentError::HostNotAllowed { host: "api.ondoperps-sandbox.xyz.evil.example".to_string() }
)]
#[case::a_subdomain_of_the_sandbox_host(
    "https://eu.api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::HostNotAllowed { host: "eu.api.ondoperps-sandbox.xyz".to_string() }
)]
#[case::the_loopback_name(
    "http://localhost:8080",
    OndoEnvironmentError::HostNotAllowed { host: "localhost".to_string() }
)]
#[case::a_private_network_address(
    "http://192.168.1.10:8080",
    OndoEnvironmentError::HostNotAllowed { host: "192.168.1.10".to_string() }
)]
#[case::an_unencrypted_remote_service(
    "http://evil.example",
    OndoEnvironmentError::HostNotAllowed { host: "evil.example".to_string() }
)]
#[case::the_sandbox_host_over_plain_http(
    "http://api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::UnsupportedScheme { scheme: "http".to_string(), expected: "https" }
)]
#[case::the_sandbox_host_on_another_port(
    "https://api.ondoperps-sandbox.xyz:8443",
    OndoEnvironmentError::PortNotAllowed { host: "api.ondoperps-sandbox.xyz".to_string(), port: 8443 }
)]
#[case::userinfo_in_front_of_the_sandbox_host(
    "https://key:secret@api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::UserInfoForbidden
)]
#[case::userinfo_in_front_of_an_unrelated_host(
    "https://api.ondoperps-sandbox.xyz:443@evil.example",
    OndoEnvironmentError::UserInfoForbidden
)]
#[case::a_homoglyph_of_the_sandbox_host(
    "https://\u{0430}pi.ondoperps-sandbox.xyz",
    OndoEnvironmentError::HostNotAllowed { host: "xn--pi-6kc.ondoperps-sandbox.xyz".to_string() }
)]
#[case::an_empty_url("", OndoEnvironmentError::MalformedUrl)]
#[case::a_bare_word("not a url", OndoEnvironmentError::MalformedUrl)]
#[case::a_scheme_less_host("api.ondoperps-sandbox.xyz:8443", OndoEnvironmentError::MalformedUrl)]
#[case::a_scheme_relative_url(
    "//api.ondoperps-sandbox.xyz/v1/markets",
    OndoEnvironmentError::MalformedUrl
)]
fn test_the_endpoint_gate_refuses_every_authority_outside_the_allowlist(
    #[case] url: &str,
    #[case] expected: OndoEnvironmentError,
) {
    let error = validate_authenticated_environment(OndoEnvironment::Sandbox, url)
        .expect_err("the allowlist admits two authorities and this URL is neither");

    assert_eq!(error, expected, "{url}");
    // The refusal says what it refused and where the rule comes from; nothing in it echoes a URL.
    assert!(error.to_string().contains("base URL"), "{error}");
}

/// ... and the two authorities it does admit. A case variant and a trailing root dot are the same
/// authority, not a different one: the URL parser normalises both, and the parser is what the
/// transport itself uses.
#[rstest]
#[case::the_official_sandbox_host("https://api.ondoperps-sandbox.xyz", OndoEndpoint::Official)]
#[case::the_official_host_in_another_case(
    "HTTPS://Api.OndoPerps-Sandbox.Xyz",
    OndoEndpoint::Official
)]
#[case::the_official_host_with_a_trailing_root_dot(
    "https://api.ondoperps-sandbox.xyz.",
    OndoEndpoint::Official
)]
#[case::the_official_host_with_its_default_port(
    "https://api.ondoperps-sandbox.xyz:443",
    OndoEndpoint::Official
)]
#[case::a_loopback_test_service("http://127.0.0.1:8080", OndoEndpoint::LoopbackTestService)]
#[case::a_loopback_test_service_with_a_path(
    "http://127.0.0.1:8080/v1/markets",
    OndoEndpoint::LoopbackTestService
)]
#[case::an_ipv6_loopback_test_service("http://[::1]:8080", OndoEndpoint::LoopbackTestService)]
#[case::an_encrypted_loopback_test_service(
    "https://127.0.0.1:8443",
    OndoEndpoint::LoopbackTestService
)]
fn test_the_endpoint_gate_admits_the_sandbox_authority_and_a_loopback_test_service(
    #[case] url: &str,
    #[case] expected: OndoEndpoint,
) {
    assert_eq!(
        validate_authenticated_environment(OndoEnvironment::Sandbox, url),
        Ok(expected),
        "`{url}` is an endpoint this adapter may sign for",
    );
}

/// The production blacklist the allowlist replaces refused every one of these, so the allowlist
/// must refuse them too: a rule that is stricter in one place and weaker in another has not made
/// production unreachable.
#[rstest]
#[case::the_production_host("https://api.ondoperps.xyz")]
#[case::the_production_apex("https://ondoperps.xyz")]
#[case::another_production_subdomain("https://ws.ondoperps.xyz")]
#[case::the_production_host_in_another_case("HTTPS://API.ONDOPERPS.XYZ")]
#[case::the_production_host_on_another_port("https://api.ondoperps.xyz:8443")]
#[case::the_production_host_with_a_trailing_root_dot("https://api.ondoperps.xyz.")]
#[case::the_production_host_behind_userinfo("https://key:secret@api.ondoperps.xyz")]
#[case::a_scheme_less_production_host("api.ondoperps.xyz:8443")]
fn test_the_endpoint_gate_is_not_weaker_than_the_production_blacklist_it_replaces(
    #[case] url: &str,
) {
    assert!(
        validate_authenticated_environment(OndoEnvironment::Sandbox, url).is_err(),
        "`{url}` was refused before and must still be refused",
    );
}

/// The authenticated transport is where the credential would actually be sent, so it applies the
/// same gate to its own base URL: a client that cannot pass it does not exist, and nothing can be
/// sent from it.
#[rstest]
#[case::an_unrelated_remote_host("https://evil.example")]
#[case::a_host_that_merely_contains_the_sandbox_host(
    "https://api.ondoperps-sandbox.xyz.evil.example"
)]
#[case::userinfo_in_front_of_the_sandbox_host("https://key:secret@api.ondoperps-sandbox.xyz")]
#[case::the_sandbox_host_over_plain_http("http://api.ondoperps-sandbox.xyz")]
#[case::the_production_host("https://api.ondoperps.xyz")]
fn test_an_authenticated_client_is_not_built_on_an_endpoint_outside_the_allowlist(
    #[case] url: &str,
) {
    let error = OndoHttpClient::builder()
        .base_url(url.to_string())
        .credential(credential())
        .build()
        .expect_err("an authenticated client cannot be built on an endpoint the gate refuses");

    assert!(
        matches!(error, OndoHttpError::Environment(_)),
        "the refusal is the environment gate's, was {error:?}",
    );
}

// ------------------------------------------------------------------------------------------------
// A skewed clock refuses to sign
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_skewed_clock_refuses_to_sign_and_says_why() {
    assert_eq!(ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS, 30);

    for offset in [0_i64, 1, -1, 28, -28] {
        assert_eq!(
            check_clock_skew(offset),
            Ok(()),
            "an offset of {offset} s is within tolerance",
        );
    }

    // The HTTP `Date` header has one-second granularity, so an observed offset of 29 s could be 30 s
    // in truth: the refusal starts at the first offset the evidence cannot prove is tolerable.
    for offset in [29_i64, 30, -30, 45, -45, i64::MIN, i64::MAX] {
        let error =
            check_clock_skew(offset).expect_err("an offset this large cannot be signed from");

        match error {
            OndoSigningError::ClockSkew {
                offset_secs,
                tolerance_secs,
            } => {
                assert_eq!(offset_secs, offset);
                assert_eq!(tolerance_secs, ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS);
            }
            other => panic!("an offset is not a pre-epoch clock: {other:?}"),
        }
    }

    let rendered = check_clock_skew(-45).unwrap_err().to_string();
    assert!(rendered.contains("-45"), "{rendered}");
    assert!(rendered.contains("30"), "{rendered}");
}

// ------------------------------------------------------------------------------------------------
// The secret wrapper cannot render its secret
// ------------------------------------------------------------------------------------------------

/// Every rendering of the wrapper - `Debug`, `Display` - and of every error that could carry it is
/// checked against the secret's own bytes, and against a distinctive slice of them, so a partially
/// redacted rendering is caught too.
#[rstest]
fn test_the_secret_wrapper_never_renders_the_secret() {
    let secret = fake_secret();
    let key_id = fixture()["key_id"].as_str().expect("key_id").to_string();
    let credential = credential();
    let fragment = &secret[secret.len() - 8..];

    let renderings = [
        format!("{credential:?}"),
        format!("{credential}"),
        format!("{credential:#?}"),
    ];

    for rendered in &renderings {
        assert!(
            !rendered.contains(&secret),
            "the secret is not printable: {rendered}",
        );
        assert!(
            !rendered.contains(fragment),
            "no fragment of the secret is printable: {rendered}",
        );
        assert!(
            rendered.contains(REDACTED),
            "the wrapper says what it is hiding: {rendered}",
        );
    }

    // The key id is an identifier rather than the HMAC key, and it is masked anyway: a log line must
    // not carry the full credential either.
    for rendered in &renderings {
        assert!(
            !rendered.contains(&key_id),
            "the key id is masked, not printed: {rendered}",
        );
    }

    // Errors are the other way a secret could reach a log. None of these carries a secret at all,
    // and the one built from a venue body is built the way the transport builds it: the credential
    // is redacted out of the body *before* the body is classified and kept
    // (`check_private_response`, proven on the wire in `tests/http_client.rs`).
    let echoing_body = format!(
        r#"{{"code":"api_key_not_found","message":"key {key_id} was not found for secret {secret}"}}"#
    );
    let errors = [
        OndoHttpError::Signing(check_clock_skew(45).unwrap_err()),
        OndoHttpError::Environment(OndoEnvironmentError::ProductionForbidden),
        OndoHttpError::Environment(OndoEnvironmentError::ProductionHostForbidden {
            host: "api.ondoperps.xyz".to_string(),
        }),
        classify_auth_rejection(401, credential.redact(&echoing_body)),
        OndoHttpError::NotAuthenticated {
            target: ORDERS_PATH.to_string(),
        },
    ];

    for error in &errors {
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains(&secret), "{rendered}");
        assert!(!rendered.contains(fragment), "{rendered}");
        assert!(!rendered.contains(&key_id), "{rendered}");
    }
}
