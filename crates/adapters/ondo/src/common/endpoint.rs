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

//! The policy that decides which endpoint an authenticated request may be signed for.
//!
//! An authenticated request carries the API key id and a signature, so the authority it goes to is a
//! credential decision rather than a routing detail. [`OndoEndpointPolicy`] is that decision, in one
//! place, for REST and for the private WebSocket alike: [`OndoSchemeFamily`] is the only thing that
//! differs between the two surfaces, and the policy itself does not know which one is asking.
//!
//! # An allowlist, not a blacklist
//!
//! The rule this replaces refused the production domain and its subdomains and accepted everything
//! else, so any other remote host - and any URL the hand-rolled host extraction could not read -
//! could be signed for. The policy here admits exactly two authorities:
//!
//! 1. [`OndoEndpoint::Official`]: the official host of the session's own environment
//!    ([`OndoEnvironment::Sandbox`]), on the scheme that environment's endpoints use, on the
//!    scheme's default port, carrying no userinfo;
//! 2. [`OndoEndpoint::LoopbackTestService`]: a loopback *address*, described below.
//!
//! Everything else is refused, and the refusal is a decision about the parsed URL:
//! [`nautilus_network::http::Url`] is the same parser the HTTP transport itself uses (reqwest's
//! `url`), so what this policy reads is what the transport will dial. A host the parser normalises
//! cannot be smuggled past a comparison - `HTTPS://API.ONDOPERPS-SANDBOX.XYZ` and
//! `https://api.ondoperps-sandbox.xyz` are one authority, and a host that merely *contains* the
//! sandbox host is a different one - and a URL the parser cannot read is refused rather than passed
//! through unjudged.
//!
//! # The loopback test service
//!
//! A loopback mock is the explicit local test service this adapter's offline tests and the factory's
//! local construction use, and it is reached only by an explicit `base_url_http` override: the
//! per-environment defaults are the official hosts ([`crate::common::consts`]), which are never
//! loopback. It is admitted only when the URL's host is a loopback **address** (`127.0.0.0/8` or
//! `::1`). A name is not an address, so `localhost` is refused: it resolves through a hosts file or
//! a DNS answer, which is exactly the indirection this policy does not trust. The class is named, is
//! returned to the caller, and is logged by the authenticated transport, so a session pointed at a
//! local test service is never silent.
//!
//! The production rules are kept alongside it rather than replaced by it: a host inside the
//! production domain is refused by name ([`OndoEnvironmentError::ProductionHostForbidden`]) before
//! the allowlist is consulted, so the policy is never weaker than the production blacklist it
//! supersedes.

use std::net::IpAddr;

use nautilus_network::http::Url;

use crate::common::enums::OndoEnvironment;

/// The registrable domain every production host belongs to.
///
/// [`crate::common::consts::ONDO_HTTP_BASE_URL_PRODUCTION`] and
/// [`crate::common::consts::ONDO_WS_URL_PRODUCTION`] are bound to it by this module's tests, so the
/// rule cannot drift from the endpoints the adapter publishes.
const ONDO_PRODUCTION_DOMAIN: &str = "ondoperps.xyz";

/// The official sandbox host, for both the REST and the WebSocket endpoints.
///
/// [`crate::common::consts::ONDO_HTTP_BASE_URL_SANDBOX`] and
/// [`crate::common::consts::ONDO_WS_URL_SANDBOX`] are bound to it by this module's tests.
const ONDO_SANDBOX_HOST: &str = "api.ondoperps-sandbox.xyz";

/// The scheme family an endpoint policy judges.
///
/// REST and the private WebSocket are different families over the same decision; the policy is
/// shared between them, so a private transport added later applies the same rules to `wss://` URLs
/// that the REST transport applies to `https://` ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoSchemeFamily {
    /// REST: `http` for a loopback test service, `https` for the official host.
    Http,
    /// WebSocket: `ws` for a loopback test service, `wss` for the official host.
    WebSocket,
}

impl OndoSchemeFamily {
    /// The scheme the environment's official endpoint uses.
    #[must_use]
    pub const fn official_scheme(self) -> &'static str {
        match self {
            Self::Http => "https",
            Self::WebSocket => "wss",
        }
    }

    /// The schemes a loopback test service may use, as an error names them.
    #[must_use]
    pub const fn test_service_schemes(self) -> &'static str {
        match self {
            Self::Http => "http or https",
            Self::WebSocket => "ws or wss",
        }
    }

    /// Whether `scheme` is a scheme this family's URLs may carry at all.
    #[must_use]
    fn admits(self, scheme: &str) -> bool {
        match self {
            Self::Http => matches!(scheme, "http" | "https"),
            Self::WebSocket => matches!(scheme, "ws" | "wss"),
        }
    }
}

/// The authority class an authenticated base URL names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoEndpoint {
    /// The official host of the session's own environment.
    Official,
    /// A loopback address: the explicit local test service.
    LoopbackTestService,
}

/// Why an authenticated session's environment and base URL were refused.
///
/// These are the endpoints gate's refusals; the credential gate returns this type
/// ([`crate::common::credential::CredentialError::Environment`]), the authenticated transport
/// returns it ([`crate::http::error::OndoHttpError::Environment`]), and none of them reads a
/// credential, opens a socket or has a fallback path.
///
/// No variant carries the URL it refused. A base URL that carries userinfo carries a credential in
/// it, and an error is the last place it should reach.
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
    /// The base URL names a host that is neither this session's official host nor a loopback test
    /// service.
    #[error(
        "the base URL host `{host}` is not an endpoint this session may sign for: an authenticated \
         Ondo Perps request goes to the official host of its own environment, or to a loopback test \
         service (plan §R0.3)"
    )]
    HostNotAllowed {
        /// The host the rule refused.
        host: String,
    },
    /// The base URL's scheme is not one this endpoint uses.
    #[error("the base URL scheme `{scheme}` is not one this endpoint uses ({expected})")]
    UnsupportedScheme {
        /// The scheme the URL carried.
        scheme: String,
        /// The schemes the endpoint accepts.
        expected: &'static str,
    },
    /// The base URL carries userinfo.
    #[error(
        "the base URL carries userinfo (`user:password@host`): a credential does not belong in the \
         URL an authenticated request is built from"
    )]
    UserInfoForbidden,
    /// The official host was given on a port other than the one its scheme uses.
    #[error(
        "the base URL host `{host}` carries port {port}, which is not the port this endpoint uses"
    )]
    PortNotAllowed {
        /// The host the rule refused.
        host: String,
        /// The port it carried.
        port: u16,
    },
    /// The base URL is not a URL the transport can read.
    #[error(
        "the base URL is not a URL the transport can read, so it names no authority to sign for"
    )]
    MalformedUrl,
}

/// The policy an authenticated session's base URL must satisfy.
///
/// See the module documentation for the two authorities it admits. It reads no credential, opens no
/// socket, and holds no state beyond the environment and the scheme family it was built for, so it
/// can be - and is - applied before a credential is resolved and before a client exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OndoEndpointPolicy {
    environment: OndoEnvironment,
    family: OndoSchemeFamily,
}

impl OndoEndpointPolicy {
    /// The policy an authenticated session of `environment` and `family` is held to.
    #[must_use]
    pub const fn authenticated(environment: OndoEnvironment, family: OndoSchemeFamily) -> Self {
        Self {
            environment,
            family,
        }
    }

    /// Returns which authority `base_url` names, or why it names none this session may sign for.
    ///
    /// # Errors
    ///
    /// Returns [`OndoEnvironmentError::ProductionForbidden`] for any environment other than
    /// [`OndoEnvironment::Sandbox`], and otherwise the reason the URL is not an endpoint this
    /// session may use: [`OndoEnvironmentError::MalformedUrl`],
    /// [`OndoEnvironmentError::ProductionHostForbidden`],
    /// [`OndoEnvironmentError::UserInfoForbidden`], [`OndoEnvironmentError::HostNotAllowed`],
    /// [`OndoEnvironmentError::UnsupportedScheme`] or [`OndoEnvironmentError::PortNotAllowed`].
    pub fn classify(&self, base_url: &str) -> Result<OndoEndpoint, OndoEnvironmentError> {
        if self.environment != OndoEnvironment::Sandbox {
            return Err(OndoEnvironmentError::ProductionForbidden);
        }

        let url = Url::parse(base_url).map_err(|_error| OndoEnvironmentError::MalformedUrl)?;

        // A URL the parser accepts without an authority (`mailto:`, `data:`) names no host to sign
        // for; the parser's refusals above and this one are the same answer.
        let Some(host) = url.host_str() else {
            return Err(OndoEnvironmentError::MalformedUrl);
        };
        let host = normalised_host(host);

        // Production is refused by name before the allowlist is consulted, so the allowlist can only
        // ever be stricter than the blacklist it replaces.
        if is_production_host(&host) {
            return Err(OndoEnvironmentError::ProductionHostForbidden { host });
        }

        if !url.username().is_empty() || url.password().is_some() {
            return Err(OndoEnvironmentError::UserInfoForbidden);
        }

        if is_loopback_address(&host) {
            if !self.family.admits(url.scheme()) {
                return Err(OndoEnvironmentError::UnsupportedScheme {
                    scheme: url.scheme().to_string(),
                    expected: self.family.test_service_schemes(),
                });
            }

            return Ok(OndoEndpoint::LoopbackTestService);
        }

        if host != official_host(self.environment) {
            return Err(OndoEnvironmentError::HostNotAllowed { host });
        }

        if url.scheme() != self.family.official_scheme() {
            return Err(OndoEnvironmentError::UnsupportedScheme {
                scheme: url.scheme().to_string(),
                expected: self.family.official_scheme(),
            });
        }

        // The parser drops a port that is the scheme's default, so an explicit `:443` on `https` is
        // the official endpoint and anything else is not.
        if let Some(port) = url.port() {
            return Err(OndoEnvironmentError::PortNotAllowed { host, port });
        }

        Ok(OndoEndpoint::Official)
    }
}

/// The official host of `environment`.
const fn official_host(environment: OndoEnvironment) -> &'static str {
    match environment {
        OndoEnvironment::Sandbox => ONDO_SANDBOX_HOST,
        OndoEnvironment::Production => "api.ondoperps.xyz",
    }
}

/// Whether `host` is inside the production domain.
fn is_production_host(host: &str) -> bool {
    host == ONDO_PRODUCTION_DOMAIN
        || host
            .strip_suffix(ONDO_PRODUCTION_DOMAIN)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

/// The host as this policy compares it.
///
/// The parser already lower-cases a host and normalises an address; one trailing root dot is
/// removed because `api.ondoperps-sandbox.xyz.` is the same authority as
/// `api.ondoperps-sandbox.xyz` - a fully qualified name, not a different host.
fn normalised_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

/// Whether `host` is a loopback *address*, as the parser renders one (an IPv6 address is bracketed).
///
/// A name is never a loopback address here: `localhost` resolves through a hosts file or DNS, and
/// the policy trusts the address it can read, not a resolution it cannot verify.
fn is_loopback_address(host: &str) -> bool {
    let bare = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);

    bare.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::consts::{
        ONDO_HTTP_BASE_URL_PRODUCTION, ONDO_HTTP_BASE_URL_SANDBOX, ONDO_WS_URL_PRODUCTION,
        ONDO_WS_URL_SANDBOX,
    };

    const SANDBOX_URL: &str = "https://api.ondoperps-sandbox.xyz";

    fn policy() -> OndoEndpointPolicy {
        OndoEndpointPolicy::authenticated(OndoEnvironment::Sandbox, OndoSchemeFamily::Http)
    }

    fn ws_policy() -> OndoEndpointPolicy {
        OndoEndpointPolicy::authenticated(OndoEnvironment::Sandbox, OndoSchemeFamily::WebSocket)
    }

    /// The rule's host and the endpoint constants are one fact, not two: the published endpoint must
    /// be exactly what the allowlist admits, and the production endpoints must be exactly what the
    /// production rule refuses.
    #[rstest]
    fn test_the_policy_hosts_are_the_endpoints_the_adapter_publishes() {
        for url in [ONDO_HTTP_BASE_URL_SANDBOX, ONDO_WS_URL_SANDBOX] {
            let host = Url::parse(url)
                .expect("the sandbox endpoint parses")
                .host_str()
                .map(normalised_host);

            assert_eq!(host.as_deref(), Some(ONDO_SANDBOX_HOST), "{url}");
        }

        for url in [ONDO_HTTP_BASE_URL_PRODUCTION, ONDO_WS_URL_PRODUCTION] {
            let host = Url::parse(url)
                .expect("the production endpoint parses")
                .host_str()
                .map(normalised_host);

            assert!(
                host.as_deref().is_some_and(is_production_host),
                "{url} must be inside the domain the production rule refuses",
            );
        }
    }

    /// A case variant, a trailing root dot and an explicit default port are the same authority, and
    /// the parser is what makes that true.
    #[rstest]
    #[case::the_official_host(SANDBOX_URL, OndoEndpoint::Official)]
    #[case::another_case("HTTPS://Api.OndoPerps-Sandbox.Xyz", OndoEndpoint::Official)]
    #[case::a_trailing_root_dot("https://api.ondoperps-sandbox.xyz.", OndoEndpoint::Official)]
    #[case::an_explicit_default_port(
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
    #[case::the_last_address_of_the_block(
        "http://127.255.255.254:8080",
        OndoEndpoint::LoopbackTestService
    )]
    fn test_the_rest_policy_admits_two_authorities(
        #[case] url: &str,
        #[case] expected: OndoEndpoint,
    ) {
        assert_eq!(policy().classify(url), Ok(expected), "{url}");
    }

    /// The WebSocket family admits the same two authorities, on its own schemes: this is the policy
    /// a private transport added later applies, and it is the same decision, not a second one.
    #[rstest]
    #[case::the_official_sandbox_websocket(ONDO_WS_URL_SANDBOX)]
    #[case::the_official_websocket_in_another_case("WSS://Api.OndoPerps-Sandbox.Xyz/ws")]
    #[case::a_loopback_test_service("ws://127.0.0.1:8080/ws")]
    #[case::an_encrypted_loopback_test_service("wss://127.0.0.1:8080/ws")]
    fn test_the_websocket_policy_admits_the_same_authorities(#[case] url: &str) {
        assert!(
            ws_policy().classify(url).is_ok(),
            "`{url}` is an endpoint a private session may sign for",
        );
    }

    /// Every one of these is an authority the old blacklist accepted, and signing for any of them
    /// would put the key id and a signature on a host this adapter does not own.
    #[rstest]
    #[case::a_host_that_contains_the_sandbox_host("https://api.ondoperps-sandbox.xyz.evil.example")]
    #[case::a_subdomain_of_the_sandbox_host("https://eu.api.ondoperps-sandbox.xyz")]
    #[case::a_sandbox_host_inside_a_longer_name("https://api.ondoperps-sandbox.xyz.evil.com")]
    #[case::a_homoglyph_of_the_sandbox_host("https://\u{0430}pi.ondoperps-sandbox.xyz")]
    #[case::the_loopback_name("http://localhost:8080")]
    #[case::the_wildcard_address("http://0.0.0.0:8080")]
    #[case::a_private_network_address("http://192.168.1.10:8080")]
    #[case::an_unrelated_remote_host("https://evil.example")]
    #[case::an_unencrypted_remote_service("http://evil.example")]
    #[case::an_unencrypted_sandbox_host("http://api.ondoperps-sandbox.xyz")]
    #[case::a_port_of_its_own("https://api.ondoperps-sandbox.xyz:8443")]
    #[case::userinfo("https://key:secret@api.ondoperps-sandbox.xyz")]
    #[case::userinfo_with_an_empty_password("https://key@api.ondoperps-sandbox.xyz")]
    #[case::userinfo_in_front_of_a_test_service("http://key:secret@127.0.0.1:8080")]
    #[case::userinfo_in_front_of_an_unrelated_host(
        "https://api.ondoperps-sandbox.xyz:443@evil.example"
    )]
    #[case::a_scheme_relative_url("//api.ondoperps-sandbox.xyz/v1/markets")]
    #[case::a_scheme_less_host("api.ondoperps-sandbox.xyz:8443")]
    #[case::a_url_with_no_authority("mailto:ops@ondoperps.xyz")]
    #[case::an_empty_url("")]
    #[case::a_bare_word("not a url")]
    fn test_the_policy_refuses_every_other_authority(#[case] url: &str) {
        assert!(
            policy().classify(url).is_err(),
            "`{url}` is not an endpoint this session may sign for",
        );
    }

    /// The scheme family is the whole difference between the REST policy and the WebSocket one, and
    /// neither admits the other's schemes on the same authority.
    #[rstest]
    #[case::a_websocket_url_for_rest("wss://api.ondoperps-sandbox.xyz/ws", false, true)]
    #[case::a_rest_url_for_a_websocket("https://api.ondoperps-sandbox.xyz", true, false)]
    #[case::a_websocket_loopback_for_rest("ws://127.0.0.1:8080", false, true)]
    #[case::a_rest_loopback_for_a_websocket("http://127.0.0.1:8080", true, false)]
    #[case::another_scheme_entirely("ftp://127.0.0.1:8080", false, false)]
    fn test_the_scheme_family_separates_the_two_policies(
        #[case] url: &str,
        #[case] rest_admits: bool,
        #[case] websocket_admits: bool,
    ) {
        assert_eq!(policy().classify(url).is_ok(), rest_admits, "REST: {url}");
        assert_eq!(
            ws_policy().classify(url).is_ok(),
            websocket_admits,
            "WebSocket: {url}",
        );
    }

    /// The refusals are distinct answers, because an operator has to be able to tell a
    /// misconfiguration from an attempt to reach another venue.
    #[rstest]
    #[case::an_unrelated_host("https://evil.example", OndoEnvironmentError::HostNotAllowed { host: "evil.example".to_string() })]
    #[case::a_lookalike_host(
        "https://api.ondoperps-sandbox.xyz.evil.example",
        OndoEnvironmentError::HostNotAllowed { host: "api.ondoperps-sandbox.xyz.evil.example".to_string() }
    )]
    #[case::a_subdomain("https://eu.api.ondoperps-sandbox.xyz", OndoEnvironmentError::HostNotAllowed { host: "eu.api.ondoperps-sandbox.xyz".to_string() })]
    #[case::the_loopback_name("http://localhost:8080", OndoEnvironmentError::HostNotAllowed { host: "localhost".to_string() })]
    #[case::a_private_network_address("http://192.168.1.10:8080", OndoEnvironmentError::HostNotAllowed { host: "192.168.1.10".to_string() })]
    #[case::the_wildcard_address("http://0.0.0.0:8080", OndoEnvironmentError::HostNotAllowed { host: "0.0.0.0".to_string() })]
    #[case::the_production_host("https://api.ondoperps.xyz", OndoEnvironmentError::ProductionHostForbidden { host: "api.ondoperps.xyz".to_string() })]
    #[case::the_production_apex("https://ondoperps.xyz", OndoEnvironmentError::ProductionHostForbidden { host: "ondoperps.xyz".to_string() })]
    #[case::another_production_subdomain("https://ws.ondoperps.xyz", OndoEnvironmentError::ProductionHostForbidden { host: "ws.ondoperps.xyz".to_string() })]
    #[case::a_lookalike_production_host("https://notondoperps.xyz", OndoEnvironmentError::HostNotAllowed { host: "notondoperps.xyz".to_string() })]
    #[case::userinfo(
        "https://key:secret@api.ondoperps-sandbox.xyz",
        OndoEnvironmentError::UserInfoForbidden
    )]
    #[case::a_plain_http_sandbox_host(
        "http://api.ondoperps-sandbox.xyz",
        OndoEnvironmentError::UnsupportedScheme { scheme: "http".to_string(), expected: "https" }
    )]
    #[case::another_port(
        "https://api.ondoperps-sandbox.xyz:8443",
        OndoEnvironmentError::PortNotAllowed { host: "api.ondoperps-sandbox.xyz".to_string(), port: 8443 }
    )]
    #[case::an_unreadable_url("not a url", OndoEnvironmentError::MalformedUrl)]
    #[case::an_empty_url("", OndoEnvironmentError::MalformedUrl)]
    #[case::a_url_with_no_authority("mailto:ops@ondoperps.xyz", OndoEnvironmentError::MalformedUrl)]
    fn test_each_refusal_names_its_reason(
        #[case] url: &str,
        #[case] expected: OndoEnvironmentError,
    ) {
        assert_eq!(policy().classify(url), Err(expected), "{url}");
    }

    /// The rule this policy replaces, kept as the reference the new rule is compared against: it
    /// refused the production domain and its subdomains - reading the host out of the raw string -
    /// and admitted every other URL, whatever it was.
    fn superseded_blacklist_refuses(base_url: &str) -> bool {
        let after_scheme = base_url
            .split_once("://")
            .map_or(base_url, |(_scheme, rest)| rest);
        let authority = after_scheme
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_userinfo, host)| host);
        let host = match authority.strip_prefix('[') {
            Some(rest) => rest
                .split_once(']')
                .map(|(host, _port)| host)
                .unwrap_or_default(),
            None => authority.split(':').next().unwrap_or_default(),
        };
        let host = host.trim_end_matches('.').to_ascii_lowercase();

        !host.is_empty()
            && (host == ONDO_PRODUCTION_DOMAIN
                || host.ends_with(&format!(".{ONDO_PRODUCTION_DOMAIN}")))
    }

    /// Every URL the superseded blacklist refused is refused here. The allowlist is stricter in both
    /// directions - it admits two authorities and nothing else - and this pins the "never weaker"
    /// half over the shapes a hand-rolled host extraction and a real parser can disagree about, so a
    /// later edit that reopens one of them fails here rather than in review.
    #[rstest]
    fn test_the_allowlist_refuses_everything_the_superseded_blacklist_refused() {
        let mut urls = Vec::new();

        for scheme in ["", "https://", "http://", "HTTPS://", "wss://", "//"] {
            for userinfo in ["", "key:secret@", "api.ondoperps-sandbox.xyz:443@"] {
                for host in [
                    "ondoperps.xyz",
                    "api.ondoperps.xyz",
                    "ws.ondoperps.xyz",
                    "api.ondoperps.xyz.",
                    "api.ondoperps-sandbox.xyz",
                    "evil.example",
                    "127.0.0.1",
                    "",
                ] {
                    for port in ["", ":443", ":8443"] {
                        for path in ["", "/v1/markets", "/?x=1", "#fragment"] {
                            urls.push(format!("{scheme}{userinfo}{host}{port}{path}"));
                        }
                    }
                }
            }
        }

        let mut refused_before = 0;

        for url in &urls {
            if superseded_blacklist_refuses(url) {
                refused_before += 1;
                assert!(
                    policy().classify(url).is_err(),
                    "`{url}` was refused before and must still be refused",
                );
            }
        }

        assert!(
            refused_before > 0,
            "the reference rule refused nothing, so the comparison proves nothing",
        );
    }

    /// The production refusal outranks every other rule, whatever else the URL carries: a host
    /// inside the production domain is never admitted by a later branch, and it is named as
    /// production rather than as an anonymous refusal.
    #[rstest]
    #[case::in_another_case("HTTPS://API.ONDOPERPS.XYZ")]
    #[case::with_a_trailing_root_dot("https://api.ondoperps.xyz.")]
    #[case::on_another_port("https://api.ondoperps.xyz:8443")]
    #[case::behind_userinfo("https://key:secret@api.ondoperps.xyz")]
    #[case::as_a_websocket_url("wss://api.ondoperps.xyz/ws")]
    fn test_production_is_refused_before_any_other_rule(#[case] url: &str) {
        assert!(
            matches!(
                policy().classify(url),
                Err(OndoEnvironmentError::ProductionHostForbidden { .. })
            ),
            "`{url}` must be refused as production: {:?}",
            policy().classify(url),
        );
    }

    /// Only the sandbox environment has an endpoint this adapter authenticates against; production
    /// is refused before the URL is read at all.
    #[rstest]
    fn test_production_is_refused_whatever_the_url_says() {
        let production =
            OndoEndpointPolicy::authenticated(OndoEnvironment::Production, OndoSchemeFamily::Http);

        for url in [
            SANDBOX_URL,
            "https://api.ondoperps.xyz",
            "http://127.0.0.1:8080",
        ] {
            assert_eq!(
                production.classify(url),
                Err(OndoEnvironmentError::ProductionForbidden),
                "{url}",
            );
        }
    }

    /// The loopback address rule is narrow on purpose, and these are the neighbours it must not
    /// admit: the address below `127.0.0.0/8` is not loopback, and neither is the IPv6 wildcard.
    #[rstest]
    #[case::the_address_below_the_block("http://126.255.255.255:8080")]
    #[case::the_address_above_the_block("http://128.0.0.1:8080")]
    #[case::the_ipv6_wildcard("http://[::]:8080")]
    #[case::an_ipv4_mapped_loopback("http://[::ffff:127.0.0.1]:8080")]
    fn test_only_the_loopback_blocks_are_a_test_service(#[case] url: &str) {
        assert!(
            policy().classify(url).is_err(),
            "`{url}` is not a loopback address",
        );
    }

    /// The default endpoints of the environment the adapter authenticates against are the official
    /// host, so a configuration that overrides nothing can never resolve to a test service.
    #[rstest]
    fn test_an_unoverridden_endpoint_is_never_a_test_service() {
        assert_eq!(
            policy().classify(ONDO_HTTP_BASE_URL_SANDBOX),
            Ok(OndoEndpoint::Official),
        );
        assert_eq!(
            ws_policy().classify(ONDO_WS_URL_SANDBOX),
            Ok(OndoEndpoint::Official),
        );
    }
}
