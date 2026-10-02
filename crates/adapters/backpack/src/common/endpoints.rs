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

//! Production endpoints and a separate, public-only loopback override.

use thiserror::Error;
use url::{Host, Url};

/// Official production REST origin.
pub const BACKPACK_REST_URL: &str = "https://api.backpack.exchange";
/// Official production WebSocket origin.
pub const BACKPACK_WEBSOCKET_URL: &str = "wss://ws.backpack.exchange";

/// Validated endpoints for public protocol work.
///
/// Arbitrary remote overrides are not accepted. Loopback endpoints are explicitly
/// selected for local protocol peers and must never receive production credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpackEndpoints {
    rest_url: String,
    websocket_url: String,
    loopback: bool,
}

impl BackpackEndpoints {
    /// Returns the official production endpoints.
    #[must_use]
    pub fn production() -> Self {
        Self {
            rest_url: BACKPACK_REST_URL.to_string(),
            websocket_url: BACKPACK_WEBSOCKET_URL.to_string(),
            loopback: false,
        }
    }

    /// Selects a pair of loopback origins for a local public protocol peer.
    ///
    /// # Errors
    ///
    /// Returns an error for non-loopback IPs, DNS names, wrong schemes, URL credentials,
    /// paths, query strings, fragments, invalid URLs, or port zero.
    pub fn loopback_override(
        rest_url: &str,
        websocket_url: &str,
    ) -> Result<Self, BackpackEndpointError> {
        let rest = validate_loopback_url(rest_url, &["http", "https"])?;
        let websocket = validate_loopback_url(websocket_url, &["ws", "wss"])?;
        Ok(Self {
            rest_url: rest.to_string(),
            websocket_url: websocket.to_string(),
            loopback: true,
        })
    }

    /// Returns the REST origin.
    #[must_use]
    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    /// Returns the WebSocket origin.
    #[must_use]
    pub fn websocket_url(&self) -> &str {
        &self.websocket_url
    }

    /// Returns whether explicit loopback origins were selected.
    #[must_use]
    pub const fn is_loopback(&self) -> bool {
        self.loopback
    }
}

/// An endpoint override outside the local public protocol boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid Backpack loopback endpoint: {0}")]
pub struct BackpackEndpointError(pub &'static str);

fn validate_loopback_url(value: &str, schemes: &[&str]) -> Result<Url, BackpackEndpointError> {
    let url = Url::parse(value).map_err(|_| BackpackEndpointError("malformed URL"))?;
    let loopback = match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };

    if !loopback || !schemes.contains(&url.scheme()) {
        return Err(BackpackEndpointError(
            "expected the correct scheme and a loopback IP",
        ));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(BackpackEndpointError("URL credentials are forbidden"));
    }

    if url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
    {
        return Err(BackpackEndpointError(
            "expected an origin without a path, query, fragment, or port zero",
        ));
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_production_endpoints() {
        let endpoints = BackpackEndpoints::production();
        assert_eq!(endpoints.rest_url(), "https://api.backpack.exchange");
        assert_eq!(endpoints.websocket_url(), "wss://ws.backpack.exchange");
        assert!(!endpoints.is_loopback());
    }

    #[rstest]
    #[case("http://127.0.0.1:8080", "ws://127.0.0.1:8081")]
    #[case("https://[::1]:8080", "wss://[::1]:8081")]
    fn test_explicit_loopback_override(#[case] rest: &str, #[case] websocket: &str) {
        let endpoints = BackpackEndpoints::loopback_override(rest, websocket).unwrap();
        assert_eq!(endpoints.rest_url(), format!("{rest}/"));
        assert_eq!(endpoints.websocket_url(), format!("{websocket}/"));
        assert!(endpoints.is_loopback());
    }

    #[rstest]
    #[case("https://api.backpack.exchange")]
    #[case("http://localhost:8080")]
    #[case("http://192.168.1.1:8080")]
    #[case("http://[::ffff:127.0.0.1]:8080")]
    #[case("ftp://127.0.0.1:8080")]
    #[case("http://user:password@127.0.0.1:8080")]
    #[case("http://127.0.0.1:8080/api")]
    #[case("http://127.0.0.1:8080/?symbol=BTC_USDC_PERP")]
    #[case("http://127.0.0.1:8080/#fragment")]
    #[case("http://127.0.0.1:0")]
    #[case("not a URL")]
    fn test_invalid_rest_override(#[case] rest: &str) {
        assert!(BackpackEndpoints::loopback_override(rest, "ws://127.0.0.1:8081").is_err());
    }

    #[rstest]
    #[case("wss://ws.backpack.exchange")]
    #[case("http://127.0.0.1:8081")]
    #[case("ws://localhost:8081")]
    #[case("ws://127.0.0.1:8081/private")]
    #[case("ws://user:password@127.0.0.1:8081")]
    fn test_invalid_websocket_override(#[case] websocket: &str) {
        assert!(BackpackEndpoints::loopback_override("http://127.0.0.1:8080", websocket).is_err());
    }
}
