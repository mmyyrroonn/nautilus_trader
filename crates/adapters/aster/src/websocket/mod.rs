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

//! Aster user data stream (private WebSocket).
//!
//! Aster's user data stream is byte-compatible with Binance USD-M: a signed
//! `POST /fapi/v3/listenKey` returns a key, the client connects to `<ws base>/<listen key>`,
//! and the venue pushes `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, `ACCOUNT_CONFIG_UPDATE`,
//! `MARGIN_CALL` and `listenKeyExpired` events in the Binance envelope.
//!
//! The frame decoding is therefore delegated to
//! [`nautilus_binance::futures::websocket::streams::client::BinanceFuturesWebSocketClient`],
//! which accepts a full URL override and does not assume a Binance host. Only the parts that
//! are Aster-specific live here: the listen-key lifecycle, the `<base>/<key>` URL shape (Binance
//! USD-M uses `?listenKey=<key>` instead), and renewal scheduling.
//!
//! # References
//!
//! - <https://asterdex.github.io/aster-api-website/futures-v3/user-data-streams/>

use std::{fmt::Debug, sync::Arc};

use futures_util::Stream;
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    futures::websocket::streams::{
        client::BinanceFuturesWebSocketClient, messages::BinanceFuturesWsStreamsMessage,
    },
};
use nautilus_core::string::secret::REDACTED;
use nautilus_live::SocketControlFactory;
use nautilus_network::{SocketState, websocket::TransportBackend};

use crate::http::{AsterHttpClient, error::AsterHttpResult};

/// Logical socket endpoint reported to the live socket registry for the private stream.
pub(crate) const ASTER_USER_STREAM_ENDPOINT: &str = "aster-user-streams";

/// Builds the Aster user data stream URL.
///
/// Aster appends the listen key as a path segment (`wss://fstream.asterdex.com/ws/<key>`),
/// unlike Binance USD-M which passes it as a `?listenKey=` query parameter. The `/ws` suffix
/// is already part of the configured base URL, and any trailing slash is normalised away.
#[must_use]
pub fn user_stream_url(ws_base_url: &str, listen_key: &str) -> String {
    format!("{}/{listen_key}", ws_base_url.trim_end_matches('/'))
}

/// Client for the Aster private user data stream.
///
/// Owns the listen key for one stream session. The caller drives the session lifecycle:
/// [`connect`](Self::connect) obtains a key and opens the socket, [`keepalive`](Self::keepalive)
/// renews it, and [`close`](Self::close) tears the session down. A `listenKeyExpired` event or a
/// dropped stream means the session is over and a fresh [`connect`](Self::connect) is required,
/// because the URL embeds the key.
///
/// Listen keys are account-scoped, not client-scoped: the keepalive and close endpoints take no
/// key parameter, so they act on whatever key the wallet currently holds. Two clients signing
/// with the same wallet therefore share one key, and [`close`](Self::close) on either one
/// invalidates the stream of both. Run a single user stream per account.
pub struct AsterUserStreamClient {
    http_client: AsterHttpClient,
    ws_base_url: String,
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
    heartbeat_secs: Option<u64>,
    connect_timeout_secs: Option<u64>,
    listen_key: Option<String>,
    ws_client: Option<BinanceFuturesWebSocketClient>,
    socket_factory: Option<SocketControlFactory>,
    socket_state_callback: Option<Arc<dyn Fn(SocketState) + Send + Sync>>,
}

impl Debug for AsterUserStreamClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The listen key authenticates the stream and travels in the URL, so it is redacted
        // here as well as kept out of logs.
        f.debug_struct(stringify!(AsterUserStreamClient))
            .field("ws_base_url", &self.ws_base_url)
            .field("transport_backend", &self.transport_backend)
            .field("heartbeat_secs", &self.heartbeat_secs)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("listen_key", &self.listen_key.as_ref().map(|_| REDACTED))
            .field("is_active", &self.is_active())
            .finish_non_exhaustive()
    }
}

impl AsterUserStreamClient {
    /// Creates a new [`AsterUserStreamClient`].
    #[must_use]
    pub fn new(
        http_client: AsterHttpClient,
        ws_base_url: &str,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
        heartbeat_secs: Option<u64>,
    ) -> Self {
        Self {
            http_client,
            ws_base_url: ws_base_url.trim_end_matches('/').to_string(),
            transport_backend,
            proxy_url,
            heartbeat_secs,
            connect_timeout_secs: None,
            listen_key: None,
            ws_client: None,
            socket_factory: None,
            socket_state_callback: None,
        }
    }

    /// Reports socket state changes to `callback`, alongside the live socket registry.
    ///
    /// The callback runs on every transport state change before the state is published, so it
    /// must not synchronously trigger another state change for the same endpoint.
    #[must_use]
    pub fn with_socket_state(
        mut self,
        factory: SocketControlFactory,
        callback: impl Fn(SocketState) + Send + Sync + 'static,
    ) -> Self {
        self.socket_factory = Some(factory);
        self.socket_state_callback = Some(Arc::new(callback));
        self
    }

    /// Overrides the per-attempt WebSocket connect timeout.
    ///
    /// The shared Binance stream pool defaults to five seconds, which is short for hosts whose
    /// egress path is slow: the handshake then times out on every attempt and the execution
    /// client can never come up. `None` keeps the shared default.
    #[must_use]
    pub const fn with_connect_timeout_secs(mut self, connect_timeout_secs: Option<u64>) -> Self {
        self.connect_timeout_secs = connect_timeout_secs;
        self
    }

    /// Returns the configured per-attempt WebSocket connect timeout in seconds, if overridden.
    #[must_use]
    pub const fn connect_timeout_secs(&self) -> Option<u64> {
        self.connect_timeout_secs
    }

    /// Returns the configured WebSocket base URL.
    #[must_use]
    pub fn ws_base_url(&self) -> &str {
        &self.ws_base_url
    }

    /// Returns the active listen key, if a session is open.
    ///
    /// The key authenticates the stream, so it is deliberately not part of [`Debug`] output
    /// and should not be logged.
    #[must_use]
    pub fn listen_key(&self) -> Option<&str> {
        self.listen_key.as_deref()
    }

    /// Returns whether the underlying socket is connected.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.ws_client
            .as_ref()
            .is_some_and(BinanceFuturesWebSocketClient::is_active)
    }

    /// Opens a user data stream session.
    ///
    /// Obtains a fresh listen key and connects to `<ws base>/<listen key>`. Any previous
    /// session is closed first.
    ///
    /// # Errors
    ///
    /// Returns an error if the listen key request fails or the socket cannot connect.
    pub async fn connect(&mut self) -> AsterHttpResult<()> {
        self.close().await;

        let listen_key = self.http_client.create_listen_key().await?;
        let url = user_stream_url(&self.ws_base_url, &listen_key);

        let mut ws_client = BinanceFuturesWebSocketClient::new(
            BinanceProductType::UsdM,
            // Aster has its own endpoints, so the resolved URL override carries the
            // environment selection and the Binance environment is irrelevant.
            BinanceEnvironment::Live,
            None, // api_key: Aster user streams authenticate through the listen key in the URL
            None, // api_secret
            Some(url),
            self.heartbeat_secs,
            self.transport_backend,
        )
        .map_err(|e| {
            crate::http::AsterHttpError::NetworkError(format!(
                "Failed to build Aster user stream client: {e}"
            ))
        })?
        .with_proxy(self.proxy_url.clone())
        .with_connect_timeout_ms(
            self.connect_timeout_secs
                .map(|secs| secs.saturating_mul(1_000)),
        );

        if let Some(factory) = self.socket_factory.clone() {
            ws_client = ws_client.with_socket_control(factory, ASTER_USER_STREAM_ENDPOINT);
        }
        if let Some(callback) = self.socket_state_callback.clone() {
            ws_client = ws_client.with_socket_state_callback(move |state| callback(state));
        }

        ws_client.connect().await.map_err(|e| {
            crate::http::AsterHttpError::NetworkError(format!(
                "Failed to connect Aster user stream: {e}"
            ))
        })?;

        self.listen_key = Some(listen_key);
        self.ws_client = Some(ws_client);
        Ok(())
    }

    /// Returns the stream of decoded user data events.
    ///
    /// Can only be consumed once per [`connect`](Self::connect); later calls yield an empty
    /// stream. Returns `None` when no session is open.
    pub fn stream(&self) -> Option<impl Stream<Item = BinanceFuturesWsStreamsMessage> + 'static> {
        self.ws_client
            .as_ref()
            .map(BinanceFuturesWebSocketClient::stream)
    }

    /// Renews the listen key so the venue does not expire the session.
    ///
    /// Aster invalidates an un-renewed key after 60 minutes. The endpoint takes no key
    /// parameter: it renews whichever key the signing wallet currently holds.
    ///
    /// # Errors
    ///
    /// Returns an error if the renewal request fails.
    pub async fn keepalive(&self) -> AsterHttpResult<()> {
        if self.listen_key.is_none() {
            return Ok(());
        }
        self.http_client.keepalive_listen_key().await
    }

    /// Closes the socket and releases the listen key.
    ///
    /// The close endpoint takes no key parameter, so it releases the account's current key:
    /// another client streaming with the same wallet loses its stream too.
    ///
    /// Failures are logged rather than propagated: teardown must not block shutdown.
    pub async fn close(&mut self) {
        if let Some(mut ws_client) = self.ws_client.take()
            && let Err(e) = ws_client.close().await
        {
            log::warn!("Aster user stream socket close failed: {e}");
        }

        if self.listen_key.take().is_some()
            && let Err(e) = self.http_client.close_listen_key().await
        {
            log::debug!("Aster listen key close failed (key may already be expired): {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::{
        consts::{ASTER_TESTNET_WS_URL, ASTER_WS_URL},
        credential::AsterCredential,
        enums::AsterEnvironment,
    };

    /// Test-only key published in CCXT's static request fixtures; holds no funds.
    const TEST_PRIVATE_KEY: &str =
        "0xff3bdd43534543d421f05aec535965b5050ad6ac15345435345435453495e771";
    const TEST_LISTEN_KEY: &str = "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv";

    fn http_client() -> AsterHttpClient {
        let credential = AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            None,
            None,
            AsterEnvironment::Testnet,
            |_| None,
        )
        .unwrap();

        AsterHttpClient::new(
            "https://fapi.asterdex-testnet.com",
            Some(credential),
            Some(30),
            None,
        )
        .unwrap()
    }

    #[rstest]
    fn test_user_stream_url_appends_listen_key_as_path_segment() {
        assert_eq!(
            user_stream_url(ASTER_WS_URL, TEST_LISTEN_KEY),
            format!("wss://fstream.asterdex.com/ws/{TEST_LISTEN_KEY}")
        );
    }

    #[rstest]
    fn test_user_stream_url_for_testnet() {
        assert_eq!(
            user_stream_url(ASTER_TESTNET_WS_URL, TEST_LISTEN_KEY),
            format!("wss://fstream5.asterdex-testnet.com/ws/{TEST_LISTEN_KEY}")
        );
    }

    #[rstest]
    fn test_user_stream_url_normalises_trailing_slash() {
        assert_eq!(
            user_stream_url("wss://fstream.asterdex.com/ws/", TEST_LISTEN_KEY),
            format!("wss://fstream.asterdex.com/ws/{TEST_LISTEN_KEY}")
        );
    }

    #[rstest]
    fn test_user_stream_url_does_not_use_the_binance_query_form() {
        // Binance USD-M uses `?listenKey=<key>`; Aster uses a path segment.
        let url = user_stream_url(ASTER_WS_URL, TEST_LISTEN_KEY);

        assert!(!url.contains("?listenKey="), "{url}");
    }

    #[rstest]
    fn test_new_client_has_no_session() {
        let client = AsterUserStreamClient::new(
            http_client(),
            &format!("{ASTER_TESTNET_WS_URL}/"),
            TransportBackend::default(),
            None,
            Some(30),
        );

        assert_eq!(client.ws_base_url(), ASTER_TESTNET_WS_URL);
        assert_eq!(client.listen_key(), None);
        assert!(!client.is_active());
        assert!(client.stream().is_none());
    }

    #[rstest]
    fn test_debug_does_not_leak_the_signing_key() {
        let client = AsterUserStreamClient::new(
            http_client(),
            ASTER_TESTNET_WS_URL,
            TransportBackend::default(),
            None,
            None,
        );

        assert!(!format!("{client:?}").contains("ff3bdd43"));
    }

    #[rstest]
    fn test_debug_redacts_the_listen_key() {
        let mut client = AsterUserStreamClient::new(
            http_client(),
            ASTER_TESTNET_WS_URL,
            TransportBackend::default(),
            None,
            None,
        );
        client.listen_key = Some(TEST_LISTEN_KEY.to_string());

        let rendered = format!("{client:?}");

        assert!(!rendered.contains(TEST_LISTEN_KEY), "{rendered}");
        assert!(rendered.contains(REDACTED), "{rendered}");
        assert!(rendered.contains(ASTER_TESTNET_WS_URL), "{rendered}");
    }

    #[rstest]
    fn test_debug_of_a_session_without_a_key_reports_none() {
        let client = AsterUserStreamClient::new(
            http_client(),
            ASTER_TESTNET_WS_URL,
            TransportBackend::default(),
            None,
            None,
        );

        let rendered = format!("{client:?}");

        assert!(rendered.contains("listen_key: None"), "{rendered}");
    }

    #[rstest]
    #[tokio::test]
    async fn test_keepalive_without_session_is_a_noop() {
        let client = AsterUserStreamClient::new(
            http_client(),
            ASTER_TESTNET_WS_URL,
            TransportBackend::default(),
            None,
            None,
        );

        // No listen key yet, so no request is made and no error is raised.
        assert!(client.keepalive().await.is_ok());
    }
}
