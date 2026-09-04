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

//! Live execution client for the Aster DEX Futures V3 API.
//!
//! Order entry, cancellation, and account queries go through [`AsterHttpClient`] (EIP-712
//! signed); order and account state arrives on the private user data stream, whose Binance-USD-M
//! frames are decoded by [`nautilus_binance`] and turned into Nautilus reports.
//!
//! # Scope
//!
//! One-way (net) position mode with `LIMIT` and `MARKET` orders. Hedge mode is detected at
//! connect and rejected rather than silently mishandled, and conditional/algo order types are
//! rejected at submission. Order modification is not supported by this adapter: cancel and
//! resubmit instead.

use std::{sync::Arc, time::Duration};

use ahash::AHashMap;
use anyhow::Context;
use async_trait::async_trait;
use futures_util::StreamExt;
use nautilus_binance::{
    common::{
        enums::{BinanceEnvironment, BinanceProductType},
        symbol::format_binance_symbol,
    },
    futures::{
        http::client::BinanceFuturesHttpClient,
        websocket::streams::{
            messages::BinanceFuturesWsStreamsMessage,
            parse_exec::{
                parse_futures_account_update, parse_futures_order_update_to_fill,
                parse_futures_order_update_to_order_status,
            },
        },
    },
};
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    messages::execution::{
        CancelAllOrders, CancelOrder, GenerateFillReports, GenerateOrderStatusReport,
        GenerateOrderStatusReports, GeneratePositionStatusReports, ModifyOrder, QueryAccount,
        QueryOrder, SubmitOrder,
    },
};
use nautilus_core::{
    Params, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter,
    task::{TaskGroup, TaskSpawner},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType, OrderSide, OrderType, PositionSide, TimeInForce},
    identifiers::{AccountId, ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Quantity},
};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{consts::ASTER_LISTEN_KEY_RENEWAL_SECS, credential::AsterCredential},
    config::AsterExecutionClientConfig,
    http::{
        AsterHttpClient, AsterParams,
        models::{AsterOrder, millis_to_nanos, parse_decimal},
    },
    websocket::AsterUserStreamClient,
};

/// Settlement currency for Aster USD-margined perpetuals.
const ASTER_SETTLEMENT_ASSET: &str = "USDT";

/// How long to wait before retrying a dropped user data stream session.
const USER_STREAM_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Snapshot of the instruments this client can trade, indexed for both lookup directions.
#[derive(Debug, Default)]
struct InstrumentIndex {
    by_id: AHashMap<InstrumentId, InstrumentAny>,
    by_symbol: AHashMap<Ustr, InstrumentAny>,
}

impl InstrumentIndex {
    fn replace(&mut self, instruments: Vec<InstrumentAny>) {
        self.by_id.clear();
        self.by_symbol.clear();

        for instrument in instruments {
            self.by_symbol
                .insert(instrument.raw_symbol().inner(), instrument.clone());
            self.by_id.insert(instrument.id(), instrument);
        }
    }

    fn by_id(&self, instrument_id: &InstrumentId) -> Option<&InstrumentAny> {
        self.by_id.get(instrument_id)
    }

    fn by_symbol(&self, symbol: &Ustr) -> Option<&InstrumentAny> {
        self.by_symbol.get(symbol)
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }
}

/// Precisions and identity resolved for one venue symbol.
#[derive(Debug, Clone, Copy)]
struct SymbolContext {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

/// Live execution client for the Aster DEX.
#[derive(Debug)]
pub struct AsterExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: AsterExecutionClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: AsterHttpClient,
    instrument_http_client: BinanceFuturesHttpClient,
    instruments: Arc<RwLock<InstrumentIndex>>,
    session_tasks: TaskGroup,
    pending_tasks: TaskGroup,
    venue: Venue,
}

impl AsterExecutionClient {
    /// Creates a new [`AsterExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if credentials cannot be resolved or an HTTP client cannot be built.
    pub fn new(
        core: ExecutionClientCore,
        config: AsterExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;

        let credential = AsterCredential::resolve(
            config.signer_private_key.as_deref(),
            config.signer_address.as_deref(),
            config.user_address.as_deref(),
            config.environment,
        )
        .context("Aster execution client requires a signer private key")?;

        let venue = config.resolved_venue();
        let http_base = config.resolved_http_url();

        let http_client = AsterHttpClient::new(
            &http_base,
            Some(credential),
            config.http_timeout_secs,
            config.proxy_url.clone(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to build Aster HTTP client: {e}"))?;

        // Instruments come from Aster's Binance-compatible public `exchangeInfo`, which needs
        // no signing, so this client is deliberately credential-free.
        let instrument_http_client = BinanceFuturesHttpClient::new(
            BinanceProductType::UsdM,
            BinanceEnvironment::Live,
            get_atomic_clock_realtime(),
            None, // api_key
            None, // api_secret
            Some(http_base),
            None, // recv_window: not used by Aster V3
            config.http_timeout_secs,
            config.proxy_url.clone(),
            config.treat_expired_as_canceled,
        )
        .map_err(|e| anyhow::anyhow!("Failed to build Aster instrument client: {e}"))?
        .with_venue(venue);

        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None, // base_currency: multi-asset margin account
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            instrument_http_client,
            instruments: Arc::new(RwLock::new(InstrumentIndex::default())),
            session_tasks: TaskGroup::new(),
            pending_tasks: TaskGroup::new(),
            venue,
        })
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &AsterExecutionClientConfig {
        &self.config
    }

    /// Returns a clone of the signed HTTP client.
    #[must_use]
    pub fn http_client(&self) -> AsterHttpClient {
        self.http_client.clone()
    }

    /// Returns the number of instruments loaded for this session.
    #[must_use]
    pub fn instrument_count(&self) -> usize {
        self.instruments.read().len()
    }

    /// Returns the settlement currency used for commissions and balances.
    fn settlement_currency() -> Currency {
        Currency::from(ASTER_SETTLEMENT_ASSET)
    }

    /// Resolves the venue symbol and precisions for an instrument.
    fn symbol_context(&self, instrument_id: &InstrumentId) -> anyhow::Result<(String, SymbolContext)> {
        let guard = self.instruments.read();
        let instrument = guard.by_id(instrument_id).ok_or_else(|| {
            anyhow::anyhow!("Aster instrument {instrument_id} is not loaded; check `load_ids`")
        })?;

        Ok((
            format_binance_symbol(instrument_id),
            SymbolContext {
                instrument_id: *instrument_id,
                price_precision: instrument.price_precision(),
                size_precision: instrument.size_precision(),
            },
        ))
    }

    /// Resolves the instrument identity and precisions for a venue symbol.
    fn context_for_symbol(instruments: &InstrumentIndex, symbol: &Ustr) -> Option<SymbolContext> {
        instruments.by_symbol(symbol).map(|instrument| SymbolContext {
            instrument_id: instrument.id(),
            price_precision: instrument.price_precision(),
            size_precision: instrument.size_precision(),
        })
    }

    async fn load_instruments(&self) -> anyhow::Result<()> {
        let instruments = self
            .instrument_http_client
            .request_instruments_with_config(&self.config.instrument_provider)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load Aster instruments: {e}"))?;

        anyhow::ensure!(
            !instruments.is_empty(),
            "Aster instrument load returned no instruments; check `instrument_provider.load_ids`"
        );

        let count = instruments.len();
        self.instruments.write().replace(instruments);
        self.core.set_instruments_initialized();

        log::info!("Loaded {count} Aster instruments for execution");
        Ok(())
    }

    /// Fails loudly when the account runs in hedge (dual-side) mode.
    ///
    /// Every position and order path in this adapter assumes one-way mode; silently trading a
    /// hedge-mode account would mis-attribute positions.
    async fn assert_one_way_mode(&self) -> anyhow::Result<()> {
        match self.http_client.query_position_mode().await {
            Ok(mode) => {
                anyhow::ensure!(
                    !mode.dual_side_position,
                    "Aster account is in hedge (dual-side) position mode, which this adapter does \
                     not support; switch the account to one-way mode"
                );
                Ok(())
            }
            Err(e) if e.is_auth_failure() => Err(anyhow::anyhow!(
                "Aster rejected the first signed request: {e}"
            )),
            Err(e) => {
                // A venue that does not expose the endpoint must not block the session; the
                // position reports still reflect reality in one-way mode.
                log::warn!("Aster position mode query failed, assuming one-way mode: {e}");
                Ok(())
            }
        }
    }

    async fn fetch_account_state(&self) -> anyhow::Result<(Vec<AccountBalance>, Vec<MarginBalance>)> {
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        let mut account_balances = Vec::new();

        for balance in &balances {
            if balance.is_zero() {
                continue;
            }

            let currency = Currency::from(balance.asset.as_str());
            match (balance.total(), balance.free()) {
                (Ok(total), Ok(free)) => {
                    match AccountBalance::from_total_and_free(total, free, currency) {
                        Ok(account_balance) => account_balances.push(account_balance),
                        Err(e) => log::warn!("Skipping Aster balance for {currency}: {e}"),
                    }
                }
                _ => log::warn!("Skipping Aster balance for {currency}: unparsable amounts"),
            }
        }

        // Aster's `/fapi/v3/balance` reports wallet balances only; per-asset initial and
        // maintenance margin are not part of the payload, so no margin balances are emitted.
        Ok((account_balances, Vec::new()))
    }

    async fn emit_account_state(&self) -> anyhow::Result<()> {
        let (balances, margins) = self.fetch_account_state().await?;

        if balances.is_empty() {
            log::warn!("Aster account reports no non-zero balances");
        }

        let ts_event = self.clock.get_time_ns();
        self.emitter
            .emit_account_state(balances, margins, true, ts_event, None);
        Ok(())
    }

    /// Reconciles open orders at connect so externally placed orders are known to the engine.
    async fn reconcile_open_orders(&self) -> anyhow::Result<()> {
        let orders = match self.http_client.query_open_orders(None).await {
            Ok(orders) => orders,
            Err(e) => {
                log::warn!("Aster open order reconciliation failed: {e}");
                return Ok(());
            }
        };

        let ts_init = self.clock.get_time_ns();
        let mut reported = 0usize;

        for order in &orders {
            match self.order_to_report(order, ts_init) {
                Ok(report) => {
                    self.emitter.send_order_status_report(report);
                    reported += 1;
                }
                Err(e) => log::warn!("Skipping Aster open order {}: {e}", order.order_id),
            }
        }

        log::info!("Reconciled {reported} open Aster orders");
        Ok(())
    }

    fn order_to_report(
        &self,
        order: &AsterOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let context = {
            let guard = self.instruments.read();
            Self::context_for_symbol(&guard, &order.symbol).ok_or_else(|| {
                anyhow::anyhow!("Aster symbol {} is not a loaded instrument", order.symbol)
            })?
        };

        order.to_order_status_report(
            self.core.account_id,
            context.instrument_id,
            context.price_precision,
            context.size_precision,
            self.config.treat_expired_as_canceled,
            ts_init,
        )
    }

    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let future = async move {
            if let Err(e) = fut.await {
                log::warn!("Aster {description} failed: {e:?}");
            }
        };

        if let Err(e) = self.pending_tasks.spawn(future) {
            log::warn!("Skipping Aster {description} after shutdown began: {e}");
        }
    }

    fn spawner(&self) -> Option<TaskSpawner> {
        match self.pending_tasks.spawner() {
            Ok(spawner) => Some(spawner),
            Err(e) => {
                log::warn!("Aster execution client is shutting down: {e}");
                None
            }
        }
    }

    /// Starts the user data stream session loop.
    fn start_user_stream(&self) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let ws_base_url = self.config.resolved_ws_url();
        let proxy_url = self.config.proxy_url.clone();
        let heartbeat_secs = self.config.ws_heartbeat_secs;
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let instruments = self.instruments.clone();
        let treat_expired_as_canceled = self.config.treat_expired_as_canceled;
        let clock = self.clock;

        self.session_tasks.spawn(async move {
            let mut stream_client = AsterUserStreamClient::new(
                http_client,
                &ws_base_url,
                nautilus_network::websocket::TransportBackend::default(),
                proxy_url,
                heartbeat_secs,
            );

            loop {
                if let Err(e) = stream_client.connect().await {
                    log::error!("Aster user stream connect failed: {e}");
                    tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                    continue;
                }

                log::info!("Aster user data stream connected");

                let Some(stream) = stream_client.stream() else {
                    log::error!("Aster user stream produced no message stream");
                    tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                    continue;
                };
                let mut stream = Box::pin(stream);
                let mut renewal =
                    tokio::time::interval(Duration::from_secs(ASTER_LISTEN_KEY_RENEWAL_SECS));
                renewal.tick().await; // The first tick completes immediately.

                loop {
                    tokio::select! {
                        _ = renewal.tick() => {
                            if let Err(e) = stream_client.keepalive().await {
                                log::warn!("Aster listen key renewal failed: {e}");
                            } else {
                                log::debug!("Aster listen key renewed");
                            }
                        }
                        message = stream.next() => {
                            let Some(message) = message else {
                                log::warn!("Aster user data stream ended; reconnecting");
                                break;
                            };

                            if matches!(message, BinanceFuturesWsStreamsMessage::ListenKeyExpired) {
                                log::warn!("Aster listen key expired; reconnecting with a new key");
                                break;
                            }

                            dispatch_user_stream_message(
                                &message,
                                &emitter,
                                account_id,
                                &instruments,
                                treat_expired_as_canceled,
                                clock.get_time_ns(),
                            );
                        }
                    }
                }

                stream_client.close().await;
                tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
            }
        })?;

        Ok(())
    }

    async fn await_task_groups(&self) {
        self.session_tasks.begin_shutdown();
        self.pending_tasks.begin_shutdown();

        if let Err(e) = self
            .session_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
        {
            log::warn!("Failed to terminate Aster session tasks: {e}");
        }

        if let Err(e) = self
            .pending_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
        {
            log::warn!("Failed to terminate Aster pending tasks: {e}");
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Order request construction
// ------------------------------------------------------------------------------------------------

/// The Aster wire representation of a Nautilus order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsterOrderRequest {
    /// Venue symbol.
    pub symbol: String,
    /// `BUY` or `SELL`.
    pub side: &'static str,
    /// `LIMIT` or `MARKET`.
    pub order_type: &'static str,
    /// Order quantity formatted at the instrument's size precision.
    pub quantity: String,
    /// Limit price formatted at the instrument's price precision.
    pub price: Option<String>,
    /// `GTC`, `IOC`, `FOK`, or `GTX` for post-only.
    pub time_in_force: Option<&'static str>,
    /// Whether the order may only reduce an existing position.
    pub reduce_only: bool,
    /// Client order ID sent verbatim as `newClientOrderId`.
    pub client_order_id: String,
}

impl AsterOrderRequest {
    /// Converts this request into ordered request parameters.
    #[must_use]
    pub fn to_params(&self) -> AsterParams {
        AsterParams::new()
            .with("symbol", &self.symbol)
            .with("side", self.side)
            .with("type", self.order_type)
            .with("quantity", &self.quantity)
            .with_opt("price", self.price.as_deref())
            .with_opt("timeInForce", self.time_in_force)
            .with_opt("reduceOnly", self.reduce_only.then_some("true"))
            .with("newClientOrderId", &self.client_order_id)
    }
}

/// Maximum length Aster accepts for `newClientOrderId`.
const MAX_CLIENT_ORDER_ID_LEN: usize = 36;

/// Returns whether `value` satisfies Aster's `^[\.A-Z\:/a-z0-9_-]{1,36}$` client-ID rule.
#[must_use]
pub fn is_valid_client_order_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CLIENT_ORDER_ID_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b':' | b'/' | b'_' | b'-'))
}

/// Builds the Aster wire request for a Nautilus order.
///
/// # Errors
///
/// Returns an error if the order type, time in force, or client order ID is not supported by
/// Aster's V3 order endpoint.
pub fn build_order_request(order: &OrderAny, symbol: String) -> anyhow::Result<AsterOrderRequest> {
    let client_order_id = order.client_order_id().to_string();
    anyhow::ensure!(
        is_valid_client_order_id(&client_order_id),
        "Client order ID '{client_order_id}' is not accepted by Aster: it must match \
         ^[.A-Z:/a-z0-9_-]{{1,36}}$"
    );

    let side = match order.order_side() {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };

    let quantity = order.quantity().to_string();

    match order.order_type() {
        OrderType::Market => Ok(AsterOrderRequest {
            symbol,
            side,
            order_type: "MARKET",
            quantity,
            price: None,
            time_in_force: None,
            reduce_only: order.is_reduce_only(),
            client_order_id,
        }),
        OrderType::Limit => {
            let price = order
                .price()
                .ok_or_else(|| anyhow::anyhow!("LIMIT order requires a price"))?;

            let time_in_force = if order.is_post_only() {
                // Aster spells post-only as the GTX time in force, like Binance USD-M.
                "GTX"
            } else {
                match order.time_in_force() {
                    TimeInForce::Gtc => "GTC",
                    TimeInForce::Ioc => "IOC",
                    TimeInForce::Fok => "FOK",
                    other => anyhow::bail!(
                        "Aster supports GTC, IOC, and FOK for LIMIT orders, received {other:?}"
                    ),
                }
            };

            Ok(AsterOrderRequest {
                symbol,
                side,
                order_type: "LIMIT",
                quantity,
                price: Some(price.to_string()),
                time_in_force: Some(time_in_force),
                reduce_only: order.is_reduce_only(),
                client_order_id,
            })
        }
        other => anyhow::bail!(
            "Aster execution supports LIMIT and MARKET orders only, received {other:?}"
        ),
    }
}

// ------------------------------------------------------------------------------------------------
// User data stream dispatch
// ------------------------------------------------------------------------------------------------

/// Turns one decoded user data stream message into Nautilus reports and emits them.
///
/// Unknown or irrelevant message types are logged at debug level and dropped; Aster multiplexes
/// venue-specific announcement events onto the same stream.
fn dispatch_user_stream_message(
    message: &BinanceFuturesWsStreamsMessage,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    instruments: &Arc<RwLock<InstrumentIndex>>,
    treat_expired_as_canceled: bool,
    ts_init: UnixNanos,
) {
    match message {
        BinanceFuturesWsStreamsMessage::OrderUpdate(msg) => {
            let symbol = msg.order.symbol;
            let Some(context) = ({
                let guard = instruments.read();
                AsterExecutionClient::context_for_symbol(&guard, &symbol)
            }) else {
                log::debug!("Ignoring Aster order update for unloaded symbol {symbol}");
                return;
            };

            let status = match parse_futures_order_update_to_order_status(
                msg,
                context.instrument_id,
                context.price_precision,
                context.size_precision,
                account_id,
                treat_expired_as_canceled,
                ts_init,
            ) {
                Ok(report) => Some(report),
                Err(e) => {
                    log::error!("Failed to parse Aster order status report: {e}");
                    None
                }
            };

            // `lastFilledQty` is non-zero only on the TRADE execution type.
            let has_fill = parse_decimal(&msg.order.last_filled_qty, "lastFilledQty")
                .is_ok_and(|qty| !qty.is_zero());

            let fill = if has_fill {
                match parse_futures_order_update_to_fill(
                    msg,
                    account_id,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    None, // taker_fee: Aster reports the commission on the event
                    Some(AsterExecutionClient::settlement_currency()),
                    AsterExecutionClient::settlement_currency(),
                    None, // venue_position_id: one-way mode
                    ts_init,
                ) {
                    Ok(report) => Some(report),
                    Err(e) => {
                        log::error!("Failed to parse Aster fill report: {e}");
                        None
                    }
                }
            } else {
                None
            };

            // Sending a fill on its own would let the engine bootstrap a synthetic order at the
            // fill quantity, which then closes early and rejects later fills for the same
            // venue order, so both reports are bundled whenever both parsed.
            match (status, fill) {
                (Some(status), Some(fill)) => emitter.send_order_with_fills(status, vec![fill]),
                (Some(status), None) => emitter.send_order_status_report(status),
                (None, Some(fill)) => emitter.send_fill_report(fill),
                (None, None) => {}
            }
        }
        BinanceFuturesWsStreamsMessage::AccountUpdate(msg) => {
            if let Some(state) = parse_futures_account_update(
                msg,
                account_id,
                AsterExecutionClient::settlement_currency(),
                ts_init,
            ) {
                emitter.send_account_state(state);
            }
        }
        BinanceFuturesWsStreamsMessage::MarginCall(msg) => {
            log::warn!(
                "Aster margin call: cross_wallet_balance={}, positions_at_risk={}",
                msg.cross_wallet_balance,
                msg.positions.len()
            );
        }
        BinanceFuturesWsStreamsMessage::Reconnected => {
            log::info!("Aster user data stream reconnected");
        }
        BinanceFuturesWsStreamsMessage::Error(msg) => {
            log::error!("Aster user data stream error: {msg:?}");
        }
        other => {
            log::debug!("Ignoring Aster user stream message: {other:?}");
        }
    }
}

// ------------------------------------------------------------------------------------------------
// ExecutionClient
// ------------------------------------------------------------------------------------------------

#[async_trait(?Send)]
impl ExecutionClient for AsterExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();

        log::info!(
            "Started Aster execution client: client_id={}, account_id={}, venue={}, environment={}",
            self.core.client_id,
            self.core.account_id,
            self.venue,
            self.config.environment,
        );

        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        log::info!("Stopping Aster execution client");

        self.session_tasks.abort();
        self.pending_tasks.abort();
        self.core.set_stopped();
        self.core.set_disconnected();

        log::info!("Aster execution client stopped");
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        self.load_instruments().await?;
        self.assert_one_way_mode().await?;
        self.emit_account_state().await?;
        self.start_user_stream()?;
        self.reconcile_open_orders().await?;

        self.core.set_connected();
        log::info!("Aster execution client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        log::info!("Disconnecting Aster execution client");
        self.await_task_groups().await;
        self.core.set_disconnected();
        log::info!("Aster execution client disconnected");
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let (symbol, _) = match self.symbol_context(&order.instrument_id()) {
            Ok(resolved) => resolved,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        // Validate before emitting OrderSubmitted: Initialized -> Denied is a legal transition
        // but Submitted -> Denied is not.
        let request = match build_order_request(&order, symbol) {
            Ok(request) => request,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        let Some(spawner) = self.spawner() else {
            self.emitter
                .emit_order_denied(&order, "Aster execution client is shutting down");
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();

        self.emitter.emit_order_submitted(&order);

        spawner.spawn(async move {
            match http_client.submit_order(request.to_params()).await {
                Ok(response) => {
                    log::debug!(
                        "Aster order accepted: client_order_id={client_order_id}, venue_order_id={}",
                        response.order_id,
                    );
                }
                Err(e) if e.is_venue_rejection() => {
                    let due_post_only = e.is_post_only_violation();
                    log::warn!("Aster rejected order {client_order_id}: {e}");
                    emitter.emit_order_rejected(
                        &order,
                        &format!("submit-order-error: {e}"),
                        clock.get_time_ns(),
                        due_post_only,
                    );
                }
                Err(e) => {
                    // Transport failures are ambiguous: the order may have reached the venue,
                    // in which case the user stream or reconciliation resolves its state.
                    log::error!(
                        "Ambiguous Aster submit failure for {client_order_id}, awaiting \
                         reconciliation: {e}"
                    );
                }
            }
        })?;

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        log::warn!(
            "Aster execution does not support order modification; cancel and resubmit \
             (client_order_id={})",
            cmd.client_order_id,
        );

        if let Ok(order) = self.core.cache().try_order_owned(&cmd.client_order_id) {
            self.emitter.emit_order_modify_rejected(
                &order,
                cmd.venue_order_id,
                "Aster does not support order modification",
                self.clock.get_time_ns(),
            );
        }

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        // Cancelling by client order ID avoids a race with orders whose venue ID has not yet
        // been observed on the user stream.
        let venue_order_id_i64 = venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());
        let use_client_id = venue_order_id_i64.is_none();

        spawner.spawn(async move {
            let result = if use_client_id {
                http_client
                    .cancel_order(&symbol, None, Some(client_order_id.as_str()))
                    .await
            } else {
                http_client
                    .cancel_order(&symbol, venue_order_id_i64, None)
                    .await
            };

            match result {
                Ok(response) => log::debug!(
                    "Aster cancel accepted: client_order_id={client_order_id}, status={:?}",
                    response.status,
                ),
                Err(e) if e.is_unknown_order() => {
                    log::warn!("Aster reported order {client_order_id} as unknown on cancel: {e}");
                }
                Err(e) => log::error!("Aster cancel failed for {client_order_id}: {e}"),
            }
        })?;

        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;

        if cmd.order_side.is_some() {
            log::warn!(
                "Aster cancels all open orders for {symbol} regardless of side; the requested \
                 side filter {:?} is ignored",
                cmd.order_side,
            );
        }

        let http_client = self.http_client.clone();

        self.spawn_task("cancel_all_orders", async move {
            match http_client.cancel_all_orders(&symbol).await {
                Ok(response) if response.is_success() => {
                    log::debug!("Aster cancelled all open orders for {symbol}");
                    Ok(())
                }
                Ok(response) => {
                    anyhow::bail!("Aster cancel-all returned {}: {}", response.code, response.msg)
                }
                Err(e) => anyhow::bail!("Aster cancel-all failed for {symbol}: {e}"),
            }
        });

        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let balances = http_client
                .query_balances()
                .await
                .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

            let account_balances: Vec<AccountBalance> = balances
                .iter()
                .filter(|balance| !balance.is_zero())
                .filter_map(|balance| {
                    let currency = Currency::from(balance.asset.as_str());
                    let total = balance.total().ok()?;
                    let free = balance.free().ok()?;
                    AccountBalance::from_total_and_free(total, free, currency).ok()
                })
                .collect();

            emitter.emit_account_state(account_balances, Vec::new(), true, clock.get_time_ns(), None);
            Ok(())
        });

        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let (symbol, context) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let clock = self.clock;
        let treat_expired_as_canceled = self.config.treat_expired_as_canceled;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());

        spawner.spawn(async move {
            let result = if venue_order_id.is_some() {
                http_client.query_order(&symbol, venue_order_id, None).await
            } else {
                http_client
                    .query_order(&symbol, None, Some(client_order_id.as_str()))
                    .await
            };

            match result {
                Ok(order) => match order.to_order_status_report(
                    account_id,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    treat_expired_as_canceled,
                    clock.get_time_ns(),
                ) {
                    Ok(report) => emitter.send_order_status_report(report),
                    Err(e) => log::error!("Failed to build Aster order status report: {e}"),
                },
                Err(e) => log::warn!("Aster order query failed for {client_order_id}: {e}"),
            }
        })?;

        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument_id = cmd
            .instrument_id
            .ok_or_else(|| anyhow::anyhow!("Aster order status report requires an instrument ID"))?;
        let (symbol, context) = self.symbol_context(&instrument_id)?;

        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());
        let client_order_id = cmd.client_order_id.map(|id| id.to_string());

        anyhow::ensure!(
            venue_order_id.is_some() || client_order_id.is_some(),
            "Aster order status report requires a venue or client order ID"
        );

        let result = if venue_order_id.is_some() {
            self.http_client
                .query_order(&symbol, venue_order_id, None)
                .await
        } else {
            self.http_client
                .query_order(&symbol, None, client_order_id.as_deref())
                .await
        };

        match result {
            Ok(order) => Ok(Some(order.to_order_status_report(
                self.core.account_id,
                context.instrument_id,
                context.price_precision,
                context.size_precision,
                self.config.treat_expired_as_canceled,
                self.clock.get_time_ns(),
            )?)),
            Err(e) if e.is_unknown_order() => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Aster order status query failed: {e}")),
        }
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let ts_init = self.clock.get_time_ns();
        let start_ms = cmd.start.map(|ts| (ts.as_u64() / 1_000_000) as i64);
        let end_ms = cmd.end.map(|ts| (ts.as_u64() / 1_000_000) as i64);

        let orders = if cmd.open_only {
            let symbol = cmd
                .instrument_id
                .map(|id| self.symbol_context(&id).map(|(symbol, _)| symbol))
                .transpose()?;

            self.http_client
                .query_open_orders(symbol.as_deref())
                .await
                .map_err(|e| anyhow::anyhow!("Aster open orders query failed: {e}"))?
        } else {
            // Aster requires a symbol for the historical order endpoint, so the request is
            // fanned out across loaded instruments when none is given.
            let symbols: Vec<String> = match cmd.instrument_id {
                Some(instrument_id) => vec![self.symbol_context(&instrument_id)?.0],
                None => {
                    let guard = self.instruments.read();
                    guard.by_id.keys().map(format_binance_symbol).collect()
                }
            };

            let mut orders = Vec::new();
            for symbol in symbols {
                match self
                    .http_client
                    .query_all_orders(&symbol, start_ms, end_ms, None)
                    .await
                {
                    Ok(mut batch) => orders.append(&mut batch),
                    Err(e) => log::warn!("Aster historical order query failed for {symbol}: {e}"),
                }
            }
            orders
        };

        let mut reports = Vec::with_capacity(orders.len());
        for order in &orders {
            match self.order_to_report(order, ts_init) {
                Ok(report) => reports.push(report),
                Err(e) => log::warn!("Skipping Aster order {}: {e}", order.order_id),
            }
        }

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let ts_init = self.clock.get_time_ns();
        let start_ms = cmd.start.map(|ts| (ts.as_u64() / 1_000_000) as i64);
        let end_ms = cmd.end.map(|ts| (ts.as_u64() / 1_000_000) as i64);

        // Aster requires a symbol on `userTrades`, so the request is fanned out across loaded
        // instruments when the command does not name one.
        let targets: Vec<(String, SymbolContext)> = match cmd.instrument_id {
            Some(instrument_id) => vec![self.symbol_context(&instrument_id)?],
            None => {
                let guard = self.instruments.read();
                guard
                    .by_id
                    .keys()
                    .filter_map(|id| {
                        Self::context_for_symbol(&guard, &Ustr::from(&format_binance_symbol(id)))
                            .map(|context| (format_binance_symbol(id), context))
                    })
                    .collect()
            }
        };

        let mut reports = Vec::new();

        for (symbol, context) in targets {
            let trades = match self
                .http_client
                .query_user_trades(&symbol, start_ms, end_ms, None)
                .await
            {
                Ok(trades) => trades,
                Err(e) => {
                    log::warn!("Aster user trades query failed for {symbol}: {e}");
                    continue;
                }
            };

            for trade in &trades {
                match trade.to_fill_report(
                    self.core.account_id,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    Self::settlement_currency(),
                    ts_init,
                ) {
                    Ok(report) => reports.push(report),
                    Err(e) => log::warn!("Skipping Aster trade {}: {e}", trade.id),
                }
            }
        }

        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let symbol = cmd
            .instrument_id
            .map(|id| self.symbol_context(&id).map(|(symbol, _)| symbol))
            .transpose()?;

        let positions = self
            .http_client
            .query_position_risk(symbol.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("Aster position risk query failed: {e}"))?;

        let ts_now = self.clock.get_time_ns();
        let mut reports = Vec::new();

        for position in &positions {
            if position.is_flat() {
                continue;
            }

            let context = {
                let guard = self.instruments.read();
                Self::context_for_symbol(&guard, &position.symbol)
            };
            let Some(context) = context else {
                log::debug!(
                    "Skipping Aster position for unloaded symbol {}",
                    position.symbol
                );
                continue;
            };

            let signed_quantity = match position.signed_quantity() {
                Ok(value) => value,
                Err(e) => {
                    log::warn!("Skipping Aster position {}: {e}", position.symbol);
                    continue;
                }
            };

            let position_side = if signed_quantity > Decimal::ZERO {
                PositionSide::Long
            } else {
                PositionSide::Short
            };

            let quantity = match Quantity::from_decimal_dp(
                signed_quantity.abs(),
                context.size_precision,
            ) {
                Ok(quantity) => quantity,
                Err(e) => {
                    log::warn!("Skipping Aster position {}: {e}", position.symbol);
                    continue;
                }
            };

            let avg_px = parse_decimal(&position.entry_price, "entryPrice").ok();
            let ts_last = position.update_time.map_or(ts_now, millis_to_nanos);

            reports.push(PositionStatusReport::new(
                self.core.account_id,
                context.instrument_id,
                position_side,
                quantity,
                ts_last,
                ts_now,
                Some(UUID4::new()),
                None, // venue_position_id: one-way mode carries no venue position ID
                avg_px,
            ));
        }

        Ok(reports)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType, TimeInForce},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        orders::builder::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.ASTER")
    }

    fn limit_order(
        side: OrderSide,
        time_in_force: TimeInForce,
        post_only: bool,
        reduce_only: bool,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-1"))
            .side(side)
            .quantity(Quantity::from("0.010"))
            .price(Price::from("50000.00"))
            .time_in_force(time_in_force)
            .post_only(post_only)
            .reduce_only(reduce_only)
            .build()
    }

    fn market_order(side: OrderSide) -> OrderAny {
        OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-2"))
            .side(side)
            .quantity(Quantity::from("0.010"))
            .build()
    }

    #[rstest]
    fn test_build_limit_order_request() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, false, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.side, "BUY");
        assert_eq!(request.order_type, "LIMIT");
        assert_eq!(request.quantity, "0.010");
        assert_eq!(request.price.as_deref(), Some("50000.00"));
        assert_eq!(request.time_in_force, Some("GTC"));
        assert!(!request.reduce_only);
        assert_eq!(request.client_order_id, "O-20260101-000000-001-001-1");
    }

    #[rstest]
    fn test_build_market_order_request_omits_price_and_tif() {
        let order = market_order(OrderSide::Sell);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.side, "SELL");
        assert_eq!(request.order_type, "MARKET");
        assert_eq!(request.price, None);
        assert_eq!(request.time_in_force, None);
    }

    #[rstest]
    fn test_post_only_maps_to_gtx() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, true, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.time_in_force, Some("GTX"));
    }

    #[rstest]
    #[case(TimeInForce::Gtc, "GTC")]
    #[case(TimeInForce::Ioc, "IOC")]
    #[case(TimeInForce::Fok, "FOK")]
    fn test_supported_time_in_force(#[case] tif: TimeInForce, #[case] expected: &str) {
        let order = limit_order(OrderSide::Buy, tif, false, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.time_in_force, Some(expected));
    }

    #[rstest]
    fn test_reduce_only_is_forwarded() {
        let order = limit_order(OrderSide::Sell, TimeInForce::Gtc, false, true);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert!(request.reduce_only);
        assert_eq!(request.to_params().get("reduceOnly"), Some("true"));
    }

    #[rstest]
    fn test_unsupported_time_in_force_is_rejected() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Day, false, false);

        let error = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap_err()
            .to_string();

        assert!(error.contains("GTC, IOC, and FOK"), "{error}");
    }

    #[rstest]
    fn test_unsupported_order_type_is_rejected() {
        let order = OrderTestBuilder::new(OrderType::StopMarket)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-3"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .trigger_price(Price::from("50000.00"))
            .build();

        let error = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap_err()
            .to_string();

        assert!(error.contains("LIMIT and MARKET"), "{error}");
    }

    #[rstest]
    fn test_order_request_parameter_order_matches_the_venue_example() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, false, false);
        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        let params = request.to_params();
        let keys: Vec<&str> = params.entries().iter().map(|(k, _)| k.as_str()).collect();

        assert_eq!(
            keys,
            [
                "symbol",
                "side",
                "type",
                "quantity",
                "price",
                "timeInForce",
                "newClientOrderId",
            ]
        );
    }

    #[rstest]
    fn test_market_order_params_omit_price_and_time_in_force() {
        let order = market_order(OrderSide::Buy);
        let params = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap()
            .to_params();

        assert_eq!(params.get("price"), None);
        assert_eq!(params.get("timeInForce"), None);
        assert_eq!(params.get("type"), Some("MARKET"));
    }

    #[rstest]
    #[case("O-20260101-000000-001-001-1", true)]
    #[case("abc_123-XYZ.7", true)]
    #[case("a/b:c", true)]
    #[case("", false)]
    #[case("has space", false)]
    #[case("bad#char", false)]
    fn test_client_order_id_validation(#[case] value: &str, #[case] expected: bool) {
        assert_eq!(is_valid_client_order_id(value), expected);
    }

    #[rstest]
    fn test_client_order_id_length_limit() {
        assert!(is_valid_client_order_id(&"a".repeat(36)));
        assert!(!is_valid_client_order_id(&"a".repeat(37)));
    }

    #[rstest]
    fn test_settlement_currency_is_usdt() {
        assert_eq!(
            AsterExecutionClient::settlement_currency(),
            Currency::from("USDT")
        );
    }

    #[rstest]
    fn test_instrument_index_replace_indexes_both_directions() {
        let mut index = InstrumentIndex::default();
        assert_eq!(index.len(), 0);

        index.replace(Vec::new());
        assert_eq!(index.len(), 0);
        assert!(index.by_id(&instrument_id()).is_none());
        assert!(index.by_symbol(&Ustr::from("BTCUSDT")).is_none());
    }
}
