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

//! Startup multi-WebSocket probing and fastest-candidate selection for Hyperliquid.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::join_all;
use nautilus_network::{
    RECONNECTED, SocketStatePromoter, SocketStateSink,
    transport::Message as TransportMessage,
    websocket::{WebSocketClient, WebSocketConfig, WebSocketReadTiming},
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use crate::execution_timing::ExecutionTiming;

const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(6);
const WINNER_DRAIN_BUDGET: Duration = Duration::from_millis(150);
const WINNER_DRAIN_IDLE: Duration = Duration::from_millis(35);
const PROBE_POST_ID_BASE: u64 = 900_000_000;
const PROBE_COINS: [&str; 2] = ["BTC", "ETH"];

/// Metric priority used when ranking candidate Hyperliquid WebSocket connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsSelectMode {
    /// Rank primarily by earliest BBO arrival relative to peer candidates, then post/ping RTT.
    MarketData,
    /// Rank primarily by WS `post` info round-trip time, then ping RTT.
    ExecutionPost,
}

type RawWsTx = mpsc::UnboundedSender<(TungsteniteMessage, Option<WebSocketReadTiming>)>;
type ProbeTx = mpsc::UnboundedSender<(usize, Vec<u8>, Instant)>;

pub(super) enum CandidateRoute {
    Probing(ProbeTx),
    Active(RawWsTx),
    Dropped,
}

struct ConnectedCandidate {
    index: usize,
    client: WebSocketClient,
    route: Arc<Mutex<CandidateRoute>>,
    promoter: Option<SocketStatePromoter>,
    connect_us: u64,
}

#[derive(Debug, Default)]
pub(super) struct CandidateStats {
    pub(super) active: bool,
    pub(super) connect_us: u64,
    pub(super) bbo_count: usize,
    pub(super) bbo_vs_best_us: Vec<u64>,
    pub(super) bbo_wall_ages_ms: Vec<i64>,
    pub(super) post_rtts_us: Vec<u64>,
    pub(super) ping_rtts_us: Vec<u64>,
}

type BboKey = (String, u64, String, String);

pub(super) async fn connect_and_select_ws(
    cfg: &WebSocketConfig,
    timing: &ExecutionTiming,
    base_state_sink: Option<SocketStateSink>,
    raw_tx: RawWsTx,
    candidates: usize,
    probe_duration: Duration,
    mode: WsSelectMode,
) -> anyhow::Result<(WebSocketClient, Option<SocketStatePromoter>)> {
    let target_count = candidates.clamp(2, 12);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel::<(usize, Vec<u8>, Instant)>();

    let connect_futs = (0..target_count).map(|index| {
        let cfg = cfg.clone();
        let timing = timing.clone();
        let deferred = base_state_sink.clone().map(SocketStateSink::deferred);
        let route = Arc::new(Mutex::new(CandidateRoute::Probing(probe_tx.clone())));
        let route_cb = Arc::clone(&route);
        async move {
            let (gated_sink, promoter) = match deferred {
                Some((sink, promoter)) => (Some(sink), Some(promoter)),
                None => (None, None),
            };
            let receive = move |msg: TransportMessage, read_completed: Option<WebSocketReadTiming>| {
                enum Action {
                    Probe(ProbeTx),
                    Active(RawWsTx),
                    Dropped,
                }
                let action = match &*route_cb.lock() {
                    CandidateRoute::Probing(tx) => Action::Probe(tx.clone()),
                    CandidateRoute::Active(tx) => Action::Active(tx.clone()),
                    CandidateRoute::Dropped => Action::Dropped,
                };
                match action {
                    Action::Probe(tx) => {
                        if let TransportMessage::Text(bytes) = msg
                            && bytes.as_ref() != RECONNECTED.as_bytes()
                        {
                            let _ = tx.send((index, bytes.to_vec(), Instant::now()));
                        }
                    }
                    Action::Active(tx) => {
                        if let Ok(message) = TungsteniteMessage::try_from(msg) {
                            let _ = tx.send((message, read_completed));
                        }
                    }
                    Action::Dropped => {}
                }
            };
            let started = Instant::now();
            let connect_fut = async {
                if timing.is_enabled() {
                    WebSocketClient::read_timing_builder()
                        .config(cfg)
                        .read_timing_handler(Arc::new(receive))
                        .maybe_state_sink(gated_sink)
                        .connect()
                        .await
                } else {
                    WebSocketClient::builder()
                        .config(cfg)
                        .message_handler(Arc::new(move |msg| receive(msg, None)))
                        .maybe_state_sink(gated_sink)
                        .connect()
                        .await
                }
            };
            match tokio::time::timeout(PROBE_CONNECT_TIMEOUT, connect_fut).await {
                Ok(Ok(client)) => Some(ConnectedCandidate {
                    index,
                    client,
                    route,
                    promoter,
                    connect_us: started.elapsed().as_micros() as u64,
                }),
                Ok(Err(err)) => {
                    log::debug!("HL candidate WS #{index} connect failed: {err}");
                    None
                }
                Err(_) => {
                    log::debug!("HL candidate WS #{index} connect timed out");
                    None
                }
            }
        }
    });

    let mut connected: Vec<ConnectedCandidate> = join_all(connect_futs)
        .await
        .into_iter()
        .flatten()
        .collect();
    drop(probe_tx);

    if connected.is_empty() {
        anyhow::bail!(
            "All {target_count} Hyperliquid WebSocket candidate connections failed for {}",
            cfg.url
        );
    }

    if connected.len() == 1 {
        let only = connected.pop().expect("single connected candidate");
        *only.route.lock() = CandidateRoute::Active(raw_tx);
        log::info!(
            "HL WS selection ({mode:?}): only 1/{target_count} candidate connected (#{} connect={:.1}ms), promoted directly",
            only.index,
            only.connect_us as f64 / 1000.0
        );
        return Ok((only.client, only.promoter));
    }

    if mode == WsSelectMode::MarketData {
        for cand in &connected {
            for coin in PROBE_COINS {
                let sub = json!({
                    "method": "subscribe",
                    "subscription": { "type": "bbo", "coin": coin }
                })
                .to_string();
                let _ = cand.client.send_text(sub, None).await;
            }
        }
    }

    let n_connected = connected.len();
    let rounds: usize = if probe_duration >= Duration::from_millis(400) {
        4
    } else {
        3
    };
    let step = (probe_duration / (rounds as u32 + 1)).max(Duration::from_millis(25));
    let start = Instant::now();
    let probe_end = start + probe_duration;
    let mut next_round_at = start;
    let mut rounds_sent: usize = 0;

    let mut pending_pings: Vec<VecDeque<Instant>> = vec![VecDeque::new(); n_connected];
    let mut pending_posts: Vec<HashMap<u64, Instant>> = vec![HashMap::new(); n_connected];
    let mut bbo_arrivals: HashMap<BboKey, Vec<(usize, Instant)>> = HashMap::new();
    let mut stats: Vec<CandidateStats> = connected
        .iter()
        .map(|cand| CandidateStats {
            active: cand.client.is_active(),
            connect_us: cand.connect_us,
            ..CandidateStats::default()
        })
        .collect();
    let idx_to_slot: HashMap<usize, usize> = connected
        .iter()
        .enumerate()
        .map(|(slot, cand)| (cand.index, slot))
        .collect();

    loop {
        let now = Instant::now();
        if rounds_sent < rounds && now >= next_round_at {
            for (slot, cand) in connected.iter().enumerate() {
                if !cand.client.is_active() {
                    continue;
                }
                let post_id = PROBE_POST_ID_BASE + (slot as u64) * 1000 + rounds_sent as u64;
                let ping_msg = r#"{"method":"ping"}"#.to_string();
                let post_msg = json!({
                    "method": "post",
                    "id": post_id,
                    "request": {
                        "type": "info",
                        "payload": { "type": "l2Book", "coin": "BTC" }
                    }
                })
                .to_string();
                pending_pings[slot].push_back(Instant::now());
                pending_posts[slot].insert(post_id, Instant::now());
                let _ = cand.client.send_text(ping_msg, None).await;
                let _ = cand.client.send_text(post_msg, None).await;
            }
            rounds_sent += 1;
            next_round_at = now + step;
        }

        let until = if rounds_sent < rounds {
            next_round_at.min(probe_end)
        } else {
            probe_end
        };
        if Instant::now() >= probe_end && rounds_sent >= rounds {
            break;
        }

        tokio::select! {
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(until)) => {
                if Instant::now() >= probe_end && rounds_sent >= rounds {
                    break;
                }
            }
            msg = probe_rx.recv() => {
                let Some((orig_idx, bytes, recv_at)) = msg else { break; };
                let Some(&slot) = idx_to_slot.get(&orig_idx) else { continue; };
                handle_probe_frame(
                    slot,
                    &bytes,
                    recv_at,
                    &mut pending_pings[slot],
                    &mut pending_posts[slot],
                    &mut bbo_arrivals,
                    &mut stats[slot],
                );
            }
        }
    }

    for (slot, cand) in connected.iter().enumerate() {
        stats[slot].active = cand.client.is_active();
    }
    populate_bbo_vs_best(&bbo_arrivals, &mut stats);

    let winner_slot = pick_winner_slot(mode, &stats);

    log_selection_summary(
        mode,
        target_count,
        n_connected,
        winner_slot,
        &connected,
        &stats,
    );

    let mut winner: Option<ConnectedCandidate> = None;
    let mut losers = Vec::with_capacity(n_connected.saturating_sub(1));
    for (slot, cand) in connected.into_iter().enumerate() {
        if slot == winner_slot {
            winner = Some(cand);
        } else {
            *cand.route.lock() = CandidateRoute::Dropped;
            losers.push(cand);
        }
    }
    let winner = winner.expect("winner slot must exist");

    if mode == WsSelectMode::MarketData {
        for coin in PROBE_COINS {
            let unsub = json!({
                "method": "unsubscribe",
                "subscription": { "type": "bbo", "coin": coin }
            })
            .to_string();
            let _ = winner.client.send_text(unsub, None).await;
        }
        let drain_deadline = tokio::time::Instant::now() + WINNER_DRAIN_BUDGET;
        loop {
            let remaining = drain_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining.min(WINNER_DRAIN_IDLE), probe_rx.recv()).await {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }

    // Spawn graceful disconnects for losing candidates without blocking winner activation;
    // CandidateRoute::Dropped and unpromoted SocketStatePromoter suppress all frames and edges.
    for loser in losers {
        tokio::spawn(async move {
            loser.client.disconnect().await;
            drop(loser.promoter);
        });
    }

    *winner.route.lock() = CandidateRoute::Active(raw_tx);
    while probe_rx.try_recv().is_ok() {}

    Ok((winner.client, winner.promoter))
}

fn handle_probe_frame(
    slot: usize,
    bytes: &[u8],
    recv_at: Instant,
    pings: &mut VecDeque<Instant>,
    posts: &mut HashMap<u64, Instant>,
    bbo_arrivals: &mut HashMap<BboKey, Vec<(usize, Instant)>>,
    stat: &mut CandidateStats,
) {
    let Ok(val) = serde_json::from_slice::<Value>(bytes) else {
        return;
    };
    let Some(channel) = val.get("channel").and_then(Value::as_str) else {
        return;
    };
    match channel {
        "pong" => {
            if let Some(sent_at) = pings.pop_front() {
                stat.ping_rtts_us
                    .push(recv_at.saturating_duration_since(sent_at).as_micros() as u64);
            }
        }
        "post" => {
            if let Some(id) = val.get("data").and_then(|d| d.get("id")).and_then(Value::as_u64)
                && let Some(sent_at) = posts.remove(&id)
            {
                stat.post_rtts_us
                    .push(recv_at.saturating_duration_since(sent_at).as_micros() as u64);
            }
        }
        "bbo" => {
            if let Some((key, time_ms)) = parse_bbo_key(&val) {
                stat.bbo_count += 1;
                if let Ok(now_ms) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    let age = (now_ms.as_millis() as i64).saturating_sub(time_ms as i64);
                    stat.bbo_wall_ages_ms.push(age);
                }
                bbo_arrivals.entry(key).or_default().push((slot, recv_at));
            }
        }
        _ => {}
    }
}

pub(super) fn parse_bbo_key(val: &Value) -> Option<(BboKey, u64)> {
    let data = val.get("data")?;
    let coin = data.get("coin").and_then(Value::as_str)?.to_string();
    let time_ms = data.get("time").and_then(Value::as_u64)?;
    if let Some(bbo) = data.get("bbo").and_then(Value::as_array) {
        let bid = bbo
            .first()
            .and_then(|v| v.get("px"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let ask = bbo
            .get(1)
            .and_then(|v| v.get("px"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        return Some(((coin, time_ms, bid, ask), time_ms));
    }
    let bid = data
        .get("bid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let ask = data
        .get("ask")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(((coin, time_ms, bid, ask), time_ms))
}

pub(super) fn populate_bbo_vs_best(
    arrivals: &HashMap<BboKey, Vec<(usize, Instant)>>,
    stats: &mut [CandidateStats],
) {
    for samples in arrivals.values() {
        let mut seen = HashSet::with_capacity(samples.len());
        let unique: Vec<(usize, Instant)> = samples
            .iter()
            .copied()
            .filter(|(slot, _)| seen.insert(*slot))
            .collect();
        if unique.len() < 2 {
            continue;
        }
        let Some(earliest) = unique.iter().map(|(_, t)| *t).min() else {
            continue;
        };
        for (slot, t) in unique {
            if let Some(stat) = stats.get_mut(slot) {
                stat.bbo_vs_best_us
                    .push(t.saturating_duration_since(earliest).as_micros() as u64);
            }
        }
    }
}

pub(super) fn median_u64(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}

pub(super) fn median_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}

pub(super) fn pick_winner_slot(mode: WsSelectMode, stats: &[CandidateStats]) -> usize {
    let any_active = stats.iter().any(|s| s.active);
    let max_bbo_common = stats
        .iter()
        .filter(|s| !any_active || s.active)
        .map(|s| s.bbo_vs_best_us.len())
        .max()
        .unwrap_or(0);
    let max_posts = stats
        .iter()
        .filter(|s| !any_active || s.active)
        .map(|s| s.post_rtts_us.len())
        .max()
        .unwrap_or(0);

    stats
        .iter()
        .enumerate()
        .filter(|(_, s)| !any_active || s.active)
        .min_by_key(|&(slot, s)| match mode {
            WsSelectMode::MarketData => {
                let bbo_sufficient =
                    max_bbo_common == 0 || s.bbo_vs_best_us.len() * 2 >= max_bbo_common;
                (
                    !bbo_sufficient,
                    median_u64(&s.bbo_vs_best_us).unwrap_or(u64::MAX),
                    median_i64(&s.bbo_wall_ages_ms).unwrap_or(i64::MAX),
                    median_u64(&s.post_rtts_us).unwrap_or(u64::MAX),
                    median_u64(&s.ping_rtts_us).unwrap_or(u64::MAX),
                    s.connect_us,
                    slot,
                )
            }
            WsSelectMode::ExecutionPost => {
                let post_sufficient = max_posts == 0 || s.post_rtts_us.len() * 2 >= max_posts;
                (
                    !post_sufficient,
                    median_u64(&s.post_rtts_us).unwrap_or(u64::MAX),
                    0_i64,
                    median_u64(&s.ping_rtts_us).unwrap_or(u64::MAX),
                    s.connect_us,
                    0_u64,
                    slot,
                )
            }
        })
        .map_or(0, |(slot, _)| slot)
}

fn fmt_opt_ms(us: Option<u64>) -> String {
    us.map_or_else(|| "-".to_string(), |v| format!("{:.2}ms", v as f64 / 1000.0))
}

fn log_selection_summary(
    mode: WsSelectMode,
    target_count: usize,
    connected_count: usize,
    winner_slot: usize,
    connected: &[ConnectedCandidate],
    stats: &[CandidateStats],
) {
    let win_stat = &stats[winner_slot];
    let win_cand = &connected[winner_slot];
    let worst_bbo = stats
        .iter()
        .filter_map(|s| median_u64(&s.bbo_vs_best_us))
        .max();
    let worst_post = stats.iter().filter_map(|s| median_u64(&s.post_rtts_us)).max();
    let worst_ping = stats.iter().filter_map(|s| median_u64(&s.ping_rtts_us)).max();
    log::info!(
        "HL WS selection ({mode:?}): connected={connected_count}/{target_count}, winner=#{} \
         (bbo_n={}, vs_best_p50={}, post_p50={}, ping_p50={}, connect={:.1}ms) vs worst \
         (vs_best_p50={}, post_p50={}, ping_p50={})",
        win_cand.index,
        win_stat.bbo_count,
        fmt_opt_ms(median_u64(&win_stat.bbo_vs_best_us)),
        fmt_opt_ms(median_u64(&win_stat.post_rtts_us)),
        fmt_opt_ms(median_u64(&win_stat.ping_rtts_us)),
        win_cand.connect_us as f64 / 1000.0,
        fmt_opt_ms(worst_bbo),
        fmt_opt_ms(worst_post),
        fmt_opt_ms(worst_ping),
    );
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn pick_winner_execution_prefers_lowest_post_rtt() {
        let stats = vec![
            CandidateStats {
                active: true,
                post_rtts_us: vec![50_000, 52_000, 48_000],
                ping_rtts_us: vec![4_000, 5_000],
                connect_us: 10_000,
                ..CandidateStats::default()
            },
            CandidateStats {
                active: true,
                post_rtts_us: vec![6_000, 7_000, 5_500],
                ping_rtts_us: vec![5_000, 6_000],
                connect_us: 25_000,
                ..CandidateStats::default()
            },
            CandidateStats {
                active: false,
                post_rtts_us: vec![1_000],
                ..CandidateStats::default()
            },
        ];

        assert_eq!(pick_winner_slot(WsSelectMode::ExecutionPost, &stats), 1);
    }

    #[rstest]
    fn pick_winner_market_data_prefers_earliest_bbo_with_coverage() {
        let stats = vec![
            CandidateStats {
                active: true,
                bbo_count: 50,
                bbo_vs_best_us: vec![18_000; 40],
                post_rtts_us: vec![8_000; 3],
                ping_rtts_us: vec![4_000; 3],
                connect_us: 10_000,
                ..CandidateStats::default()
            },
            CandidateStats {
                active: true,
                bbo_count: 1,
                // Sparse 1-sample candidate must not beat candidates with full coverage.
                bbo_vs_best_us: vec![0],
                post_rtts_us: vec![50_000; 3],
                connect_us: 10_000,
                ..CandidateStats::default()
            },
            CandidateStats {
                active: true,
                bbo_count: 48,
                bbo_vs_best_us: vec![2_000; 38],
                post_rtts_us: vec![45_000; 3],
                ping_rtts_us: vec![5_000; 3],
                connect_us: 18_000,
                ..CandidateStats::default()
            },
        ];

        assert_eq!(pick_winner_slot(WsSelectMode::MarketData, &stats), 2);
    }

    #[rstest]
    fn parse_bbo_key_handles_official_and_simple_shapes() {
        let official = json!({
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1703875200123_u64,
                "bbo": [
                    {"px": "98450.0", "sz": "1.5", "n": 1},
                    {"px": "98451.0", "sz": "2.0", "n": 1}
                ]
            }
        });
        let (k1, t1) = parse_bbo_key(&official).unwrap();
        assert_eq!(t1, 1703875200123);
        assert_eq!(
            k1,
            (
                "BTC".to_string(),
                1703875200123,
                "98450.0".to_string(),
                "98451.0".to_string()
            )
        );
    }
}
