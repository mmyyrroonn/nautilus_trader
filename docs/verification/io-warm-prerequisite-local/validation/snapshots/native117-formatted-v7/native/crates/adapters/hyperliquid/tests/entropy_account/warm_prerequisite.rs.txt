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

//! Original prerequisite provenance and new financial proofs through normal native sources.

use super::*;

const SELECTED: &str = "io:SNDK-USD-PERP.HYPERLIQUID";
const FACTS_MAX_BYTES: usize = 1024 * 1024;

fn configure(config: &mut HyperliquidExecutionClientConfig, directory: &TempDir) {
    config.account_snapshot_max_age_ms = 2000;
    let mut selected = policy(directory);
    selected["action_timeout_ms"] = json!(2000);
    selected["recovery_timeout_ms"] = json!(3000);
    selected["metadata_max_age_ms"] = json!(30000);
    config.io_execution_policy_json = Some(selected.to_string());
}

async fn normal() -> Harness {
    Harness::with_configuration("100", |peer| peer.record_http = true, configure).await
}

fn command() -> QueryAccount {
    QueryAccount::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from("HYPERLIQUID")),
        AccountId::from("HYPERLIQUID-ENTROPY"),
        UUID4::new(),
        UnixNanos::from(now_ms() * 1_000_000),
        None,
        None,
    )
}

fn issue(harness: &Harness) {
    harness.client.query_account(command()).unwrap();
    assert_eq!(harness.scope()["query_in_flight"], true);
    assert_eq!(harness.scope()["recovery_complete"], false);
}

async fn finished(harness: &Harness) -> Value {
    tokio::time::timeout(Duration::from_millis(3800), async {
        loop {
            harness.apply_time_events();
            let scope = harness.scope();
            if scope["query_in_flight"] == false {
                return scope;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "normal prerequisite query did not settle: {}",
            harness.scope()
        )
    })
}

fn requests_since(harness: &Harness, after: usize) -> Vec<Value> {
    harness.peer.state.data.lock().requests[after..].to_vec()
}

fn count(requests: &[Value], endpoint: &str) -> usize {
    requests
        .iter()
        .filter(|row| row["type"] == endpoint)
        .count()
}

fn source_cost(requests: &[Value]) -> u32 {
    requests
        .iter()
        .map(|row| match row["type"].as_str().unwrap() {
            "userRole" => 60,
            "clearinghouseState" | "orderStatus" => 2,
            _ => 20,
        })
        .sum()
}

fn override_response(harness: &Harness, endpoint: &str, delay: u64, raw: String) {
    harness
        .peer
        .state
        .data
        .lock()
        .execution
        .as_mut()
        .unwrap()
        .startup_responses
        .insert(endpoint.into(), (delay, raw));
}

async fn await_request(harness: &Harness, endpoint: &str, after: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if requests_since(harness, after)
                .iter()
                .any(|row| row["type"] == endpoint)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("normal query did not request actual {endpoint}"));
}

fn assert_actual_body(harness: &Harness, endpoint: &str, raw: &str, since: u64) {
    assert!(
        harness
            .peer
            .state
            .data
            .lock()
            .http_observations
            .iter()
            .any(|row| {
                row["request"]["type"] == endpoint
                    && row["started_ms"].as_u64().unwrap() >= since
                    && row["status"] == 200
                    && row["raw_body"] == raw
            }),
        "the bad source was not actually served at the requested boundary: {endpoint}"
    );
}

fn assert_no_prerequisite_reads(requests: &[Value]) {
    for endpoint in [
        "userRole",
        "userAbstraction",
        "userDexAbstraction",
        "meta",
        "spotMeta",
        "perpDexs",
    ] {
        assert_eq!(
            count(requests, endpoint),
            0,
            "reused source reread {endpoint}"
        );
    }
}

fn assert_full_account_reads(requests: &[Value]) {
    for (endpoint, expected) in [
        ("userRole", 1),
        ("userAbstraction", 2),
        ("userDexAbstraction", 2),
        ("meta", 1),
        ("spotMeta", 1),
        ("clearinghouseState", 1),
    ] {
        assert_eq!(
            count(requests, endpoint),
            expected,
            "missing complete {endpoint}"
        );
    }
}

fn assert_current(scope: &Value) {
    assert_eq!(scope["recovery_complete"], true, "{scope}");
    assert_eq!(scope["account"]["trusted"], true, "{scope}");
    assert_eq!(scope["query_in_flight"], false);
    assert_eq!(scope["native_projection_recovery_required"], false);
    assert_eq!(scope["journal_tainted"], false);
    assert!(scope["recovery_source_debt"].is_null());
}

fn assert_no_financial_projection(harness: &Harness) {
    assert_eq!(harness.fill_events, 0);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    assert!(
        harness
            .cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        harness
            .cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        harness
            .portfolio
            .net_position(&InstrumentId::from(SELECTED)),
        Decimal::ZERO
    );
}

async fn applied_frame(harness: &Harness, frame: Value) {
    let sequence = harness.scope()["startup_private_ingress"]["received_sequence"]
        .as_u64()
        .unwrap();
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(frame))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ingress = harness.scope()["startup_private_ingress"].clone();
            if ingress["received_sequence"].as_u64().unwrap() > sequence
                && ingress["is_applied"] == true
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual private frame did not reach the normal applied ingress");
}

async fn actual_fill(harness: &mut Harness, id: &str, close: bool, tid: u64) -> OrderAny {
    let order = harness.order(
        id,
        if close {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        "0.12",
        if close { "99" } else { "100" },
        close,
        TimeInForce::Ioc,
    );
    let post_index = harness.peer.state.writes.load(Ordering::SeqCst);
    harness.submit(&order).unwrap();
    harness.wait_posts(post_index + 1).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if harness.scope()["diagnostic"]
                .as_str()
                .unwrap_or("")
                .contains("RecoveryIncomplete")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("aggregate native ACK without actual trade must remain incomplete");
    let fill = harness.fill(
        post_index,
        tid,
        "0.12",
        if close { "99" } else { "100" },
        "0.001",
        if close { "0.12" } else { "0" },
    );
    harness.terminal(post_index, "filled");
    harness.set_position(if close { "0" } else { "0.12" });
    harness.send_fill(fill);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    harness.wait_latest_position_snapshot().await;
    let cached = harness
        .cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .clone();
    assert_eq!(cached.trade_ids().len(), 1);
    assert_eq!(
        cached.filled_qty().as_decimal(),
        Decimal::from_str_exact("0.12").unwrap()
    );
    assert_eq!(
        cached.commissions()[&Currency::USDC()].as_decimal(),
        Decimal::from_str_exact("0.001").unwrap()
    );
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        if close {
            Decimal::ZERO
        } else {
            Decimal::from_str_exact("0.12").unwrap()
        }
    );
    order
}

async fn journal_after_drop(mut harness: Harness) -> (TempDir, Value, usize) {
    if harness.client.is_connected() {
        harness.stop().await;
    }
    let Harness {
        client,
        factory,
        directory,
        ..
    } = harness;
    drop(client);
    drop(factory);
    let path = directory.path().join("io-intents.jsonl");
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.len() <= 16 * 1024 * 1024);
    let last = raw.lines().last().unwrap();
    assert!(last.len() <= FACTS_MAX_BYTES);
    let facts: Value = serde_json::from_str(last).unwrap();
    (directory, facts, last.len())
}

fn prerequisite(harness: &Harness) -> Value {
    let scope = harness.scope();
    let source = scope["warm_prerequisite_source"].clone();
    assert!(
        source.is_object(),
        "normal qualified transaction has no prerequisite: {scope}"
    );
    assert_eq!(source["account_id"], "HYPERLIQUID-ENTROPY");
    assert_eq!(source["address"], USER);
    assert_eq!(source["policy"], scope["policy"]);
    assert_eq!(
        source["generation"],
        scope["startup_private_ingress"]["generation"]
    );
    assert_eq!(source["epoch"], scope["startup_private_ingress"]["epoch"]);
    assert!(source["received_sequence"].as_u64().is_some());
    assert!(source["hard_revision"].as_u64().is_some());
    let started = source["verification_started_ms"].as_u64().unwrap();
    let finished = source["verification_finished_ms"].as_u64().unwrap();
    assert!(started <= finished && finished <= now_ms());
    assert_eq!(
        source["effective_expires_ms"].as_u64().unwrap(),
        started + 2000
    );
    assert_eq!(
        source["metadata_context"]["origin"],
        scope["warm_metadata_origin"]
    );
    assert!(source["metadata_context"]["origin"]["native_instruments"].is_object());
    assert!(source["native_http"][SELECTED].is_object());
    let group = &source["sources"];
    assert!(group["unavailable_sources"].as_array().unwrap().is_empty());
    let rows = group["original_sources"].as_array().unwrap();
    assert!(rows.len() >= 8);
    assert!(
        rows.iter()
            .map(|row| row["raw_text"].as_str().unwrap().len())
            .sum::<usize>()
            <= 4 * 1024 * 1024
    );
    for (endpoint, expected) in [
        ("userRole", 1),
        ("userAbstraction", 2),
        ("userDexAbstraction", 2),
        ("meta", 1),
        ("spotMeta", 1),
        ("clearinghouseState", 1),
    ] {
        assert_eq!(
            rows.iter().filter(|row| {
                serde_json::from_str::<Value>(row["request_json"].as_str().unwrap()).unwrap()["type"]
                    == endpoint
            }).count(),
            expected,
            "original source did not retain complete {endpoint}"
        );
    }
    let data = harness.peer.state.data.lock();
    for row in rows {
        let request: Value = serde_json::from_str(row["request_json"].as_str().unwrap()).unwrap();
        let received = row["received_ms"].as_u64().unwrap();
        if matches!(
            request["type"].as_str().unwrap(),
            "userRole"
                | "userAbstraction"
                | "userDexAbstraction"
                | "meta"
                | "spotMeta"
                | "clearinghouseState"
        ) {
            assert!(received >= started && received <= finished);
        }
        assert!(
            data.http_observations.iter().any(|actual| {
                actual["request"] == request
                    && actual["status"] == 200
                    && actual["raw_body"] == row["raw_text"]
                    && actual["completed_ms"].as_u64().unwrap() <= received
            }),
            "original source has no actual complete peer response: {row}"
        );
    }
    source
}

fn assert_composed_current(scope: &Value, old: &Value, financial_started: u64) {
    assert_current(scope);
    assert_eq!(scope["last_query_reused_prerequisite"], true);
    assert_eq!(scope["warm_prerequisite_source"], *old);
    assert_eq!(
        scope["account"]["http_verification_started_time_ms"],
        old["verification_started_ms"]
    );
    assert!(scope["account"]["http_received_time_ms"].as_u64().unwrap() >= financial_started);
    assert!(scope["account"]["http_source_time_ms"].as_u64().unwrap() >= financial_started);
    assert!(now_ms() < old["effective_expires_ms"].as_u64().unwrap());
}

fn assert_two_origins(harness: &Harness, old: &Value, token: &Value, new_started: u64) -> Value {
    let scope = harness.scope();
    let debt = scope["recovery_source_debt"].clone();
    assert_eq!(debt["kind"], "unknown_sources", "{scope}");
    assert_eq!(debt["reused_prerequisite_source"], *old);
    assert_eq!(debt["identity"]["query_token"], *token);
    assert_eq!(debt["identity"]["account_id"], "HYPERLIQUID-ENTROPY");
    assert_eq!(debt["identity"]["address"], USER);
    assert_eq!(debt["identity"]["generation"], old["generation"]);
    assert_eq!(debt["identity"]["epoch"], old["epoch"]);
    assert_eq!(debt["identity"]["policy"], old["policy"]);
    let rows = debt["original_sources"].as_array().unwrap();
    assert!(!rows.is_empty());
    let data = harness.peer.state.data.lock();
    for row in rows {
        let received = row["received_ms"].as_u64().unwrap();
        assert!(received >= new_started && received <= now_ms());
        let request: Value = serde_json::from_str(row["request_json"].as_str().unwrap()).unwrap();
        assert!(
            data.http_observations.iter().any(|actual| {
                actual["request"] == request
                    && actual["raw_body"] == row["raw_text"]
                    && actual["completed_ms"].as_u64().unwrap() <= received
            }),
            "new raw body was not actually received: {row}"
        );
    }
    for row in old["sources"]["original_sources"].as_array().unwrap() {
        assert!(row["received_ms"].as_u64().unwrap() <= new_started);
    }
    assert_eq!(scope["recovery_complete"], false);
    assert_eq!(scope["native_projection_recovery_required"], true);
    debt
}

#[tokio::test]
async fn qualified_normal_postbinding_recovery_retains_actual_original_eight_sources() {
    let mut harness = normal().await;
    let source = prerequisite(&harness);
    let rows = source["sources"]["original_sources"].as_array().unwrap();
    assert_eq!(rows.len(), 8);
    {
        let data = harness.peer.state.data.lock();
        let clears: Vec<_> = data
            .http_observations
            .iter()
            .filter(|row| {
                row["request"]["type"] == "clearinghouseState" && row["request"]["dex"] == "io"
            })
            .collect();
        assert!(
            clears.len() >= 2,
            "normal connect must include its early prebinding and recovery transactions"
        );
        let retained_clear = rows
        .iter()
        .find(|row| {
            serde_json::from_str::<Value>(row["request_json"].as_str().unwrap()).unwrap()["type"]
                == "clearinghouseState"
        })
        .unwrap();
        assert_eq!(
            retained_clear["raw_text"],
            clears.last().unwrap()["raw_body"]
        );
        assert!(
            source["verification_started_ms"].as_u64().unwrap()
                >= clears[0]["completed_ms"].as_u64().unwrap()
        );
    }
    assert_current(&harness.scope());
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn identical_applied_frame_revokes_startup_finance_but_not_original_prerequisites() {
    let mut harness = normal().await;
    let source = prerequisite(&harness);
    let metadata = harness.scope()["metadata"].clone();
    assert!(harness.scope()["startup_account_source"].is_object());
    applied_frame(&harness, harness.peer.state.clearinghouse_frame()).await;
    assert_eq!(harness.scope()["account"]["trusted"], true);
    assert_eq!(harness.scope()["warm_prerequisite_source"], source);
    let startup = harness.scope()["startup_account_source"].clone();
    if startup.is_object() {
        assert_ne!(
            startup["received_sequence"],
            harness.scope()["startup_private_ingress"]["received_sequence"]
        );
    }
    for _ in 0..2 {
        let after = harness.peer.state.data.lock().requests.len();
        let started = now_ms();
        issue(&harness);
        let result = finished(&harness).await;
        harness.apply_events();
        assert_composed_current(&result, &source, started);
        assert_eq!(result["metadata"], metadata);
        let requests = requests_since(&harness, after);
        assert_no_prerequisite_reads(&requests);
        assert_eq!(count(&requests, "frontendOpenOrders"), 1);
        assert_eq!(count(&requests, "userFills"), 1);
        assert_eq!(count(&requests, "clearinghouseState"), 1);
        assert_eq!(count(&requests, "activeAssetData"), 2);
        assert_eq!(source_cost(&requests), 82);
    }
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn actual_owned_entry_then_reduce_close_reuses_old_prerequisites_and_new_finances() {
    let mut harness = normal().await;
    let source = prerequisite(&harness);
    let metadata = harness.scope()["metadata"].clone();
    for (id, close, tid) in [
        ("E-PREREQ-ENTRY", false, 1801),
        ("E-PREREQ-CLOSE", true, 1802),
    ] {
        let order = actual_fill(&mut harness, id, close, tid).await;
        let before = harness.scope();
        assert_eq!(before["warm_prerequisite_source"], source);
        assert_eq!(before["recovery_complete"], false);
        let after = harness.peer.state.data.lock().requests.len();
        let started = now_ms();
        issue(&harness);
        let result = finished(&harness).await;
        harness.apply_events();
        assert_composed_current(&result, &source, started);
        assert_eq!(result["metadata"], metadata);
        assert_eq!(result["actual_fills"], before["actual_fills"]);
        assert_eq!(
            result["owned_intents"][order.client_order_id().as_str()]["phase"],
            "terminal"
        );
        assert_eq!(
            result["owned_intents"][order.client_order_id().as_str()]["reservation"],
            "0"
        );
        assert_eq!(result["account"]["flat"], close);
        let requests = requests_since(&harness, after);
        assert_no_prerequisite_reads(&requests);
        assert_eq!(count(&requests, "frontendOpenOrders"), 1);
        assert_eq!(count(&requests, "userFills"), 1);
        assert_eq!(count(&requests, "orderStatus"), 1);
        assert_eq!(count(&requests, "clearinghouseState"), 1);
        assert_eq!(
            count(&requests, "activeAssetData"),
            if close { 2 } else { 0 }
        );
        assert_eq!(source_cost(&requests), if close { 84 } else { 44 });
        let cached = harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .clone();
        assert_eq!(cached.status(), OrderStatus::Filled);
        assert_eq!(cached.trade_ids().len(), 1);
        assert_eq!(
            cached.commissions()[&Currency::USDC()].as_decimal(),
            Decimal::from_str_exact("0.001").unwrap()
        );
    }
    assert_eq!(harness.fill_events, 2);
    assert_eq!(
        harness.scope()["actual_fills"].as_object().unwrap().len(),
        2
    );
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 2);
    assert_eq!(
        harness
            .portfolio
            .net_position(&InstrumentId::from(SELECTED)),
        Decimal::ZERO
    );
    harness.stop().await;
}

async fn crossed_connect() -> Harness {
    let peer = Peer::start(clearinghouse("100", "100", "0", "100", false)).await;
    let state = peer.state.clone();
    state.data.lock().execution = Some(PeerExecution {
        record_http: true,
        ..PeerExecution::default()
    });
    state.data.lock().io_response_delay_ms = 200;
    let directory = TempDir::new().unwrap();
    let mut config = execution_config(&peer, 2000);
    configure(&mut config, &directory);
    let crossing = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let requests = state.data.lock().requests.clone();
                if count(&requests, "clearinghouseState") >= 2
                    && count(&requests, "activeAssetData") >= 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("normal recovery did not reach the postbinding full account transaction");
        state
            .instructions
            .send(PeerInstruction::Frame(state.clearinghouse_frame()))
            .unwrap();
    };
    let (harness, ()) = tokio::join!(Harness::from_parts(peer, directory, config), crossing);
    harness.peer.state.data.lock().io_response_delay_ms = 0;
    assert_eq!(harness.scope()["account"]["trusted"], true);
    assert_eq!(harness.scope()["recovery_complete"], true);
    assert!(harness.scope()["warm_prerequisite_source"].is_null());
    assert!(harness.scope()["startup_account_source"].is_null());
    harness
}

#[tokio::test]
async fn crossed_original_transaction_cannot_be_retrosealed_and_requires_actual_full_warm() {
    let mut harness = crossed_connect().await;
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let result = finished(&harness).await;
    harness.apply_events();
    assert_current(&result);
    assert_eq!(result["last_query_reused_prerequisite"], false);
    assert_full_account_reads(&requests_since(&harness, after));
    let source = prerequisite(&harness);
    assert!(source["verification_started_ms"].as_u64().unwrap() >= started);
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

async fn expire_original_source(harness: &Harness, source: &Value) {
    let expires = source["effective_expires_ms"].as_u64().unwrap();
    let current = now_ms();
    if current <= expires {
        tokio::time::sleep(Duration::from_millis(expires - current + 20)).await;
    }
    assert!(now_ms() > expires);
    assert_eq!(harness.scope()["warm_prerequisite_source"], *source);
}

async fn fresh_current_private(harness: &Harness, old: &Value) {
    harness.peer.state.data.lock().io["time"] = json!(now_ms());
    applied_frame(harness, harness.peer.state.clearinghouse_frame()).await;
    assert_eq!(harness.scope()["warm_prerequisite_source"], *old);
}

#[tokio::test]
async fn original_expiry_requires_new_complete_sources_and_successful_owner_before_replacement() {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    expire_original_source(&harness, &old).await;
    fresh_current_private(&harness, &old).await;
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let result = finished(&harness).await;
    harness.apply_events();
    assert_current(&result);
    assert_eq!(result["last_query_reused_prerequisite"], false);
    assert_full_account_reads(&requests_since(&harness, after));
    let new = prerequisite(&harness);
    assert_ne!(new, old);
    assert!(new["verification_started_ms"].as_u64().unwrap() >= started);
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[rstest]
#[case("userRole", "{\"role\":\"vault\"}")]
#[case("userAbstraction", "\"unifiedAccount\"")]
#[case("userDexAbstraction", "true")]
#[case("meta", "{\"universe\":[],\"collateralToken\":1}")]
#[case("spotMeta", "{\"universe\":[],\"tokens\":[]}")]
#[tokio::test]
async fn genuinely_expired_prerequisite_fallback_receives_bad_actual_target_before_refusal(
    #[case] endpoint: &str,
    #[case] raw: &str,
) {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    expire_original_source(&harness, &old).await;
    fresh_current_private(&harness, &old).await;
    override_response(&harness, endpoint, 0, raw.into());
    let started = now_ms();
    issue(&harness);
    let result = finished(&harness).await;
    harness.apply_events();
    assert_actual_body(&harness, endpoint, raw, started);
    assert_eq!(result["recovery_complete"], false);
    assert_eq!(result["account"]["trusted"], false);
    assert_eq!(result["last_query_reused_prerequisite"], false);
    assert!(
        result["warm_prerequisite_source"].is_null() || result["warm_prerequisite_source"] == old
    );
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[rstest]
#[case("frontendOpenOrders", "null")]
#[case("userFills", "[")]
#[case("clearinghouseState", "{}")]
#[case(
    "activeAssetData",
    "{\"user\":\"0x1111111111111111111111111111111111111111\",\"coin\":\"io:SNDK\",\"leverage\":{\"type\":\"isolated\",\"value\":1}}"
)]
#[tokio::test]
async fn original_nonfinancial_source_cannot_replace_any_missing_new_actual_financial_source(
    #[case] endpoint: &str,
    #[case] raw: &str,
) {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    override_response(&harness, endpoint, 0, raw.into());
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    let result = finished(&harness).await;
    harness.apply_events();
    assert_actual_body(&harness, endpoint, raw, started);
    assert_no_prerequisite_reads(&requests_since(&harness, after));
    assert_eq!(result["recovery_complete"], false);
    assert_eq!(result["account"]["trusted"], false);
    let debt = assert_two_origins(&harness, &old, &token, started);
    assert_no_financial_projection(&harness);
    let (_directory, persisted, _) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
}

#[rstest]
#[case("withdrawable")]
#[case("raw_balance")]
#[case("used_margin")]
#[case("cross_maintenance")]
#[case("money_precision")]
#[case("stale_time")]
#[tokio::test]
async fn complete_new_http_finances_must_match_private_facts_precision_and_source_age(
    #[case] fault: &str,
) {
    let mut harness = normal().await;
    harness.apply_events();
    let old = prerequisite(&harness);
    let mut body = clearinghouse("100", "100", "0", "100", false);
    body["time"] = json!(now_ms());
    match fault {
        "withdrawable" => body["withdrawable"] = json!("99"),
        "raw_balance" => body["marginSummary"]["totalRawUsd"] = json!("99"),
        "used_margin" => body["marginSummary"]["totalMarginUsed"] = json!("1"),
        "cross_maintenance" => body["crossMaintenanceMarginUsed"] = json!("0.01"),
        "money_precision" => body["marginSummary"]["accountValue"] = json!("100.000000001"),
        "stale_time" => body["time"] = json!(now_ms() - 2001),
        _ => unreachable!(),
    }
    let raw = body.to_string();
    override_response(&harness, "clearinghouseState", 0, raw.clone());
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    let result = finished(&harness).await;
    assert_actual_body(&harness, "clearinghouseState", &raw, started);
    assert_no_prerequisite_reads(&requests_since(&harness, after));
    assert_eq!(result["recovery_complete"], false, "{fault}: {result}");
    assert_two_origins(&harness, &old, &token, started);
    while let Ok(event) = harness.receiver.try_recv() {
        assert!(
            !matches!(&event, ExecutionEvent::Account(_)),
            "invalid new finances emitted AccountState"
        );
        harness.apply_event(event);
    }
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[rstest]
#[case("missing_fill")]
#[case("missing_fee")]
#[case("conflicting_fee")]
#[case("foreign_fill")]
#[case("unknown_status")]
#[tokio::test]
async fn composed_query_must_confirm_every_actual_owned_trade_and_exact_commission(
    #[case] fault: &str,
) {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    let order = actual_fill(&mut harness, "E-PREREQ-FACT", false, 1810).await;
    let before = harness.scope();
    let reservation =
        before["owned_intents"][order.client_order_id().as_str()]["reservation"].clone();
    let mut fills = harness
        .peer
        .state
        .data
        .lock()
        .execution
        .as_ref()
        .unwrap()
        .fills
        .clone();
    let endpoint = if fault == "unknown_status" {
        "orderStatus"
    } else {
        "userFills"
    };
    let raw = match fault {
        "missing_fill" => "[]".to_string(),
        "missing_fee" => {
            fills[0].as_object_mut().unwrap().remove("fee");
            json!(fills).to_string()
        }
        "conflicting_fee" => {
            fills[0]["fee"] = json!("0.002");
            json!(fills).to_string()
        }
        "foreign_fill" => {
            fills[0]["oid"] = json!(999999);
            fills[0]["tid"] = json!(999998);
            fills[0]["cloid"] = json!("0xffffffffffffffffffffffffffffffff");
            json!(fills).to_string()
        }
        "unknown_status" => "{\"status\":\"unknownOid\"}".to_string(),
        _ => unreachable!(),
    };
    override_response(&harness, endpoint, 0, raw.clone());
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    let result = finished(&harness).await;
    harness.apply_events();
    assert_actual_body(&harness, endpoint, &raw, started);
    assert_eq!(result["recovery_complete"], false);
    assert_eq!(result["actual_fills"], before["actual_fills"]);
    assert_eq!(
        result["owned_intents"][order.client_order_id().as_str()]["reservation"],
        reservation
    );
    assert_two_origins(&harness, &old, &token, started);
    assert_eq!(harness.fill_events, 1);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    let cached = harness
        .cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .clone();
    assert_eq!(cached.trade_ids().len(), 1);
    assert_eq!(
        cached.commissions()[&Currency::USDC()].as_decimal(),
        Decimal::from_str_exact("0.001").unwrap()
    );
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.12").unwrap()
    );
    harness.stop().await;
}

#[rstest]
#[case("identical")]
#[case("funds")]
#[case("reader_close")]
#[case("unsupported_raw")]
#[case("engine_metadata")]
#[tokio::test]
async fn retained_prerequisites_cannot_authorize_crossed_new_financial_source_or_reader(
    #[case] fault: &str,
) {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    override_response(&harness, "userFills", 180, "[]".into());
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    await_request(&harness, "userFills", after).await;
    match fault {
        "identical" => applied_frame(&harness, harness.peer.state.clearinghouse_frame()).await,
        "funds" => {
            let mut changed = clearinghouse("99", "99", "0", "99", false);
            changed["time"] = json!(now_ms());
            harness.peer.state.data.lock().io = changed;
            applied_frame(&harness, harness.peer.state.clearinghouse_frame()).await;
        }
        "reader_close" => {
            harness
                .peer
                .state
                .instructions
                .send(PeerInstruction::Close)
                .unwrap();
        }
        "unsupported_raw" => {
            harness
                .peer
                .state
                .instructions
                .send(PeerInstruction::RawFrame(
                    "{\"channel\":\"user\",\"data\":{\"funding\":{}}}".into(),
                ))
                .unwrap();
        }
        "engine_metadata" => {
            let original = harness.peer.state.data.lock().meta_override.clone();
            harness.peer.state.data.lock().meta_override = Some(
                json!({"universe":[{"name":"io:SNDK",
                "szDecimals":3,"maxLeverage":10,"onlyIsolated":true,"marginMode":"strictIsolated"}],"collateralToken":0}),
            );
            let mut provider =
                HyperliquidHttpClient::new(HyperliquidEnvironment::Testnet, 2, None).unwrap();
            provider.set_base_info_url(format!("http://{}/info", harness.peer.addr));
            for instrument in provider.request_instruments().await.unwrap() {
                harness
                    .cache
                    .borrow_mut()
                    .add_instrument(instrument)
                    .unwrap();
            }
            harness.peer.state.data.lock().meta_override = original;
        }
        _ => unreachable!(),
    }
    let result = finished(&harness).await;
    harness.apply_events();
    assert_eq!(result["recovery_complete"], false, "{fault}: {result}");
    let debt = assert_two_origins(&harness, &old, &token, started);
    assert_no_financial_projection(&harness);
    let requests = harness.peer.state.data.lock().requests.len();
    assert!(harness.client.query_account(command()).is_err());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(harness.peer.state.data.lock().requests.len(), requests);
    assert_eq!(harness.scope()["recovery_source_debt"], debt);
    let (_directory, persisted, _) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
}

#[tokio::test]
async fn old_prerequisite_expiring_on_owner_channel_cannot_commit_new_account_state() {
    let mut harness = normal().await;
    harness.apply_events();
    let old = prerequisite(&harness);
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.time_receiver.borrow().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("normal owner callback was not staged");
    expire_original_source(&harness, &old).await;
    harness.apply_time_events();
    let result = finished(&harness).await;
    assert_eq!(result["recovery_complete"], false);
    let debt = assert_two_origins(&harness, &old, &token, started);
    while let Ok(event) = harness.receiver.try_recv() {
        assert!(
            !matches!(&event, ExecutionEvent::Account(_)),
            "expired owner emitted AccountState"
        );
        harness.apply_event(event);
    }
    assert_no_financial_projection(&harness);
    let (_directory, persisted, _) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
}

#[tokio::test]
async fn composed_source_timeout_keeps_original_deadline_and_both_provenance_origins() {
    let harness = normal().await;
    let old = prerequisite(&harness);
    override_response(&harness, "frontendOpenOrders", 1500, "[]".into());
    override_response(&harness, "userFills", 3500, "[]".into());
    let started = now_ms();
    let elapsed = tokio::time::Instant::now();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    let result = finished(&harness).await;
    eprintln!(
        "original total deadline fixture: {}",
        json!({
            "elapsed_ms": elapsed.elapsed().as_millis(),
            "http_timeout_secs": 2,
            "frontend_open_orders_delay_ms": 1500,
            "user_fills_delay_ms": 3500,
            "query_started_ms": started,
            "scope": result,
            "http_observations": harness.peer.state.data.lock().http_observations,
        }),
    );
    assert!(elapsed.elapsed() >= Duration::from_millis(2800));
    assert!(elapsed.elapsed() < Duration::from_millis(3800));
    assert_eq!(result["recovery_complete"], false);
    let debt = assert_two_origins(&harness, &old, &token, started);
    assert!(debt["unavailable_sources"].as_array().unwrap().iter().any(|row| {
        serde_json::from_str::<Value>(row["request_json"].as_str().unwrap()).unwrap()["type"]
            == "userFills" && row["received_ms"].is_null()
    }));
    assert_no_financial_projection(&harness);
    let (_directory, persisted, _) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
}

#[rstest]
#[case(300_000, false)]
#[case(400_000, true)]
#[tokio::test]
async fn escaped_old_and_new_raw_use_actual_record_bound_and_survive_normal_drop(
    #[case] padding: usize,
    #[case] exceeds_record: bool,
) {
    let old_raw = format!("{}{{\"role\":\"user\"}}", " ".repeat(padding));
    let mut harness = Harness::with_configuration(
        "100",
        |peer| {
            peer.record_http = true;
            peer.startup_responses
                .insert("userRole".into(), (0, old_raw.clone()));
        },
        configure,
    )
    .await;
    let old = prerequisite(&harness);
    assert!(
        old["sources"]["original_sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| { row["raw_text"] == old_raw })
    );
    let new_raw = format!("{}[]", "\n".repeat(padding));
    assert!(old_raw.len() < FACTS_MAX_BYTES && new_raw.len() < FACTS_MAX_BYTES);
    override_response(&harness, "frontendOpenOrders", 0, new_raw.clone());
    override_response(&harness, "userFills", 0, "[".into());
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    let result = finished(&harness).await;
    harness.apply_events();
    assert_actual_body(&harness, "frontendOpenOrders", &new_raw, started);
    assert_actual_body(&harness, "userFills", "[", started);
    assert_eq!(result["recovery_complete"], false);
    assert_eq!(result["native_projection_recovery_required"], true);
    assert_eq!(
        result["journal_tainted"], false,
        "bounded persisted refusal is not an I/O failure"
    );
    let debt = if exceeds_record {
        let debt = result["recovery_source_debt"].clone();
        assert_eq!(debt["kind"], "retention_failed");
        assert_eq!(debt["identity"]["query_token"], token);
        assert!(debt["diagnostic"].as_str().unwrap().contains("retained"));
        debt
    } else {
        let debt = assert_two_origins(&harness, &old, &token, started);
        assert!(
            debt["original_sources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["raw_text"] == new_raw)
        );
        debt
    };
    assert_no_financial_projection(&harness);
    let (_directory, persisted, bytes) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
    if exceeds_record {
        assert!(
            bytes < 64 * 1024,
            "bounded refusal still retained an oversized raw record"
        );
    } else {
        assert!(
            bytes > 850_000,
            "fixture did not exercise the actual escaped near-bound record"
        );
        assert_eq!(
            persisted["recovery_source_debt"]["reused_prerequisite_source"],
            old
        );
    }
}

#[tokio::test]
async fn silent_unrequested_mode_change_is_only_previous_bounded_evidence_not_a_new_mode_read() {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    harness.peer.state.data.lock().mode = json!("unifiedAccount");
    let after = harness.peer.state.data.lock().requests.len();
    let started = now_ms();
    issue(&harness);
    let result = finished(&harness).await;
    harness.apply_events();
    assert_composed_current(&result, &old, started);
    assert_no_prerequisite_reads(&requests_since(&harness, after));
    assert!(old["sources"]["original_sources"].as_array().unwrap().iter().any(|row| {
        serde_json::from_str::<Value>(row["request_json"].as_str().unwrap()).unwrap()["type"]
            == "userAbstraction" && row["raw_text"] == "\"disabled\""
    }));
    assert_no_financial_projection(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn actual_owned_fill_not_consumed_by_engine_cannot_borrow_original_prerequisites() {
    let mut harness = normal().await;
    let old = prerequisite(&harness);
    let order = harness.order(
        "E-PREREQ-UNCONSUMED",
        OrderSide::Buy,
        "0.12",
        "100",
        false,
        TimeInForce::Ioc,
    );
    harness.submit(&order).unwrap();
    harness.wait_posts(1).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !harness.scope()["diagnostic"]
            .as_str()
            .unwrap_or("")
            .contains("RecoveryIncomplete")
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("aggregate marker recovery did not settle without actual fills");
    let fill = harness.fill(0, 1820, "0.12", "100", "0.001", "0");
    harness.terminal(0, "filled");
    harness.set_position("0.12");
    harness.send_fill(fill);
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.scope()["actual_fills"].as_object().unwrap().len() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("normal actual fill was not retained natively");
    harness.wait_latest_position_snapshot().await;
    assert_eq!(harness.scope()["warm_prerequisite_source"], old);
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::ZERO
    );
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    let reservation =
        harness.scope()["owned_intents"][order.client_order_id().as_str()]["reservation"].clone();
    let requests = harness.peer.state.data.lock().requests.len();
    assert!(harness.client.query_account(command()).is_err());
    assert_eq!(harness.peer.state.data.lock().requests.len(), requests);
    assert_eq!(harness.scope()["recovery_complete"], false);
    assert_eq!(
        harness.scope()["owned_intents"][order.client_order_id().as_str()]["reservation"],
        reservation
    );
    assert_eq!(harness.fill_events, 0);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    assert_eq!(harness.fill_events, 1);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.12").unwrap()
    );
    harness.stop().await;
}

#[tokio::test]
async fn normal_stop_releases_queued_owner_and_preserves_two_origin_raw_without_housekeeping() {
    let mut harness = normal().await;
    harness.apply_events();
    let old = prerequisite(&harness);
    let started = now_ms();
    issue(&harness);
    let token = harness.scope()["query_token"].clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.time_receiver.borrow().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("normal completed sources did not stage owner work");
    harness.stop().await;
    let debt = assert_two_origins(&harness, &old, &token, started);
    harness.apply_time_events();
    while let Ok(event) = harness.receiver.try_recv() {
        assert!(
            !matches!(&event, ExecutionEvent::Account(_)),
            "stopped late owner emitted an account"
        );
        harness.apply_event(event);
    }
    assert_no_financial_projection(&harness);
    let (_directory, persisted, _) = journal_after_drop(harness).await;
    assert_eq!(persisted["recovery_source_debt"], debt);
}
