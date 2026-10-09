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

//! Actual native reporting consumers against owned HTTP/WS peers; all values are synthetic.

use std::{fs, str::FromStr};

use super::*;

fn economics_policy(directory: &TempDir) -> Value {
    json!({"schema_version":1,
        "checkpoint_path":directory.path().join("economic-report.json"),
        "instruments":["io:SNDK-USD-PERP.HYPERLIQUID"],
        "history_start_ms":now_ms().saturating_sub(60000),
        "history_max_window_ms":86400000,"history_timeout_ms":3000,
        "history_max_pages":8,"history_max_records":2000,
        "max_observations":10000,"max_receipts":5000,
        "max_checkpoint_bytes":16777216,
        "max_raw_frame_bytes":65536,"max_history_body_bytes":1048576})
}

fn funding(hash: u64, amount: &str, time: u64) -> Value {
    json!({"time":time,"hash":format!("0x{hash:064x}"),"delta":{
        "type":"funding","coin":"io:SNDK","usdc":amount,
        "szi":"0.125","fundingRate":"0.0001","nSamples":1}})
}

async fn readonly_harness(fundings: Vec<Value>, ledger: Vec<Value>) -> Harness {
    Harness::with_configuration(
        "100",
        move |execution| {
            execution.funding_pages = VecDeque::from([json!(fundings).to_string()]);
            execution.ledger_pages = VecDeque::from([json!(ledger).to_string()]);
        },
        |config, directory| {
            config.io_execution_policy_json = None;
            config.io_economics_policy_json = Some(economics_policy(directory).to_string());
        },
    )
    .await
}

fn snapshot(harness: &Harness) -> Value {
    serde_json::from_str(
        &harness
            .factory
            .economics_scope_snapshot_json()
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

fn consume(harness: &Harness) -> Value {
    serde_json::from_str(&harness.factory.persist_economics().unwrap().unwrap()).unwrap()
}

fn observations(state: &Value) -> Vec<&Value> {
    state["observations"]
        .as_object()
        .unwrap()
        .values()
        .collect()
}

fn receipts(state: &Value) -> usize {
    state["report"]["receipts"].as_object().unwrap().len()
}

fn total(state: &Value, field: &str) -> Decimal {
    Decimal::from_str(state["report"][field].as_str().unwrap()).unwrap()
}

async fn wait_observations(harness: &mut Harness, minimum: usize) -> Value {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            harness.apply_events();
            let state = snapshot(harness);
            if observations(&state).len() >= minimum {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "missing actual economic observations: {}",
            snapshot(harness)
        )
    })
}

fn assert_no_actions(harness: &Harness) {
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    assert!(
        harness
            .cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
}

async fn restart_same_checkpoint(mut old: Harness) -> Harness {
    let policy = snapshot(&old)["policy"].to_string();
    old.stop().await;
    let mut config = execution_config(&old.peer, 30000);
    config.io_economics_policy_json = Some(policy);
    let Harness {
        peer,
        directory,
        factory,
        client,
        ..
    } = old;
    // Actual client destruction releases the exclusive checkpoint consumer lease.
    drop(client);
    assert!(factory.economics_scope_snapshot_json().unwrap().is_none());
    drop(factory);
    Harness::from_parts(peer, directory, config).await
}

async fn wait_coverage(harness: &mut Harness, minimum: usize) -> Value {
    let timeout_ms = snapshot(harness)["policy"]["history_timeout_ms"]
        .as_u64()
        .unwrap()
        + 2000;
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        loop {
            harness.apply_events();
            let state = snapshot(harness);
            if state["coverage"].as_array().unwrap().len() >= minimum {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "history did not reach bounded termination: {}",
            snapshot(harness)
        )
    })
}

#[tokio::test]
async fn normal_factory_restart_preserves_pending_then_same_consumer_receipts() {
    let mut first = readonly_harness(
        vec![funding(70, "0.75", now_ms().saturating_sub(500))],
        vec![],
    )
    .await;
    let source = wait_observations(&mut first, 1).await;
    wait_coverage(&mut first, 2).await;
    assert_eq!(receipts(&source), 0);
    let mut second = restart_same_checkpoint(first).await;
    let restored = snapshot(&second);
    assert_eq!(restored["consumer_id"], source["consumer_id"]);
    assert_eq!(receipts(&restored), 0);
    assert!(restored["pending"].as_u64().unwrap() >= 1);
    let consumed = consume(&second);
    assert_eq!(
        total(&consumed, "funding_usdc"),
        Decimal::from_str("0.75").unwrap()
    );
    assert_eq!(receipts(&consumed), 1);
    wait_coverage(&mut second, 6).await;
    consume(&second);
    let mut third = restart_same_checkpoint(second).await;
    let restored = snapshot(&third);
    assert_eq!(
        restored["report"]["receipts"],
        consumed["report"]["receipts"]
    );
    assert_eq!(restored["consumer_id"], source["consumer_id"]);
    wait_coverage(&mut third, 10).await;
    assert_eq!(
        total(&consume(&third), "funding_usdc"),
        Decimal::from_str("0.75").unwrap()
    );
    assert_eq!(receipts(&consume(&third)), 1);
    assert_no_actions(&third);
    third.stop().await;
}

#[tokio::test]
async fn actual_full_page_preserves_distinct_facts_at_the_inclusive_timestamp_boundary() {
    let first_time = now_ms().saturating_sub(2000);
    let first: Vec<_> = (0..500)
        .map(|i| funding(1000 + i, "0.001", first_time + i))
        .collect();
    let boundary = first_time + 499;
    let next = vec![first[499].clone(), funding(1500, "0.001", boundary)];
    let mut harness = Harness::with_configuration(
        "100",
        move |execution| {
            execution.funding_pages =
                VecDeque::from([json!(first).to_string(), json!(next).to_string()]);
        },
        |config, directory| {
            config.io_execution_policy_json = None;
            config.io_economics_policy_json = Some(economics_policy(directory).to_string());
        },
    )
    .await;
    let before = wait_coverage(&mut harness, 2).await;
    assert_eq!(observations(&before).len(), 502);
    assert!(
        before["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["complete"] == true)
    );
    let requests = harness.peer.state.data.lock().requests.clone();
    let requests: Vec<_> = requests
        .iter()
        .filter(|row| row["type"] == "userFunding")
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["startTime"], boundary);
    let after = consume(&harness);
    assert_eq!(receipts(&after), 501);
    assert_eq!(
        total(&after, "funding_usdc"),
        Decimal::from_str("0.501").unwrap()
    );
    assert!(
        fs::metadata(harness.directory.path().join("economic-report.json"))
            .unwrap()
            .len()
            < 3000000
    );
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn saturated_equal_time_page_terminates_with_unknown_coverage_and_preserves_observed_facts() {
    let time = now_ms().saturating_sub(500);
    let page: Vec<_> = (0..500).map(|i| funding(2000 + i, "0", time)).collect();
    let mut harness = readonly_harness(page, vec![]).await;
    let state = wait_coverage(&mut harness, 2).await;
    let funding_coverage = state["coverage"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["endpoint"] == "userFunding")
        .unwrap();
    assert_eq!(funding_coverage["complete"], false);
    assert!(
        funding_coverage["diagnostic"]
            .as_str()
            .unwrap()
            .contains("no progress")
    );
    assert_eq!(funding_coverage["pages"], 2);
    assert_eq!(observations(&state).len(), 1000);
    assert_eq!(receipts(&consume(&harness)), 500);
    assert_eq!(total(&consume(&harness), "funding_usdc"), Decimal::ZERO);
    assert_eq!(state["retention"], "Unknown");
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn conflicting_strong_history_never_becomes_recognized_cash() {
    let time = now_ms().saturating_sub(500);
    let mut harness =
        readonly_harness(vec![funding(80, "1", time), funding(80, "2", time)], vec![]).await;
    wait_observations(&mut harness, 2).await;
    let report = consume(&harness);
    assert_eq!(receipts(&report), 0);
    assert_eq!(total(&report, "funding_usdc"), Decimal::ZERO);
    assert!(
        observations(&report)
            .iter()
            .any(|row| row["identity_quality"] == "Conflict")
    );
    assert_eq!(
        report["report"]["unknown_observations"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn actual_history_deadline_preserves_unknown_coverage_and_cannot_enable_execution() {
    let mut harness = Harness::with_configuration(
        "100",
        |execution| {
            execution.history_delay_ms = 1000;
        },
        |config, directory| {
            config.io_execution_policy_json = None;
            let mut policy = economics_policy(directory);
            policy["history_timeout_ms"] = json!(100);
            config.io_economics_policy_json = Some(policy.to_string());
        },
    )
    .await;
    let state = wait_coverage(&mut harness, 2).await;
    assert!(
        state["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["complete"] == false)
    );
    assert!(
        state["coverage"][0]["diagnostic"]
            .as_str()
            .unwrap()
            .contains("deadline")
    );
    assert_eq!(receipts(&consume(&harness)), 0);
    assert_eq!(state["retention"], "Unknown");
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn normal_factory_signed_funding_is_pending_until_durable_report_consumption() {
    let time = now_ms().saturating_sub(500);
    let mut harness = readonly_harness(
        vec![
            funding(1, "1.000000000000000001", time),
            funding(2, "-0.000000000000000001", time),
            funding(3, "0", time),
        ],
        vec![],
    )
    .await;
    let before = wait_observations(&mut harness, 3).await;
    assert_eq!(receipts(&before), 0);
    assert_eq!(total(&before, "funding_usdc"), Decimal::ZERO);
    assert!(
        before["report"]["consumed_observations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(harness.factory.pending_economics_json().unwrap().is_some());
    assert!(observations(&before).iter().any(|row| {
        row["raw_text"]
            .as_str()
            .unwrap()
            .contains("1.000000000000000001")
    }));
    let after = consume(&harness);
    assert_eq!(total(&after, "funding_usdc"), Decimal::ONE);
    assert_eq!(receipts(&after), 3);
    assert_eq!(consume(&harness)["report"], after["report"]);
    assert_eq!(after["native_applied"], "Unknown");
    assert_eq!(after["balance_adjustment"], false);
    assert_eq!(after["native_cache_recovery"], false);
    let subscriptions = harness.peer.state.data.lock().subscriptions.clone();
    for channel in ["userFundings", "userNonFundingLedgerUpdates"] {
        assert!(
            subscriptions
                .iter()
                .any(|row| row["type"] == channel && row["user"] == USER)
        );
    }
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn history_same_time_distinct_hash_and_exact_replay_do_not_double_recognized_cash() {
    let time = now_ms().saturating_sub(500);
    let mut same = funding(10, "1.00", time);
    same["delta"]["fundingRate"] = json!("0.000100");
    let mut harness = readonly_harness(
        vec![funding(10, "1", time), same, funding(11, "1", time)],
        vec![],
    )
    .await;
    wait_observations(&mut harness, 3).await;
    let report = consume(&harness);
    assert_eq!(receipts(&report), 2);
    assert_eq!(total(&report, "funding_usdc"), Decimal::from(2));
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn websocket_no_id_replay_and_history_overlap_preserve_every_weak_occurrence() {
    let time = now_ms().saturating_sub(500);
    let mut harness = readonly_harness(vec![funding(20, "0.5", time)], vec![]).await;
    wait_observations(&mut harness, 1).await;
    let row =
        json!({"time":time,"coin":"io:SNDK","usdc":"0.5","szi":"0.125","fundingRate":"0.0001"});
    for snapshot_flag in [true, false] {
        harness.peer.state.instructions.send(PeerInstruction::Frame(json!({
            "channel":"userFundings","data":{"user":USER,"isSnapshot":snapshot_flag,"fundings":[row.clone()]}
        }))).unwrap();
    }
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(json!({
            "channel":"user","data":{"funding":row}
        })))
        .unwrap();
    let before = wait_observations(&mut harness, 4).await;
    assert_eq!(receipts(&before), 0);
    let after = consume(&harness);
    assert_eq!(
        total(&after, "funding_usdc"),
        Decimal::from_str("0.5").unwrap()
    );
    assert_eq!(receipts(&after), 1);
    assert_eq!(observations(&after).len(), 4);
    assert!(
        after["report"]["unknown_observations"]
            .as_array()
            .unwrap()
            .len()
            >= 3
    );
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn account_wide_ledger_keeps_attribution_and_cashflow_interpretation_unknown() {
    let time = now_ms().saturating_sub(500);
    let ledger = vec![
        json!({"time":time,"hash":"0xaaa","delta":{"type":"deposit","usdc":"3.125"}}),
        json!({"time":time,"hash":"0xbbb","delta":{"type":"withdraw","usdc":"2","fee":"0.25"}}),
        json!({"time":time,"hash":"0xccc","delta":{"type":"liquidation","accountValue":"999","leverageType":"isolated","liquidatedPositions":[]}}),
    ];
    let mut harness = readonly_harness(vec![], ledger).await;
    wait_observations(&mut harness, 3).await;
    let state = consume(&harness);
    for row in observations(&state) {
        assert!(row["fact"].get("dex").is_some());
        assert!(row["fact"]["instrument_id"].is_null());
        assert!(row["fact"]["dex"].is_null());
    }
    assert_eq!(total(&state, "funding_usdc"), Decimal::ZERO);
    assert_eq!(total(&state, "actual_fee_usdc"), Decimal::ZERO);
    assert_eq!(state["balance_adjustment"], false);
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn original_numeric_lexeme_is_preserved_without_fabricating_exact_cash() {
    let time = now_ms().saturating_sub(500);
    let body = format!(
        r#"[{{"time":{time},"hash":"0xnumeric","delta":{{"type":"funding","coin":"io:SNDK","usdc":0.1234567890123456789012345,"szi":"0.125","fundingRate":"0.0001","nSamples":1}}}}]"#
    );
    let mut harness = Harness::with_configuration(
        "100",
        move |execution| {
            execution.funding_pages = VecDeque::from([body]);
        },
        |config, directory| {
            config.io_execution_policy_json = None;
            config.io_economics_policy_json = Some(economics_policy(directory).to_string());
        },
    )
    .await;
    let before = wait_observations(&mut harness, 1).await;
    assert!(
        observations(&before)[0]["raw_text"]
            .as_str()
            .unwrap()
            .contains("0.1234567890123456789012345")
    );
    let report = consume(&harness);
    assert_eq!(total(&report, "funding_usdc"), Decimal::ZERO);
    assert!(
        !report["report"]["unknown_observations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_no_actions(&harness);
    harness.stop().await;
}

#[rstest]
#[case("wrong_user")]
#[case("foreign_coin")]
#[case("numeric_ws")]
#[tokio::test]
async fn unsupported_economic_frames_preserve_raw_and_cannot_create_io_income(#[case] fault: &str) {
    let mut harness = readonly_harness(vec![], vec![]).await;
    let time = now_ms();
    let user = if fault == "wrong_user" {
        "0x1111111111111111111111111111111111111111"
    } else {
        USER
    };
    let coin = if fault == "foreign_coin" {
        "xyz:SNDK"
    } else {
        "io:SNDK"
    };
    let amount = if fault == "numeric_ws" {
        "0.123456789012345678901"
    } else {
        r#""0.5""#
    };
    let text = format!(
        r#"{{"channel":"userFundings","data":{{"user":"{user}","isSnapshot":false,"fundings":[{{"time":{time},"coin":"{coin}","usdc":{amount},"szi":"0.1","fundingRate":"0.0001"}}]}}}}"#
    );
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::RawFrame(text.clone()))
        .unwrap();
    let before = wait_observations(&mut harness, 1).await;
    assert!(observations(&before).iter().any(|row| {
        let reference = row["raw_envelope_ref"].as_str().unwrap();
        before["raw_envelopes"][reference].as_str() == Some(text.as_str())
    }));
    let after = consume(&harness);
    assert_eq!(total(&after, "funding_usdc"), Decimal::ZERO);
    assert_eq!(receipts(&after), 0);
    assert_no_actions(&harness);
    harness.stop().await;
}

#[rstest]
#[case("numeric_fee")]
#[case("duplicate_fee")]
#[case("missing_coin")]
#[tokio::test]
async fn malformed_actual_user_fill_retains_raw_without_native_projection_or_fee_receipt(
    #[case] fault: &str,
) {
    let mut harness = Harness::with_configuration(
        "100",
        |_| {},
        |config, directory| {
            config.io_economics_policy_json = Some(economics_policy(directory).to_string());
        },
    )
    .await;
    wait_coverage(&mut harness, 2).await;
    harness.wait_ready().await;
    let order = harness.order(
        "IO-ECON-RAW-INVALID",
        OrderSide::Buy,
        "0.1",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let mut fill = harness.fill(0, 9999, "0.1", "99.75", "-0.001", "0");
    if fault == "missing_coin" {
        fill.as_object_mut().unwrap().remove("coin");
    }
    let text = json!({"channel":"user","data":{"fills":[fill]}}).to_string();
    let text = match fault {
        "numeric_fee" => text.replace(r#""fee":"-0.001""#, r#""fee":-0.001"#),
        "duplicate_fee" => text.replace(r#""fee":"-0.001""#, r#""fee":"-0.002","fee":"-0.001""#),
        _ => text,
    };
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::RawFrame(text.clone()))
        .unwrap();
    let before = wait_observations(&mut harness, 1).await;
    assert!(
        before["raw_envelopes"]
            .as_object()
            .unwrap()
            .values()
            .any(|raw| raw.as_str() == Some(text.as_str()))
    );
    let report = consume(&harness);
    harness.apply_events();
    assert_eq!(harness.fill_events, 0);
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .commissions()
            .is_empty()
    );
    assert_eq!(receipts(&report), 0);
    assert_eq!(total(&report, "actual_fee_usdc"), Decimal::ZERO);
    assert_eq!(harness.scope()["account"]["trusted"], false);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness.stop().await;
}

#[tokio::test]
async fn economic_consumer_failure_retains_durable_pending_source() {
    let mut harness =
        readonly_harness(vec![funding(30, "1", now_ms().saturating_sub(500))], vec![]).await;
    let before = wait_observations(&mut harness, 1).await;
    let checkpoint = harness.directory.path().join("economic-report.json");
    assert!(checkpoint.is_file());
    let preserved = fs::read(&checkpoint).unwrap();
    // An actual same-directory write fault, without a production fixture switch.
    let next_revision = before["checkpoint_revision"].as_u64().unwrap() + 1;
    let temporary = checkpoint.with_file_name(format!(
        "economic-report.json.pending-{}-{next_revision}",
        std::process::id()
    ));
    fs::create_dir(&temporary).unwrap();
    let result = harness.factory.persist_economics();
    assert!(
        result.is_err(),
        "consumer persisted despite its inaccessible temporary file"
    );
    assert_eq!(fs::read(&checkpoint).unwrap(), preserved);
    let failed = snapshot(&harness);
    assert_eq!(receipts(&failed), 0);
    assert_eq!(failed["report"], before["report"]);
    assert_no_actions(&harness);
    fs::remove_dir(&temporary).unwrap();
    harness.stop().await;
}

#[rstest]
#[case("mode")]
#[case("role")]
#[case("collateral")]
#[tokio::test]
async fn failed_full_account_revalidation_revokes_prior_economic_attribution(#[case] fault: &str) {
    let mut harness = readonly_harness(vec![], vec![]).await;
    wait_coverage(&mut harness, 2).await;
    {
        let mut data = harness.peer.state.data.lock();
        match fault {
            "mode" => data.mode = json!("unifiedAccount"),
            "role" => data.role = json!({"role":"agent","data":{"user":USER}}),
            "collateral" => data.token_id = "0x11111111111111111111111111111111".into(),
            _ => unreachable!(),
        }
    }
    harness
        .client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from("HYPERLIQUID")),
            AccountId::from("HYPERLIQUID-ENTROPY"),
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
            None,
            None,
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let state: Value = serde_json::from_str(
                &harness
                    .factory
                    .account_scope_snapshot_json()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            if state["diagnostic"]
                .as_str()
                .is_some_and(|message| message.contains("io HTTP account proof failed"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let time = now_ms();
    let fill = json!({"coin":"io:SNDK","px":"100","sz":"0.1","side":"B",
        "time":time,"startPosition":"0","dir":"Open Long","closedPnl":"0",
        "hash":"0x001122","oid":123,"crossed":true,"fee":"-0.001",
        "builderFee":"0.0002","tid":9911,"feeToken":"USDC","cloid":null});
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(json!({
            "channel":"user","data":{"fills":[fill]}
        })))
        .unwrap();
    let state = wait_observations(&mut harness, 1).await;
    assert!(
        observations(&state)
            .iter()
            .all(|row| row["fact"]["instrument_id"].is_null())
    );
    let report = consume(&harness);
    assert_eq!(receipts(&report), 0);
    assert_eq!(total(&report, "actual_fee_usdc"), Decimal::ZERO);
    assert_no_actions(&harness);
    harness.stop().await;
}

#[tokio::test]
async fn actual_owned_fill_fee_reporting_never_applies_a_second_native_commission() {
    let mut harness = Harness::with_configuration(
        "100",
        |_| {},
        |config, directory| {
            config.io_economics_policy_json = Some(economics_policy(directory).to_string());
        },
    )
    .await;
    wait_coverage(&mut harness, 2).await;
    harness.wait_ready().await;
    let order = harness.order(
        "IO-ECON-FEE",
        OrderSide::Buy,
        "0.1",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    harness.set_position("0.1");
    harness.send_fill(harness.fill(0, 9001, "0.1", "99.75", "-0.001", "0"));
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    wait_observations(&mut harness, 1).await;
    let commission = harness
        .cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .commissions()
        .clone();
    assert_eq!(harness.fill_events, 1);
    let state = consume(&harness);
    assert_eq!(
        total(&state, "actual_fee_usdc"),
        Decimal::from_str("-0.001").unwrap(),
        "Actual fee consumer state: {state}"
    );
    consume(&harness);
    harness.apply_events();
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .commissions(),
        &commission
    );
    assert_eq!(harness.fill_events, 1);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    assert_eq!(state["native_applied"], "Unknown");
    harness.stop().await;
}

#[tokio::test]
async fn persisted_funding_does_not_restore_ready_but_current_account_recovery_can_absorb_exact_replay()
 {
    let time = now_ms().saturating_sub(500);
    let mut harness = Harness::with_configuration(
        "100",
        move |execution| {
            execution.funding_pages =
                VecDeque::from([json!([funding(90, "0.25", time)]).to_string()]);
        },
        |config, directory| {
            let mut economic = economics_policy(directory);
            // This second complete recovery includes the production HTTP quota wait.
            economic["history_timeout_ms"] = json!(30000);
            config.io_economics_policy_json = Some(economic.to_string());
        },
    )
    .await;
    wait_coverage(&mut harness, 2).await;
    assert_eq!(harness.scope()["account"]["trusted"], false);
    let report = consume(&harness);
    assert_eq!(receipts(&report), 1);
    assert_eq!(harness.scope()["account"]["trusted"], false);
    let denied = harness.order(
        "IO-ECON-PENDING",
        OrderSide::Buy,
        "0.1",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&denied);
    harness
        .wait_status(denied.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    harness.refresh().await;
    wait_coverage(&mut harness, 4).await;
    assert_eq!(
        harness.scope()["account"]["trusted"],
        true,
        "Independent account state: {}; economic state: {}",
        harness.scope(),
        snapshot(&harness)
    );
    assert_eq!(
        total(&consume(&harness), "funding_usdc"),
        Decimal::from_str("0.25").unwrap()
    );
    assert_eq!(receipts(&consume(&harness)), 1);
    let admitted = harness.order(
        "IO-ECON-CURRENT",
        OrderSide::Buy,
        "0.1",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&admitted).unwrap();
    harness
        .wait_status(admitted.client_order_id(), OrderStatus::Accepted)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness.stop().await;
}
