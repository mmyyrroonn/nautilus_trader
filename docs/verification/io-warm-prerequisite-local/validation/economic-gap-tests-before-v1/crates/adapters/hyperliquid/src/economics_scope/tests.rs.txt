// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Synthetic contract tests; no private venue observations.

use rstest::rstest;

use super::*;

fn policy(directory: &std::path::Path) -> IoEconomicsPolicy {
    IoEconomicsPolicy::parse(&json!({"schema_version":1,"checkpoint_path":directory.join("economics.json"),"instruments":["io:SNDK-USD-PERP.HYPERLIQUID"],"history_start_ms":0,"history_max_window_ms":86400000,"history_timeout_ms":2000,"history_max_pages":8,"history_max_records":2000,"max_observations":100,"max_receipts":100,"max_checkpoint_bytes":1048576}).to_string()).unwrap()
}

fn runtime(policy: &IoEconomicsPolicy) -> IoEconomicsRuntime {
    IoEconomicsRuntime::new(
        policy,
        AccountId::from("HYPERLIQUID-ENTROPY"),
        "user".into(),
        "Testnet".into(),
    )
    .unwrap()
}

fn proof() -> IoEconomicsScopeProof {
    IoEconomicsScopeProof {
        account: HyperliquidAccountScopeSnapshot {
            dex: "io".into(),
            address: "user".into(),
            account_mode: "standard_disabled_inferred".into(),
            collateral_token_id: USDC_TOKEN_ID.into(),
            balance: Decimal::from(100),
            equity: Decimal::from(100),
            withdrawable: Decimal::from(100),
            used: Decimal::ZERO,
            free: Decimal::from(100),
            total_maintenance: None,
            positions: Vec::new(),
            http_source_time_ms: 1000,
            http_received_time_ms: 1000,
            http_verification_started_time_ms: 1000,
            private_stream_epoch: 7,
            ws_received_time_ms: Some(1000),
            ws_source_time_ms: None,
            trusted: true,
            flat: Some(true),
            diagnostic: String::new(),
            provenance: "Synthetic only".into(),
        },
        attribution_verified: true,
        verified_instruments: BTreeSet::from(["io:SNDK-USD-PERP.HYPERLIQUID".into()]),
    }
}

fn funding(hash: &str, amount: Value) -> Value {
    json!({"time":1000,"hash":hash,"delta":{"type":"funding","coin":"io:SNDK","usdc":amount,"szi":"1","fundingRate":"0.0001","nSamples":null}})
}

fn history(runtime: &IoEconomicsRuntime, rows: Value) -> IoEconomicsObservationResult {
    runtime
        .observe_history(
            "userFunding",
            &rows.to_string(),
            0,
            2000,
            2,
            7,
            1000,
            Some(&proof()),
        )
        .unwrap()
}

fn snapshot(runtime: &IoEconomicsRuntime) -> Value {
    serde_json::from_str(&runtime.diagnostics().snapshot_json().unwrap().unwrap()).unwrap()
}
fn consume(runtime: &IoEconomicsRuntime) -> Value {
    serde_json::from_str(&runtime.diagnostics().persist_economics().unwrap().unwrap()).unwrap()
}

#[rstest]
#[case("max_observations",json!(0))]
#[case("history_timeout_ms",json!(30001))]
#[case("max_raw_frame_bytes",json!(1048577))]
#[case("history_max_window_ms",json!(86400001_u64))]
#[case("unexpected",json!(true))]
fn policy_rejects_unknown_or_unbounded_fields(#[case] field: &str, #[case] value: Value) {
    let directory = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(policy(directory.path())).unwrap();
    raw[field] = value;
    assert!(IoEconomicsPolicy::parse(&raw.to_string()).is_err());
}

#[rstest]
#[case("1.1234567890123456789012345678")]
#[case("-0.000001")]
#[case("0")]
fn exact_actual_funding_has_separate_source_and_consumer_checkpoints(#[case] amount: &str) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let observed = history(&runtime, json!([funding("hash1", json!(amount))]));
    assert!(observed.handled);
    assert_eq!(observed.observed, 1);
    assert_eq!(observed.unknown, 0);
    let before = snapshot(&runtime);
    assert_eq!(before["pending"], 1);
    assert_eq!(before["durable_receipts"], 0);
    assert_eq!(before["report"]["funding_usdc"], "0");
    let after = consume(&runtime);
    assert_eq!(after["report"]["funding_usdc"], amount);
    assert_eq!(after["durable_receipts"], 1);
    assert_eq!(after["pending"], 0);
    assert_eq!(
        consume(&runtime)["checkpoint_revision"],
        after["checkpoint_revision"]
    );
    assert_eq!(after["native_applied"], "Unknown");
    assert_eq!(after["balance_adjustment"], false);
}

#[rstest]
fn source_and_consumer_restart_boundaries_preserve_pending_and_exact_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let first = runtime(&policy);
    let diagnostics = first.diagnostics();
    history(&first, json!([funding("hash1", json!("2"))]));
    drop(first);
    assert!(diagnostics.snapshot_json().unwrap().is_none());
    let second = runtime(&policy);
    assert_eq!(snapshot(&second)["pending"], 1);
    assert_eq!(consume(&second)["report"]["funding_usdc"], "2");
    drop(second);
    let third = runtime(&policy);
    history(&third, json!([funding("hash1", json!("2"))]));
    let state = consume(&third);
    assert_eq!(state["report"]["funding_usdc"], "2");
    assert_eq!(state["durable_receipts"], 1);
    assert_eq!(state["consumed_observations"], 2);
}

#[rstest]
fn same_time_distinct_hashes_are_distinct_but_changed_payload_is_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    history(
        &runtime,
        json!([funding("hash1", json!("2")), funding("hash2", json!("2"))]),
    );
    assert_eq!(consume(&runtime)["report"]["funding_usdc"], "4");
    assert_eq!(
        history(&runtime, json!([funding("hash1", json!("3"))])).unknown,
        1
    );
    let state = consume(&runtime);
    assert_eq!(state["report"]["funding_usdc"], "4");
    assert_eq!(state["durable_receipts"], 2);
    assert_eq!(state["financial_identity_complete"], false);
}

#[rstest]
fn conflict_before_consumption_excludes_both_cash_candidates() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    history(
        &runtime,
        json!([funding("hash1", json!("2")), funding("hash1", json!("3"))]),
    );
    let state = consume(&runtime);
    assert_eq!(state["report"]["funding_usdc"], "0");
    assert_eq!(state["durable_receipts"], 0);
    assert_eq!(
        state["report"]["unknown_observations"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[rstest]
fn weak_ws_occurrences_and_history_overlap_do_not_create_duplicate_cash() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let raw=json!({"channel":"userFundings","data":{"user":"user","isSnapshot":true,"fundings":[{"time":1000,"coin":"io:SNDK","usdc":"2","szi":"1","fundingRate":"0.0001"}]}}).to_string();
    for sequence in [1, 2] {
        let result = runtime
            .observe_ws(&raw, 2, 7, sequence, 1000, Some(&proof()))
            .unwrap();
        assert_eq!(result.unknown, 1);
        assert!(result.attribution_complete);
    }
    history(&runtime, json!([funding("hash1", json!("2"))]));
    let state = consume(&runtime);
    assert_eq!(state["report"]["funding_usdc"], "2");
    assert_eq!(state["durable_receipts"], 1);
    assert_eq!(state["consumed_observations"], 3);
    assert_eq!(
        state["report"]["unknown_observations"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        state["observations"]["observation-1"]["source"]["generation"],
        2
    );
}

#[rstest]
fn numeric_literal_lexeme_is_preserved_and_excluded_from_exact_cash() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let raw = r#"[{"time":1000,"hash":"hash1","delta":{"type":"funding","coin":"io:SNDK","usdc":1.1234567890123456789012345678,"szi":"1","fundingRate":"0.0001"}}]"#;
    let result = runtime
        .observe_history("userFunding", raw, 0, 2000, 2, 7, 1000, Some(&proof()))
        .unwrap();
    assert_eq!(result.unknown, 1);
    assert!(!result.attribution_complete);
    let state = consume(&runtime);
    assert_eq!(state["report"]["funding_usdc"], "0");
    assert!(
        state["raw_envelopes"][state["observations"]["observation-1"]["raw_envelope_ref"]
            .as_str()
            .unwrap()]
        .as_str()
        .unwrap()
        .contains("1.1234567890123456789012345678")
    );
}

#[rstest]
#[case("coin",json!("xyz:SNDK"))]
#[case("usdc", Value::Null)]
#[case("fundingRate",json!("NaN"))]
fn unknown_required_identity_or_amount_is_raw_evidence(#[case] field: &str, #[case] value: Value) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let mut row = funding("hash1", json!("2"));
    row["delta"][field] = value;
    assert_eq!(history(&runtime, json!([row])).unknown, 1);
    assert_eq!(consume(&runtime)["durable_receipts"], 0);
}

#[rstest]
fn wrong_user_stream_and_unverified_metadata_cannot_recognize_funding() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let raw = json!({"channel":"userFundings","data":{"user":"wrong","fundings":[]}}).to_string();
    assert_eq!(
        runtime
            .observe_ws(&raw, 2, 7, 1, 1000, Some(&proof()))
            .unwrap()
            .unknown,
        1
    );
    let mut unverified = proof();
    unverified.verified_instruments.clear();
    let result = runtime
        .observe_history(
            "userFunding",
            &json!([funding("hash1", json!("2"))]).to_string(),
            0,
            2000,
            2,
            7,
            1000,
            Some(&unverified),
        )
        .unwrap();
    assert_eq!(result.unknown, 1);
    assert_eq!(consume(&runtime)["report"]["funding_usdc"], "0");
}

#[rstest]
fn account_wide_ledger_has_no_fake_io_attribution_and_same_hash_effects_are_ambiguous() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let raw=json!([{"time":1000,"hash":"tx","delta":{"type":"internalTransfer","usdc":"5","user":"user","destination":"other","fee":"1"}},{"time":1000,"hash":"tx","delta":{"type":"internalTransfer","usdc":"5","user":"user","destination":"another","fee":"1"}}]).to_string();
    runtime
        .observe_history(
            "userNonFundingLedgerUpdates",
            &raw,
            0,
            2000,
            2,
            7,
            1000,
            Some(&proof()),
        )
        .unwrap();
    let state = consume(&runtime);
    assert_eq!(state["durable_receipts"], 0);
    assert_eq!(
        state["observations"]["observation-1"]["fact"]["instrument_id"],
        Value::Null
    );
    assert_eq!(state["report"]["funding_usdc"], "0");
}

#[rstest]
fn raw_actual_fee_rebate_is_once_without_builder_double_count_or_native_application_claim() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let row = json!({"time":1000,"coin":"io:SNDK","oid":10,"tid":20,"hash":"fill","side":"B","px":"100","sz":"1","startPosition":"0","closedPnl":"0","fee":"-0.000001","feeToken":"USDC","builderFee":"0.0000001"});
    for _ in 0..2 {
        runtime
            .observe_history(
                "userFillsByTime",
                &json!([row.clone()]).to_string(),
                0,
                2000,
                2,
                7,
                1000,
                Some(&proof()),
            )
            .unwrap();
    }
    let state = consume(&runtime);
    assert_eq!(state["report"]["actual_fee_usdc"], "-0.000001");
    assert_eq!(state["durable_receipts"], 1);
    assert_eq!(state["native_applied"], "Unknown");
}

#[rstest]
fn verified_attribution_with_untrusted_funds_reports_fee_without_authorizing_native_application() {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let first = runtime(&policy);
    let mut attribution = proof();
    attribution.account.trusted = false;
    attribution.account.flat = None;
    attribution.account.diagnostic =
        "Validated private position changed; funds require refresh".into();
    let raw = json!({"channel":"user","data":{"fills":[fill_row()]}}).to_string();
    let result = first
        .observe_ws(&raw, 2, 7, 1, 1000, Some(&attribution))
        .unwrap();
    assert_eq!(result.unknown, 0);
    assert!(result.attribution_complete);
    let state = consume(&first);
    assert_eq!(state["report"]["actual_fee_usdc"], "-0.001");
    assert_eq!(state["native_applied"], "Unknown");
    assert_eq!(state["balance_adjustment"], false);
    let evidence = &state["observations"]["observation-1"]["scope_evidence"];
    assert_eq!(evidence["trusted"], false);
    assert_eq!(evidence["attribution_verified"], true);
    drop(first);
    let restarted = runtime(&policy);
    assert_eq!(snapshot(&restarted)["report"]["actual_fee_usdc"], "-0.001");
}

#[rstest]
#[case(true)]
#[case(false)]
fn unverified_attribution_cannot_recognize_fee_regardless_of_funds_trust(
    #[case] funds_trusted: bool,
) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let mut attribution = proof();
    attribution.attribution_verified = false;
    attribution.account.trusted = funds_trusted;
    let raw = json!({"channel":"user","data":{"fills":[fill_row()]}}).to_string();
    let result = runtime
        .observe_ws(&raw, 2, 7, 1, 1000, Some(&attribution))
        .unwrap();
    assert_eq!(result.unknown, 1);
    assert!(!result.attribution_complete);
    let state = consume(&runtime);
    assert_eq!(state["report"]["actual_fee_usdc"], "0");
    assert_eq!(state["durable_receipts"], 0);
    assert_eq!(state["native_applied"], "Unknown");
}

#[rstest]
#[case(json!({"channel":"userFundings","data":{"user":"user","isSnapshot":true,"fundings":[]}}))]
#[case(json!({"channel":"userFundings","data":{"user":"user","isSnapshot":"true","fundings":[]}}))]
#[case(json!({"channel":"userFundings","data":{"user":"user","fundings":[]},"extra":true}))]
fn empty_or_invalid_envelopes_do_not_grant_attribution(#[case] frame: Value) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let result = runtime
        .observe_ws(&frame.to_string(), 2, 7, 1, 1000, Some(&proof()))
        .unwrap();
    assert!(!result.attribution_complete);
    assert_eq!(consume(&runtime)["durable_receipts"], 0);
}

#[rstest]
fn checkpoint_exclusive_owner_and_missing_initialized_file_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let first = runtime(&policy);
    assert!(
        IoEconomicsRuntime::new(
            &policy,
            AccountId::from("HYPERLIQUID-ENTROPY"),
            "user".into(),
            "Testnet".into()
        )
        .is_err()
    );
    drop(first);
    std::fs::remove_file(&policy.checkpoint_path).unwrap();
    assert!(
        IoEconomicsRuntime::new(
            &policy,
            AccountId::from("HYPERLIQUID-ENTROPY"),
            "user".into(),
            "Testnet".into()
        )
        .is_err()
    );
}

#[rstest]
fn runtime_bound_preserves_prior_durable_facts_and_does_not_evict_pending() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = policy(directory.path());
    policy.max_observations = 1;
    let runtime = runtime(&policy);
    history(&runtime, json!([funding("hash1", json!("1"))]));
    assert!(
        runtime
            .observe_history(
                "userFunding",
                &json!([funding("hash2", json!("2"))]).to_string(),
                0,
                2000,
                2,
                7,
                1000,
                Some(&proof())
            )
            .is_err()
    );
    let state = snapshot(&runtime);
    assert_eq!(state["source_seq"], 1);
    assert_eq!(state["pending"], 1);
    assert_eq!(state["tainted"], true);
}

#[rstest]
fn semantic_checkpoint_corruption_is_not_validated_by_recomputed_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let runtime = runtime(&policy);
    history(&runtime, json!([funding("hash1", json!("2"))]));
    consume(&runtime);
    drop(runtime);
    let mut envelope: Value =
        serde_json::from_slice(&std::fs::read(&policy.checkpoint_path).unwrap()).unwrap();
    envelope["checkpoint"]["report"]["funding_usdc"] = json!("3");
    let checkpoint: Checkpoint = serde_json::from_value(envelope["checkpoint"].clone()).unwrap();
    envelope["checksum"] = json!(digest(&serde_json::to_vec(&checkpoint).unwrap()));
    std::fs::write(&policy.checkpoint_path, format!("{}\n", envelope)).unwrap();
    assert!(
        IoEconomicsRuntime::new(
            &policy,
            AccountId::from("HYPERLIQUID-ENTROPY"),
            "user".into(),
            "Testnet".into()
        )
        .is_err()
    );
}

#[rstest]
fn exact_sum_never_rounds_a_small_payment_into_a_large_total() {
    assert!(exact_add(Decimal::MAX, Decimal::from_str_exact("0.1").unwrap()).is_err());
    assert_eq!(
        exact_add(
            Decimal::from(1),
            Decimal::from_str_exact("0.000001").unwrap()
        )
        .unwrap()
        .to_string(),
        "1.000001"
    );
}

#[rstest]
fn complete_five_hundred_row_page_stores_one_shared_raw_envelope() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = policy(directory.path());
    policy.max_observations = 1000;
    policy.max_receipts = 1000;
    policy.max_checkpoint_bytes = 16777216;
    let runtime = runtime(&policy);
    let rows: Vec<_> = (0..500)
        .map(|index| funding(&format!("synthetic-hash-{index}"), json!("0.001")))
        .collect();
    let raw = serde_json::to_string(&rows).unwrap();
    runtime
        .observe_history("userFunding", &raw, 0, 2000, 2, 7, 1000, Some(&proof()))
        .unwrap();
    let state = snapshot(&runtime);
    assert_eq!(state["raw_envelopes"].as_object().unwrap().len(), 1);
    assert_eq!(state["observations"].as_object().unwrap().len(), 500);
    let reference = state["observations"]["observation-1"]["raw_envelope_ref"]
        .as_str()
        .unwrap();
    assert_eq!(state["raw_envelopes"][reference], raw);
    assert!(
        state["observations"]
            .as_object()
            .unwrap()
            .values()
            .all(|row| row.get("raw_frame_text").is_none() && row["raw_envelope_ref"] == reference)
    );
    assert!(std::fs::metadata(&policy.checkpoint_path).unwrap().len() < 2_000_000);
    assert_eq!(consume(&runtime)["report"]["funding_usdc"], "0.5");
}

#[rstest]
fn only_phase_a_exact_strong_history_replay_can_preserve_current_ready() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let old = json!([funding("hash1", json!("2"))]).to_string();
    assert!(runtime.history_requires_invalidation(
        "userFunding",
        &old,
        0,
        2000,
        2,
        7,
        1000,
        Some(&proof())
    ));
    runtime
        .observe_history("userFunding", &old, 0, 2000, 2, 7, 1000, Some(&proof()))
        .unwrap();
    let before = snapshot(&runtime);
    assert_eq!(before["durable_receipts"], 0);
    assert!(!runtime.history_requires_invalidation(
        "userFunding",
        &old,
        0,
        2000,
        2,
        7,
        1000,
        Some(&proof())
    ));
    assert_eq!(
        snapshot(&runtime)["checkpoint_revision"],
        before["checkpoint_revision"]
    );
    for raw in [
        json!([funding("hash2", json!("2"))]).to_string(),
        json!([funding("hash1", json!("3"))]).to_string(),
        json!([funding("", json!("2"))]).to_string(),
    ] {
        assert!(runtime.history_requires_invalidation(
            "userFunding",
            &raw,
            0,
            2000,
            2,
            7,
            1000,
            Some(&proof())
        ));
    }
    assert!(runtime.history_requires_invalidation(
        "userFunding",
        &old,
        0,
        2000,
        2,
        8,
        1000,
        Some(&proof())
    ));
    assert!(runtime.history_requires_invalidation("userFunding", &old, 0, 2000, 2, 7, 1000, None));
    assert!(!runtime.history_requires_invalidation("userFunding", "[]", 0, 2000, 2, 7, 1000, None));
}

#[rstest]
#[case("raw_amount")]
#[case("consumed_without_receipt")]
#[case("weak_to_strong")]
fn recomputed_checksum_cannot_override_raw_facts_or_consumer_receipt_completeness(
    #[case] corruption: &str,
) {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let runtime = runtime(&policy);
    if corruption == "weak_to_strong" {
        let raw=json!({"channel":"userFundings","data":{"user":"user","fundings":[{"time":1000,"coin":"io:SNDK","usdc":"2","szi":"1","fundingRate":"0.0001"}]}}).to_string();
        runtime
            .observe_ws(&raw, 2, 7, 1, 1000, Some(&proof()))
            .unwrap();
    } else {
        history(&runtime, json!([funding("hash1", json!("2"))]));
    }
    drop(runtime);
    let mut envelope: Value =
        serde_json::from_slice(&std::fs::read(&policy.checkpoint_path).unwrap()).unwrap();
    let mut checkpoint: Checkpoint =
        serde_json::from_value(envelope["checkpoint"].clone()).unwrap();
    let row = checkpoint.observations.get_mut("observation-1").unwrap();
    match corruption {
        "raw_amount" => {
            row.fact.amount = Some("3".into());
            row.financial_digest = digest(&serde_json::to_vec(&row.fact).unwrap());
        }
        "consumed_without_receipt" => {
            checkpoint
                .report
                .consumed_observations
                .insert("observation-1".into());
        }
        _ => {
            row.identity_quality = "EvidencedLocalComposite".into();
        }
    }
    envelope["checkpoint"] = serde_json::to_value(&checkpoint).unwrap();
    envelope["checksum"] = json!(digest(&serde_json::to_vec(&checkpoint).unwrap()));
    std::fs::write(&policy.checkpoint_path, format!("{}\n", envelope)).unwrap();
    assert!(
        IoEconomicsRuntime::new(
            &policy,
            AccountId::from("HYPERLIQUID-ENTROPY"),
            "user".into(),
            "Testnet".into()
        )
        .is_err()
    );
}

fn fill_row() -> Value {
    json!({"time":1000,"coin":"io:SNDK","oid":10,"tid":20,"hash":"fill","side":"B","px":"100","sz":"1","startPosition":"0","closedPnl":"0","fee":"-0.001","feeToken":"USDC","builderFee":"0.0001"})
}

#[rstest]
#[case("user")]
#[case("userEvents")]
fn exact_bound_user_fill_variant_reports_fee_without_fabricated_echo(#[case] channel: &str) {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let first = runtime(&policy);
    let raw = json!({"channel":channel,"data":{"fills":[fill_row()]}}).to_string();
    let outcome = first
        .observe_ws(&raw, 2, 7, 1, 1000, Some(&proof()))
        .unwrap();
    assert_eq!(outcome.unknown, 0);
    assert_eq!(outcome.observed, 1);
    assert_eq!(consume(&first)["report"]["actual_fee_usdc"], "-0.001");
    drop(first);
    let restarted = runtime(&policy);
    assert_eq!(snapshot(&restarted)["report"]["actual_fee_usdc"], "-0.001");
    assert_eq!(snapshot(&restarted)["native_applied"], "Unknown");
}

#[rstest]
#[case("mixed")]
#[case("numeric")]
#[case("duplicate")]
#[case("missing")]
fn unsupported_user_fill_variants_remain_raw_unknown(#[case] variant: &str) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(&policy(directory.path()));
    let mut row = fill_row();
    if variant == "numeric" {
        row["fee"] = json!(0.001);
    }
    if variant == "missing" {
        row.as_object_mut().unwrap().remove("feeToken");
    }
    let mut frame = json!({"channel":"user","data":{"fills":[row]}});
    if variant == "mixed" {
        frame["data"]["funding"] = json!({"usdc":"2"});
    }
    let mut raw = frame.to_string();
    if variant == "duplicate" {
        raw = raw.replace(
            "\"fee\":\"-0.001\"",
            "\"fee\":\"-0.001\",\"fee\":\"-0.002\"",
        );
    }
    assert_eq!(
        runtime
            .observe_ws(&raw, 2, 7, 1, 1000, Some(&proof()))
            .unwrap()
            .unknown,
        1
    );
    assert_eq!(consume(&runtime)["report"]["actual_fee_usdc"], "0");
}

#[rstest]
fn lifetime_coverage_capacity_preserves_last_checkpoint_when_exhausted() {
    let directory = tempfile::tempdir().unwrap();
    let policy = policy(directory.path());
    let runtime = runtime(&policy);
    let coverage = IoHistoryCoverage {
        generation: 2,
        epoch: 7,
        endpoint: "userFunding".into(),
        start_ms: 0,
        end_ms: 2000,
        pages: 1,
        records: 0,
        complete: false,
        diagnostic: "Synthetic empty observed window; retention unknown".into(),
    };
    for _ in 0..policy.history_max_pages * 3 {
        runtime.record_history_coverage(coverage.clone()).unwrap();
    }
    let before = std::fs::read(&policy.checkpoint_path).unwrap();
    let revision = snapshot(&runtime)["checkpoint_revision"].clone();
    assert!(runtime.record_history_coverage(coverage).is_err());
    assert_eq!(std::fs::read(&policy.checkpoint_path).unwrap(), before);
    assert_eq!(snapshot(&runtime)["checkpoint_revision"], revision);
    assert_eq!(snapshot(&runtime)["coverage_record_limit"], 24);
}
