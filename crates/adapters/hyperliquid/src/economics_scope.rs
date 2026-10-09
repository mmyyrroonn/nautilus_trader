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

//! Bounded economic observations and a durable native report consumer.
//!
//! This report never changes balances or proves native fill/cache application.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Weak},
};

use anyhow::Context;
use nautilus_model::identifiers::AccountId;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::account_scope::HyperliquidAccountScopeSnapshot;

mod parsing;
mod store;
#[cfg(test)]
mod tests;

const USDC_TOKEN_ID: &str = "0x6d1e7cde53ba9467b783cb7c530ce054";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IoEconomicsPolicy {
    pub(crate) schema_version: u32,
    pub(crate) checkpoint_path: PathBuf,
    pub(crate) instruments: Vec<String>,
    pub(crate) history_start_ms: u64,
    pub(crate) history_max_window_ms: u64,
    pub(crate) history_timeout_ms: u64,
    pub(crate) history_max_pages: usize,
    pub(crate) history_max_records: usize,
    pub(crate) max_observations: usize,
    pub(crate) max_receipts: usize,
    pub(crate) max_checkpoint_bytes: usize,
    #[serde(default = "default_raw_frame_bytes")]
    pub(crate) max_raw_frame_bytes: usize,
    #[serde(default = "default_history_body_bytes")]
    pub(crate) max_history_body_bytes: usize,
}

fn default_raw_frame_bytes() -> usize {
    65536
}
fn default_history_body_bytes() -> usize {
    1048576
}

impl IoEconomicsPolicy {
    pub(crate) fn parse(raw: &str) -> anyhow::Result<Self> {
        let policy: Self = serde_json::from_str(raw)?;
        anyhow::ensure!(
            policy.schema_version == 1 && policy.checkpoint_path.is_absolute(),
            "io economics requires schema 1 and absolute checkpoint path"
        );
        anyhow::ensure!(
            !policy.checkpoint_path.components().any(|part| part
                .as_os_str()
                .to_string_lossy()
                .to_ascii_lowercase()
                .starts_with(".env")),
            "io economics checkpoint cannot reference credentials"
        );
        anyhow::ensure!(
            (1..=32).contains(&policy.instruments.len()),
            "io economics instrument count outside bounds"
        );
        let mut unique = BTreeSet::new();
        for instrument in &policy.instruments {
            anyhow::ensure!(
                instrument_coin(instrument).is_some() && unique.insert(instrument),
                "io economics requires unique full io native instruments"
            );
        }
        anyhow::ensure!(
            (1..=86400000).contains(&policy.history_max_window_ms)
                && (1..=30000).contains(&policy.history_timeout_ms)
                && (1..=32).contains(&policy.history_max_pages)
                && (1..=10000).contains(&policy.history_max_records)
                && (1..=10000).contains(&policy.max_observations)
                && (1..=10000).contains(&policy.max_receipts)
                && (1..=16777216).contains(&policy.max_checkpoint_bytes)
                && (1..=1048576).contains(&policy.max_raw_frame_bytes)
                && (1..=16777216).contains(&policy.max_history_body_bytes),
            "io economics finite policy bound exceeded"
        );
        Ok(policy)
    }
}

fn instrument_coin(instrument: &str) -> Option<&str> {
    let coin = instrument.strip_suffix("-USD-PERP.HYPERLIQUID")?;
    let symbol = coin.strip_prefix("io:")?;
    (!symbol.is_empty()
        && symbol
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_'))
    .then_some(coin)
}

#[derive(Clone, Debug)]
pub(crate) struct IoEconomicsScopeProof {
    pub(crate) account: HyperliquidAccountScopeSnapshot,
    /// Independently verified attribution; does not authorize trading or available funds.
    pub(crate) attribution_verified: bool,
    pub(crate) verified_instruments: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct IoHistoryCoverage {
    pub(crate) generation: u64,
    pub(crate) epoch: u64,
    pub(crate) endpoint: String,
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
    pub(crate) pages: usize,
    pub(crate) records: usize,
    pub(crate) complete: bool,
    pub(crate) diagnostic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Source {
    transport: String,
    received_ms: u64,
    generation: Option<u64>,
    epoch: Option<u64>,
    sequence: Option<u64>,
    snapshot: Option<bool>,
    request_start_ms: Option<u64>,
    request_end_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FinancialFact {
    category: String,
    dex: Option<String>,
    amount: Option<String>,
    currency: Option<String>,
    coin: Option<String>,
    instrument_id: Option<String>,
    venue_time_ms: Option<u64>,
    event_hash: Option<String>,
    components: BTreeMap<String, String>,
    native_applied: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ScopeEvidence {
    actual_user: String,
    dex: String,
    mode: String,
    collateral_token_id: String,
    trusted: bool,
    attribution_verified: bool,
    private_stream_epoch: u64,
    verified_instruments: BTreeSet<String>,
}

impl From<&IoEconomicsScopeProof> for ScopeEvidence {
    fn from(proof: &IoEconomicsScopeProof) -> Self {
        Self {
            actual_user: proof.account.address.clone(),
            dex: proof.account.dex.clone(),
            mode: proof.account.account_mode.clone(),
            collateral_token_id: proof.account.collateral_token_id.clone(),
            trusted: proof.account.trusted,
            attribution_verified: proof.attribution_verified,
            private_stream_epoch: proof.account.private_stream_epoch,
            verified_instruments: proof.verified_instruments.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Observation {
    id: String,
    source_seq: u64,
    raw_text: String,
    raw_envelope_ref: String,
    #[serde(skip)]
    raw_frame_text: Arc<str>,
    source: Source,
    fact: FinancialFact,
    receipt_key: Option<String>,
    effect_bucket: Option<String>,
    financial_digest: String,
    identity_quality: String,
    diagnostic: String,
    ownership_basis: String,
    scope_evidence: Option<ScopeEvidence>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Receipt {
    key: String,
    financial_digest: String,
    observation_id: String,
    source_seq: u64,
    consumer_id: String,
    consumed_revision: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct EconomicReport {
    funding_usdc: String,
    actual_fee_usdc: String,
    receipts: BTreeMap<String, Receipt>,
    consumed_observations: BTreeSet<String>,
    unknown_observations: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema_version: u32,
    account_id: String,
    actual_user: String,
    network: String,
    policy_digest: String,
    consumer_id: String,
    revision: u64,
    source_seq: u64,
    observations: BTreeMap<String, Observation>,
    raw_envelopes: BTreeMap<String, String>,
    coverage: Vec<IoHistoryCoverage>,
    report: EconomicReport,
}

#[derive(Debug)]
struct IoEconomicsState {
    checkpoint: Checkpoint,
    store: store::CheckpointStore,
    tainted: bool,
    diagnostic: String,
}

#[derive(Clone, Debug)]
pub(crate) struct IoEconomicsRuntime {
    policy: Arc<IoEconomicsPolicy>,
    state: Arc<Mutex<IoEconomicsState>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IoEconomicsDiagnostics {
    policy: Arc<IoEconomicsPolicy>,
    state: Weak<Mutex<IoEconomicsState>>,
}

#[derive(Debug)]
pub(crate) struct IoEconomicsObservationResult {
    pub(crate) handled: bool,
    pub(crate) observed: usize,
    pub(crate) unknown: usize,
    /// Valid financial attribution, independent of strong event identity or available funds.
    pub(crate) attribution_complete: bool,
}

#[derive(Deserialize)]
struct EconomicFrameHeader {
    channel: String,
}

impl IoEconomicsRuntime {
    pub(crate) fn new(
        policy: &IoEconomicsPolicy,
        account_id: AccountId,
        actual_user: String,
        network: String,
    ) -> anyhow::Result<Self> {
        let policy = IoEconomicsPolicy::parse(&serde_json::to_string(policy)?)?;
        anyhow::ensure!(
            !actual_user.is_empty() && !network.is_empty(),
            "Missing economics actual account/network identity"
        );
        let policy_digest = digest(&serde_json::to_vec(&policy)?);
        let consumer_id = digest(&serde_json::to_vec(&(
            &account_id,
            &actual_user,
            &network,
            &policy_digest,
            "native-economic-report-v1",
        ))?);
        let initial = Checkpoint {
            schema_version: 1,
            account_id: account_id.to_string(),
            actual_user,
            network,
            policy_digest,
            consumer_id,
            revision: 0,
            source_seq: 0,
            observations: BTreeMap::new(),
            raw_envelopes: BTreeMap::new(),
            coverage: Vec::new(),
            report: EconomicReport {
                funding_usdc: "0".into(),
                actual_fee_usdc: "0".into(),
                ..EconomicReport::default()
            },
        };
        let (store, checkpoint) = store::CheckpointStore::open(&policy, &initial)?;
        Ok(Self {
            policy: Arc::new(policy),
            state: Arc::new(Mutex::new(IoEconomicsState {
                checkpoint,
                store,
                tainted: false,
                diagnostic: "Bounded economic report only; no native cache application proof"
                    .into(),
            })),
        })
    }

    pub(crate) fn policy(&self) -> &IoEconomicsPolicy {
        &self.policy
    }
    pub(crate) fn diagnostics(&self) -> IoEconomicsDiagnostics {
        IoEconomicsDiagnostics {
            policy: self.policy.clone(),
            state: Arc::downgrade(&self.state),
        }
    }

    pub(crate) fn observe_ws(
        &self,
        raw_text: &str,
        generation: u64,
        epoch: u64,
        sequence: u64,
        received_ms: u64,
        scope: Option<&IoEconomicsScopeProof>,
    ) -> anyhow::Result<IoEconomicsObservationResult> {
        anyhow::ensure!(
            raw_text.len() <= self.policy.max_raw_frame_bytes,
            "io economics raw frame acceptance limit exceeded"
        );
        let source = Source {
            transport: "websocket".into(),
            received_ms,
            generation: Some(generation),
            epoch: Some(epoch),
            sequence: Some(sequence),
            snapshot: None,
            request_start_ms: None,
            request_end_ms: None,
        };
        let mut state = self.state.lock();
        let parsed = parsing::parse_ws(raw_text, source, &self.policy, &state.checkpoint, scope)?;
        let handled = serde_json::from_str::<EconomicFrameHeader>(raw_text).is_ok_and(|header| {
            matches!(
                header.channel.as_str(),
                "userFundings"
                    | "userNonFundingLedgerUpdates"
                    | "userFills"
                    | "user"
                    | "userEvents"
            )
        });
        let mut result = state.observe(parsed, &self.policy)?;
        result.handled = handled;
        Ok(result)
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn history_requires_invalidation(
        &self,
        endpoint: &str,
        raw_text: &str,
        start_ms: u64,
        end_ms: u64,
        generation: u64,
        epoch: u64,
        received_ms: u64,
        scope: Option<&IoEconomicsScopeProof>,
    ) -> bool {
        if raw_text.len() > self.policy.max_history_body_bytes
            || end_ms < start_ms
            || end_ms - start_ms > self.policy.history_max_window_ms
        {
            return true;
        }
        let source = Source {
            transport: endpoint.into(),
            received_ms,
            generation: Some(generation),
            epoch: Some(epoch),
            sequence: None,
            snapshot: None,
            request_start_ms: Some(start_ms),
            request_end_ms: Some(end_ms),
        };
        let state = self.state.lock();
        if state.tainted {
            return true;
        }
        let rows = match parsing::parse_history(
            endpoint,
            raw_text,
            source,
            &self.policy,
            &state.checkpoint,
            scope,
        ) {
            Ok(rows) => rows,
            Err(_) => return true,
        };
        for row in rows {
            if row.identity_quality != "EvidencedLocalComposite"
                || row.fact.dex.as_deref() != Some("io")
                || row.fact.instrument_id.is_none()
                || row.receipt_key.is_none()
            {
                return true;
            }
            let mut matching = false;
            for old in state
                .checkpoint
                .observations
                .values()
                .filter(|old| old.receipt_key == row.receipt_key)
            {
                if old.identity_quality != "EvidencedLocalComposite"
                    || old.financial_digest != row.financial_digest
                    || old.fact.dex != row.fact.dex
                    || old.fact.instrument_id != row.fact.instrument_id
                {
                    return true;
                }
                matching = true;
            }
            if !matching {
                return true;
            }
        }
        false
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn observe_history(
        &self,
        endpoint: &str,
        raw_text: &str,
        start_ms: u64,
        end_ms: u64,
        generation: u64,
        epoch: u64,
        received_ms: u64,
        scope: Option<&IoEconomicsScopeProof>,
    ) -> anyhow::Result<IoEconomicsObservationResult> {
        anyhow::ensure!(
            raw_text.len() <= self.policy.max_history_body_bytes
                && end_ms >= start_ms
                && end_ms - start_ms <= self.policy.history_max_window_ms,
            "io economics history body/window acceptance limit exceeded"
        );
        let source = Source {
            transport: endpoint.into(),
            received_ms,
            generation: Some(generation),
            epoch: Some(epoch),
            sequence: None,
            snapshot: None,
            request_start_ms: Some(start_ms),
            request_end_ms: Some(end_ms),
        };
        let mut state = self.state.lock();
        let parsed = parsing::parse_history(
            endpoint,
            raw_text,
            source,
            &self.policy,
            &state.checkpoint,
            scope,
        )?;
        state.observe(parsed, &self.policy)
    }

    pub(crate) fn observe_owned_fill(
        &self,
        fill: &crate::execution_scope::IoFillFacts,
        generation: u64,
        epoch: u64,
        received_ms: u64,
        scope: Option<&IoEconomicsScopeProof>,
    ) -> anyhow::Result<IoEconomicsObservationResult> {
        let raw = serde_json::to_string(
            &json!({"coin":fill.coin,"oid":fill.oid,"tid":fill.tid,"time":fill.time,"side":if fill.is_buy {"B"} else {"A"},"sz":fill.quantity.to_string(),"px":fill.price.to_string(),"fee":fill.fee.to_string(),"feeToken":fill.fee_token,"startPosition":fill.start_position.to_string(),"closedPnl":fill.closed_pnl.to_string(),"hash":fill.hash}),
        )?;
        let mut value: Value = serde_json::from_str(&raw)?;
        if let Some(builder) = &fill.builder_fee {
            value["builderFee"] = json!(builder);
        }
        let raw = serde_json::to_string(&vec![value])?;
        let source = Source {
            transport: "accepted_owned_raw_fill".into(),
            received_ms,
            generation: Some(generation),
            epoch: Some(epoch),
            sequence: None,
            snapshot: None,
            request_start_ms: Some(fill.time),
            request_end_ms: Some(fill.time),
        };
        let mut state = self.state.lock();
        let mut rows = parsing::parse_history(
            "userFillsByTime",
            &raw,
            source,
            &self.policy,
            &state.checkpoint,
            scope,
        )?;
        for row in &mut rows {
            row.ownership_basis = "AcceptedOwnedRaw; native durable application Unknown".into();
        }
        state.observe(rows, &self.policy)
    }

    pub(crate) fn record_history_coverage(
        &self,
        coverage: IoHistoryCoverage,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            coverage.end_ms >= coverage.start_ms
                && coverage.end_ms - coverage.start_ms <= self.policy.history_max_window_ms
                && coverage.pages <= self.policy.history_max_pages
                && coverage.records <= self.policy.history_max_records,
            "io economic history coverage exceeds finite bounds"
        );
        let mut state = self.state.lock();
        let mut candidate = state.checkpoint.clone();
        anyhow::ensure!(
            candidate.coverage.len() < self.policy.history_max_pages * 3,
            "io economic coverage retention exhausted"
        );
        candidate.coverage.push(coverage);
        state.commit(candidate, &self.policy)
    }
}

impl IoEconomicsDiagnostics {
    pub(crate) fn snapshot_json(&self) -> anyhow::Result<Option<String>> {
        self.state
            .upgrade()
            .map(|state| snapshot(&state.lock(), &self.policy))
            .transpose()
    }
    pub(crate) fn pending_json(&self) -> anyhow::Result<Option<String>> {
        self.state
            .upgrade()
            .map(|state| pending(&state.lock()))
            .transpose()
    }
    pub(crate) fn persist_economics(&self) -> anyhow::Result<Option<String>> {
        self.state
            .upgrade()
            .map(|state| state.lock().consume(&self.policy))
            .transpose()
    }
}

fn digest(bytes: &[u8]) -> String {
    alloy_primitives::keccak256(bytes).to_string()
}

fn receipt_identity(
    fact: &FinancialFact,
    checkpoint: &Checkpoint,
) -> anyhow::Result<Option<String>> {
    let bytes = match fact.category.as_str() {
        "funding" => {
            let (Some(hash), Some(time), Some(coin)) =
                (&fact.event_hash, fact.venue_time_ms, &fact.coin)
            else {
                return Ok(None);
            };
            serde_json::to_vec(&(
                &checkpoint.network,
                &checkpoint.actual_user,
                "funding",
                hash,
                Some(time),
                Some(coin),
            ))?
        }
        "fill_fee" => {
            let (Some(coin), Some(tid)) = (&fact.coin, fact.components.get("tid")) else {
                return Ok(None);
            };
            serde_json::to_vec(&(
                &checkpoint.network,
                &checkpoint.actual_user,
                "actual_trade_fee",
                Some(coin),
                tid,
            ))?
        }
        "ledger" => {
            let Some(hash) = &fact.event_hash else {
                return Ok(None);
            };
            serde_json::to_vec(&(
                &checkpoint.network,
                &checkpoint.actual_user,
                "ledger",
                hash,
                fact.venue_time_ms,
            ))?
        }
        _ => return Ok(None),
    };
    Ok(Some(digest(&bytes)))
}

fn exact_add(left: Decimal, right: Decimal) -> anyhow::Result<Decimal> {
    let scale = left.scale().max(right.scale());
    let left = left
        .mantissa()
        .checked_mul(
            10_i128
                .checked_pow(scale - left.scale())
                .context("Exact decimal scale overflow")?,
        )
        .context("Exact decimal alignment overflow")?;
    let right = right
        .mantissa()
        .checked_mul(
            10_i128
                .checked_pow(scale - right.scale())
                .context("Exact decimal scale overflow")?,
        )
        .context("Exact decimal alignment overflow")?;
    Decimal::try_from_i128_with_scale(
        left.checked_add(right)
            .context("Exact economic total overflow")?,
        scale,
    )
    .context("Exact economic total outside decimal range")
}

fn snapshot(state: &IoEconomicsState, policy: &IoEconomicsPolicy) -> anyhow::Result<String> {
    let checkpoint = &state.checkpoint;
    let financial_identity_complete = !checkpoint.observations.is_empty()
        && checkpoint
            .observations
            .values()
            .all(|row| row.identity_quality == "EvidencedLocalComposite");
    Ok(serde_json::to_string(
        &json!({"schema_version":1,"account_id":checkpoint.account_id,"actual_user":checkpoint.actual_user,"network":checkpoint.network,"consumer_id":checkpoint.consumer_id,"policy":policy,"source_seq":checkpoint.source_seq,"checkpoint_revision":checkpoint.revision,"tainted":state.tainted,"diagnostic":state.diagnostic,"observations":checkpoint.observations,"raw_envelopes":checkpoint.raw_envelopes,"coverage":checkpoint.coverage,"report":checkpoint.report,"pending":checkpoint.observations.len()-checkpoint.report.consumed_observations.len(),"durable_receipts":checkpoint.report.receipts.len(),"consumed_observations":checkpoint.report.consumed_observations.len(),"financial_identity_complete":financial_identity_complete,"recognized_amount_basis":"immutable locally recognized receipt history; unresolved at consumption excluded; later conflicts retain historical totals and remain Unknown","coverage_record_limit":policy.history_max_pages*3,"retention":"Unknown","native_applied":"Unknown","balance_adjustment":false,"native_cache_recovery":false}),
    )?)
}

fn pending(state: &IoEconomicsState) -> anyhow::Result<String> {
    let rows: Vec<_> = state
        .checkpoint
        .observations
        .values()
        .filter(|row| {
            !state
                .checkpoint
                .report
                .consumed_observations
                .contains(&row.id)
        })
        .collect();
    Ok(serde_json::to_string(&rows)?)
}

impl IoEconomicsState {
    fn commit(
        &mut self,
        mut candidate: Checkpoint,
        policy: &IoEconomicsPolicy,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.tainted,
            "io economic store tainted; facts retained but writes prohibited"
        );
        candidate.revision = self
            .checkpoint
            .revision
            .checked_add(1)
            .context("io economic revision exhausted")?;
        if let Err(error) = self.store.commit(&candidate, &self.checkpoint, policy) {
            self.tainted = true;
            self.diagnostic = format!("io economic persistence failed: {error}");
            return Err(error);
        }
        self.checkpoint = candidate;
        Ok(())
    }

    fn observe(
        &mut self,
        rows: Vec<Observation>,
        policy: &IoEconomicsPolicy,
    ) -> anyhow::Result<IoEconomicsObservationResult> {
        let observed = rows.len();
        let mut candidate = self.checkpoint.clone();
        for mut row in rows {
            let envelope_ref = digest(row.raw_frame_text.as_bytes());
            if !candidate.raw_envelopes.contains_key(&envelope_ref) {
                candidate
                    .raw_envelopes
                    .insert(envelope_ref.clone(), row.raw_frame_text.to_string());
            }
            row.raw_envelope_ref = envelope_ref;
            row.raw_frame_text = Arc::from("");
            candidate.source_seq = candidate
                .source_seq
                .checked_add(1)
                .context("io economic source sequence exhausted")?;
            row.source_seq = candidate.source_seq;
            row.id = format!("observation-{}", row.source_seq);
            if let Some(key) = &row.receipt_key {
                let conflict = candidate.observations.values().any(|old| {
                    old.receipt_key.as_ref() == Some(key)
                        && old.financial_digest != row.financial_digest
                });
                if conflict {
                    for old in candidate
                        .observations
                        .values()
                        .filter(|old| old.receipt_key.as_ref() == Some(key))
                    {
                        if candidate.report.consumed_observations.contains(&old.id) {
                            candidate.report.unknown_observations.insert(old.id.clone());
                        }
                    }
                    row.identity_quality = "Conflict".into();
                    row.diagnostic =
                        "Stable economic identity has conflicting financial payload".into();
                }
            }
            if row.fact.category == "ledger"
                && let Some(bucket) = &row.effect_bucket
            {
                let ambiguous = candidate.observations.values().any(|old| {
                    old.effect_bucket.as_ref() == Some(bucket)
                        && old.financial_digest != row.financial_digest
                });
                if ambiguous {
                    for old in candidate
                        .observations
                        .values()
                        .filter(|old| old.effect_bucket.as_ref() == Some(bucket))
                    {
                        if candidate.report.consumed_observations.contains(&old.id) {
                            candidate.report.unknown_observations.insert(old.id.clone());
                        }
                    }
                    row.identity_quality = "Ambiguous".into();
                    row.diagnostic =
                        "Multiple same-hash ledger effects cannot be uniquely attributed".into();
                }
            }
            candidate.observations.insert(row.id.clone(), row);
        }
        let attribution_complete = observed > 0
            && candidate
                .observations
                .values()
                .filter(|row| row.source_seq > self.checkpoint.source_seq)
                .all(|row| {
                    matches!(
                        row.identity_quality.as_str(),
                        "EvidencedLocalComposite" | "Weak"
                    ) && matches!(row.fact.category.as_str(), "funding" | "fill_fee")
                        && row.fact.amount.is_some()
                        && row.fact.dex.as_deref() == Some("io")
                        && row.fact.currency.as_deref() == Some("USDC")
                        && row.fact.instrument_id.is_some()
                        && row.fact.venue_time_ms.is_some()
                        && row
                            .scope_evidence
                            .as_ref()
                            .is_some_and(|scope| scope.attribution_verified)
                });
        let unknown = candidate
            .observations
            .values()
            .filter(|row| {
                row.source_seq > self.checkpoint.source_seq
                    && row.identity_quality != "EvidencedLocalComposite"
            })
            .count();
        self.commit(candidate, policy)?;
        Ok(IoEconomicsObservationResult {
            handled: true,
            observed,
            unknown,
            attribution_complete,
        })
    }

    fn consume(&mut self, policy: &IoEconomicsPolicy) -> anyhow::Result<String> {
        anyhow::ensure!(
            !self.tainted,
            "io economic report consumer unavailable: tainted checkpoint"
        );
        let mut candidate = self.checkpoint.clone();
        let mut rows: Vec<_> = candidate
            .observations
            .values()
            .filter(|row| !candidate.report.consumed_observations.contains(&row.id))
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.source_seq);
        if rows.is_empty() {
            return snapshot(self, policy);
        }
        let mut funding = Decimal::from_str_exact(&candidate.report.funding_usdc)?;
        let mut fees = Decimal::from_str_exact(&candidate.report.actual_fee_usdc)?;
        for row in rows {
            let conflict = row.receipt_key.as_ref().is_some_and(|key| {
                candidate.observations.values().any(|other| {
                    other.receipt_key.as_ref() == Some(key)
                        && other.financial_digest != row.financial_digest
                })
            });
            let ambiguous_bucket = row.fact.category == "ledger"
                && row.effect_bucket.as_ref().is_some_and(|bucket| {
                    candidate.observations.values().any(|other| {
                        other.effect_bucket.as_ref() == Some(bucket)
                            && other.financial_digest != row.financial_digest
                    })
                });
            if row.identity_quality != "EvidencedLocalComposite" || conflict || ambiguous_bucket {
                candidate.report.unknown_observations.insert(row.id.clone());
            } else if let Some(key) = &row.receipt_key {
                if let Some(receipt) = candidate.report.receipts.get(key) {
                    anyhow::ensure!(
                        receipt.financial_digest == row.financial_digest,
                        "Consumed immutable economic receipt conflict"
                    );
                } else {
                    if let Some(amount) = &row.fact.amount {
                        let amount = Decimal::from_str_exact(amount)?;
                        match row.fact.category.as_str() {
                            "funding" => funding = exact_add(funding, amount)?,
                            "fill_fee" => fees = exact_add(fees, amount)?,
                            _ => {}
                        }
                    }
                    candidate.report.receipts.insert(
                        key.clone(),
                        Receipt {
                            key: key.clone(),
                            financial_digest: row.financial_digest.clone(),
                            observation_id: row.id.clone(),
                            source_seq: row.source_seq,
                            consumer_id: candidate.consumer_id.clone(),
                            consumed_revision: candidate
                                .revision
                                .checked_add(1)
                                .context("io consumer revision exhausted")?,
                        },
                    );
                }
            }
            candidate.report.consumed_observations.insert(row.id);
        }
        candidate.report.funding_usdc = funding.normalize().to_string();
        candidate.report.actual_fee_usdc = fees.normalize().to_string();
        self.commit(candidate, policy)?;
        snapshot(self, policy)
    }
}
