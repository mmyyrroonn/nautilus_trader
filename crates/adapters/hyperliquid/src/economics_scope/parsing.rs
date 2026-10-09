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

//! Lossless raw lexemes with strict string-decimal normalization.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{
    Checkpoint, FinancialFact, IoEconomicsPolicy, IoEconomicsScopeProof, Observation,
    ScopeEvidence, Source, USDC_TOKEN_ID, digest, instrument_coin,
};

#[derive(Debug)]
struct RawObject(BTreeMap<String, Box<RawValue>>);

impl<'de> Deserialize<'de> for RawObject {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RawObject;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut result = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if result.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("Duplicate economic JSON field"));
                    }
                }
                Ok(RawObject(result))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

fn object(raw: &str) -> anyhow::Result<RawObject> {
    Ok(serde_json::from_str(raw)?)
}
fn field<'a>(row: &'a RawObject, key: &str) -> Option<&'a str> {
    row.0.get(key).map(|value| value.get())
}
fn string(row: &RawObject, key: &str) -> Option<String> {
    serde_json::from_str(field(row, key)?).ok()
}
fn integer(row: &RawObject, key: &str) -> Option<u64> {
    serde_json::from_str(field(row, key)?).ok()
}

pub(super) fn exact_string_decimal(raw: &str) -> anyhow::Result<String> {
    let text: String = serde_json::from_str(raw)
        .context("Numeric literal is raw evidence, not a supported exact string amount")?;
    let text = text.trim();
    let digits = text.strip_prefix('-').unwrap_or(text);
    anyhow::ensure!(
        !digits.is_empty()
            && digits.bytes().all(|c| c.is_ascii_digit() || c == b'.')
            && digits.bytes().filter(|c| *c == b'.').count() <= 1
            && digits.bytes().any(|c| c.is_ascii_digit()),
        "Unsupported decimal string syntax"
    );
    let value = Decimal::from_str_exact(text)
        .context("Decimal precision/range unsupported; raw preserved")?;
    Ok(value.normalize().to_string())
}

fn decimal(row: &RawObject, key: &str) -> anyhow::Result<String> {
    exact_string_decimal(
        field(row, key).with_context(|| format!("Missing {key}; never coerce to zero"))?,
    )
}

fn supported_instrument(
    coin: &str,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&ScopeEvidence>,
) -> Option<String> {
    let scope = scope?;
    if !scope.attribution_verified || scope.actual_user != checkpoint.actual_user {
        return None;
    }
    if scope.dex != "io"
        || scope.collateral_token_id != USDC_TOKEN_ID
        || scope.mode != "standard_disabled_inferred"
    {
        return None;
    }
    policy
        .instruments
        .iter()
        .find(|instrument| {
            instrument_coin(instrument) == Some(coin)
                && scope.verified_instruments.contains(*instrument)
        })
        .cloned()
}

fn empty_fact(category: &str) -> FinancialFact {
    FinancialFact {
        category: category.into(),
        dex: None,
        amount: None,
        currency: None,
        coin: None,
        instrument_id: None,
        venue_time_ms: None,
        event_hash: None,
        components: BTreeMap::new(),
        native_applied: "Unknown".into(),
    }
}

fn observation(
    raw: &str,
    source: Source,
    fact: FinancialFact,
    key: Option<String>,
    bucket: Option<String>,
    quality: &str,
    diagnostic: &str,
) -> anyhow::Result<Observation> {
    let financial_digest = digest(&serde_json::to_vec(&fact)?);
    Ok(Observation {
        id: String::new(),
        source_seq: 0,
        raw_text: raw.into(),
        raw_envelope_ref: String::new(),
        raw_frame_text: Arc::from(raw),
        source,
        fact,
        receipt_key: key,
        effect_bucket: bucket,
        financial_digest,
        identity_quality: quality.into(),
        diagnostic: diagnostic.into(),
        ownership_basis: "AccountRaw; strategy/native application Unknown".into(),
        scope_evidence: None,
    })
}

fn unknown(raw: &str, source: Source, diagnostic: &str) -> anyhow::Result<Observation> {
    observation(
        raw,
        source,
        empty_fact("unknown"),
        None,
        None,
        "Unknown",
        diagnostic,
    )
}

fn funding(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&IoEconomicsScopeProof>,
    history: bool,
) -> anyhow::Result<Observation> {
    let evidence = scope.map(ScopeEvidence::from);
    funding_evidence(raw, source, policy, checkpoint, evidence.as_ref(), history)
}

fn funding_evidence(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&ScopeEvidence>,
    history: bool,
) -> anyhow::Result<Observation> {
    let mut row = funding_inner(raw, source, policy, checkpoint, scope, history)?;
    if row.fact.category == "funding" {
        row.scope_evidence = scope.cloned();
    }
    Ok(row)
}

fn funding_inner(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&ScopeEvidence>,
    history: bool,
) -> anyhow::Result<Observation> {
    let scope = scope.filter(|proof| source.epoch == Some(proof.private_stream_epoch));
    let row = match object(raw) {
        Ok(row) => row,
        Err(error) => return unknown(raw, source, &error.to_string()),
    };
    let delta = if history {
        field(&row, "delta").and_then(|raw| object(raw).ok())
    } else {
        object(raw).ok()
    };
    let Some(delta) = delta else {
        return unknown(raw, source, "Missing complete actual funding delta");
    };
    let mut fact = empty_fact("funding");
    fact.venue_time_ms = integer(&row, "time");
    fact.event_hash = history.then(|| string(&row, "hash")).flatten();
    fact.coin = string(&delta, "coin");
    let normalized = (|| {
        anyhow::ensure!(
            !history || string(&delta, "type").as_deref() == Some("funding"),
            "Wrong actual funding delta type"
        );
        let coin = fact.coin.as_deref().context("Missing funding coin")?;
        let instrument = supported_instrument(coin, policy, checkpoint, scope)
            .context("Unverified funding instrument/account mode/currency")?;
        anyhow::ensure!(
            fact.venue_time_ms.is_some(),
            "Missing funding occurrence time"
        );
        let amount = decimal(&delta, "usdc")?;
        let size = decimal(&delta, "szi")?;
        let rate = decimal(&delta, "fundingRate")?;
        fact.amount = Some(amount);
        fact.currency = Some("USDC".into());
        fact.instrument_id = Some(instrument);
        fact.dex = Some("io".into());
        fact.components.insert("szi".into(), size);
        fact.components.insert("fundingRate".into(), rate);
        if let Some(samples) = field(&delta, "nSamples") {
            anyhow::ensure!(
                samples == "null" || serde_json::from_str::<u64>(samples).is_ok(),
                "Unknown funding sample count"
            );
            fact.components.insert("nSamples".into(), samples.into());
        }
        Ok::<_, anyhow::Error>(())
    })();
    if let Err(error) = normalized {
        return observation(raw, source, fact, None, None, "Unknown", &error.to_string());
    }
    if !history {
        return observation(
            raw,
            source,
            fact,
            None,
            None,
            "Weak",
            "Funding stream lacks a stable venue hash; occurrence preserved, excluded from recognized cash",
        );
    }
    let Some(hash) = fact.event_hash.as_ref().filter(|hash| !hash.is_empty()) else {
        return observation(
            raw,
            source,
            fact,
            None,
            None,
            "Weak",
            "Missing funding hash; cannot identify unique cash event",
        );
    };
    let key = digest(&serde_json::to_vec(&(
        &checkpoint.network,
        &checkpoint.actual_user,
        "funding",
        hash,
        fact.venue_time_ms,
        &fact.coin,
    ))?);
    observation(
        raw,
        source,
        fact,
        Some(key),
        None,
        "EvidencedLocalComposite",
        "Local hash/time/coin composite policy; venue uniqueness and retention remain unknown",
    )
}

fn fill(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&IoEconomicsScopeProof>,
) -> anyhow::Result<Observation> {
    let evidence = scope.map(ScopeEvidence::from);
    fill_evidence(raw, source, policy, checkpoint, evidence.as_ref())
}

fn fill_evidence(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&ScopeEvidence>,
) -> anyhow::Result<Observation> {
    let mut row = fill_inner(raw, source, policy, checkpoint, scope)?;
    if row.fact.category == "fill_fee" {
        row.scope_evidence = scope.cloned();
    }
    Ok(row)
}

fn fill_inner(
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&ScopeEvidence>,
) -> anyhow::Result<Observation> {
    let scope = scope.filter(|proof| source.epoch == Some(proof.private_stream_epoch));
    let row = match object(raw) {
        Ok(row) => row,
        Err(error) => return unknown(raw, source, &error.to_string()),
    };
    let mut fact = empty_fact("fill_fee");
    fact.coin = string(&row, "coin");
    fact.venue_time_ms = integer(&row, "time");
    fact.event_hash = string(&row, "hash");
    let normalized = (|| {
        fact.instrument_id = Some(
            supported_instrument(
                fact.coin.as_deref().context("Missing fill coin")?,
                policy,
                checkpoint,
                scope,
            )
            .context("Unverified fee instrument/collateral/account")?,
        );
        fact.dex = Some("io".into());
        anyhow::ensure!(
            string(&row, "feeToken").as_deref() == Some("USDC") && fact.venue_time_ms.is_some(),
            "Unknown actual fee token/time"
        );
        let oid = integer(&row, "oid").context("Missing real fill order ID")?;
        let tid = integer(&row, "tid").context("Missing real venue trade ID")?;
        let side = string(&row, "side").context("Missing fill side")?;
        anyhow::ensure!(side == "B" || side == "A", "Unknown fill side");
        fact.amount = Some(decimal(&row, "fee")?);
        fact.currency = Some("USDC".into());
        fact.components.insert("oid".into(), oid.to_string());
        fact.components.insert("tid".into(), tid.to_string());
        fact.components.insert("side".into(), side);
        for key in ["px", "sz", "startPosition", "closedPnl"] {
            fact.components.insert(key.into(), decimal(&row, key)?);
        }
        anyhow::ensure!(
            Decimal::from_str_exact(&fact.components["px"])? > Decimal::ZERO
                && Decimal::from_str_exact(&fact.components["sz"])? > Decimal::ZERO,
            "Nonpositive actual fill size/price"
        );
        if let Some(builder) = field(&row, "builderFee") {
            fact.components
                .insert("builderFee".into(), exact_string_decimal(builder)?);
        }
        Ok::<_, anyhow::Error>(())
    })();
    if let Err(error) = normalized {
        return observation(raw, source, fact, None, None, "Unknown", &error.to_string());
    }
    let key = digest(&serde_json::to_vec(&(
        &checkpoint.network,
        &checkpoint.actual_user,
        "actual_trade_fee",
        &fact.coin,
        &fact.components["tid"],
    ))?);
    observation(
        raw,
        source,
        fact,
        Some(key),
        None,
        "EvidencedLocalComposite",
        "Actual fee report only; builder fee already included; strategy/native durable application remains Unknown",
    )
}

fn ledger(raw: &str, source: Source, checkpoint: &Checkpoint) -> anyhow::Result<Observation> {
    let row = match object(raw) {
        Ok(row) => row,
        Err(error) => return unknown(raw, source, &error.to_string()),
    };
    let mut fact = empty_fact("ledger");
    fact.venue_time_ms = integer(&row, "time");
    fact.event_hash = string(&row, "hash");
    let Some(delta) = field(&row, "delta").and_then(|raw| object(raw).ok()) else {
        return observation(
            raw,
            source,
            fact,
            None,
            None,
            "Unknown",
            "Missing non-funding ledger delta",
        );
    };
    for (key, value) in &delta.0 {
        fact.components.insert(key.clone(), value.get().into());
    }
    let kind = string(&delta, "type");
    let supported = matches!(
        kind.as_deref(),
        Some(
            "deposit"
                | "withdraw"
                | "internalTransfer"
                | "subAccountTransfer"
                | "liquidation"
                | "vaultCreate"
                | "vaultDeposit"
                | "vaultWithdraw"
                | "vaultWithdrawal"
                | "vaultDistribution"
                | "vaultLeaderCommission"
                | "spotTransfer"
                | "accountClassTransfer"
                | "spotGenesis"
                | "rewardsClaim"
        )
    );
    let hash = fact.event_hash.as_ref().filter(|hash| !hash.is_empty());
    let bucket = hash
        .map(|hash| {
            serde_json::to_vec(&(
                &checkpoint.network,
                &checkpoint.actual_user,
                "ledger",
                hash,
                fact.venue_time_ms,
            ))
        })
        .transpose()?
        .map(|bytes| digest(&bytes));
    let key = bucket.clone();
    let numeric_amount = ["usdc", "fee", "accountValue", "amount"]
        .iter()
        .any(|key| field(&delta, key).is_some_and(|raw| !raw.starts_with('"')));
    let quality = if supported && hash.is_some() && fact.venue_time_ms.is_some() && !numeric_amount
    {
        "EvidencedLocalComposite"
    } else {
        "Unknown"
    };
    // Account-wide attribution and gross/net amount interpretation remain unset.
    observation(
        raw,
        source,
        fact,
        key,
        bucket,
        quality,
        "Account-wide raw components only; token/direction/net-income semantics and same-hash multiplicity remain unknown",
    )
}

pub(super) fn parse_history(
    endpoint: &str,
    raw: &str,
    source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&IoEconomicsScopeProof>,
) -> anyhow::Result<Vec<Observation>> {
    let rows: Vec<Box<RawValue>> = match serde_json::from_str(raw) {
        Ok(rows) => rows,
        Err(error) => return Ok(vec![unknown(raw, source, &error.to_string())?]),
    };
    anyhow::ensure!(
        rows.len() <= policy.history_max_records,
        "io history response record bound exceeded"
    );
    let mut output = Vec::new();
    let mut previous = None;
    for row in rows {
        let parsed = match object(row.get()) {
            Ok(parsed) => parsed,
            Err(error) => {
                output.push(unknown(row.get(), source.clone(), &error.to_string())?);
                continue;
            }
        };
        let Some(time) = integer(&parsed, "time") else {
            output.push(unknown(
                row.get(),
                source.clone(),
                "Missing history occurrence time",
            )?);
            continue;
        };
        if !source.request_start_ms.is_some_and(|start| time >= start)
            || !source.request_end_ms.is_some_and(|end| time <= end)
            || previous.is_some_and(|previous| time < previous)
        {
            output.push(unknown(
                row.get(),
                source.clone(),
                "Out-of-window or unordered economic history",
            )?);
            continue;
        }
        previous = Some(time);
        let observation = match endpoint {
            "userFunding" => funding(row.get(), source.clone(), policy, checkpoint, scope, true)?,
            "userNonFundingLedgerUpdates" => ledger(row.get(), source.clone(), checkpoint)?,
            "userFillsByTime" => fill(row.get(), source.clone(), policy, checkpoint, scope)?,
            _ => anyhow::bail!("Unknown economic history endpoint"),
        };
        output.push(observation);
    }
    let envelope: Arc<str> = Arc::from(raw);
    for row in &mut output {
        row.raw_frame_text = envelope.clone();
    }
    Ok(output)
}

pub(super) fn parse_ws(
    raw: &str,
    mut source: Source,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
    scope: Option<&IoEconomicsScopeProof>,
) -> anyhow::Result<Vec<Observation>> {
    let root = match object(raw) {
        Ok(root) => root,
        Err(error) => return Ok(vec![unknown(raw, source, &error.to_string())?]),
    };
    if root.0.len() != 2 || !root.0.contains_key("channel") || !root.0.contains_key("data") {
        return Ok(vec![unknown(
            raw,
            source,
            "Unsupported economic frame envelope fields",
        )?]);
    }
    let channel = string(&root, "channel").context("Missing economic stream channel")?;
    let data = field(&root, "data").context("Missing raw economic stream data")?;
    let data_object = match object(data) {
        Ok(data) => data,
        Err(error) => return Ok(vec![unknown(raw, source, &error.to_string())?]),
    };
    if let Some(snapshot) = field(&data_object, "isSnapshot") {
        source.snapshot = serde_json::from_str(snapshot).ok();
        if source.snapshot.is_none() {
            return Ok(vec![unknown(
                raw,
                source,
                "Invalid economic snapshot flag",
            )?]);
        }
    }
    let user = string(&data_object, "user");
    if channel != "user"
        && channel != "userEvents"
        && user.as_deref() != Some(&checkpoint.actual_user)
    {
        return Ok(vec![unknown(
            raw,
            source,
            "Economic subscription user mismatch or missing owner",
        )?]);
    }
    let expected = match channel.as_str() {
        "userFundings" => Some("fundings"),
        "userNonFundingLedgerUpdates" => Some("nonFundingLedgerUpdates"),
        "userFills" => Some("fills"),
        _ => None,
    };
    if let Some(expected) = expected
        && data_object
            .0
            .keys()
            .any(|key| key != "user" && key != "isSnapshot" && key != expected)
    {
        return Ok(vec![unknown(
            raw,
            source,
            "Unsupported additional economic envelope fields",
        )?]);
    }
    let (array, kind) = match channel.as_str() {
        "userFundings" => (field(&data_object, "fundings"), "funding"),
        "userNonFundingLedgerUpdates" => (field(&data_object, "nonFundingLedgerUpdates"), "ledger"),
        "userFills" => (field(&data_object, "fills"), "fill"),
        "user" | "userEvents" => {
            if data_object.0.len() != 1 {
                return Ok(vec![unknown(
                    raw,
                    source,
                    "Unsupported mixed user-event economic envelope",
                )?]);
            }
            if let Some(event) = field(&data_object, "funding") {
                let mut row = funding(event, source, policy, checkpoint, scope, false)?;
                row.raw_frame_text = Arc::from(raw);
                return Ok(vec![row]);
            }
            if data_object.0.contains_key("fills") {
                (field(&data_object, "fills"), "fill")
            } else {
                return Ok(vec![unknown(
                    raw,
                    source,
                    "Unsupported economic user-event alternative",
                )?]);
            }
        }
        _ => return Ok(Vec::new()),
    };
    let Some(array) = array else {
        return Ok(vec![unknown(
            raw,
            source,
            "Missing complete economic stream array",
        )?]);
    };
    let rows: Vec<Box<RawValue>> = match serde_json::from_str(array) {
        Ok(rows) => rows,
        Err(error) => return Ok(vec![unknown(raw, source, &error.to_string())?]),
    };
    anyhow::ensure!(
        rows.len() <= policy.max_observations,
        "io economic stream item bound exceeded"
    );
    let envelope: Arc<str> = Arc::from(raw);
    rows.into_iter()
        .map(|row| {
            let mut observation = match kind {
                "funding" => funding(row.get(), source.clone(), policy, checkpoint, scope, false),
                "ledger" => ledger(row.get(), source.clone(), checkpoint),
                _ => fill(row.get(), source.clone(), policy, checkpoint, scope),
            }?;
            observation.raw_frame_text = envelope.clone();
            Ok(observation)
        })
        .collect()
}

pub(super) fn validate_raw_observation(
    row: &Observation,
    policy: &IoEconomicsPolicy,
    checkpoint: &Checkpoint,
) -> anyhow::Result<()> {
    if row.source.transport == "websocket" && row.fact.category != "unknown" {
        let frame = checkpoint
            .raw_envelopes
            .get(&row.raw_envelope_ref)
            .context("Missing original economic frame")?;
        let root = object(frame)?;
        let channel = string(&root, "channel").context("Missing original economic channel")?;
        let data = object(field(&root, "data").context("Missing original economic data")?)?;
        if channel != "user" && channel != "userEvents" {
            anyhow::ensure!(
                string(&data, "user").as_deref() == Some(&checkpoint.actual_user),
                "Original economic frame owner mismatch"
            );
        }
        let expected = match row.fact.category.as_str() {
            "funding" => matches!(channel.as_str(), "userFundings" | "user" | "userEvents"),
            "fill_fee" => {
                channel == "userFills"
                    || (matches!(channel.as_str(), "user" | "userEvents")
                        && data.0.len() == 1
                        && data.0.contains_key("fills"))
            }
            "ledger" => channel == "userNonFundingLedgerUpdates",
            _ => false,
        };
        anyhow::ensure!(
            expected,
            "Original channel does not prove claimed economic category"
        );
    }
    let derived = match row.fact.category.as_str() {
        "funding" => funding_evidence(
            &row.raw_text,
            row.source.clone(),
            policy,
            checkpoint,
            row.scope_evidence.as_ref(),
            row.source.transport != "websocket",
        )?,
        "fill_fee" => fill_evidence(
            &row.raw_text,
            row.source.clone(),
            policy,
            checkpoint,
            row.scope_evidence.as_ref(),
        )?,
        "ledger" => ledger(&row.raw_text, row.source.clone(), checkpoint)?,
        "unknown" => unknown(
            &row.raw_text,
            row.source.clone(),
            "Raw unsupported economic observation",
        )?,
        _ => anyhow::bail!("Unsupported persisted economic category"),
    };
    anyhow::ensure!(
        row.fact == derived.fact
            && row.receipt_key == derived.receipt_key
            && row.effect_bucket == derived.effect_bucket
            && row.scope_evidence == derived.scope_evidence,
        "Persisted normalized economics is not derived from exact original source lexemes/scope evidence"
    );
    let conflict = row.receipt_key.as_ref().is_some_and(|key| {
        checkpoint.observations.values().any(|other| {
            other.receipt_key.as_ref() == Some(key)
                && other.financial_digest != row.financial_digest
        })
    });
    let ambiguity = row.fact.category == "ledger"
        && row.effect_bucket.as_ref().is_some_and(|bucket| {
            checkpoint.observations.values().any(|other| {
                other.effect_bucket.as_ref() == Some(bucket)
                    && other.financial_digest != row.financial_digest
            })
        });
    anyhow::ensure!(
        row.identity_quality == derived.identity_quality
            || (row.identity_quality == "Conflict" && conflict)
            || (row.identity_quality == "Ambiguous" && ambiguity),
        "Persisted economic identity quality has no raw conflict/ambiguity evidence"
    );
    let ownership = if row.source.transport == "accepted_owned_raw_fill" {
        "AcceptedOwnedRaw; native durable application Unknown"
    } else {
        "AccountRaw; strategy/native application Unknown"
    };
    anyhow::ensure!(
        row.ownership_basis == ownership,
        "Persisted economic ownership provenance unsupported"
    );
    Ok(())
}
