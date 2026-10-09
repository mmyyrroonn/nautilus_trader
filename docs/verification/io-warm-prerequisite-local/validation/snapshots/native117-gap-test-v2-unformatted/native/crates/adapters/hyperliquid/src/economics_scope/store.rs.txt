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

//! One exclusively owned authoritative checkpoint, including source and consumer facts.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use anyhow::Context;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{Checkpoint, IoEconomicsPolicy, digest, exact_add, instrument_coin, receipt_identity};

#[derive(Debug)]
pub(super) struct CheckpointStore {
    path: PathBuf,
    _lease: File,
    disk_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    checksum: String,
    checkpoint: Checkpoint,
}

fn bytes(checkpoint: &Checkpoint, policy: &IoEconomicsPolicy) -> anyhow::Result<Vec<u8>> {
    let envelope = Envelope {
        checksum: digest(&serde_json::to_vec(checkpoint)?),
        checkpoint: checkpoint.clone(),
    };
    let mut bytes = serde_json::to_vec(&envelope)?;
    bytes.push(b'\n');
    anyhow::ensure!(
        bytes.len() <= policy.max_checkpoint_bytes,
        "io economic checkpoint byte capacity exhausted"
    );
    Ok(bytes)
}

fn read_bounded(path: &std::path::Path, limit: usize) -> anyhow::Result<Vec<u8>> {
    let file = File::open(path)?;
    anyhow::ensure!(
        file.metadata()?.len() <= limit as u64,
        "io economic checkpoint exceeds byte bound"
    );
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= limit && bytes.last() == Some(&b'\n'),
        "io economic checkpoint is partial or oversized"
    );
    Ok(bytes)
}

impl CheckpointStore {
    pub(super) fn open(
        policy: &IoEconomicsPolicy,
        initial: &Checkpoint,
    ) -> anyhow::Result<(Self, Checkpoint)> {
        let name = policy
            .checkpoint_path
            .file_name()
            .context("Missing economic checkpoint filename")?;
        let parent = policy
            .checkpoint_path
            .parent()
            .context("Missing economic checkpoint parent")?
            .canonicalize()
            .context("Economic checkpoint parent must already exist")?;
        let path = parent.join(name);
        if path.exists() {
            anyhow::ensure!(
                !fs::symlink_metadata(&path)?.file_type().is_symlink(),
                "Economic checkpoint cannot be a symbolic link"
            );
        }
        let lease_path = parent.join(format!("{}.lease", name.to_string_lossy()));
        let existed = lease_path.exists();
        if existed {
            anyhow::ensure!(
                !fs::symlink_metadata(&lease_path)?.file_type().is_symlink(),
                "Economic lease cannot be a symbolic link"
            );
        }
        let mut lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lease_path)?;
        lease
            .try_lock()
            .map_err(|error| anyhow::anyhow!("io economic checkpoint already owned: {error}"))?;
        let marker = serde_json::to_vec(&(
            &initial.schema_version,
            &initial.account_id,
            &initial.actual_user,
            &initial.network,
            &initial.consumer_id,
        ))?;
        let checkpoint;
        let data;
        if existed {
            anyhow::ensure!(
                lease.metadata()?.len() <= 4096,
                "Invalid economic initialization marker"
            );
            let mut stored = Vec::new();
            lease.read_to_end(&mut stored)?;
            anyhow::ensure!(
                stored == marker,
                "Economic initialized owner/consumer identity changed or marker incomplete"
            );
            data = read_bounded(&path, policy.max_checkpoint_bytes).context(
                "Initialized economic checkpoint is missing or corrupt; never initialize empty",
            )?;
            let envelope: Envelope = serde_json::from_slice(&data)?;
            anyhow::ensure!(
                envelope.checksum == digest(&serde_json::to_vec(&envelope.checkpoint)?),
                "Economic checkpoint checksum mismatch"
            );
            checkpoint = envelope.checkpoint;
            validate(&checkpoint, None, initial, policy)?;
        } else {
            anyhow::ensure!(
                !path.exists(),
                "Economic checkpoint has no authoritative initialization marker"
            );
            lease.write_all(&marker)?;
            lease.sync_all()?;
            checkpoint = initial.clone();
            validate(&checkpoint, None, initial, policy)?;
            data = bytes(&checkpoint, policy)?;
            replace(&path, &data, checkpoint.revision)?;
        }
        Ok((
            Self {
                path,
                _lease: lease,
                disk_digest: digest(&data),
            },
            checkpoint,
        ))
    }

    pub(super) fn commit(
        &mut self,
        candidate: &Checkpoint,
        previous: &Checkpoint,
        policy: &IoEconomicsPolicy,
    ) -> anyhow::Result<()> {
        validate(candidate, Some(previous), previous, policy)?;
        let current = read_bounded(&self.path, policy.max_checkpoint_bytes)?;
        anyhow::ensure!(
            digest(&current) == self.disk_digest,
            "Economic checkpoint changed outside its exclusive owner"
        );
        let data = bytes(candidate, policy)?;
        replace(&self.path, &data, candidate.revision)?;
        self.disk_digest = digest(&data);
        Ok(())
    }
}

fn replace(path: &std::path::Path, bytes: &[u8], revision: u64) -> anyhow::Result<()> {
    let name = path
        .file_name()
        .context("Missing checkpoint name")?
        .to_string_lossy();
    let temporary =
        path.with_file_name(format!("{name}.pending-{}-{revision}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .context("Economic checkpoint transaction temporary already exists")?;
    file.write_all(bytes)
        .context("Writing economic checkpoint transaction failed")?;
    file.sync_all()
        .context("Flushing economic checkpoint transaction failed")?;
    drop(file);
    fs::rename(&temporary, path).context("Atomic economic checkpoint replacement failed")?;
    // Windows FlushFileBuffers requires write access to the replaced file.
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .context("Opening replaced economic checkpoint for durable flush failed")?
        .sync_all()
        .context("Flushing replaced economic checkpoint failed")?;
    #[cfg(unix)]
    File::open(path.parent().context("Missing checkpoint directory")?)?.sync_all()?;
    // Windows file flush is evidenced; no generic power-loss directory guarantee is claimed.
    Ok(())
}

fn validate(
    candidate: &Checkpoint,
    previous: Option<&Checkpoint>,
    identity: &Checkpoint,
    policy: &IoEconomicsPolicy,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        candidate.schema_version == 1
            && candidate.account_id == identity.account_id
            && candidate.actual_user == identity.actual_user
            && candidate.network == identity.network
            && candidate.policy_digest == identity.policy_digest
            && candidate.consumer_id == identity.consumer_id,
        "Economic checkpoint owner/policy/consumer identity conflict"
    );
    anyhow::ensure!(
        candidate.observations.len() <= policy.max_observations
            && candidate.report.receipts.len() <= policy.max_receipts
            && candidate.coverage.len() <= policy.history_max_pages * 3,
        "Economic checkpoint finite count capacity exhausted"
    );
    anyhow::ensure!(
        candidate.source_seq == candidate.observations.len() as u64,
        "Economic source sequence/observation count mismatch"
    );
    anyhow::ensure!(
        candidate.raw_envelopes.len() <= candidate.observations.len(),
        "Economic raw envelope count exceeds referenced observations"
    );
    for (key, envelope) in &candidate.raw_envelopes {
        anyhow::ensure!(
            key == &digest(envelope.as_bytes())
                && envelope.len()
                    <= policy
                        .max_history_body_bytes
                        .max(policy.max_raw_frame_bytes),
            "Economic raw envelope hash/byte bound invalid"
        );
        anyhow::ensure!(
            candidate
                .observations
                .values()
                .any(|row| &row.raw_envelope_ref == key),
            "Economic raw envelope is not referenced by a durable observation"
        );
    }
    let mut sequences = BTreeSet::new();
    for (key, row) in &candidate.observations {
        anyhow::ensure!(
            key == &row.id
                && row.id == format!("observation-{}", row.source_seq)
                && row.source_seq > 0
                && row.source_seq <= candidate.source_seq
                && sequences.insert(row.source_seq)
                && row.financial_digest == digest(&serde_json::to_vec(&row.fact)?),
            "Invalid economic observation sequence/key/financial digest"
        );
        anyhow::ensure!(
            row.raw_text.len()
                <= policy
                    .max_history_body_bytes
                    .max(policy.max_raw_frame_bytes)
                && row.raw_frame_text.is_empty()
                && row.fact.native_applied == "Unknown",
            "Unsupported economic raw/native projection evidence"
        );
        let envelope = candidate
            .raw_envelopes
            .get(&row.raw_envelope_ref)
            .context("Economic observation raw envelope is missing")?;
        anyhow::ensure!(
            envelope.contains(&row.raw_text),
            "Economic item lexeme is not contained in its immutable source envelope"
        );
        super::parsing::validate_raw_observation(row, policy, candidate)?;
        if let Some(amount) = &row.fact.amount {
            anyhow::ensure!(
                Decimal::from_str_exact(amount)?.normalize().to_string() == *amount,
                "Economic exact amount is not canonical"
            );
        }
        anyhow::ensure!(
            row.source.generation.is_some() && row.source.epoch.is_some(),
            "Economic source is missing captured connection provenance"
        );
        if row.source.transport == "websocket" {
            anyhow::ensure!(
                row.source.sequence.is_some(),
                "Economic WS source sequence is missing"
            );
        }
        if let Some(key) = &row.receipt_key {
            anyhow::ensure!(
                receipt_identity(&row.fact, candidate)?.as_ref() == Some(key),
                "Economic stable receipt identity is inconsistent with source facts"
            );
        }
        if row.fact.category == "ledger" {
            anyhow::ensure!(
                row.fact.dex.is_none()
                    && row.fact.coin.is_none()
                    && row.fact.instrument_id.is_none()
                    && row.fact.amount.is_none()
                    && row.fact.currency.is_none()
                    && row.effect_bucket == row.receipt_key,
                "Account-wide ledger has fabricated market/currency/net-amount attribution"
            );
        }
        if row.identity_quality == "EvidencedLocalComposite"
            && matches!(row.fact.category.as_str(), "funding" | "fill_fee")
        {
            let instrument = row
                .fact
                .instrument_id
                .as_ref()
                .context("Recognized economic instrument missing")?;
            anyhow::ensure!(
                policy.instruments.contains(instrument)
                    && instrument_coin(instrument) == row.fact.coin.as_deref()
                    && row.fact.currency.as_deref() == Some("USDC")
                    && row.fact.dex.as_deref() == Some("io")
                    && row.fact.amount.is_some()
                    && row.fact.venue_time_ms.is_some(),
                "Recognized economic currency/market/amount proof is inconsistent"
            );
            let keys: &[&str] = if row.fact.category == "funding" {
                &["szi", "fundingRate"]
            } else {
                &["px", "sz", "startPosition", "closedPnl"]
            };
            for key in keys {
                let value = row
                    .fact
                    .components
                    .get(*key)
                    .context("Missing exact economic context")?;
                anyhow::ensure!(
                    Decimal::from_str_exact(value)?.normalize().to_string() == *value,
                    "Economic financial component is not exact canonical decimal"
                );
            }
            if row.fact.category == "fill_fee" {
                anyhow::ensure!(
                    row.fact
                        .components
                        .get("tid")
                        .is_some_and(|id| id.parse::<u64>().is_ok())
                        && row
                            .fact
                            .components
                            .get("oid")
                            .is_some_and(|id| id.parse::<u64>().is_ok())
                        && row
                            .fact
                            .components
                            .get("side")
                            .is_some_and(|side| side == "A" || side == "B"),
                    "Missing actual fill identity/side"
                );
            }
        }
    }
    for id in candidate
        .report
        .consumed_observations
        .iter()
        .chain(candidate.report.unknown_observations.iter())
    {
        anyhow::ensure!(
            candidate.observations.contains_key(id),
            "Economic consumer references missing raw observation"
        );
    }
    anyhow::ensure!(
        candidate
            .report
            .unknown_observations
            .is_subset(&candidate.report.consumed_observations),
        "Unknown economic observation not durably consumed"
    );
    for id in &candidate.report.consumed_observations {
        let row = &candidate.observations[id];
        let conflict = row.receipt_key.as_ref().is_some_and(|key| {
            candidate.observations.values().any(|other| {
                other.receipt_key.as_ref() == Some(key)
                    && other.financial_digest != row.financial_digest
            })
        });
        let ambiguous = row.fact.category == "ledger"
            && row.effect_bucket.as_ref().is_some_and(|bucket| {
                candidate.observations.values().any(|other| {
                    other.effect_bucket.as_ref() == Some(bucket)
                        && other.financial_digest != row.financial_digest
                })
            });
        if row.identity_quality == "EvidencedLocalComposite" && !conflict && !ambiguous {
            let key = row
                .receipt_key
                .as_ref()
                .context("Recognized consumed economic fact missing stable identity")?;
            let receipt =
                candidate.report.receipts.get(key).context(
                    "Recognized consumed economic fact missing immutable consumer receipt",
                )?;
            anyhow::ensure!(
                receipt.financial_digest == row.financial_digest
                    && !candidate.report.unknown_observations.contains(id),
                "Recognized consumed economic fact has inconsistent receipt/unknown state"
            );
        } else {
            anyhow::ensure!(
                candidate.report.unknown_observations.contains(id),
                "Consumed unresolved economics missing explicit unknown disposition"
            );
        }
    }
    let mut funding = Decimal::ZERO;
    let mut fees = Decimal::ZERO;
    for (key, receipt) in &candidate.report.receipts {
        let row = candidate
            .observations
            .get(&receipt.observation_id)
            .context("Receipt source observation missing")?;
        anyhow::ensure!(
            receipt.key == *key
                && row.receipt_key.as_ref() == Some(key)
                && row.identity_quality == "EvidencedLocalComposite"
                && receipt.financial_digest == row.financial_digest
                && receipt.source_seq == row.source_seq
                && receipt.consumer_id == candidate.consumer_id
                && receipt.consumed_revision > 0
                && receipt.consumed_revision <= candidate.revision
                && candidate.report.consumed_observations.contains(&row.id),
            "Invalid immutable economic consumer receipt"
        );
        if let Some(amount) = &row.fact.amount {
            let amount = Decimal::from_str_exact(amount)?;
            match row.fact.category.as_str() {
                "funding" => funding = exact_add(funding, amount)?,
                "fill_fee" => fees = exact_add(fees, amount)?,
                _ => {}
            }
        }
    }
    anyhow::ensure!(
        candidate.report.funding_usdc == funding.normalize().to_string()
            && candidate.report.actual_fee_usdc == fees.normalize().to_string(),
        "Economic report totals do not match immutable receipts"
    );
    for coverage in &candidate.coverage {
        anyhow::ensure!(
            coverage.end_ms >= coverage.start_ms
                && coverage.end_ms - coverage.start_ms <= policy.history_max_window_ms
                && coverage.pages <= policy.history_max_pages
                && coverage.records <= policy.history_max_records,
            "Invalid bounded history coverage"
        );
    }
    if let Some(previous) = previous {
        anyhow::ensure!(
            candidate.revision
                == previous
                    .revision
                    .checked_add(1)
                    .context("Economic revision exhausted")?
                && candidate.source_seq >= previous.source_seq
                && candidate
                    .report
                    .consumed_observations
                    .is_superset(&previous.report.consumed_observations)
                && candidate
                    .report
                    .unknown_observations
                    .is_superset(&previous.report.unknown_observations)
                && candidate.coverage.starts_with(&previous.coverage),
            "Economic source/consumer state regression"
        );
        for (id, row) in &previous.observations {
            anyhow::ensure!(
                candidate.observations.get(id) == Some(row),
                "Previously durable raw economic fact changed or disappeared"
            );
        }
        for (key, envelope) in &previous.raw_envelopes {
            anyhow::ensure!(
                candidate.raw_envelopes.get(key) == Some(envelope),
                "Previously durable raw source envelope changed or disappeared"
            );
        }
        for (key, receipt) in &previous.report.receipts {
            anyhow::ensure!(
                candidate.report.receipts.get(key) == Some(receipt),
                "Previously consumed immutable economic receipt changed or disappeared"
            );
        }
    }
    Ok(())
}
