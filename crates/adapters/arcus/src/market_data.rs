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

//! Shared subscriptions and exact decoding for Arcus reference prices and predicted funding.

use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use nautilus_common::messages::DataEvent;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::websocket::ArcusWebSocket;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ReferenceKind {
    Mark,
    Index,
    Funding,
}

impl ReferenceKind {
    const fn index(self) -> usize {
        match self {
            Self::Mark => 0,
            Self::Index => 1,
            Self::Funding => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Intent {
    active: [bool; 3],
    revisions: [u64; 3],
}

#[derive(Debug, Default)]
pub(crate) struct ReferenceSubscriptions {
    intents: HashMap<InstrumentId, Intent>,
    revision: u64,
}

impl ReferenceSubscriptions {
    pub(crate) fn clear(&mut self) {
        self.intents.clear();
    }

    pub(crate) fn set(
        &mut self,
        id: InstrumentId,
        market: &str,
        kind: ReferenceKind,
        active: bool,
        socket: &ArcusWebSocket,
    ) -> anyhow::Result<()> {
        let current = self.intents.get(&id).copied().unwrap_or_default();
        let index = kind.index();

        if current.active[index] == active {
            return Ok(());
        }
        let revision = self
            .revision
            .checked_add(1)
            .context("Arcus subscription revision overflow")?;
        let mut next = current;
        next.active[index] = active;
        next.revisions[index] = revision;
        let (subscription, wire_change) = match kind {
            ReferenceKind::Funding => (json!({"channel":"predictedFunding","id":market}), true),
            ReferenceKind::Mark | ReferenceKind::Index => {
                let before = current.active[0] || current.active[1];
                let after = next.active[0] || next.active[1];
                (
                    json!({"channel":"oraclePrices","id":market}),
                    before != after,
                )
            }
        };

        if wire_change {
            if active {
                socket.try_subscribe(subscription)?;
            } else {
                socket.try_unsubscribe(subscription)?;
            }
        }
        self.intents.insert(id, next);
        self.revision = revision;
        Ok(())
    }
}

#[derive(Default)]
struct ReferenceState {
    revisions: [u64; 3],
    mark_epoch: Option<u64>,
    index_epoch: Option<u64>,
    funding_snapshot_seen: bool,
    index_snapshot_seen: bool,
}

#[derive(Default)]
pub(crate) struct ReferenceDecoder {
    states: HashMap<InstrumentId, ReferenceState>,
}

impl ReferenceDecoder {
    pub(crate) fn clear(&mut self) {
        self.states.clear();
    }

    pub(crate) fn decode(
        &mut self,
        instruments: &HashMap<String, InstrumentAny>,
        message: &Value,
        subscriptions: &ReferenceSubscriptions,
        received_at: UnixNanos,
    ) -> anyhow::Result<Vec<DataEvent>> {
        if message["type"] == "degraded" && message["channel"] == "predictedFunding" {
            if let Some(instrument) = message["id"]
                .as_str()
                .and_then(|market| instruments.get(market))
                && let Some(state) = self.states.get_mut(&instrument.id())
            {
                state.funding_snapshot_seen = false;
            }
            return Ok(Vec::new());
        }

        if !matches!(
            message["type"].as_str(),
            Some("subscribed" | "channel_data")
        ) {
            return Ok(Vec::new());
        }
        let contents = &message["contents"];

        if message["type"] == "subscribed"
            && contents.as_object().is_some_and(serde_json::Map::is_empty)
        {
            return Ok(Vec::new());
        }

        match message["channel"].as_str() {
            Some("oraclePrices") => {
                let epoch = contents["epoch"]
                    .as_u64()
                    .context("Arcus oracle batch requires a nanosecond epoch")?;
                let untimed_snapshot = epoch == 0 && message["type"] == "subscribed";
                anyhow::ensure!(
                    epoch > 0 || untimed_snapshot,
                    "Arcus live oracle epoch must be positive"
                );
                let rows = contents["prices"]
                    .as_array()
                    .context("Arcus oracle prices must be an array")?;
                let mut events = Vec::new();

                for row in rows {
                    let market = row["marketDisplayName"]
                        .as_str()
                        .context("Arcus oracle market missing")?;
                    let Some(instrument) = instruments.get(market) else {
                        continue;
                    };
                    let Some(intent) = subscriptions.intents.get(&instrument.id()) else {
                        continue;
                    };

                    if !intent.active[0] && !intent.active[1] {
                        continue;
                    }

                    if let Some(scope) = message["id"].as_str().filter(|scope| !scope.is_empty()) {
                        anyhow::ensure!(
                            scope == market,
                            "Arcus oracle row is outside its subscription market"
                        );
                    }
                    let InstrumentAny::CryptoPerpetual(perpetual) = instrument else {
                        anyhow::bail!("Arcus reference data requires a perpetual instrument")
                    };
                    let expected_id = perpetual
                        .info
                        .as_ref()
                        .and_then(|info| info.get("arcus_market_id"))
                        .and_then(Value::as_u64)
                        .context("Arcus instrument market identity missing")?;
                    anyhow::ensure!(
                        row["marketId"].as_u64() == Some(expected_id),
                        "Arcus oracle market ID does not match its ticker"
                    );
                    let state = self.states.entry(instrument.id()).or_default();

                    if state.revisions[0] != intent.revisions[0] {
                        state.mark_epoch = None;
                        state.revisions[0] = intent.revisions[0];
                    }

                    if state.revisions[1] != intent.revisions[1] {
                        state.index_epoch = None;
                        state.index_snapshot_seen = false;
                        state.revisions[1] = intent.revisions[1];
                    }

                    if intent.active[0]
                        && let Some(price) = reference_price(&row["markPrice"])?
                        && let Some(mark_epoch) =
                            row["markEpochNanos"].as_u64().filter(|epoch| *epoch > 0)
                        && state
                            .mark_epoch
                            .is_none_or(|previous| mark_epoch > previous)
                    {
                        events.push(DataEvent::Data(Data::MarkPrice(MarkPriceUpdate::new(
                            instrument.id(),
                            price,
                            UnixNanos::from(mark_epoch),
                            received_at,
                        ))));
                        state.mark_epoch = Some(mark_epoch);
                    }

                    if intent.active[1]
                        && let Some(price) = reference_price(&row["price"])?
                    {
                        if untimed_snapshot {
                            if !state.index_snapshot_seen && state.index_epoch.is_none() {
                                // Receipt time labels the untimed snapshot, never the source-event watermark
                                events.push(DataEvent::Data(Data::IndexPrice(
                                    IndexPriceUpdate::new(
                                        instrument.id(),
                                        price,
                                        received_at,
                                        received_at,
                                    ),
                                )));
                                state.index_snapshot_seen = true;
                            }
                        } else if state.index_epoch.is_none_or(|previous| epoch > previous) {
                            events.push(DataEvent::Data(Data::IndexPrice(IndexPriceUpdate::new(
                                instrument.id(),
                                price,
                                UnixNanos::from(epoch),
                                received_at,
                            ))));
                            state.index_epoch = Some(epoch);
                        }
                    }
                }
                Ok(events)
            }
            Some("predictedFunding") => {
                let market = contents["market"]
                    .as_str()
                    .context("Arcus funding prediction market missing")?;

                if let Some(id) = message["id"].as_str() {
                    anyhow::ensure!(
                        id == market,
                        "Arcus funding prediction market does not match subscription"
                    );
                }
                let Some(instrument) = instruments.get(market) else {
                    return Ok(Vec::new());
                };
                let Some(intent) = subscriptions
                    .intents
                    .get(&instrument.id())
                    .filter(|intent| intent.active[2])
                else {
                    return Ok(Vec::new());
                };
                let rate = Decimal::from_str(
                    contents["rate1h"]
                        .as_str()
                        .context("Arcus predicted funding rate must be a decimal string")?,
                )?;
                let state = self.states.entry(instrument.id()).or_default();

                if state.revisions[2] != intent.revisions[2] {
                    state.funding_snapshot_seen = false;
                    state.revisions[2] = intent.revisions[2];
                }

                if message["type"] == "subscribed" {
                    if state.funding_snapshot_seen {
                        return Ok(Vec::new());
                    }
                    state.funding_snapshot_seen = true;
                }
                // The venue publishes a one-hour prediction without an event time or settlement deadline
                Ok(vec![DataEvent::FundingRate(FundingRateUpdate::new(
                    instrument.id(),
                    rate,
                    Some(60),
                    None,
                    received_at,
                    received_at,
                ))])
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn reference_price(value: &Value) -> anyhow::Result<Option<Price>> {
    if value.is_null() {
        return Ok(None);
    }
    let decimal = Decimal::from_str(
        value
            .as_str()
            .context("Arcus reference price must be a decimal string")?,
    )?;
    anyhow::ensure!(
        decimal >= Decimal::ZERO,
        "Arcus reference price cannot be negative"
    );

    if decimal.is_zero() {
        return Ok(None);
    }
    let price = Price::from_decimal(decimal.normalize())?;
    anyhow::ensure!(
        price.as_decimal() == decimal,
        "Arcus reference price exceeds supported precision"
    );
    Ok(Some(price))
}

pub(crate) fn funding_history_update(
    instrument: &InstrumentAny,
    row: &crate::http::ArcusMarketFundingRate,
) -> anyhow::Result<FundingRateUpdate> {
    anyhow::ensure!(
        row.market_display_name == instrument.raw_symbol().as_str(),
        "Arcus historical funding market does not match instrument"
    );
    let InstrumentAny::CryptoPerpetual(perpetual) = instrument else {
        anyhow::bail!("Arcus funding history requires a perpetual instrument")
    };
    let market_id = perpetual
        .info
        .as_ref()
        .and_then(|info| info.get("arcus_market_id"))
        .and_then(Value::as_u64)
        .context("Arcus instrument market identity missing")?;
    anyhow::ensure!(
        u64::from(row.market_id) == market_id,
        "Arcus historical funding market ID does not match instrument"
    );
    let timestamp = row
        .time
        .checked_mul(1_000)
        .context("Arcus funding timestamp overflow")?;
    Ok(FundingRateUpdate::new(
        instrument.id(),
        row.funding_rate,
        Some(60),
        None,
        UnixNanos::from(timestamp),
        UnixNanos::from(timestamp),
    ))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::models::ArcusMarket;

    fn fixture() -> (HashMap<String, InstrumentAny>, ReferenceSubscriptions) {
        let market: ArcusMarket =
            serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
        let instrument = crate::instruments::parse_instrument(&market, UnixNanos::from(1)).unwrap();
        let id = instrument.id();
        (
            HashMap::from([("BTC-USD".to_owned(), instrument)]),
            ReferenceSubscriptions {
                intents: HashMap::from([(
                    id,
                    Intent {
                        active: [true, true, true],
                        revisions: [1, 2, 3],
                    },
                )]),
                revision: 3,
            },
        )
    }

    fn oracle(kind: &str, epoch: u64, mark_epoch: Option<u64>, mark: Value) -> Value {
        let mut message = json!({"type":kind,"channel":"oraclePrices","id":"BTC-USD","contents":{"epoch":epoch,"prices":[{"marketDisplayName":"BTC-USD","marketId":1,"price":"50000.1234"}]}});
        message["contents"]["prices"][0]["markPrice"] = mark;

        if let Some(epoch) = mark_epoch {
            message["contents"]["prices"][0]["markEpochNanos"] = epoch.into();
        }
        message
    }

    #[rstest]
    fn oracle_prices_preserve_distinct_values_precision_and_nanosecond_clocks() {
        let (instruments, subscriptions) = fixture();
        let mut decoder = ReferenceDecoder::default();
        let events = decoder
            .decode(
                &instruments,
                &oracle(
                    "channel_data",
                    123456789,
                    Some(123456000),
                    json!("50000.9876"),
                ),
                &subscriptions,
                UnixNanos::from(200000000),
            )
            .unwrap();
        assert_eq!(events.len(), 2);
        let DataEvent::Data(Data::MarkPrice(mark)) = &events[0] else {
            panic!("Expected mark")
        };
        assert_eq!(mark.value.as_decimal(), dec!(50000.9876));
        assert_eq!(mark.ts_event.as_u64(), 123456000);
        let DataEvent::Data(Data::IndexPrice(index)) = &events[1] else {
            panic!("Expected index")
        };
        assert_eq!(index.value.as_decimal(), dec!(50000.1234));
        assert_eq!(index.ts_event.as_u64(), 123456789);
        assert_eq!(index.ts_init.as_u64(), 200000000);
        assert!(
            decoder
                .decode(
                    &instruments,
                    &oracle(
                        "channel_data",
                        123456789,
                        Some(123456000),
                        json!("50000.9876")
                    ),
                    &subscriptions,
                    UnixNanos::from(300000000)
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            decoder
                .decode(
                    &instruments,
                    &oracle(
                        "channel_data",
                        123456788,
                        Some(123455999),
                        json!("50000.9876")
                    ),
                    &subscriptions,
                    UnixNanos::from(300000000)
                )
                .unwrap()
                .is_empty()
        );
    }

    #[rstest]
    fn untimed_snapshot_does_not_forge_mark_time_or_advance_source_watermark() {
        let (instruments, subscriptions) = fixture();
        let mut decoder = ReferenceDecoder::default();
        let events = decoder
            .decode(
                &instruments,
                &oracle("subscribed", 0, None, json!("50000.2")),
                &subscriptions,
                UnixNanos::from(200),
            )
            .unwrap();
        assert_eq!(events.len(), 1);
        let DataEvent::Data(Data::IndexPrice(index)) = &events[0] else {
            panic!("Untimed snapshot must emit only index")
        };
        assert_eq!(index.ts_event.as_u64(), 200);
        assert!(
            decoder
                .decode(
                    &instruments,
                    &oracle("subscribed", 0, None, json!("50000.2")),
                    &subscriptions,
                    UnixNanos::from(201)
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            decoder
                .decode(
                    &instruments,
                    &oracle("channel_data", 100, Some(99), json!("50000.3")),
                    &subscriptions,
                    UnixNanos::from(202)
                )
                .unwrap()
                .len(),
            2
        );
        decoder.clear();
        assert_eq!(
            decoder
                .decode(
                    &instruments,
                    &oracle("channel_data", 90, Some(89), json!("50000.3")),
                    &subscriptions,
                    UnixNanos::from(203)
                )
                .unwrap()
                .len(),
            2
        );
    }

    #[rstest]
    #[case(Value::Null)]
    #[case(json!("0"))]
    fn unavailable_mark_never_falls_back_to_oracle(#[case] mark: Value) {
        let (instruments, subscriptions) = fixture();
        let events = ReferenceDecoder::default()
            .decode(
                &instruments,
                &oracle("channel_data", 100, Some(99), mark),
                &subscriptions,
                UnixNanos::from(200),
            )
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DataEvent::Data(Data::IndexPrice(_))));
    }

    #[rstest]
    fn cross_market_identity_and_malformed_prices_are_rejected() {
        let (instruments, subscriptions) = fixture();
        let mut message = oracle("channel_data", 100, Some(99), json!("50000.3"));
        message["contents"]["prices"][0]["marketId"] = 2.into();
        assert!(
            ReferenceDecoder::default()
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(200))
                .is_err()
        );
        message["contents"]["prices"][0]["marketId"] = 1.into();
        message["id"] = "ETH-USD".into();
        assert!(
            ReferenceDecoder::default()
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(200))
                .is_err()
        );
        message["id"] = "BTC-USD".into();
        message["contents"]["prices"][0]["markPrice"] = "-1".into();
        assert!(
            ReferenceDecoder::default()
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(200))
                .is_err()
        );
    }

    #[rstest]
    fn predicted_funding_is_hourly_negative_exact_and_never_invents_a_deadline() {
        let (instruments, subscriptions) = fixture();
        let mut decoder = ReferenceDecoder::default();
        let mut message =
            json!({"type":"subscribed","channel":"predictedFunding","id":"BTC-USD","contents":{}});
        assert!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(1))
                .unwrap()
                .is_empty()
        );
        message["contents"] = json!({"market":"BTC-USD","rate1h":"-0.000012345678901234"});
        let events = decoder
            .decode(&instruments, &message, &subscriptions, UnixNanos::from(200))
            .unwrap();
        let DataEvent::FundingRate(rate) = &events[0] else {
            panic!("Expected funding")
        };
        assert_eq!(rate.rate, dec!(-0.000012345678901234));
        assert_eq!(rate.interval, Some(60));
        assert_eq!(rate.next_funding_ns, None);
        assert_eq!(rate.ts_event.as_u64(), 200);
        assert!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(201))
                .unwrap()
                .is_empty()
        );
        message["type"] = "channel_data".into();
        assert_eq!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(202))
                .unwrap()
                .len(),
            1
        );
        message["contents"]["market"] = "ETH-USD".into();
        assert!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(203))
                .is_err()
        );
    }

    #[rstest]
    fn subscriber_revision_and_reconnect_reset_deduplication_without_enabling_other_kinds() {
        let (instruments, mut subscriptions) = fixture();
        let id = instruments["BTC-USD"].id();
        let intent = subscriptions.intents.get_mut(&id).unwrap();
        intent.active[1] = false;
        let message = oracle("channel_data", 100, Some(99), json!("50000.3"));
        let mut decoder = ReferenceDecoder::default();
        assert_eq!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(200))
                .unwrap()
                .len(),
            1
        );
        subscriptions.intents.get_mut(&id).unwrap().active[0] = false;
        assert!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(201))
                .unwrap()
                .is_empty()
        );
        let intent = subscriptions.intents.get_mut(&id).unwrap();
        intent.active[0] = true;
        intent.revisions[0] += 1;
        assert_eq!(
            decoder
                .decode(&instruments, &message, &subscriptions, UnixNanos::from(202))
                .unwrap()
                .len(),
            1
        );
    }
}
