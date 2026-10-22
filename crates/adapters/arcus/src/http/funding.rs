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

//! Bounded retrieval of hourly market funding rates.

use std::{collections::BTreeMap, time::SystemTime};

use nautilus_core::serialization::deserialize_decimal_from_str;
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::{ArcusHttpClient, ArcusHttpError, ArcusHttpResult, MAX_PAGES, PAGE_SIZE, UNIX_EPOCH};

const MIN_TIMESTAMP_US: u64 = 100_000_000_000_000;
const MAX_TIMESTAMP_US: u64 = u64::MAX / 1_000;
const MAX_HISTORY_ROWS: usize = MAX_PAGES * PAGE_SIZE;

/// One settled hourly rate, timestamped in epoch microseconds.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArcusMarketFundingRate {
    pub market_id: u16,
    pub market_display_name: String,
    #[serde(deserialize_with = "deserialize_decimal_from_str")]
    pub funding_rate: Decimal,
    pub time: u64,
}

impl ArcusHttpClient {
    /// Validates funding request bounds and fixes an omitted end to the current time.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid market identity, timestamp bounds, limits, or system time.
    pub(crate) fn validate_funding_rates_query(
        market: &str,
        from_us: Option<u64>,
        to_us: Option<u64>,
        limit: Option<usize>,
    ) -> ArcusHttpResult<u64> {
        let upper = match to_us {
            Some(value) => value,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_micros()).ok())
                .ok_or_else(|| ArcusHttpError::Validation("Invalid system clock".into()))?,
        };
        validate_query(market, from_us, upper, limit)?;
        Ok(upper)
    }

    /// Retrieves available hourly rates, returning the latest requested rows in ascending time order.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, inconsistent data, transport failure, or incomplete paging.
    pub(crate) async fn funding_rates(
        &self,
        market: &str,
        from_us: Option<u64>,
        to_us: Option<u64>,
        limit: Option<usize>,
    ) -> ArcusHttpResult<Vec<ArcusMarketFundingRate>> {
        let upper = Self::validate_funding_rates_query(market, from_us, to_us, limit)?;
        let market_id = market.parse::<u16>().ok();
        let mut cursor = upper;
        let mut result = BTreeMap::<u64, ArcusMarketFundingRate>::new();

        for _ in 0..MAX_PAGES {
            let page_size = limit.map_or(PAGE_SIZE, |limit| {
                (limit - result.len() + usize::from(!result.is_empty())).min(PAGE_SIZE)
            });
            let mut query = vec![
                ("market".into(), market.into()),
                ("to".into(), cursor.to_string()),
                ("limit".into(), page_size.to_string()),
            ];

            if let Some(from_us) = from_us {
                query.push(("from".into(), from_us.to_string()));
            }
            let page = self
                .request::<FundingRatesResponse>(
                    Method::GET,
                    "/v1/fundingRates",
                    &query,
                    None,
                    None,
                    20 + u32::try_from(page_size / 20).unwrap_or(50),
                )
                .await?
                .funding_rates;
            let complete = page.len() < page_size;

            if page.len() > page_size {
                return Err(ArcusHttpError::Decode(
                    "Funding page exceeded its limit".into(),
                ));
            }
            let oldest = page.iter().map(|row| row.time).min();

            for row in page {
                let matching_market = market_id.map_or_else(
                    || row.market_display_name.eq_ignore_ascii_case(market),
                    |id| row.market_id == id,
                );

                if !matching_market
                    || row.time < MIN_TIMESTAMP_US
                    || row.time > cursor
                    || row.time % 1_000 != 0
                    || from_us.is_some_and(|lower| row.time < lower)
                {
                    return Err(ArcusHttpError::Decode(
                        "Funding row has an unexpected market or timestamp".into(),
                    ));
                }

                if let Some(previous) = result.get(&row.time) {
                    if previous != &row {
                        return Err(ArcusHttpError::Decode(
                            "Conflicting funding rows at one timestamp".into(),
                        ));
                    }
                } else {
                    result.insert(row.time, row);
                }
            }

            if complete || limit.is_some_and(|limit| result.len() >= limit) {
                let skip = result.len().saturating_sub(limit.unwrap_or(result.len()));
                return Ok(result.into_values().skip(skip).collect());
            }
            let oldest = oldest
                .ok_or_else(|| ArcusHttpError::Incomplete("Empty full funding page".into()))?;

            if oldest >= cursor {
                return Err(ArcusHttpError::Incomplete(
                    "Funding pagination failed to advance".into(),
                ));
            }

            // Keep the boundary inclusive so a page cannot silently skip a settlement
            cursor = oldest;
        }
        Err(ArcusHttpError::Incomplete(
            "Funding history exceeded 64 pages; narrow the time range".into(),
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRatesResponse {
    funding_rates: Vec<ArcusMarketFundingRate>,
}

fn validate_query(
    market: &str,
    from_us: Option<u64>,
    to_us: u64,
    limit: Option<usize>,
) -> ArcusHttpResult<()> {
    if market.is_empty()
        || !market
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        || !(MIN_TIMESTAMP_US..=MAX_TIMESTAMP_US).contains(&to_us)
        || from_us.is_some_and(|from| !(MIN_TIMESTAMP_US..=to_us).contains(&from))
        || limit.is_some_and(|limit| limit == 0 || limit > MAX_HISTORY_ROWS)
    {
        return Err(ArcusHttpError::Validation(
            "Invalid market, microsecond funding bounds, or history limit (1..=64000)".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        Json, Router,
        extract::{Query, State},
        http::HeaderMap,
        routing::get,
    };
    use parking_lot::Mutex;
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    const START: u64 = 1_790_000_000_000_000;
    const HOUR: u64 = 3_600_000_000;

    #[derive(Clone)]
    struct Fixture {
        rows: Arc<[Value]>,
        queries: Arc<Mutex<Vec<HashMap<String, String>>>>,
    }

    async fn funding(
        State(fixture): State<Fixture>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        assert!(headers.get("x-api-key").is_none());
        assert!(headers.get("x-signature").is_none());
        fixture.queries.lock().push(query.clone());
        let to = query["to"].parse::<u64>().unwrap();
        let from = query
            .get("from")
            .map_or(0, |value| value.parse::<u64>().unwrap());
        let count = query["limit"].parse::<usize>().unwrap();
        let rows: Vec<_> = fixture
            .rows
            .iter()
            .filter(|row| (from..=to).contains(&row["time"].as_u64().unwrap()))
            .rev()
            .take(count)
            .cloned()
            .collect();
        Json(json!({"total": rows.len(), "fundingRates": rows}))
    }

    async fn server(rows: Vec<Value>) -> (ArcusHttpClient, Fixture, JoinHandle<()>) {
        let fixture = Fixture {
            rows: rows.into(),
            queries: Arc::default(),
        };
        let app = Router::new()
            .route("/v1/fundingRates", get(funding))
            .with_state(fixture.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = ArcusHttpClient::new(
            format!("http://{}", listener.local_addr().unwrap()),
            3,
            None,
        )
        .unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (client, fixture, task)
    }

    fn row(time: u64) -> Value {
        json!({"marketId":1,"marketDisplayName":"BTC-USD","fundingRate":"-0.000012345678901234","time":time})
    }

    #[tokio::test]
    async fn funding_pages_deduplicate_boundaries_and_keep_latest_limit() {
        let rows = (0..1001).map(|index| row(START + index * HOUR)).collect();
        let (client, fixture, task) = server(rows).await;
        let end = START + 1000 * HOUR;
        let rates = client
            .funding_rates("BTC-USD", Some(START), Some(end), None)
            .await
            .unwrap();
        assert_eq!(rates.len(), 1001);
        assert_eq!(rates.first().unwrap().time, START);
        assert_eq!(rates.last().unwrap().time, end);
        assert_eq!(rates[0].funding_rate, dec!(-0.000012345678901234));
        assert_eq!(fixture.queries.lock().len(), 2);
        assert_eq!(fixture.queries.lock()[1]["to"], (START + HOUR).to_string());
        let latest = client
            .funding_rates("1", Some(START), Some(end), Some(2))
            .await
            .unwrap();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].time, end - HOUR);
        assert_eq!(latest[1].time, end);
        assert_eq!(fixture.queries.lock().last().unwrap()["limit"], "2");
        task.abort();
    }

    #[tokio::test]
    async fn funding_single_timestamp_is_inclusive_and_empty_history_is_valid() {
        let (client, _, task) = server(vec![row(START)]).await;
        let rates = client
            .funding_rates("btc-usd", Some(START), Some(START), None)
            .await
            .unwrap();
        assert_eq!(rates.len(), 1);
        let empty = client
            .funding_rates("BTC-USD", Some(START + 1), Some(START + 999), None)
            .await
            .unwrap();
        assert!(empty.is_empty());
        task.abort();
    }

    #[tokio::test]
    async fn funding_conflicts_and_mismatched_markets_fail_closed() {
        let mut conflict = row(START);
        conflict["fundingRate"] = "0.01".into();
        let (client, _, task) = server(vec![row(START), conflict]).await;
        assert!(matches!(
            client
                .funding_rates("BTC-USD", None, Some(START), None)
                .await,
            Err(ArcusHttpError::Decode(_))
        ));
        assert!(matches!(
            client
                .funding_rates("ETH-USD", None, Some(START), None)
                .await,
            Err(ArcusHttpError::Decode(_))
        ));
        task.abort();
    }

    #[tokio::test]
    async fn funding_full_duplicate_page_cannot_silently_truncate() {
        let (client, _, task) = server(vec![row(START); PAGE_SIZE]).await;
        assert!(matches!(
            client
                .funding_rates("BTC-USD", None, Some(START + HOUR), None)
                .await,
            Err(ArcusHttpError::Incomplete(_))
        ));
        task.abort();
    }

    #[rstest]
    #[case(Some(START / 1_000), START, None)]
    #[case(Some(START + 1), START, None)]
    #[case(None, u64::MAX, None)]
    #[case(None, START, Some(0))]
    #[case(None, START, Some(MAX_HISTORY_ROWS + 1))]
    fn funding_invalid_bounds_reject_before_network(
        #[case] from: Option<u64>,
        #[case] to: u64,
        #[case] limit: Option<usize>,
    ) {
        assert!(validate_query("BTC-USD", from, to, limit).is_err());
    }
}
