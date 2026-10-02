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

//! Bounded public venue observation with explicitly synthetic framework economics.
//! This example has no credentials, private subscriptions, or execution client.
use std::{collections::BTreeMap, time::Duration};

use nautilus_backpack::{
    config::{BackpackConfig, BackpackDataClientConfig},
    data::BackpackDataClient,
    instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics},
};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{DataEvent, data::SubscribeQuotes},
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_model::identifiers::{ClientId, InstrumentId};
use rust_decimal::Decimal;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let seconds = std::env::args()
        .nth(1)
        .map(|s| s.parse::<u64>())
        .transpose()?
        .unwrap_or(10);
    anyhow::ensure!(
        (1..=60).contains(&seconds),
        "observation duration must be 1..60 seconds"
    );
    let symbol = "BTC_USDC_PERP";
    let economics = BackpackInstrumentEconomics::new_checked(
        Decimal::new(1, 1),
        Decimal::new(5, 2),
        Decimal::ZERO,
        Decimal::ZERO,
        BackpackEconomicsSource::Synthetic,
        "public example only; never execution economics".into(),
    )?;
    let config = BackpackDataClientConfig::new_checked(
        BackpackConfig::new_checked(vec![symbol.into()])?,
        BTreeMap::from([(symbol.into(), economics)]),
    )?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BACKPACK-PUBLIC"), config.clone())?;
    client.subscribe_quotes(SubscribeQuotes::new(
        InstrumentId::from("BTC_USDC_PERP.BACKPACK"),
        Some(ClientId::from("BACKPACK-PUBLIC")),
        None,
        UUID4::new(),
        get_atomic_clock_realtime().get_time_ns(),
        None,
        None,
    ))?;
    client.connect().await?;
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    loop {
        tokio::select! {()=&mut deadline=>break,event=rx.recv()=>match event {Some(DataEvent::Instrument(_))=>println!("validated public instrument published; execution_ready=false"),Some(DataEvent::Data(_))=>println!("public observation received"),None=>break,_=>{}}}
    }
    client.disconnect().await?;
    println!("{}", serde_json::to_string(&config.telemetry().snapshot())?);
    Ok(())
}
