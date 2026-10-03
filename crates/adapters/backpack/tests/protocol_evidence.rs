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

//! Source-classified protocol evidence exercised through production native parsers.

use std::{collections::HashMap, time::Duration};

use nautilus_backpack::{
    account::{
        BackpackEvidenceGap,
        models::{BackpackAccountPolicy, BackpackFill, BackpackFundingPayment, BackpackOrder},
        pagination::{BackpackHistoryWindow, BackpackPageHeaders, BackpackReadBudget},
        reports::{
            BackpackFundingObservation, BackpackOrderBindings, BackpackOrderOwnership,
            BackpackReportContext, fill_report,
        },
    },
    config::BackpackConfig,
    execution_client::{
        BackpackAccountTelemetry,
        private::{BackpackPrivateFact, decode_private},
    },
    identity::{BackpackClientIdNamespace, BackpackClientIdStore, BackpackSubmissionIntent},
    models::BackpackMarket,
    provider::BackpackInstrumentProvider,
    public::{BackpackPublicEvent, BackpackPublicStreamParser},
};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{AccountId, ClientOrderId, VenueOrderId};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tempfile::TempDir;

fn synthetic(name: &str) -> Value {
    serde_json::from_str::<Value>(include_str!("../test_data/protocol/synthetic_cases.json"))
        .unwrap()[name]
        .clone()
}
fn decimal(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

struct Scope {
    provider: BackpackInstrumentProvider,
    store: BackpackClientIdStore,
    _directory: TempDir,
}
impl Scope {
    fn new() -> Self {
        let mut provider = BackpackInstrumentProvider::new(
            BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
        );
        let market: BackpackMarket =
            serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
        provider
            .replace_markets(&[market], UnixNanos::from(1))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let namespace = BackpackClientIdNamespace::new_checked(
            "synthetic-protocol",
            "synthetic-account",
            Some("2"),
        )
        .unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace).unwrap();
        let id = store
            .reserve_intent(
                BackpackSubmissionIntent::new_checked(
                    ClientOrderId::from("O-PROTOCOL-1"),
                    "{}".into(),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(id, 1);
        Self {
            provider,
            store,
            _directory: directory,
        }
    }
    fn context<'a>(
        &'a self,
        bindings: Option<&'a BackpackOrderBindings>,
    ) -> BackpackReportContext<'a> {
        BackpackReportContext {
            account_id: AccountId::from("BACKPACK-SYNTHETIC"),
            instruments: &self.provider,
            identities: &self.store,
            confirmed_orders: bindings,
            ts_init: UnixNanos::from(2),
        }
    }
}

#[rstest]
fn official_balance_example_is_absolute_with_unverified_identity() {
    let scope = Scope::new();
    let payload: Value = serde_json::from_str(include_str!(
        "../test_data/protocol/official_balance_update.json"
    ))
    .unwrap();
    let frame =
        serde_json::to_vec(&json!({"stream":"account.balanceUpdate","data":payload})).unwrap();
    for _ in 0..2 {
        let observation = decode_private(&frame, &scope.context(None)).unwrap();
        assert!(
            observation
                .gaps
                .contains(&BackpackEvidenceGap::AccountIdentityUnverified)
        );
        let BackpackPrivateFact::Wallet {
            balance, ts_event, ..
        } = &observation.facts[0]
        else {
            panic!("expected documented balance observation");
        };
        assert_eq!(balance.trading_balance.free.as_decimal(), decimal("122.35"));
        assert_eq!(balance.trading_balance.locked.as_decimal(), decimal("10"));
        assert_eq!(balance.wallet_total.as_decimal(), decimal("132.35"));
        assert_eq!(*ts_event, UnixNanos::from(1_694_687_692_980_000_000));
    }
}

#[rstest]
fn synthetic_success_controls_never_attest_private_subscription_or_identity() {
    let scope = Scope::new();
    for control in synthetic("controls").as_array().unwrap() {
        let observation =
            decode_private(&serde_json::to_vec(control).unwrap(), &scope.context(None)).unwrap();
        assert!(observation.facts.is_empty());
        assert!(observation.topic.is_none());
        assert!(
            observation
                .gaps
                .contains(&BackpackEvidenceGap::UnknownVenueState)
        );
        assert!(
            observation
                .gaps
                .contains(&BackpackEvidenceGap::AccountIdentityUnverified)
        );
    }
    assert!(
        !BackpackAccountTelemetry::default()
            .snapshot()
            .private_subscription_confirmed
    );
}

#[rstest]
fn observed_estimate_preserves_raw_rate_separately_from_synthetic_settled_cashflow() {
    let scope = Scope::new();
    let metadata = scope.provider.all().next().unwrap().clone();
    let mut parser = BackpackPublicStreamParser::new(metadata, 1);
    let raw = include_str!("../test_data/protocol/public_mark_price.json");
    let wire: Value = serde_json::from_str(raw).unwrap();
    let BackpackPublicEvent::Mark { funding, .. } = parser
        .decode(1, raw.as_bytes(), UnixNanos::from(2))
        .unwrap()
    else {
        panic!("expected actual public mark estimate");
    };
    assert_eq!(
        funding.raw_rate,
        decimal(wire["data"]["f"].as_str().unwrap())
    );
    assert_eq!(
        funding.next_funding_ns,
        UnixNanos::from(wire["data"]["n"].as_u64().unwrap() * 1_000_000)
    );
    let payment: BackpackFundingPayment = serde_json::from_value(synthetic("funding")).unwrap();
    let observation = BackpackFundingObservation::from(payment);
    assert!(observation.raw.quantity.0 < Decimal::ZERO);
    assert!(
        observation
            .gaps
            .contains(&BackpackEvidenceGap::FundingCurrencyUnknown)
    );
    assert!(
        observation
            .gaps
            .contains(&BackpackEvidenceGap::FundingTimezoneUnknown)
    );
}

#[rstest]
fn synthetic_basis_point_policy_and_negative_cash_fee_are_not_conflated() {
    let policy: BackpackAccountPolicy = serde_json::from_value(synthetic("policy")).unwrap();
    assert_eq!(policy.futures_maker_fee.0, decimal("-0.1"));
    let scope = Scope::new();
    let fill: BackpackFill = serde_json::from_value(synthetic("fill")).unwrap();
    let observation = fill_report(fill, &scope.context(None)).unwrap();
    assert_eq!(
        observation.ownership,
        BackpackOrderOwnership::Reserved(ClientOrderId::from("O-PROTOCOL-1"))
    );
    assert!(
        observation
            .gaps
            .contains(&BackpackEvidenceGap::OwnershipUnverified)
    );
    let report = observation.report.unwrap();
    assert_eq!(report.commission.as_decimal(), decimal("-0.000001"));
    assert!(report.client_order_id.is_none());
}

#[rstest]
#[case("system_fill", false)]
#[case("future_system_fill", true)]
fn synthetic_system_fill_cannot_adopt_independently_bound_numeric_client_id(
    #[case] name: &str,
    #[case] unknown: bool,
) {
    let scope = Scope::new();
    let fill: BackpackFill = serde_json::from_value(synthetic(name)).unwrap();
    let mut bindings = BackpackOrderBindings::default();
    bindings
        .confirm_acknowledged(
            ClientOrderId::from("O-PROTOCOL-1"),
            VenueOrderId::from(fill.order_id.as_str()),
            scope.provider.all().next().unwrap().instrument_id,
            &scope.store,
        )
        .unwrap();
    let observation = fill_report(fill, &scope.context(Some(&bindings))).unwrap();
    assert_eq!(observation.ownership, BackpackOrderOwnership::External);
    assert_eq!(
        observation
            .gaps
            .contains(&BackpackEvidenceGap::UnknownVenueState),
        unknown
    );
    let report = observation.report.unwrap();
    assert!(report.client_order_id.is_none());
    assert_eq!(report.commission.as_decimal(), decimal("-0.000001"));
}

#[rstest]
fn synthetic_client_id_boundaries_match_order_uint32_and_fill_decimal_string() {
    let mut order = synthetic("history_order");
    order["clientId"] = json!(u32::MAX);
    assert_eq!(
        serde_json::from_value::<BackpackOrder>(order.clone())
            .unwrap()
            .client_id,
        Some(u32::MAX)
    );
    order["clientId"] = json!(u64::from(u32::MAX) + 1);
    assert!(serde_json::from_value::<BackpackOrder>(order).is_err());
    let scope = Scope::new();
    let mut fill = synthetic("fill");
    fill["clientId"] = json!("4294967296");
    assert!(fill_report(serde_json::from_value(fill).unwrap(), &scope.context(None)).is_err());
}

#[rstest]
fn synthetic_pagination_headers_and_window_fail_closed_without_missing_defaults() {
    let mut headers: HashMap<String, String> =
        serde_json::from_value(synthetic("page_headers")).unwrap();
    let page = BackpackPageHeaders::parse(&headers).unwrap();
    assert_eq!(page.current_page, 0);
    assert_eq!(page.total, 3);
    headers.remove("x-total");
    assert!(BackpackPageHeaders::parse(&headers).is_err());
    assert!(BackpackReadBudget::new(1000, 2, 2000, Duration::from_secs(1)).is_ok());
    assert!(BackpackReadBudget::new(1001, 2, 2002, Duration::from_secs(1)).is_err());
    assert!(BackpackHistoryWindow::new(10, 11).is_ok());
    assert!(BackpackHistoryWindow::new(11, 11).is_err());
}
