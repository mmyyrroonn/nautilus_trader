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

//! Offline tests for the Ondo Perps reconciliation, account and dead man's switch surface
//! (plan §6.3, §6.4, Task 8).
//!
//! Everything here is offline. The state machine, the account judgments, the ledger journal and
//! the dead man's switch are driven directly, and the client-level tests go to a scripted HTTP/1.1
//! server bound to `127.0.0.1:0` inside the test process - the harness `tests/execution.rs`
//! established.
//!
//! # What is deliberately *not* claimed here
//!
//! The dead man's switch is a WebSocket channel with account-level effect. Its real renewal message
//! and its 30-second trigger can only be established against the venue (plan §6.4, Task 8's last
//! checklist item), and this process holds no credential, so **nothing in this file is evidence
//! that the switch behaves as documented against a live venue**. What is tested is the local state
//! machine: the frames this adapter would send, the deadline arithmetic, and what it refuses to
//! conclude when the switch fires.

use std::{
    cell::RefCell,
    collections::VecDeque,
    net::SocketAddr,
    num::NonZeroU32,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{CancelOrder, ExecutionReport, SubmitOrder, SubmitOrderList},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TraderId,
        VenueOrderId,
    },
    orders::{Order, OrderAny, OrderList, builder::OrderTestBuilder},
    reports::FillReport,
    types::{Price, Quantity},
};
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_ondo::{
    common::{consts::ONDO_VENUE, credential::OndoCredential, enums::OndoEnvironment},
    config::OndoExecutionClientConfig,
    execution::OndoExecutionClient,
    http::{
        orders::{OndoApiOrder, OndoOrderStatus},
        private::OndoApiFill,
        rate_limit::OndoRateBudget,
    },
    reconciliation::{
        AccountReading, Admission, BalanceReading, DeadMansSwitch, DeadMansSwitchMessage,
        DeadMansSwitchState, Finding, LedgerJournal, MetadataValidity, NewRiskRefusal,
        ONDO_DMS_CHANNEL, ONDO_SUBMISSION_PROBE_INTERVAL, ONDO_SUBMISSION_UNKNOWN_SECS,
        OrderReading, PositionDirection, PositionReading, ProbeDisposition, ProbeOutcome,
        ReconciliationBuffer, ReconciliationMachine, ReconciliationState, StopStep, UncertainKind,
    },
};
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
    task::JoinHandle,
};

const ACCOUNT_ID: &str = "ONDO-SANDBOX-001";
const CLIENT_ID: &str = "ONDO-EXEC";
/// The plan's own fake credential. It is never a real key and never leaves this process.
const TEST_KEY_ID: &str = "ondoKeyId_UNIT_TEST_ONLY";
const TEST_API_SECRET: &str = "ondoApiSecret_UNIT_TEST_ONLY";
const NVDA: &str = "NVDA-USD-PERP.ONDO";
const NVDA_MARKET: &str = "NVDA-USD.P";
const VENUE_ORDER_ID: &str = "197ec08e001658690721be129e7fa595";
const CLIENT_ORDER_ID: &str = "ondo_probe_1";

// ------------------------------------------------------------------------------------------------
// Fixtures
// ------------------------------------------------------------------------------------------------

fn account_id() -> AccountId {
    AccountId::from(ACCOUNT_ID)
}

fn client_order_id(value: &str) -> ClientOrderId {
    ClientOrderId::from(value)
}

fn nvda() -> InstrumentId {
    InstrumentId::from(NVDA)
}

/// A nanosecond timestamp `seconds` after the epoch.
fn secs(seconds: u64) -> UnixNanos {
    UnixNanos::from(seconds * 1_000_000_000)
}

/// One `ApiOrder` payload in the vendors' documented shape.
fn api_order(order_id: &str, client_order_id: &str, status: &str, filled_size: &str) -> String {
    format!(
        r#"{{"orderId":"{order_id}","clientOrderId":"{client_order_id}","side":"buy","price":"227.50","size":"1.00","market":"{NVDA_MARKET}","filledSize":"{filled_size}","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
    )
}

/// One `ApiFill` payload in the venue's documented shape.
fn api_fill(id: &str, order_id: &str, client_order_id: &str, size: &str) -> String {
    format!(
        r#"{{"id":"{id}","orderId":"{order_id}","clientOrderId":"{client_order_id}","market":"{NVDA_MARKET}","price":"0.01","size":"{size}","side":"buy","direction":"openLong","fee":"0.00","time":"2025-03-05T14:30:01.000000000Z","isMaker":false}}"#,
    )
}

fn fill_from(text: &str) -> OndoApiFill {
    OndoApiFill::from_raw(
        &serde_json::value::RawValue::from_string(text.to_string()).expect("JSON"),
    )
    .expect("the fill fixture is an ApiFill")
}

/// One `ApiOrder` fixture, read the way the REST pages and the private stream read one.
fn order_from(text: &str) -> OndoApiOrder {
    OndoApiOrder::from_text(text).expect("the order fixture is an ApiOrder")
}

/// The recovery generation the buffer tests record their reports under.
///
/// Nothing in these tests changes generation, so one number is enough to say "current": the
/// superseded case is the one that names another.
const RECOVERY: u64 = 1;

/// An order reading for a tracked order, filled to `applied` by this client's own fills.
fn tracked_order(status: OndoOrderStatus, venue_filled: &str, applied: &str) -> OrderReading {
    OrderReading {
        venue_order_id: VENUE_ORDER_ID.to_string(),
        client_order_id: Some(CLIENT_ORDER_ID.to_string()),
        market: NVDA_MARKET.to_string(),
        status,
        venue_filled: Some(rust_decimal::Decimal::from_str_exact(venue_filled).expect("decimal")),
        applied_filled: Some(rust_decimal::Decimal::from_str_exact(applied).expect("decimal")),
        tracked: true,
    }
}

/// A position reading for NVDA.
fn position(direction: PositionDirection, net_quantity: &str, signed: &str) -> PositionReading {
    PositionReading {
        market: NVDA_MARKET.to_string(),
        instrument_id: Some(nvda()),
        direction,
        net_quantity: rust_decimal::Decimal::from_str_exact(net_quantity).expect("decimal"),
        signed: rust_decimal::Decimal::from_str_exact(signed).expect("decimal"),
    }
}

/// A balance reading carrying the five documented members.
fn balance(
    wallet: &str,
    margin: &str,
    used: &str,
    available: &str,
    withdrawable: &str,
) -> BalanceReading {
    let decimal =
        |value: &str| Some(rust_decimal::Decimal::from_str_exact(value).expect("decimal"));

    BalanceReading {
        wallet_balance: decimal(wallet),
        margin_balance: decimal(margin),
        used_margin: decimal(used),
        available_margin: decimal(available),
        withdrawable_margin: decimal(withdrawable),
        unmapped: Vec::new(),
        raw: String::new(),
    }
}

/// An empty but complete reading: no orders, no positions, a healthy USDC balance.
fn clean_reading() -> AccountReading {
    AccountReading {
        orders: Vec::new(),
        positions: Vec::new(),
        balance: Some(balance("5000.00", "4950.00", "0.00", "4950.00", "4950.00")),
        applied_net: std::collections::BTreeMap::new(),
        fills: Vec::new(),
    }
}

/// A machine that has recovered: a session, two agreeing passes, current metadata.
fn recovered_machine() -> ReconciliationMachine {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));
    machine.conclude_pass(&clean_reading(), secs(1));
    machine.conclude_pass(&clean_reading(), secs(2));

    assert_eq!(machine.state(), ReconciliationState::Ready);

    machine
}

// ------------------------------------------------------------------------------------------------
// The state machine and the fail-closed predicate (plan §6.4)
// ------------------------------------------------------------------------------------------------

/// F01: a machine that has never held a session used to be excused from governing new risk, on the
/// reasoning that there was no venue state to be uncertain about yet. There is: the account has
/// never been read, which is the least verified state there is, and an order placed against it is
/// the thing the entrance check exists to stop.
#[rstest]
fn test_a_machine_that_has_never_held_a_session_refuses_new_risk() {
    let machine = ReconciliationMachine::new(account_id(), 30);

    assert_eq!(machine.state(), ReconciliationState::Disconnected);
    assert!(!machine.session_established());

    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());

    // And the decision says why, rather than leaving a caller to infer it from a state.
    let refusal = machine
        .new_risk_refusal()
        .expect("a fresh machine refuses new risk");

    assert_eq!(
        machine.admission(),
        Admission::Refused {
            reason: refusal.clone()
        },
    );
    assert_eq!(
        refusal,
        NewRiskRefusal::AccountState(ReconciliationState::Disconnected),
    );
    assert_eq!(refusal.reason(), "the account is disconnected");
}

#[rstest]
fn test_recovery_needs_two_agreeing_passes_before_it_is_ready() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    assert_eq!(machine.state(), ReconciliationState::Recovering);
    assert!(!machine.can_submit_new_orders());

    let first = machine.conclude_pass(&clean_reading(), secs(1));

    // One pass is a reading, not a boundary: plan §6.4 asks for a second converging confirmation.
    assert_eq!(first, ReconciliationState::Recovering);
    assert_eq!(machine.confirmations(), 1);
    assert!(!machine.can_submit_new_orders());

    let second = machine.conclude_pass(&clean_reading(), secs(2));

    assert_eq!(second, ReconciliationState::Ready);
    assert_eq!(machine.confirmations(), 2);
    assert!(machine.can_submit_new_orders());
}

#[rstest]
fn test_two_passes_that_read_different_accounts_are_not_a_confirmation() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    // The account moved, and it moved cleanly: this client's fill built a position and the venue's
    // own list carries it. The position is what makes the reading *different* rather than
    // *disagreeing* - a fill that built a position the venue's list does not carry is the
    // disagreement `Finding::PositionAbsent` reports, and it is not what this test is about.
    let mut moved = clean_reading();
    moved.positions = vec![position(PositionDirection::Long, "0.5", "0.5")];
    moved.applied_net.insert(
        nvda(),
        rust_decimal::Decimal::from_str_exact("0.5").expect("decimal"),
    );

    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(1)),
        ReconciliationState::Recovering
    );
    // The account changed between the two reads, so they are not a converged pair.
    assert_eq!(
        machine.conclude_pass(&moved, secs(2)),
        ReconciliationState::Recovering
    );
    assert_eq!(machine.confirmations(), 1);
    // The second reading repeated, however, is a converging confirmation.
    assert_eq!(
        machine.conclude_pass(&moved, secs(3)),
        ReconciliationState::Ready
    );
}

#[rstest]
fn test_an_uncertain_finding_never_becomes_ready_however_often_it_repeats() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let mut reading = clean_reading();
    reading.orders = vec![tracked_order(
        OndoOrderStatus::Unknown("settling".to_string()),
        "0.00",
        "0.00",
    )];

    assert_eq!(
        machine.conclude_pass(&reading, secs(1)),
        ReconciliationState::Uncertain
    );
    assert_eq!(
        machine.conclude_pass(&reading, secs(2)),
        ReconciliationState::Uncertain
    );
    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());
}

#[rstest]
fn test_stale_metadata_or_a_disarmed_switch_keeps_a_ready_account_from_trading() {
    let mut machine = recovered_machine();

    assert!(machine.can_submit_new_orders());

    machine.set_metadata(MetadataValidity::Stale {
        reason: "the last metadata refresh failed".to_string(),
    });

    // Ready is a statement about the account, not a licence: the plan's predicate also requires
    // usable metadata and a switch that permits orders.
    assert_eq!(machine.state(), ReconciliationState::Ready);
    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());

    machine.set_metadata(MetadataValidity::Current);

    // A switch this client is required to keep armed, but has not armed, is not a licence either.
    machine.dead_mans_switch_mut().require();

    assert!(!machine.can_submit_new_orders());

    machine.dead_mans_switch_mut().arm(secs(4));
    machine.dead_mans_switch_mut().confirm_armed(secs(4));

    assert!(machine.can_submit_new_orders());
}

#[rstest]
fn test_a_disconnect_after_a_session_refuses_new_risk_until_it_is_recovered_again() {
    let mut machine = recovered_machine();

    machine.note_disconnected(secs(3));

    assert_eq!(machine.state(), ReconciliationState::Disconnected);
    assert!(machine.session_established());
    assert!(machine.refuses_new_risk());

    machine.begin_recovery(secs(4));
    machine.conclude_pass(&clean_reading(), secs(4));

    assert_eq!(machine.state(), ReconciliationState::Recovering);
    assert!(machine.refuses_new_risk());

    machine.conclude_pass(&clean_reading(), secs(5));

    assert_eq!(machine.state(), ReconciliationState::Ready);
    assert!(!machine.refuses_new_risk());
}

#[rstest]
fn test_a_pass_that_could_not_read_the_account_leaves_it_uncertain() {
    let mut machine = recovered_machine();

    machine.begin_recovery(secs(3));
    let state = machine.note_pass_failed("the order history read failed".to_string(), secs(3));

    assert_eq!(state, ReconciliationState::Uncertain);
    assert!(!machine.can_submit_new_orders());
    assert!(
        machine
            .last_judgment()
            .expect("the failed pass is judged")
            .is_uncertain()
    );
}

// ------------------------------------------------------------------------------------------------
// Submissions whose outcome is unknown (plan §6.3)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_lost_submission_answer_is_probed_under_its_own_client_id() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(1),
    );

    let unknown = machine.unknown_submissions();

    assert_eq!(unknown.len(), 1);
    // The reference is the client order id this adapter sent: a probe never invents a new one.
    assert_eq!(unknown[0].lookup, format!("client:{CLIENT_ORDER_ID}"));
    assert_eq!(unknown[0].client_order_id, client_order_id(CLIENT_ORDER_ID));
    assert_eq!(
        machine.probe_due(secs(1)),
        vec![client_order_id(CLIENT_ORDER_ID)]
    );

    assert_eq!(
        machine.note_probe(
            &client_order_id(CLIENT_ORDER_ID),
            ProbeOutcome::Found,
            secs(2)
        ),
        ProbeDisposition::Resolved
    );
    assert!(machine.unknown_submissions().is_empty());
    assert!(machine.probe_due(secs(3)).is_empty());
}

#[rstest]
fn test_a_404_inside_the_window_is_not_evidence_the_order_was_never_submitted() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(1),
    );

    let disposition = machine.note_probe(
        &client_order_id(CLIENT_ORDER_ID),
        ProbeOutcome::NotFound,
        secs(2),
    );

    assert!(matches!(disposition, ProbeDisposition::KeepProbing { .. }));
    assert_eq!(machine.unknown_submissions().len(), 1);
    // The next probe is bounded, and it is still inside the window.
    assert!(machine.probe_due(secs(2)).is_empty());
    assert_eq!(
        machine
            .probe_due(secs(2) + UnixNanos::from(ONDO_SUBMISSION_PROBE_INTERVAL * 1_000_000_000)),
        vec![client_order_id(CLIENT_ORDER_ID)]
    );
}

#[rstest]
fn test_an_inconclusive_probe_keeps_the_submission_unknown() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(1),
    );

    let disposition = machine.note_probe(
        &client_order_id(CLIENT_ORDER_ID),
        ProbeOutcome::Inconclusive {
            reason: "the transport failed".to_string(),
        },
        secs(2),
    );

    assert!(matches!(disposition, ProbeDisposition::KeepProbing { .. }));
    assert_eq!(machine.unknown_submissions().len(), 1);
}

#[rstest]
fn test_thirty_seconds_of_an_unresolved_submission_ends_the_probe_and_keeps_the_ids() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(1),
    );

    // Inside the window the probe keeps going.
    assert!(
        machine
            .expire_unknown_submissions(secs(1 + ONDO_SUBMISSION_UNKNOWN_SECS - 1))
            .is_empty()
    );

    let abandoned = machine.expire_unknown_submissions(secs(1 + ONDO_SUBMISSION_UNKNOWN_SECS));

    assert_eq!(abandoned.len(), 1);
    assert_eq!(
        abandoned[0].client_order_id,
        client_order_id(CLIENT_ORDER_ID)
    );
    assert_eq!(abandoned[0].lookup, format!("client:{CLIENT_ORDER_ID}"));
    assert_eq!(
        abandoned[0].elapsed_ns,
        ONDO_SUBMISSION_UNKNOWN_SECS * 1_000_000_000
    );

    // The probe stops, and the submission stays unknown: plan §6.3 keeps it for a human, and
    // there is no path here that would create the order a second time.
    assert!(
        machine
            .probe_due(secs(1 + ONDO_SUBMISSION_UNKNOWN_SECS))
            .is_empty()
    );
    assert_eq!(machine.unknown_submissions().len(), 1);

    machine
        .clear_unknown_submission(&client_order_id(CLIENT_ORDER_ID))
        .expect("the submission is cleared by hand");

    assert!(machine.unknown_submissions().is_empty());
}

#[rstest]
fn test_an_outstanding_unknown_submission_keeps_a_recovered_account_uncertain() {
    let mut machine = recovered_machine();

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(3),
    );

    let state = machine.conclude_pass(&clean_reading(), secs(3));

    assert_eq!(state, ReconciliationState::Uncertain);
    assert!(!machine.can_submit_new_orders());
    assert!(
        machine
            .last_judgment()
            .expect("the pass is judged")
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::UnknownSubmission { .. }))
    );
}

// ------------------------------------------------------------------------------------------------
// Cancels whose answer was lost (plan §6.3)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_cancel_whose_answer_was_lost_is_confirmed_by_a_query_before_the_account_is_ready() {
    let mut machine = recovered_machine();

    machine.note_unconfirmed_cancel(
        client_order_id(CLIENT_ORDER_ID),
        None,
        "the cancel request was not answered".to_string(),
        secs(3),
    );

    let cancels = machine.unconfirmed_cancels();

    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].client_order_id, client_order_id(CLIENT_ORDER_ID));
    assert_eq!(cancels[0].kind, UncertainKind::Cancel);
    assert_eq!(cancels[0].lookup, format!("client:{CLIENT_ORDER_ID}"));

    let state = machine.conclude_pass(&clean_reading(), secs(3));

    assert_eq!(state, ReconciliationState::Uncertain);
    assert!(!machine.can_submit_new_orders());

    assert!(machine.confirm_cancel(&client_order_id(CLIENT_ORDER_ID)));
    assert!(machine.unconfirmed_cancels().is_empty());

    machine.conclude_pass(&clean_reading(), secs(4));

    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(5)),
        ReconciliationState::Ready
    );
}

/// A cancel is settled by the same bounded probe a submission is, under the reference that
/// identifies it - and settling it returns the account to trading, which is what keeps the gate
/// from being a permanent lock.
#[rstest]
fn test_an_unconfirmed_cancel_is_settled_by_a_probe_and_returns_the_account_to_trading() {
    let mut machine = recovered_machine();

    machine.note_unconfirmed_cancel(
        client_order_id(CLIENT_ORDER_ID),
        Some(VenueOrderId::from(VENUE_ORDER_ID)),
        "the cancel request was not answered".to_string(),
        secs(3),
    );

    assert!(!machine.can_submit_new_orders());
    assert_eq!(
        machine.probe_due(secs(3)),
        vec![client_order_id(CLIENT_ORDER_ID)],
        "the probe is due immediately, not at the end of the window",
    );

    // The venue order id is what a cancel is asked about under: this session already holds it, and
    // it is unambiguous in a way the client order id is not.
    assert_eq!(
        machine
            .uncertain_outcome(&client_order_id(CLIENT_ORDER_ID))
            .map(|outcome| outcome.lookup.clone()),
        Some(VENUE_ORDER_ID.to_string()),
    );

    // A 404 settles nothing, so the cancel is still outstanding and still stops new risk.
    assert!(matches!(
        machine.note_probe(
            &client_order_id(CLIENT_ORDER_ID),
            ProbeOutcome::NotFound,
            secs(4)
        ),
        ProbeDisposition::KeepProbing { .. }
    ));
    assert!(!machine.can_submit_new_orders());
    assert_eq!(machine.unconfirmed_cancels().len(), 1);

    // The venue's own answer settles it.
    assert_eq!(
        machine.note_probe(
            &client_order_id(CLIENT_ORDER_ID),
            ProbeOutcome::Found,
            secs(5)
        ),
        ProbeDisposition::Resolved,
    );
    assert!(machine.unconfirmed_cancels().is_empty());
    assert!(machine.can_submit_new_orders());
}

/// A permit is a decision about a moment, and the moment passes. The events that revoke permission
/// move the machine's generation, so a permit issued before one is refused afterwards even though
/// the account is tradable again by the time the request would go out - which is the race a
/// second, later admission check exists to close.
#[rstest]
fn test_a_permit_issued_before_a_revocation_is_not_valid_afterwards() {
    let mut machine = recovered_machine();
    let permit = machine.admission();

    assert!(permit.is_granted());
    assert_eq!(
        machine.revalidate(&permit),
        permit,
        "a permit with nothing against it is still the current decision",
    );

    machine.note_unknown_submission(
        client_order_id(CLIENT_ORDER_ID),
        "the create request was not answered".to_string(),
        secs(3),
    );

    assert_eq!(
        machine.revalidate(&permit),
        Admission::Refused {
            reason: NewRiskRefusal::UnknownSubmissions {
                client_order_ids: vec![client_order_id(CLIENT_ORDER_ID)],
            },
        },
        "the outcome is named, not merely counted",
    );

    assert_eq!(
        machine.note_probe(
            &client_order_id(CLIENT_ORDER_ID),
            ProbeOutcome::Found,
            secs(4)
        ),
        ProbeDisposition::Resolved,
    );
    assert!(
        machine.can_submit_new_orders(),
        "the venue's answer settles the account",
    );

    // The old permit is still refused: the decision it carries was taken before this client learned
    // something, and `Found` is not evidence that the earlier decision was safe to act on.
    assert_eq!(
        machine.revalidate(&permit),
        Admission::Refused {
            reason: NewRiskRefusal::Superseded,
        },
    );

    let current = machine.admission();

    assert_eq!(
        machine.revalidate(&current),
        current,
        "a decision taken now is the current one",
    );
}

// ------------------------------------------------------------------------------------------------
// The account: positions, balances and out-of-run orders (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_short_position_is_negative_from_the_venues_own_positive_number() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    // The venue documents `netQuantity` as positive with the direction carrying the sign; the
    // mapping, not the reader, applies it.
    let reading = position(PositionDirection::Short, "1.5489", "-1.5489");

    assert_eq!(
        reading.signed,
        rust_decimal::Decimal::from_str_exact("-1.5489").expect("decimal")
    );
    assert!(!reading.is_flat());
    assert!(machine.evaluate(&clean_reading()).is_clean());
}

#[rstest]
fn test_a_short_position_the_venue_already_signed_is_not_negated_twice() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    // Should a response ever carry a signed `netQuantity` with a `short` direction, the value is
    // taken as the venue sent it rather than negated a second time.
    let mut reading = clean_reading();
    reading.positions = vec![position(PositionDirection::Short, "-1.5489", "-1.5489")];

    assert!(!machine.evaluate(&reading).is_uncertain());
    assert_eq!(reading.positions[0].signed.to_string(), "-1.5489");
}

#[rstest]
fn test_a_neutral_position_is_an_explicit_zero_and_not_an_absent_one() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let mut open = clean_reading();
    open.positions = vec![position(PositionDirection::Long, "1.5489", "1.5489")];

    // The first read adopts the venue's own position as the baseline: a position carried into the
    // run is not a discrepancy, it is the starting point.
    assert_eq!(
        machine.conclude_pass(&open, secs(1)),
        ReconciliationState::Recovering
    );

    // The position is closed by a fill this client applied, so the venue's flat state is the one
    // the fills explain rather than a position that vanished unexplained.
    let mut flat = clean_reading();
    flat.positions = vec![position(PositionDirection::Neutral, "0.00", "0.00")];
    flat.applied_net.insert(
        nvda(),
        rust_decimal::Decimal::from_str_exact("-1.5489").expect("decimal"),
    );

    assert_eq!(
        machine.conclude_pass(&flat, secs(2)),
        ReconciliationState::Recovering
    );
    assert_eq!(
        machine.conclude_pass(&flat, secs(3)),
        ReconciliationState::Ready
    );
}

#[rstest]
fn test_a_position_that_vanishes_with_no_fill_to_explain_it_is_uncertain() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let mut open = clean_reading();
    open.positions = vec![position(PositionDirection::Long, "1.5489", "1.5489")];

    assert_eq!(
        machine.conclude_pass(&open, secs(1)),
        ReconciliationState::Recovering
    );

    // Nothing was filled, and the position is gone. That is the disagreement the comparison
    // exists for: a flat venue the client's own fills do not account for.
    let mut vanished = clean_reading();
    vanished.positions = vec![position(PositionDirection::Neutral, "0.00", "0.00")];

    assert_eq!(
        machine.conclude_pass(&vanished, secs(2)),
        ReconciliationState::Uncertain
    );
    assert!(
        machine
            .last_judgment()
            .expect("judged")
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::PositionMismatch { .. }))
    );
}

#[rstest]
fn test_a_position_the_applied_fills_do_not_explain_is_uncertain_and_never_ready() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let flat = clean_reading();

    assert_eq!(
        machine.conclude_pass(&flat, secs(1)),
        ReconciliationState::Recovering
    );

    // This client's fills add 0.5 to the position, the venue's position does not move.
    let mut disagreeing = clean_reading();
    disagreeing.positions = vec![position(PositionDirection::Neutral, "0.00", "0.00")];
    disagreeing.applied_net.insert(
        nvda(),
        rust_decimal::Decimal::from_str_exact("0.5").expect("decimal"),
    );

    assert_eq!(
        machine.conclude_pass(&disagreeing, secs(2)),
        ReconciliationState::Uncertain
    );
    assert_eq!(
        machine.conclude_pass(&disagreeing, secs(3)),
        ReconciliationState::Uncertain
    );

    let judgment = machine.last_judgment().expect("the pass is judged");

    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::PositionMismatch { instrument_id, .. } if *instrument_id == nvda()
    )));
}

// ------------------------------------------------------------------------------------------------
// A position the venue stops listing (F04)
// ------------------------------------------------------------------------------------------------

/// A reading carrying the long NVDA position this client has carried in, or holds.
fn long_position(net_quantity: &str) -> AccountReading {
    let mut reading = clean_reading();
    reading.positions = vec![position(
        PositionDirection::Long,
        net_quantity,
        net_quantity,
    )];

    reading
}

/// A recovery whose first two passes read `carried`, so the account is Ready with its baseline
/// adopted.
fn machine_holding(carried: &AccountReading) -> ReconciliationMachine {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    assert_eq!(
        machine.conclude_pass(carried, secs(1)),
        ReconciliationState::Recovering
    );
    assert_eq!(
        machine.conclude_pass(carried, secs(2)),
        ReconciliationState::Ready
    );

    machine
}

/// F04: the venue documents its position list as the whole set of open positions, so an instrument
/// missing from a read that succeeded - and whose every row this adapter could read - is the venue
/// stating the position is gone. Comparing the listed rows alone never reads that statement: the
/// position is neither reported nor retired, and the carried-in expectation the baseline still
/// holds is one nothing can ever reconcile.
#[rstest]
fn test_a_position_the_venue_stops_listing_is_reported_and_its_baseline_retired() {
    let carried = long_position("1.5489");
    let mut machine = machine_holding(&carried);

    assert_eq!(
        machine.position_baseline().get(&nvda()).copied(),
        Some(rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")),
        "the first pass adopts what the account carried in",
    );

    // The venue's next complete read does not carry that market. The position was closed - or
    // liquidated - at the venue, and no fill of this client's accounts for it.
    let gone = clean_reading();

    assert_eq!(
        machine.conclude_pass(&gone, secs(3)),
        ReconciliationState::Uncertain,
        "the account was Ready, and a position it expected is not one the venue carries",
    );
    assert!(!machine.can_submit_new_orders());

    let judgment = machine.last_judgment().expect("the pass is judged");

    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::PositionAbsent { instrument_id, expected }
            if *instrument_id == nvda()
                && *expected == rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")
    )));

    // The carried-in expectation is retired with the finding, and retired means the entry leaves
    // the map rather than being zeroed: a stale entry that survives is the original defect in
    // another form, and one that could never be retired would leave the account uncertain for good
    // over a position the venue has already closed.
    assert_eq!(
        machine.position_baseline().get(&nvda()).copied(),
        None,
        "the retired entry is gone, not zeroed",
    );
    assert!(
        machine.position_baseline().is_empty(),
        "nothing else was left behind either",
    );

    assert_eq!(
        machine.conclude_pass(&gone, secs(4)),
        ReconciliationState::Recovering,
        "the second read of a flat venue is a reading the retirement accounts for",
    );
    assert!(
        machine.last_judgment().expect("judged").is_clean(),
        "the disappearance is reported once, not on every pass that follows it",
    );
}

/// F04: a position this client's own fills closed out is not a position that vanished. The venue's
/// flat statement is the one those fills explain, so nothing is reported - and the carried-in entry
/// that makes them net to flat is left where it is. Retiring it would turn the closing fills into
/// an unexplained net no later pass could clear.
#[rstest]
fn test_a_position_this_clients_own_fills_closed_is_not_one_that_vanished() {
    let carried = long_position("1.5489");
    let mut machine = machine_holding(&carried);

    let sold = rust_decimal::Decimal::from_str_exact("-1.5489").expect("decimal");
    let mut closed = clean_reading();
    closed.applied_net.insert(nvda(), sold);

    assert_eq!(
        machine.conclude_pass(&closed, secs(3)),
        ReconciliationState::Ready,
        "a Ready account that reads clean again stays Ready, and this read is clean",
    );
    assert!(
        machine.last_judgment().expect("judged").is_clean(),
        "the venue is flat and the fills this client applied are what made it flat",
    );
    assert_eq!(
        machine.position_baseline().get(&nvda()).copied(),
        Some(rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")),
        "the entry the closing fills net against is not retired: it is still carrying the entry",
    );

    // And it is still the entry a later position on the same instrument is measured against.
    let mut reopened = clean_reading();
    reopened.positions = vec![position(PositionDirection::Long, "0.5", "0.5")];
    reopened.applied_net.insert(
        nvda(),
        rust_decimal::Decimal::from_str_exact("-1.0489").expect("decimal"),
    );

    assert_eq!(
        machine.conclude_pass(&reopened, secs(5)),
        ReconciliationState::Ready,
        "the account trades without leaving Ready: an account that trades is not an account that is \
         unrecovered",
    );
    assert!(
        machine.last_judgment().expect("judged").is_clean(),
        "1.5489 carried in, 0.5 bought since, and the venue states 0.5",
    );
}

/// F04: a liquidation is the same event as any other external close for this judgment, and what
/// matters about it is the state it leaves the account in - out of [`ReconciliationState::Ready`],
/// and refusing new risk until a pass has read the account whole again.
#[rstest]
fn test_a_liquidation_leaves_ready_and_stops_new_risk() {
    let carried = long_position("1.5489");
    let mut machine = machine_holding(&carried);

    assert!(machine.can_submit_new_orders());

    // The account was carried in long, and the venue's next read is flat with nothing filled:
    // the position was liquidated at the venue rather than closed by this client.
    let mut liquidated = clean_reading();
    liquidated.balance = Some(balance("5000.00", "4400.00", "0.00", "4400.00", "4400.00"));

    assert_eq!(
        machine.conclude_pass(&liquidated, secs(3)),
        ReconciliationState::Uncertain
    );
    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());
    assert_eq!(machine.state(), ReconciliationState::Uncertain);
}

/// F04: an explicit `neutral` row and a missing row are two different statements, and the account
/// keeps them apart. One is the venue stating the position is flat while it listed the instrument;
/// the other is the venue not carrying the instrument at all.
#[rstest]
fn test_a_flat_row_and_an_absent_row_are_different_statements() {
    let carried = long_position("1.5489");

    // The venue listed the instrument and said it is flat.
    let mut stated_flat = machine_holding(&carried);
    let mut flat_row = clean_reading();
    flat_row.positions = vec![position(PositionDirection::Neutral, "0.00", "0.00")];

    assert_eq!(
        stated_flat.conclude_pass(&flat_row, secs(3)),
        ReconciliationState::Uncertain
    );

    let findings = stated_flat
        .last_judgment()
        .expect("judged")
        .clone()
        .findings;

    assert!(findings.iter().any(|finding| matches!(
        finding,
        Finding::PositionMismatch { venue, expected, .. }
            if venue.is_zero() && *expected == rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")
    )));
    assert!(
        !findings
            .iter()
            .any(|finding| matches!(finding, Finding::PositionAbsent { .. })),
        "the venue carried the instrument: nothing about it is absent",
    );

    // The venue did not carry the instrument at all.
    let mut absent = machine_holding(&carried);

    assert_eq!(
        absent.conclude_pass(&clean_reading(), secs(3)),
        ReconciliationState::Uncertain
    );

    let findings = absent.last_judgment().expect("judged").clone().findings;

    assert!(findings.iter().any(|finding| matches!(
        finding,
        Finding::PositionAbsent { instrument_id, .. } if *instrument_id == nvda()
    )));
    assert!(
        !findings
            .iter()
            .any(|finding| matches!(finding, Finding::PositionMismatch { .. })),
        "the venue stated nothing about this instrument, so no stated position disagrees",
    );
}

/// F04: an account with nothing in it - no positions, no baseline, no fills - is clean. Coverage of
/// an empty list is not in doubt, and a machine that treated the empty list as unverified would
/// never leave recovery at all.
#[rstest]
fn test_an_account_with_nothing_in_it_is_clean() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let judgment = machine.evaluate(&clean_reading());

    assert!(judgment.is_clean());
    assert!(!judgment.is_uncertain());
    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(1)),
        ReconciliationState::Recovering
    );
    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(2)),
        ReconciliationState::Ready
    );
}

/// F04: coverage is established, never assumed. A read carrying a row this adapter cannot map is
/// not a complete list of the account's positions, so the instruments it does *not* mention prove
/// nothing: they are neither reported as gone nor retired, and the account is uncertain until a
/// read it can read whole arrives.
#[rstest]
fn test_a_row_that_cannot_be_read_is_not_a_statement_about_the_rows_that_are_missing() {
    let carried = long_position("1.5489");
    let mut machine = machine_holding(&carried);

    // A market this adapter has no instrument for, and a direction it cannot read.
    let mut unreadable = clean_reading();
    unreadable.positions = vec![
        PositionReading {
            market: "SOMETHING-USD".to_string(),
            instrument_id: None,
            direction: PositionDirection::Long,
            net_quantity: rust_decimal::Decimal::ONE,
            signed: rust_decimal::Decimal::ONE,
        },
        PositionReading::new(
            NVDA_MARKET,
            "sideways",
            rust_decimal::Decimal::from_str_exact("4.00").expect("decimal"),
        ),
    ];

    assert_eq!(
        machine.conclude_pass(&unreadable, secs(3)),
        ReconciliationState::Uncertain
    );

    let findings = machine.last_judgment().expect("judged").clone().findings;

    assert!(findings.iter().any(|finding| matches!(
        finding,
        Finding::UnmappablePosition { market } if market == "SOMETHING-USD"
    )));
    assert!(findings.iter().any(|finding| matches!(
        finding,
        Finding::UnreadablePosition { direction, .. } if direction == "sideways"
    )));
    assert!(
        !findings
            .iter()
            .any(|finding| matches!(finding, Finding::PositionAbsent { .. })),
        "a list with a row this adapter could not read is not evidence about the rest",
    );
    assert_eq!(
        machine.position_baseline().get(&nvda()).copied(),
        Some(rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")),
        "the carried-in expectation is left where it is, not retired on a read that proved nothing",
    );

    // The same position, read by a pass that can read every row: nothing was lost, so the account
    // converges on the reading it was already holding.
    assert_eq!(
        machine.conclude_pass(&carried, secs(4)),
        ReconciliationState::Recovering
    );
    assert!(machine.last_judgment().expect("judged").is_clean());
}

/// F04: a pass that could not read the account at all has not seen the positions, so it has seen
/// no absence either. The baseline is left exactly as it was, and the account is uncertain.
#[rstest]
fn test_a_pass_that_could_not_read_leaves_the_baseline_alone() {
    let carried = long_position("1.5489");
    let mut machine = machine_holding(&carried);

    assert_eq!(
        machine.note_pass_failed(
            "the positions could not be read: connection reset".to_string(),
            secs(3)
        ),
        ReconciliationState::Uncertain,
    );
    assert_eq!(
        machine.position_baseline().get(&nvda()).copied(),
        Some(rust_decimal::Decimal::from_str_exact("1.5489").expect("decimal")),
        "a read that failed is not a venue stating the position is gone",
    );

    let judgment = machine.last_judgment().expect("the failure is judged");

    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::ReadFailed { reason } if reason.contains("connection reset")
    )));
}

/// F04: fills applied before the first pass adopted the baseline are judged too. The first pass
/// calibrates what the venue *listed*; an instrument it does not list has no carried-in position to
/// adopt, so the expectation is what this client's own fills say - and a position those fills built
/// and the venue does not carry is exactly the disagreement an account is not clean with.
#[rstest]
fn test_fills_applied_before_the_first_pass_are_judged_and_not_skipped() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let mut filled = clean_reading();
    filled.applied_net.insert(
        nvda(),
        rust_decimal::Decimal::from_str_exact("0.5").expect("decimal"),
    );

    assert_eq!(
        machine.conclude_pass(&filled, secs(1)),
        ReconciliationState::Uncertain,
        "the account is read whole and the venue does not carry the position the fills built",
    );

    let judgment = machine.last_judgment().expect("judged");

    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::PositionAbsent { instrument_id, expected }
            if *instrument_id == nvda()
                && *expected == rust_decimal::Decimal::from_str_exact("0.5").expect("decimal")
    )));

    // Once the venue carries it, the two readings agree and the account converges.
    let mut listed = filled.clone();
    listed.positions = vec![position(PositionDirection::Long, "0.5", "0.5")];

    assert_eq!(
        machine.conclude_pass(&listed, secs(2)),
        ReconciliationState::Recovering
    );
    assert_eq!(
        machine.conclude_pass(&listed, secs(3)),
        ReconciliationState::Ready
    );
}

#[rstest]
fn test_an_order_whose_status_is_unknown_leaves_the_account_uncertain() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.orders = vec![tracked_order(
        OndoOrderStatus::Unknown("settling".to_string()),
        "0.00",
        "0.00",
    )];

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(!judgment.is_clean());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::UnresolvedOrder { reason, .. } if reason.contains("settling")
    )));
}

#[rstest]
fn test_an_order_the_fills_do_not_add_up_to_leaves_the_account_uncertain() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    // The venue says the order filled 0.50; the fills this client applied total 0.20. That is the
    // account the reconciliation read must not call clean (plan §6.3).
    let mut reading = clean_reading();
    reading.orders = vec![tracked_order(OndoOrderStatus::FullyFilled, "0.50", "0.20")];

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::UnresolvedOrder { reason, .. } if reason.contains("filledSize")
    )));
}

#[rstest]
fn test_a_terminal_order_with_no_readable_filled_quantity_is_not_confirmed() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    // The venue says the order ended and sends no readable `filledSize`. The status alone is not
    // enough: what the order filled is exactly what cannot then be checked, so it stays unresolved.
    let mut reading = clean_reading();
    reading.orders = vec![OrderReading {
        venue_order_id: VENUE_ORDER_ID.to_string(),
        client_order_id: Some(CLIENT_ORDER_ID.to_string()),
        market: NVDA_MARKET.to_string(),
        status: OndoOrderStatus::FullyFilled,
        venue_filled: None,
        applied_filled: Some(rust_decimal::Decimal::ZERO),
        tracked: true,
    }];

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::UnresolvedOrder { reason, .. } if reason.contains("filledSize")
    )));
}

#[rstest]
fn test_an_order_this_run_did_not_create_is_preserved_and_never_cancelled() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.orders = vec![OrderReading {
        venue_order_id: "abcd0000000000000000000000000001".to_string(),
        client_order_id: Some("placed_by_hand_1".to_string()),
        market: NVDA_MARKET.to_string(),
        status: OndoOrderStatus::Open,
        venue_filled: Some(rust_decimal::Decimal::ZERO),
        applied_filled: None,
        tracked: false,
    }];

    let judgment = machine.evaluate(&reading);

    // It is identified rather than dropped, and it is not this adapter's to take over: the account
    // is not clean, and no finding asks for a cancel.
    assert!(!judgment.is_clean());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::ForeignOrder { client_order_id, .. }
            if client_order_id.as_deref() == Some("placed_by_hand_1")
    )));
    // A readable foreign order is a known state: it does not by itself make the account unknown.
    assert!(!judgment.is_uncertain());
}

#[rstest]
fn test_an_untriggered_foreign_order_is_an_unknown_state_the_adapter_does_not_create() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.orders = vec![OrderReading {
        venue_order_id: "abcd0000000000000000000000000002".to_string(),
        client_order_id: None,
        market: NVDA_MARKET.to_string(),
        status: OndoOrderStatus::Untriggered,
        venue_filled: Some(rust_decimal::Decimal::ZERO),
        applied_filled: None,
        tracked: false,
    }];

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
}

#[rstest]
fn test_a_balance_carries_every_documented_member_distinctly_and_adds_up() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.balance = Some(balance(
        "5000.00", "4950.00", "1125.00", "3825.00", "3825.00",
    ));

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_clean());

    let mapped = machine
        .last_balance()
        .expect("the balance is mapped")
        .clone();

    assert_eq!(mapped.total().to_string(), "4950.00");
    assert_eq!(mapped.locked().to_string(), "1125.00");
    assert_eq!(mapped.free().to_string(), "3825.00");
    // `total = free + locked` is verified against the venue's own numbers, not asserted.
    assert_eq!(mapped.total(), mapped.free() + mapped.locked());
    // The two members the Nautilus balance cannot carry are kept, not folded in.
    assert_eq!(
        mapped.reading().wallet_balance,
        Some(rust_decimal::Decimal::from_str_exact("5000.00").expect("decimal"))
    );
    assert_eq!(
        mapped.reading().withdrawable_margin,
        Some(rust_decimal::Decimal::from_str_exact("3825.00").expect("decimal"))
    );
}

#[rstest]
fn test_a_balance_whose_own_numbers_do_not_add_up_is_uncertain() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    // 4950.00 != 1125.00 + 1000.00
    reading.balance = Some(balance(
        "5000.00", "4950.00", "1125.00", "1000.00", "1000.00",
    ));

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(
        judgment
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::BalanceInconsistent { .. }))
    );
}

#[rstest]
fn test_multi_collateral_is_reported_as_unsupported_rather_than_folded_in() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    let mut multi = balance("5000.00", "4950.00", "1125.00", "3825.00", "3825.00");
    multi.unmapped = vec![("USDT".to_string(), "250.00".to_string())];
    reading.balance = Some(multi);

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::UnsupportedCollateral { member, .. } if member == "USDT"
    )));
}

#[rstest]
fn test_negative_equity_is_kept_raw_and_stops_new_risk_rather_than_clamped_to_zero() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    // Equity is negative: -1250.00 = 100.00 locked + -1350.00 free.
    reading.balance = Some(balance("100.00", "-1250.00", "100.00", "-1350.00", "0.00"));

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(
        judgment
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::NegativeEquity { .. }))
    );
    assert_eq!(
        machine
            .last_balance()
            .expect("the balance is still mapped")
            .total()
            .to_string(),
        "-1250.00"
    );
}

#[rstest]
fn test_a_balance_with_no_member_the_mapping_can_use_is_not_a_zero_balance() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.balance = Some(BalanceReading {
        wallet_balance: None,
        margin_balance: None,
        used_margin: None,
        available_margin: None,
        withdrawable_margin: None,
        unmapped: Vec::new(),
        raw: r#"{"somethingElse":"1.00"}"#.to_string(),
    });

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(
        judgment
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::BalanceUnreadable { .. }))
    );
    assert!(machine.last_balance().is_none());
}

#[rstest]
fn test_a_position_on_a_market_that_does_not_map_is_uncertain() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    let mut reading = clean_reading();
    reading.positions = vec![PositionReading {
        market: "SOMETHING-USD.P".to_string(),
        instrument_id: None,
        direction: PositionDirection::Long,
        net_quantity: rust_decimal::Decimal::ONE,
        signed: rust_decimal::Decimal::ONE,
    }];

    let judgment = machine.evaluate(&reading);

    assert!(judgment.is_uncertain());
    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::UnmappablePosition { market } if market == "SOMETHING-USD.P"
    )));
}

// ------------------------------------------------------------------------------------------------
// The dedup ledger across a restart (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_restored_ledger_still_dedupes_the_fills_it_holds() {
    use nautilus_ondo::execution::OndoFillLedger;

    let mut ledger = OndoFillLedger::new();

    assert!(ledger.record(account_id(), "fill-1"));
    assert!(ledger.record(account_id(), "fill-2"));

    let journal = LedgerJournal::from_ledger(&ledger, account_id(), Some(secs(120)));
    let text = journal.to_json().expect("the journal serializes");

    // A restart: the process is new, the ledger is empty, and the journal is what survives.
    let restored = LedgerJournal::from_json(&text).expect("the journal parses");
    let mut fresh = OndoFillLedger::new();

    assert_eq!(
        restored
            .restore(&mut fresh, account_id())
            .expect("the journal is this account's"),
        2
    );
    assert_eq!(fresh.len(), 2);
    // The point of the ledger: a fill that was applied before the restart is not applied again.
    assert!(!fresh.record(account_id(), "fill-1"));
    assert!(fresh.record(account_id(), "fill-3"));
    assert_eq!(restored.watermark(), Some(secs(120)));
}

#[rstest]
fn test_a_journal_from_another_account_is_refused_rather_than_merged() {
    use nautilus_ondo::execution::OndoFillLedger;

    let mut ledger = OndoFillLedger::new();

    ledger.record(account_id(), "fill-1");

    let journal = LedgerJournal::from_ledger(&ledger, account_id(), None);
    let mut fresh = OndoFillLedger::new();

    let error = journal
        .restore(&mut fresh, AccountId::from("ONDO-SANDBOX-002"))
        .expect_err("another account's journal is not this account's");

    assert!(error.to_string().contains("ONDO-SANDBOX-002"));
    assert!(fresh.is_empty());
}

// ------------------------------------------------------------------------------------------------
// The dead man's switch (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_switch_frame_is_the_documented_channel_and_timeout() {
    let mut switch = DeadMansSwitch::new(30);

    let frame = switch.arm(secs(1));

    assert_eq!(
        frame,
        DeadMansSwitchMessage {
            op: nautilus_ondo::websocket::WsOp::Subscribe,
            channel: ONDO_DMS_CHANNEL.to_string(),
            timeout_seconds: 30,
        }
    );
    assert_eq!(
        serde_json::to_string(&frame).expect("the frame serializes"),
        r#"{"op":"subscribe","channel":"cancelAllOrdersAfterPerps","timeout_seconds":30}"#
    );
}

#[rstest]
fn test_the_switch_permits_orders_only_once_the_venue_has_confirmed_it() {
    let mut switch = DeadMansSwitch::new(30);

    // A switch nobody asked for does not govern this client.
    assert!(!switch.is_required());
    assert!(switch.permits_new_orders());

    switch.arm(secs(1));

    // Asked for, sent, and not yet acknowledged: the plan requires a confirmed arm before an order.
    assert!(switch.is_required());
    assert_eq!(switch.state(), DeadMansSwitchState::Arming);
    assert!(!switch.permits_new_orders());

    switch.confirm_armed(secs(2));

    assert_eq!(switch.state(), DeadMansSwitchState::Armed);
    assert!(switch.permits_new_orders());
    assert_eq!(switch.expires_at(), Some(secs(2 + 30)));
}

#[rstest]
fn test_a_renewal_moves_the_deadline_and_repeats_the_arm_frame() {
    let mut switch = DeadMansSwitch::new(30);

    let armed = switch.arm(secs(1));

    switch.confirm_armed(secs(1));

    assert_eq!(switch.expires_at(), Some(secs(31)));
    assert!(!switch.has_expired(secs(30)));

    // The renewal is the subscribe frame re-sent. **Which message actually renews the venue's
    // switch is unverified**: the frozen material documents the channel and the timeout, not the
    // renewal, so this asserts only what this adapter would send.
    let renewal = switch.renew(secs(20)).expect("an armed switch renews");

    assert_eq!(renewal, armed);
    assert_eq!(switch.expires_at(), Some(secs(50)));
    assert_eq!(switch.renewals(), 1);
    assert!(!switch.has_expired(secs(49)));
    assert!(switch.has_expired(secs(50)));
}

#[rstest]
fn test_a_renewal_of_a_switch_that_was_never_armed_is_not_sent() {
    let mut switch = DeadMansSwitch::new(30);

    assert!(switch.renew(secs(1)).is_none());

    switch.arm(secs(1));
    // Arming but unacknowledged: there is no deadline to move yet.
    assert!(switch.renew(secs(2)).is_none());
}

#[rstest]
fn test_a_failed_switch_stops_new_orders() {
    let mut machine = recovered_machine();

    assert!(machine.can_submit_new_orders());

    machine.dead_mans_switch_mut().require();
    machine.dead_mans_switch_mut().arm(secs(4));
    machine
        .dead_mans_switch_mut()
        .fail("the venue refused the subscribe frame".to_string());

    assert_eq!(
        machine.dead_mans_switch().state(),
        DeadMansSwitchState::Failed {
            reason: "the venue refused the subscribe frame".to_string()
        }
    );
    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());
}

#[rstest]
fn test_a_switch_that_fired_stops_new_risk_and_does_not_report_a_position_flat() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);

    machine.set_metadata(MetadataValidity::Current);
    machine.begin_recovery(secs(1));

    let mut open = clean_reading();
    open.positions = vec![position(PositionDirection::Long, "1.5489", "1.5489")];

    machine.conclude_pass(&open, secs(1));
    machine.conclude_pass(&open, secs(2));
    machine.dead_mans_switch_mut().arm(secs(2));
    machine.dead_mans_switch_mut().confirm_armed(secs(2));

    assert!(machine.can_submit_new_orders());

    // The switch fires: the venue cancels every resting order. It does **not** close a position,
    // so the position the last read saw is still there until a read says otherwise.
    machine.note_switch_fired(secs(40));

    assert!(!machine.can_submit_new_orders());
    assert!(machine.refuses_new_risk());
    assert_eq!(machine.state(), ReconciliationState::Uncertain);

    let position = machine
        .last_reading()
        .expect("the last reading is kept")
        .positions
        .first()
        .expect("the position was read");

    assert!(!position.is_flat());
    assert_eq!(position.signed.to_string(), "1.5489");
}

#[rstest]
fn test_releasing_the_switch_sends_the_unsubscribe_frame_and_leaves_the_state_unknown() {
    let mut switch = DeadMansSwitch::new(30);

    switch.arm(secs(1));
    switch.confirm_armed(secs(1));

    let frame = switch.release(secs(5));

    assert_eq!(frame.op, nautilus_ondo::websocket::WsOp::Unsubscribe);
    assert_eq!(frame.channel, ONDO_DMS_CHANNEL);
    assert_eq!(switch.state(), DeadMansSwitchState::NotRequired);
    // A released switch permits orders again, which is why the stop sequence cancels this run's
    // orders *before* it releases anything.
    assert!(switch.permits_new_orders());
}

// ------------------------------------------------------------------------------------------------
// The buffer of reports that arrived while the account was being read (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_buffer_dedupes_by_id_and_keeps_arrival_order() {
    let mut buffer = ReconciliationBuffer::new();

    assert!(buffer.record_fill(
        fill_from(&api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")),
        RECOVERY,
    ));
    // The same fill delivered twice is held once: a fill is identified by the venue's own fill id.
    assert!(!buffer.record_fill(
        fill_from(&api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")),
        RECOVERY,
    ));
    assert!(buffer.record_fill(
        fill_from(&api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3")),
        RECOVERY,
    ));
    assert!(buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.20")),
        RECOVERY,
    ));

    assert_eq!(buffer.len(), 3);

    let drained = buffer.drain_generation(RECOVERY);

    assert_eq!(drained.orders.len(), 1);
    assert_eq!(
        drained
            .fills
            .iter()
            .map(OndoApiFill::id)
            .collect::<Vec<_>>(),
        vec!["fill-1", "fill-2"]
    );
    assert_eq!(drained.superseded, 0);
    assert!(buffer.is_empty());
}

#[rstest]
fn test_the_buffer_holds_every_update_of_one_order_in_arrival_order() {
    let mut buffer = ReconciliationBuffer::new();

    // One order's reports are a sequence, not a set: the venue worked the order between them, and
    // which of the three is the order's state is exactly what the arrival order decides.
    assert!(buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")),
        RECOVERY,
    ));
    assert!(buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.50")),
        RECOVERY,
    ));
    assert!(buffer.record_order(
        order_from(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "canceled",
            "0.50"
        )),
        RECOVERY,
    ));
    // The first update delivered a second time is the same fact, not a fourth: it is the one report
    // of this order the buffer refuses.
    assert!(!buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")),
        RECOVERY,
    ));

    assert_eq!(buffer.order_count(), 3, "all three updates are held");

    let drained = buffer.drain_generation(RECOVERY);

    assert_eq!(
        drained
            .orders
            .iter()
            .map(|order| (
                order.status().as_str().to_string(),
                order.filled_size().to_string()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("open".to_string(), "0.00".to_string()),
            ("open".to_string(), "0.50".to_string()),
            ("canceled".to_string(), "0.50".to_string()),
        ],
        "the sequence is replayed in the order it arrived, so the last one is the order's state",
    );
}

#[rstest]
fn test_the_buffer_refuses_reports_from_a_superseded_recovery_and_counts_them() {
    let mut buffer = ReconciliationBuffer::new();

    assert!(buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")),
        RECOVERY,
    ));
    assert!(buffer.record_fill(
        fill_from(&api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")),
        RECOVERY,
    ));

    // The next pass is reconciling a recovery the reports above were not recorded for.
    let drained = buffer.drain_generation(RECOVERY + 1);

    assert!(
        drained.orders.is_empty(),
        "a superseded report is not applied"
    );
    assert!(drained.fills.is_empty());
    assert_eq!(
        drained.superseded, 2,
        "and it is counted, not dropped quietly"
    );
    assert!(buffer.is_empty());
}

#[rstest]
fn test_a_buffer_that_is_full_refuses_reports_and_counts_them() {
    let mut buffer = ReconciliationBuffer::with_capacity(2);

    assert!(buffer.record_order(
        order_from(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")),
        RECOVERY,
    ));
    assert!(buffer.record_fill(
        fill_from(&api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")),
        RECOVERY,
    ));
    assert_eq!(buffer.take_dropped(), 0);

    // The buffer is bounded, and the report it cannot hold is a fact about the account this client
    // saw and does not have - which is a count the caller reports, never a silent discard.
    assert!(!buffer.record_fill(
        fill_from(&api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3")),
        RECOVERY,
    ));
    assert_eq!(buffer.take_dropped(), 1);
    assert_eq!(
        buffer.take_dropped(),
        0,
        "the count is taken, not read twice"
    );

    // A duplicate is not a drop: the buffer refuses it because it already holds that fact.
    assert!(!buffer.record_fill(
        fill_from(&api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")),
        RECOVERY,
    ));
    assert_eq!(buffer.take_dropped(), 0);
}

#[rstest]
fn test_a_recovery_generation_changes_when_a_recovery_starts_or_a_session_ends() {
    let mut machine = ReconciliationMachine::new(account_id(), 30);
    let start = machine.recovery_generation();

    machine.begin_recovery(secs(1));
    let first = machine.recovery_generation();

    // The same recovery, begun again: the reports of the recovery in flight are still its own.
    machine.begin_recovery(secs(2));

    assert_eq!(machine.recovery_generation(), first);

    // A session that ends, and the recovery that follows it, are a different read of the account.
    machine.note_disconnected(secs(3));

    assert_ne!(machine.recovery_generation(), first);

    machine.begin_recovery(secs(4));

    assert_ne!(machine.recovery_generation(), start);
    assert_eq!(machine.recovery_generation(), first.wrapping_add(2));
}

#[rstest]
fn test_reports_the_recovery_could_not_apply_cost_a_bounded_re_read_rather_than_a_silent_drop() {
    let mut machine = recovered_machine();

    assert!(machine.can_submit_new_orders());

    machine.note_lost_reports(3, "the recovery buffer refused 3 more".to_string());

    // The loss stops new risk at the instant it happens: a converged account with reports missing
    // from it is not an account that has been read.
    assert_eq!(machine.state(), ReconciliationState::Uncertain);
    assert!(!machine.can_submit_new_orders(), "a loss is not tradable");

    // The pass that reads the account judges the loss with everything else, and judging it is what
    // clears it: what keeps the account uncertain afterwards is that judgment, not the record.
    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(3)),
        ReconciliationState::Uncertain,
    );

    let judged = machine.last_judgment().expect("the pass is judged");

    assert!(judged.is_uncertain());
    assert!(judged.findings.iter().any(|finding| matches!(
        finding,
        Finding::LostReports { count: 3, reason } if reason.contains("refused 3 more")
    )));

    // Ready takes two agreeing passes again, and the pass that carried the loss is not one of them:
    // the re-read is bounded, and it is a re-read.
    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(4)),
        ReconciliationState::Recovering,
        "the loss is not judged twice",
    );
    assert!(
        !machine.can_submit_new_orders(),
        "one pass is not a recovery"
    );

    assert_eq!(
        machine.conclude_pass(&clean_reading(), secs(5)),
        ReconciliationState::Ready,
    );
    assert!(machine.can_submit_new_orders());
}

// ------------------------------------------------------------------------------------------------
// Scripted mock HTTP server (the harness `tests/execution.rs` establishes)
// ------------------------------------------------------------------------------------------------

/// One request as the mock server saw it.
#[derive(Debug, Clone)]
struct CapturedRequest {
    method: String,
    target: String,
    body: String,
}

/// One scripted reply. The last reply of a script is sticky, so a script of one reply answers every
/// connection the same way.
#[derive(Debug, Clone)]
enum Reply {
    Answer { status: u16, body: String },
}

impl Reply {
    fn ok(body: impl Into<String>) -> Self {
        Self::Answer {
            status: 200,
            body: body.into(),
        }
    }

    fn answer(status: u16, body: impl Into<String>) -> Self {
        Self::Answer {
            status,
            body: body.into(),
        }
    }
}

#[derive(Debug)]
struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    handle: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockServer {
    async fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock server");
        let addr = listener.local_addr().expect("read the mock server address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let script = Arc::new(Mutex::new(VecDeque::from(script)));

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let script = Arc::clone(&script);
                let seen = Arc::clone(&seen);

                tokio::spawn(async move {
                    serve_connection(stream, script, seen).await;
                });
            }
        });

        Self {
            addr,
            requests,
            handle,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn captured(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .expect("mock server request log")
            .clone()
    }

    fn targets(&self) -> Vec<String> {
        self.captured()
            .into_iter()
            .map(|request| request.target)
            .collect()
    }

    /// The requests the venue would have answered with this method.
    fn with_method(&self, method: &str) -> Vec<CapturedRequest> {
        self.captured()
            .into_iter()
            .filter(|request| request.method == method)
            .collect()
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    script: Arc<Mutex<VecDeque<Reply>>>,
    seen: Arc<Mutex<Vec<CapturedRequest>>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(request) = parse_request(&buffer) {
            // The execution client's private transport is pointed at this same address (see
            // `build_harness`), and a WebSocket upgrade is not one of this harness's scripted REST
            // replies: answering it from the script would shift every reply the test scripted. It
            // is refused here and consumes nothing, which is what "this harness serves REST only"
            // means - and it is why the private session stays unauthenticated in these tests.
            if is_websocket_upgrade(&buffer) {
                write_response(&mut stream, 400, r#"{"success":false}"#).await;

                return;
            }

            seen.lock().expect("mock server request log").push(request);
            break;
        }
    }

    let reply = {
        let mut script = script.lock().expect("mock server script");
        if script.len() > 1 {
            script.pop_front()
        } else {
            script.front().cloned()
        }
    };

    match reply {
        Some(Reply::Answer { status, body }) => write_response(&mut stream, status, &body).await,
        None => drop(stream),
    }
}

/// Whether one request asks for a WebSocket upgrade rather than being one of this harness's REST
/// calls.
fn is_websocket_upgrade(buffer: &[u8]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let head = text.split("\r\n\r\n").next().unwrap_or_default();

    head.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.trim().eq_ignore_ascii_case("websocket")
        })
    })
}

/// Parses one HTTP/1.1 request once its body is complete.
fn parse_request(buffer: &[u8]) -> Option<CapturedRequest> {
    let text = String::from_utf8_lossy(buffer);
    let (head, rest) = text.split_once("\r\n\r\n")?;

    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _value)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    if rest.len() < content_length {
        return None;
    }

    Some(CapturedRequest {
        method,
        target,
        body: rest[..content_length].to_string(),
    })
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    };

    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Wraps a `result` payload in the `GenericResponse` envelope every documented endpoint uses.
fn envelope(result: &str) -> String {
    format!(r#"{{"success":true,"result":{result}}}"#)
}

/// A page of orders and the cursor that ends the walk.
fn orders_page(orders: &[String], cursor: Option<&str>) -> String {
    let items = orders.join(",");
    match cursor {
        Some(cursor) => format!(r#"{{"success":true,"result":[{items}],"cursor":"{cursor}"}}"#),
        None => format!(r#"{{"success":true,"result":[{items}]}}"#),
    }
}

/// A page of fills and the cursor that ends the walk.
fn fills_page(fills: &[String], cursor: Option<&str>) -> String {
    let items = fills.join(",");
    match cursor {
        Some(cursor) => format!(r#"{{"success":true,"result":[{items}],"cursor":"{cursor}"}}"#),
        None => format!(r#"{{"success":true,"result":[{items}]}}"#),
    }
}

/// One `ApiPosition` in the documented shape.
fn position_json(direction: &str, net_quantity: &str) -> String {
    format!(
        r#"{{"market":"{NVDA_MARKET}","direction":"{direction}","netQuantity":"{net_quantity}","averageEntryPrice":"225.00","usedMargin":"1125.00","unrealizedPnl":"25.00","markPrice":"227.50","liquidationPrice":"180.00","bankruptcyPrice":"170.00","maintenanceMargin":"112.50","notionalValue":"2275.00","leverage":"2.0","netFundingSinceNeutral":"-1.23","returnOnEquity":"0.022"}}"#,
    )
}

/// The balance summary in the documented shape.
fn balance_json() -> String {
    r#"{"walletBalance":"5000.00","realizedPnl":"250.00","unrealizedPnl":"-50.00","marginBalance":"4950.00","usedMargin":"0.00","availableMargin":"4950.00","withdrawableMargin":"4950.00","maintenanceMarginRequirement":"112.50","totalMaintenanceMargin":"200.00","marginRatio":"0.04","leverage":"0.46","underLiquidation":false,"totalFundingPayments":"-5.67","totalTradingFees":"12.34","totalPnL":"232.00","netInvested":"4750.00"}"#.to_string()
}

/// Two passes' worth of the four reads one reconciliation pass makes, in the order it makes them.
fn two_clean_passes() -> Vec<Reply> {
    let mut script = clean_pass_script();

    script.extend(clean_pass_script());

    script
}

/// The four reads one reconciliation pass makes, in the order it makes them.
fn clean_pass_script() -> Vec<Reply> {
    vec![
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "1.5489")))),
        Reply::ok(envelope(&balance_json())),
    ]
}

// ------------------------------------------------------------------------------------------------
// Client-level harness
// ------------------------------------------------------------------------------------------------

struct Harness {
    client: OndoExecutionClient,
    exec_rx: UnboundedReceiver<ExecutionEvent>,
    cache: Rc<RefCell<Cache>>,
}

fn build_harness(mock: &MockServer, config: OndoExecutionClientConfig) -> Harness {
    let account_id = account_id();
    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        ClientId::from(CLIENT_ID),
        *ONDO_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None, // base_currency
        cache.clone(),
    );

    let config = OndoExecutionClientConfig {
        base_url_http: Some(mock.url()),
        // The private transport dials the same loopback authority, which refuses the upgrade: this
        // harness is a REST surface, and the account session stays unauthenticated here. The
        // endpoint is named rather than left to the environment default so that no test can reach
        // the venue's own host by omission.
        base_url_ws: Some(format!("ws://{}/ws", mock.addr)),
        account_id: Some(account_id),
        ..config
    };

    let credential = OndoCredential::new(
        OndoEnvironment::Sandbox,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the plan's fake credential is well formed");

    let budget = OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).expect("a nonzero quota"))
            .expect("a burst this size replenishes"),
    );

    let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    let client = OndoExecutionClient::with_credential(core, config, Some(credential), Some(budget))
        .expect("the client builds");

    Harness {
        client,
        exec_rx,
        cache,
    }
}

fn sandbox_config() -> OndoExecutionClientConfig {
    OndoExecutionClientConfig {
        environment: OndoEnvironment::Sandbox,
        account_id: Some(account_id()),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        ..Default::default()
    }
}

/// Seeds `order` into the cache, exactly as the execution engine does before a submit command.
fn seed_order(harness: &Harness, order: &OrderAny) {
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("the order should enter the cache");
}

fn limit_order(client_order_id: &str, side: OrderSide) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(nvda())
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(side)
        .quantity(Quantity::from("1.00"))
        .price(Price::from("227.50"))
        .time_in_force(TimeInForce::Gtc)
        .build()
}

fn submit_command(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::new(
        order.trader_id(),
        Some(ClientId::from(CLIENT_ID)),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None, // exec_algorithm_id
        None, // position_id
        None, // params
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    )
}

fn cancel_command(order: &OrderAny) -> CancelOrder {
    CancelOrder::new(
        order.trader_id(),
        Some(ClientId::from(CLIENT_ID)),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        None, // venue_order_id
        UUID4::new(),
        UnixNanos::default(),
        None, // params
        None, // correlation_id
    )
}

/// A batch submission carrying `orders` as one native list.
fn order_list_command(orders: &[OrderAny]) -> SubmitOrderList {
    let inits: Vec<_> = orders
        .iter()
        .map(|order| order.init_event().clone())
        .collect();

    let list = OrderList::new(
        OrderListId::from("OL-1"),
        nvda(),
        StrategyId::from("S-001"),
        orders.iter().map(|order| order.client_order_id()).collect(),
        UnixNanos::default(),
    );

    SubmitOrderList::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        list,
        inits,
        None, // exec_algorithm_id
        None, // position_id
        None, // params
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    )
}

/// Drains whatever the client has emitted so far, without waiting.
fn drain(harness: &mut Harness) -> Vec<ExecutionEvent> {
    let mut events = Vec::new();
    while let Ok(event) = harness.exec_rx.try_recv() {
        events.push(event);
    }
    events
}

/// Appends events to `events` until `done` holds for the accumulated sequence.
async fn collect_until(
    harness: &mut Harness,
    events: &mut Vec<ExecutionEvent>,
    mut done: impl FnMut(&[ExecutionEvent]) -> bool,
) {
    let start = Instant::now();

    loop {
        while let Ok(event) = harness.exec_rx.try_recv() {
            events.push(event);
        }

        if done(events) {
            return;
        }

        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "timed out after {:?} with {} events",
            start.elapsed(),
            events.len(),
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The reads the two reconciliation passes make before a test's own replies: four per pass.
const RECOVERY_READS: usize = 8;

/// Waits until the mock server has received `count` requests.
async fn wait_for_requests(mock: &MockServer, count: usize) {
    let start = Instant::now();

    while mock.captured().len() < count {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "timed out waiting for {count} requests; saw {:?}",
            mock.targets(),
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Waits until the mock server has received `count` requests of this test's own commands.
///
/// The recovery reads a client makes before it admits anything are not requests a test's own
/// commands caused, and every count below is about those.
async fn wait_for_writes(mock: &MockServer, count: usize) {
    wait_for_requests(mock, count + RECOVERY_READS).await;
}

/// The four reads one pass makes when the account is empty, in the order the pass makes them.
fn empty_pass_script() -> Vec<Reply> {
    vec![
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(&balance_json())),
    ]
}

/// `replies` behind the two agreeing passes a new order is admitted after.
///
/// The account they read is empty, so the baseline the first pass adopts is empty too: a test that
/// then trades sees its own commands and nothing else.
fn admitted_script(replies: Vec<Reply>) -> Vec<Reply> {
    let mut script = empty_pass_script();

    script.extend(empty_pass_script());
    script.extend(replies);

    script
}

/// A harness whose account is recovered: current metadata, two agreeing passes, nothing unknown.
///
/// New risk is refused from construction (plan §6.4), so a test that submits has to establish this
/// first - and the mock has to be started on [`admitted_script`], which answers the eight reads the
/// two passes make before the test's own replies.
async fn recovered_harness(mock: &MockServer) -> Harness {
    let mut harness = build_harness(mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass reads an empty account");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass reads an empty account");

    assert!(
        harness.client.can_submit_new_orders(),
        "the harness is admitted: a submission test needs an account new risk is allowed on",
    );

    harness
}

impl MockServer {
    /// A server whose script answers the two reconciliation passes a submission is admitted after,
    /// then the given replies.
    async fn start_admitted(script: Vec<Reply>) -> Self {
        Self::start(admitted_script(script)).await
    }
}

// ------------------------------------------------------------------------------------------------
// The client's reconciliation pass (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_a_recovery_reads_the_account_and_needs_two_agreeing_passes_to_be_ready() {
    let mock = MockServer::start(two_clean_passes()).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    harness.client.begin_recovery(secs(1));

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Recovering
    );
    assert!(!harness.client.can_submit_new_orders());

    let first = harness.client.reconcile_account(secs(1)).await;

    assert!(first.is_ok(), "the first pass reads: {first:?}");
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Recovering
    );

    let second = harness.client.reconcile_account(secs(2)).await;

    assert!(second.is_ok(), "the second pass reads: {second:?}");
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready
    );

    // A recovered account is not yet a licence: nothing has told this client its instrument
    // metadata is current, so the predicate stays false (plan Task 8).
    assert!(!harness.client.can_submit_new_orders());

    harness.client.set_metadata(MetadataValidity::Current);

    assert!(harness.client.can_submit_new_orders());

    // Four reads per pass, in the documented order: orders, fills, positions, balance.
    let targets = mock.targets();

    assert_eq!(targets.len(), 8, "two passes of four reads: {targets:?}");
    assert!(targets[0].starts_with("/v1/perps/orders"));
    assert!(targets[1].starts_with("/v1/perps/fills"));
    assert!(targets[2].starts_with("/v1/perps/positions"));
    assert!(targets[3].starts_with("/v1/perps/balance"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_disconnect_stops_new_risk_until_the_account_is_recovered_again() {
    // Two recoveries: the one that makes the account ready, and the one after the disconnect.
    let mut script = two_clean_passes();

    script.extend(two_clean_passes());

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("pass");

    assert!(harness.client.can_submit_new_orders());

    harness.client.disconnect().await.expect("disconnect");

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Disconnected
    );
    assert!(harness.client.refuses_new_risk());

    harness.client.begin_recovery(secs(3));
    harness
        .client
        .reconcile_account(secs(3))
        .await
        .expect("pass");

    assert!(harness.client.refuses_new_risk());

    harness
        .client
        .reconcile_account(secs(4))
        .await
        .expect("pass");

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready
    );
    assert!(!harness.client.refuses_new_risk());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_submission_may_be_refused_while_the_account_is_being_recovered() {
    let mock = MockServer::start(clean_pass_script()).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(secs(1));

    let order = limit_order("ondo_probe_recovering", OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    // A batch is new risk too: the same predicate refuses it, and no request exists for it either.
    let second = limit_order("ondo_probe_recovering_2", OrderSide::Sell);

    seed_order(&harness, &second);

    let list = OrderList::new(
        OrderListId::from("OL-1"),
        InstrumentId::from(NVDA),
        StrategyId::from("S-001"),
        vec![second.client_order_id()],
        UnixNanos::default(),
    );
    let list_command = SubmitOrderList::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        list,
        vec![second.init_event().clone()],
        None, // exec_algorithm_id
        None, // position_id
        None, // params
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    harness
        .client
        .submit_order_list(list_command)
        .expect("the command is handled");

    // A new order is not merely left unreported while the account is unknown: no request exists.
    assert!(
        mock.with_method("POST").is_empty(),
        "no create request while recovering: {:?}",
        mock.targets(),
    );

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .filter(|event| matches!(event, OrderEventAny::Denied(denied) if !denied.reason.is_empty()))
        .collect();

    assert_eq!(
        denied.len(),
        2,
        "the order and the batch item are both denied rather than left in flight: {events:?}",
    );
    assert!(
        denied.iter().all(|event| match event {
            OrderEventAny::Denied(denied) => denied.reason.contains("order-denied: reconciliation"),
            _ => false,
        }),
        "each denial names the reason: {denied:?}",
    );
}

/// F01: a client that has never established a session used to pass the entrance check and send
/// the order. New risk is refused from construction, so neither a single order nor a batch becomes
/// a request. The events are asserted as well as the count: an order that is silently dropped is
/// not a refusal a strategy can see.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_fresh_client_refuses_a_submission_and_a_batch_before_any_session_exists() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let single = limit_order("ondo_probe_fresh", OrderSide::Buy);

    seed_order(&harness, &single);
    harness
        .client
        .submit_order(submit_command(&single))
        .expect("the command is handled");

    let batched = limit_order("ondo_probe_fresh_2", OrderSide::Sell);

    seed_order(&harness, &batched);
    harness
        .client
        .submit_order_list(order_list_command(&[batched]))
        .expect("the command is handled");

    // A request that should not exist is given every chance to appear, and the mock time to record
    // it: "not yet" is not the property under test.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        mock.with_method("POST").is_empty(),
        "no create request exists for an account this client has never read: {:?}",
        mock.targets(),
    );

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();

    assert_eq!(
        denied.len(),
        2,
        "the order and the batch item are both denied: {events:?}",
    );
    assert!(
        denied.iter().all(|event| match event {
            OrderEventAny::Denied(denied) => denied.reason.contains("order-denied: reconciliation"),
            _ => false,
        }),
        "each denial names the reason: {denied:?}",
    );
}

/// The other half of F01: the account was read and recovered, and then the session ended. What the
/// session established is unverified from that moment, so the next order is refused without a
/// request - the state a reconnect leaves behind is not a licence to trade.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_disconnect_after_a_recovery_refuses_the_next_submission_without_a_request() {
    let mock = MockServer::start(two_clean_passes()).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass");

    assert!(
        harness.client.can_submit_new_orders(),
        "the account is recovered"
    );

    harness.client.disconnect().await.expect("disconnect");

    let single = limit_order("ondo_probe_after_disconnect", OrderSide::Buy);

    seed_order(&harness, &single);
    harness
        .client
        .submit_order(submit_command(&single))
        .expect("the command is handled");

    let batched = limit_order("ondo_probe_after_disconnect_2", OrderSide::Sell);

    seed_order(&harness, &batched);
    harness
        .client
        .submit_order_list(order_list_command(&[batched]))
        .expect("the command is handled");

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        mock.with_method("POST").is_empty(),
        "a session that ended leaves nothing to trade on: {:?}",
        mock.targets(),
    );

    let events = drain(&mut harness);
    let denied = events
        .iter()
        .filter(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
        .count();

    assert_eq!(denied, 2, "both commands are denied: {events:?}");
}

/// The gate is evaluated twice: once when the command is accepted, and once inside the submission
/// task immediately before the request exists. This is the second one. The command is admissible
/// when it is taken and the account is invalidated while it is still only queued, which is the
/// window a rate-limited write path spends waiting for its budget.
///
/// The runtime is the single-threaded one on purpose: `submit_order` is synchronous, so a task it
/// spawns cannot poll until this future yields, and the test - not a sleep - decides the order of
/// the two events.
#[tokio::test]
async fn test_a_submission_is_refused_when_the_account_is_invalidated_before_its_request() {
    let mock = MockServer::start(two_clean_passes()).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass");

    assert!(harness.client.can_submit_new_orders());

    let order = limit_order("ondo_probe_queued", OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    // The account is invalidated while the command is queued: the admission it was given is no
    // longer the current one, and no request may follow from it.
    harness.client.set_metadata(MetadataValidity::Stale {
        reason: "the metadata refresh failed".to_string(),
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        mock.with_method("POST").is_empty(),
        "the queued submission is refused before its request exists: {:?}",
        mock.targets(),
    );

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();

    assert_eq!(
        denied.len(),
        1,
        "the refused command is denied rather than left in flight: {events:?}",
    );
}

/// F02: a submission whose answer was lost used to leave the account tradable. It does not any
/// more - not after a reconciliation pass, and not before one either. The next order's write
/// request count is zero, which is the property the plan asks for; the deny event is how the
/// strategy learns.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_lost_submission_answer_stops_the_next_order_from_being_sent() {
    let mut script = two_clean_passes();

    // The create is answered and the answer never reaches this process.
    script.push(Reply::answer(500, r#"{"success":false,"error":"gateway"}"#));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass");

    let first = limit_order("ondo_probe_lost", OrderSide::Buy);

    seed_order(&harness, &first);
    harness
        .client
        .submit_order(submit_command(&first))
        .expect("the command is handled");

    let start = Instant::now();

    while harness.client.unknown_submissions().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the lost answer leaves the submission unknown: {:?}",
            mock.targets(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        !harness.client.can_submit_new_orders(),
        "an unsettled submission is not a licence to trade",
    );

    let second = limit_order("ondo_probe_after_lost", OrderSide::Sell);

    seed_order(&harness, &second);
    harness
        .client
        .submit_order(submit_command(&second))
        .expect("the command is handled");

    // Give a request that should not exist every chance to appear.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        mock.with_method("POST").len(),
        1,
        "only the lost submission was ever sent: {:?}",
        mock.targets(),
    );

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();

    assert_eq!(
        denied.len(),
        1,
        "the second submission is denied by name: {events:?}",
    );
}

/// An unsettled outcome blocks new risk, and settling it restores the account. A gate that could
/// never reopen would be a different defect from the one F02 describes.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_settled_submission_restores_the_account_without_a_permanent_lock() {
    let mut script = two_clean_passes();

    // The lost answer, then the probe that finds the order the venue did apply.
    script.push(Reply::answer(500, r#"{"success":false,"error":"gateway"}"#));
    script.push(Reply::ok(envelope(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "0.00",
    ))));
    // The order that follows the recovery.
    script.push(Reply::ok(envelope(&api_order(
        VENUE_ORDER_ID,
        "ondo_probe_after_settlement",
        "open",
        "0.00",
    ))));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass");

    let first = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &first);
    harness
        .client
        .submit_order(submit_command(&first))
        .expect("the command is handled");

    let start = Instant::now();

    while harness.client.unknown_submissions().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the lost answer leaves the submission unknown",
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(!harness.client.can_submit_new_orders());

    let first_seen = harness.client.unknown_submissions()[0].first_seen;
    let probe = harness.client.probe_unknown_submissions(first_seen).await;

    assert_eq!(probe.len(), 1);
    assert_eq!(probe[0].disposition, ProbeDisposition::Resolved);
    assert!(harness.client.unknown_submissions().is_empty());

    assert!(
        harness.client.can_submit_new_orders(),
        "the venue's own answer settles the submission and the account is tradable again",
    );

    let second = limit_order("ondo_probe_after_settlement", OrderSide::Sell);

    seed_order(&harness, &second);
    harness
        .client
        .submit_order(submit_command(&second))
        .expect("the command is handled");

    wait_for_requests(&mock, 11).await;

    assert_eq!(
        mock.with_method("POST").len(),
        2,
        "the settled account sends the next order: {:?}",
        mock.targets(),
    );
}

/// A cancel the venue accepted without reporting the order is not a cancel, and a confirming query
/// that fails leaves it exactly there. F02's other half: that has to be registered, or the account
/// goes on trading on a state no answer has stated.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_cancel_that_carried_no_order_stays_unconfirmed_when_its_query_fails() {
    let mut script = two_clean_passes();

    // The cancel is answered with a success carrying no readable order, and the query that would
    // settle it fails.
    script.push(Reply::ok(envelope("{}")));
    script.push(Reply::answer(
        404,
        r#"{"success":false,"error":"not found"}"#,
    ));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("the first pass");
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("the second pass");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .cancel_order(cancel_command(&order))
        .expect("the command is handled");

    // The cancel and its confirming query.
    wait_for_requests(&mock, 10).await;

    assert!(
        !harness.client.unconfirmed_cancels().is_empty(),
        "the cancel stays unconfirmed until an answer states the order's state: {:?}",
        mock.targets(),
    );
    assert!(!harness.client.can_submit_new_orders());
    assert!(harness.client.refuses_new_risk());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_pass_that_cannot_walk_the_fills_page_leaves_the_account_uncertain() {
    // The fills endpoint repeats a cursor: a walk that stopped early must not look like a history
    // that was read.
    let mock = MockServer::start(vec![
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(
            &[api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")],
            Some("cursor-A"),
        )),
        Reply::ok(fills_page(
            &[api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3")],
            Some("cursor-A"),
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "1.5489")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(secs(1));

    let outcome = harness.client.reconcile_account(secs(1)).await;

    assert!(
        outcome.is_err(),
        "a repeated cursor is not a read: {outcome:?}"
    );
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Uncertain
    );
    assert!(!harness.client.can_submit_new_orders());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_submission_whose_answer_was_lost_is_probed_by_its_client_id_and_never_resent() {
    let mock = MockServer::start_admitted(vec![
        // The create is answered, the answer never reaches this process (the venue applied it).
        Reply::answer(500, r#"{"success":false,"error":"gateway"}"#),
        // The probe by client order id finds it.
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_for_writes(&mock, 1).await;

    // Give the spawned submission task time to record the unknown outcome.
    let start = Instant::now();

    while harness.client.unknown_submissions().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the outcome stays unknown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let first_seen = harness.client.unknown_submissions()[0].first_seen;
    let probe = harness.client.probe_unknown_submissions(first_seen).await;

    assert_eq!(probe.len(), 1);
    assert_eq!(probe[0].client_order_id, client_order_id(CLIENT_ORDER_ID));
    assert_eq!(probe[0].disposition, ProbeDisposition::Resolved);
    assert!(harness.client.unknown_submissions().is_empty());

    // The whole sequence created the order exactly once, and the probe used the same client id.
    let creates = mock.with_method("POST");

    assert_eq!(creates.len(), 1, "one create only: {:?}", mock.targets());
    assert!(creates[0].body.contains(CLIENT_ORDER_ID));

    let lookups: Vec<String> = mock
        .targets()
        .into_iter()
        .filter(|target| target.contains("client%3A") || target.contains("client:"))
        .collect();

    assert_eq!(lookups.len(), 1, "one lookup, by client id: {lookups:?}");
    assert!(lookups[0].contains(&format!("client%3A{CLIENT_ORDER_ID}")));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_404_probe_inside_the_window_neither_settles_nor_resubmits_the_order() {
    let mock = MockServer::start_admitted(vec![
        Reply::answer(500, r#"{"success":false,"error":"gateway"}"#),
        Reply::answer(404, r#"{"success":false,"error":"not found"}"#),
        Reply::answer(404, r#"{"success":false,"error":"not found"}"#),
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_for_writes(&mock, 1).await;

    let start = Instant::now();

    while harness.client.unknown_submissions().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the outcome stays unknown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let first_seen = harness.client.unknown_submissions()[0].first_seen;
    let first = harness.client.probe_unknown_submissions(first_seen).await;

    assert_eq!(first.len(), 1);
    assert!(matches!(
        first[0].disposition,
        ProbeDisposition::KeepProbing { .. }
    ));

    // Inside the window, the second probe goes out; the order is still unknown afterwards.
    let second = harness
        .client
        .probe_unknown_submissions(
            first_seen + UnixNanos::from(ONDO_SUBMISSION_PROBE_INTERVAL * 1_000_000_000),
        )
        .await;

    assert_eq!(second.len(), 1);
    assert!(matches!(
        second[0].disposition,
        ProbeDisposition::KeepProbing { .. }
    ));
    assert_eq!(harness.client.unknown_submissions().len(), 1);

    // Two 404s later, the venue has still been asked to create the order exactly once.
    assert_eq!(mock.with_method("POST").len(), 1);

    // And the window is a window, not a verdict: when the order does turn up, the same probe
    // finds it and the submission is settled without a second create.
    let third = harness
        .client
        .probe_unknown_submissions(
            first_seen + UnixNanos::from(2 * ONDO_SUBMISSION_PROBE_INTERVAL * 1_000_000_000),
        )
        .await;

    assert_eq!(third.len(), 1);
    assert_eq!(third[0].disposition, ProbeDisposition::Resolved);
    assert!(harness.client.unknown_submissions().is_empty());
    assert_eq!(mock.with_method("POST").len(), 1);
    assert_eq!(
        harness
            .client
            .order_state(&client_order_id(CLIENT_ORDER_ID))
            .map(|state| state.status),
        Some(OndoOrderStatus::Open),
        "the order the probe found is the order this client submitted",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_thirty_seconds_without_an_answer_stops_the_probe_and_keeps_the_order_unknown() {
    let mock = MockServer::start_admitted(vec![Reply::answer(
        500,
        r#"{"success":false,"error":"gateway"}"#,
    )])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_for_writes(&mock, 1).await;

    let start = Instant::now();

    while harness.client.unknown_submissions().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the outcome stays unknown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let unknown = harness.client.unknown_submissions();
    let first_seen = unknown[0].first_seen;
    let window_end = first_seen + UnixNanos::from(ONDO_SUBMISSION_UNKNOWN_SECS * 1_000_000_000);

    let abandoned = harness.client.abandon_unknown_submissions(window_end);

    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].lookup, format!("client:{CLIENT_ORDER_ID}"));

    // The probe stopped: a later probe request finds nothing due, and no create was ever repeated.
    let probe = harness.client.probe_unknown_submissions(window_end).await;

    assert!(probe.is_empty());
    assert_eq!(harness.client.unknown_submissions().len(), 1);
    assert_eq!(mock.with_method("POST").len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_the_fill_history_is_walked_across_pages_and_every_fill_is_applied_once() {
    // Two pages, and the second repeats the first page's fill the way a paginated endpoint can.
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.50",
        ))),
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(
            &[api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")],
            Some("cursor-A"),
        )),
        Reply::ok(fills_page(
            &[
                api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3"),
                api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2"),
            ],
            None,
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.50")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;
    harness.client.begin_recovery(secs(1));

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    // Both pages were read, and the fill the second page repeated is still one fill.
    assert_eq!(harness.client.applied_fill_count(), 2);
    assert_eq!(
        mock.targets()[RECOVERY_READS..]
            .iter()
            .filter(|target| target.starts_with("/v1/perps/fills"))
            .count(),
        2,
        "the walk requested both pages: {:?}",
        mock.targets(),
    );
    assert_eq!(
        harness
            .client
            .order_state(&client_order_id(CLIENT_ORDER_ID))
            .map(|state| state.filled.to_string()),
        Some("0.5".to_string()),
        "two fills, each applied once, add up to the venue's filled size",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fill_that_lands_while_a_cancel_is_in_flight_is_counted_and_the_position_agrees() {
    let mock = MockServer::start_admitted(vec![
        // The create is acknowledged.
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        // The cancel is answered with the partial fill the venue applied before it took effect.
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "canceled",
            "0.20",
        ))),
        // The reconciliation pass reads the ended order, the fill, and the position it left.
        Reply::ok(orders_page(
            &[api_order(
                VENUE_ORDER_ID,
                CLIENT_ORDER_ID,
                "canceled",
                "0.20",
            )],
            None,
        )),
        Reply::ok(fills_page(
            &[api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")],
            None,
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.20")))),
        Reply::ok(envelope(&balance_json())),
        // The second, converging pass.
        Reply::ok(orders_page(
            &[api_order(
                VENUE_ORDER_ID,
                CLIENT_ORDER_ID,
                "canceled",
                "0.20",
            )],
            None,
        )),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.20")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let harness = recovered_harness(&mock).await;
    harness.client.set_metadata(MetadataValidity::Current);

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;
    harness
        .client
        .cancel_order(cancel_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 2).await;

    harness.client.begin_recovery(secs(1));

    let first = harness.client.reconcile_account(secs(1)).await;

    assert!(first.is_ok(), "the pass reads: {first:?}");

    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(
        judgment.is_clean(),
        "the fill the cancel raced is counted, so the ended order and the position agree: {:?}",
        judgment.reasons(),
    );

    let second = harness.client.reconcile_account(secs(2)).await;

    assert!(second.is_ok(), "the converging pass reads: {second:?}");
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready
    );
    assert_eq!(harness.client.applied_fill_count(), 1);
    assert_eq!(
        harness
            .client
            .order_state(&client_order_id(CLIENT_ORDER_ID))
            .map(|state| (state.status.clone(), state.filled.to_string())),
        Some((OndoOrderStatus::Canceled, "0.2".to_string())),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_report_that_arrived_while_the_account_was_read_is_replayed_once() {
    let fills = vec![api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")];
    // The create answer acknowledges the order; the REST history then carries the same fill the
    // private stream buffered while the account was being read.
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.20",
        ))),
        Reply::ok(orders_page(
            &[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.20")],
            None,
        )),
        Reply::ok(fills_page(&fills, None)),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "1.5489")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Order(_)))
    })
    .await;

    harness.client.begin_recovery(secs(1));

    // The private stream delivered the fill while the account was being read.
    harness.client.buffer_stream_fill(fill_from(&fills[0]));

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    events.extend(drain(&mut harness));

    // One fill, delivered twice - once by the buffered stream report and once by the REST history
    // the pass read - and applied once (plan §6.3: `(account_id, fill.id)`).
    assert_eq!(
        harness.client.applied_fill_count(),
        1,
        "the fill is applied once: {events:?}",
    );

    let reports: Vec<&FillReport> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => Some(&**report),
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(_report, fills)) => {
                fills.first()
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        reports.len(),
        1,
        "the duplicate delivery is not a second fill: {events:?}",
    );
    assert_eq!(reports[0].trade_id.to_string(), "fill-1");
}

/// F03, scenario 1: an order's reports are a sequence, and the last one is the order's state.
///
/// The venue worked the order while the pass was reading the account: the stream delivered `open`
/// with no fill, then `open` with half of it, then `canceled`. All three are facts about the order,
/// and the reading the pass judges has to be the state the last of them describes - not the first
/// one, which is what an order-keyed set of one payload per order would have left behind.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_reports_of_one_order_are_replayed_in_arrival_order_and_the_last_one_wins() {
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        Reply::ok(orders_page(
            &[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")],
            None,
        )),
        Reply::ok(fills_page(
            &[
                api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2"),
                api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3"),
            ],
            None,
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.50")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let harness = recovered_harness(&mock).await;
    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    harness.client.begin_recovery(secs(1));

    // The stream delivered the order's progress while the pass was reading the account.
    for (status, filled) in [("open", "0.00"), ("open", "0.50"), ("canceled", "0.50")] {
        harness.client.buffer_stream_order(order_from(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            status,
            filled,
        )));
    }

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let state = harness
        .client
        .order_state(&client_order_id(CLIENT_ORDER_ID))
        .expect("the order is tracked");

    assert_eq!(
        state.status,
        OndoOrderStatus::Canceled,
        "the last report the stream delivered is the order's state, not the first",
    );
    assert_eq!(state.filled.to_string(), "0.5");
    assert!(
        state.resolved,
        "the canceled order's fills add up to the venue's own filledSize",
    );
    assert_eq!(harness.client.applied_fill_count(), 2);

    // The pass converged on the account the sequence describes, and one agreeing pass is still not
    // a recovery: the third report does not make the account Ready on its own.
    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(
        judgment.is_clean(),
        "the replayed sequence leaves nothing unexplained: {:?}",
        judgment.reasons(),
    );
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Recovering,
    );
    assert!(!harness.client.can_submit_new_orders());
}

/// F03, scenario 2: one frame delivered twice is one fact, and it moves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_duplicated_frame_is_applied_once_and_leaves_the_state_where_it_was() {
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        Reply::ok(orders_page(
            &[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.20")],
            None,
        )),
        Reply::ok(fills_page(
            &[api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2")],
            None,
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.20")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = recovered_harness(&mock).await;
    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    harness.client.begin_recovery(secs(1));

    // The same frame twice, both as the stream delivered it and as the pages carry it.
    for _ in 0..2 {
        harness.client.buffer_stream_fill(fill_from(&api_fill(
            "fill-1",
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "0.2",
        )));
        harness.client.buffer_stream_order(order_from(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.20",
        )));
    }

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let events = drain(&mut harness);
    let reports = events
        .iter()
        .filter(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))))
        .count();

    assert_eq!(
        harness.client.applied_fill_count(),
        1,
        "one fill, delivered four times, is counted once",
    );
    assert_eq!(reports, 1, "and reported once: {events:?}");

    let state = harness
        .client
        .order_state(&client_order_id(CLIENT_ORDER_ID))
        .expect("the order is tracked");

    assert_eq!(state.status, OndoOrderStatus::Open);
    assert_eq!(state.filled.to_string(), "0.2");
    assert!(harness.client.unresolved_orders().is_empty());
}

/// F03, scenario 3: fills and order reports interleave, and each fill is applied exactly once.
///
/// The stream does not deliver an order's fills after its reports: it delivers what happened. The
/// fill that arrived between two order reports is the one a replay that sorted by kind would apply
/// against the wrong state, and the one a replay that dropped either would lose.
#[tokio::test(flavor = "multi_thread")]
async fn test_interleaved_fill_and_order_reports_are_each_applied_exactly_once() {
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        Reply::ok(orders_page(
            &[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")],
            None,
        )),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.50")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = recovered_harness(&mock).await;
    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    harness.client.begin_recovery(secs(1));

    harness.client.buffer_stream_order(order_from(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "0.20",
    )));
    harness.client.buffer_stream_fill(fill_from(&api_fill(
        "fill-1",
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "0.2",
    )));
    harness.client.buffer_stream_order(order_from(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "0.50",
    )));
    harness.client.buffer_stream_fill(fill_from(&api_fill(
        "fill-2",
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "0.3",
    )));

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let events = drain(&mut harness);
    let reports = events
        .iter()
        .filter(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))))
        .count();

    assert_eq!(harness.client.applied_fill_count(), 2);
    assert_eq!(reports, 2, "each fill is reported once: {events:?}");

    let state = harness
        .client
        .order_state(&client_order_id(CLIENT_ORDER_ID))
        .expect("the order is tracked");

    assert_eq!(state.filled.to_string(), "0.5");
    assert!(harness.client.unresolved_orders().is_empty());
}

/// F03, scenario 6: a report from a recovery this machine has left is never current state.
///
/// The report below arrived before the recovery began. It says the order ended; the pass that reads
/// the account finds it working. Applying the older report after that read would move the order
/// backwards into a state the venue has already moved it on from, so it is refused - and refused
/// out loud, because it is still a fact this client saw and does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_report_from_a_superseded_recovery_is_refused_and_named() {
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        Reply::ok(orders_page(
            &[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open", "0.00")],
            None,
        )),
        Reply::ok(fills_page(
            &[
                api_fill("fill-1", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.2"),
                api_fill("fill-2", VENUE_ORDER_ID, CLIENT_ORDER_ID, "0.3"),
            ],
            None,
        )),
        Reply::ok(envelope(&format!("[{}]", position_json("long", "0.50")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let harness = recovered_harness(&mock).await;
    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    // The report the previous session delivered, still in the buffer when the new recovery starts.
    harness.client.buffer_stream_order(order_from(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "canceled",
        "0.50",
    )));
    harness.client.begin_recovery(secs(1));

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let state = harness
        .client
        .order_state(&client_order_id(CLIENT_ORDER_ID))
        .expect("the order is tracked");

    assert_eq!(
        state.status,
        OndoOrderStatus::Open,
        "the superseded report is not applied as the order's current state",
    );
    assert_eq!(state.filled.to_string(), "0.5");

    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(
        judgment.findings.iter().any(|finding| matches!(
            finding,
            Finding::LostReports { count: 1, reason } if reason.contains("superseded")
        )),
        "the refused report is a loss the pass names: {:?}",
        judgment.reasons(),
    );
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Uncertain,
        "an account with a report missing from it is not an account that was read",
    );
}

/// F03, scenario 7: a buffer that overflows is an uncertain account, not a silent drop.
///
/// The buffer is bounded so that a stream healthier than the read cannot exhaust this process. What
/// the bound must not do is lose a report quietly: the account stops being tradable the moment one
/// is refused, the pass that reads it says how many were lost, and only the bounded re-read that
/// follows makes it Ready again.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_buffer_that_overflows_leaves_the_account_uncertain_until_a_re_read() {
    let mut script = empty_pass_script();

    script.extend(empty_pass_script());
    script.extend(empty_pass_script());

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(secs(1));

    let capacity = ReconciliationBuffer::new().capacity();

    // Every frame is a distinct update - the venue worked the order between them - so none of them
    // is the duplicate the buffer is allowed to drop.
    for step in 0..=capacity {
        harness.client.buffer_stream_order(order_from(&api_order(
            "abcd0000000000000000000000000009",
            "placed_by_hand_1",
            "open",
            &format!("0.{step:04}"),
        )));
    }

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Uncertain,
        "the report the buffer could not hold is a condition, not a log line",
    );
    assert!(!harness.client.can_submit_new_orders());

    let first = harness.client.reconcile_account(secs(1)).await;

    assert!(first.is_ok(), "the pass reads: {first:?}");

    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(
        judgment.findings.iter().any(|finding| matches!(
            finding,
            Finding::LostReports { count: 1, reason }
                if reason.contains(&capacity.to_string())
        )),
        "the pass says how many reports were lost and how full the buffer was: {:?}",
        judgment.reasons(),
    );
    assert!(judgment.is_uncertain());
    assert_ne!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
        "a pass that lost a report does not converge the account",
    );

    // The bounded re-read: two agreeing passes over the account as the venue states it.
    let second = harness.client.reconcile_account(secs(2)).await;

    assert!(second.is_ok(), "the pass reads: {second:?}");
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Recovering,
    );

    let third = harness.client.reconcile_account(secs(3)).await;

    assert!(third.is_ok(), "the pass reads: {third:?}");
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
    );
    assert!(harness.client.can_submit_new_orders());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_neutral_position_the_venue_reports_flat_is_read_as_flat_and_zeroed() {
    let mock = MockServer::start(vec![
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("neutral", "0.00")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(secs(1));

    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let state = harness.client.reconciliation_state();

    assert!(
        matches!(
            state,
            ReconciliationState::Recovering | ReconciliationState::Ready
        ),
        "a flat position is a readable state, not a discrepancy: {state:?}",
    );

    let reading = harness
        .client
        .last_reading()
        .expect("the pass keeps its reading");

    assert_eq!(reading.positions.len(), 1);
    assert!(reading.positions[0].is_flat());
    assert_eq!(reading.positions[0].signed, rust_decimal::Decimal::ZERO);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_an_order_this_run_did_not_place_is_reported_and_the_account_is_never_swept() {
    let mock = MockServer::start(vec![
        Reply::ok(orders_page(
            &[api_order(
                "abcd0000000000000000000000000009",
                "placed_by_hand_1",
                "open",
                "0.00",
            )],
            None,
        )),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("neutral", "0.00")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(secs(1));
    harness
        .client
        .reconcile_account(secs(1))
        .await
        .expect("pass");

    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(judgment.findings.iter().any(|finding| matches!(
        finding,
        Finding::ForeignOrder { client_order_id, .. }
            if client_order_id.as_deref() == Some("placed_by_hand_1")
    )));

    // The account is not clean, and nothing was swept: the run does not own that order.
    assert!(!judgment.is_clean());
    assert!(
        mock.with_method("DELETE").is_empty(),
        "an out-of-run order is never cancelled: {:?}",
        mock.targets(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_cancel_whose_answer_was_lost_is_settled_by_a_query_not_by_the_cancel_call() {
    let mock = MockServer::start_admitted(vec![
        // The create is acknowledged.
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        // The cancel request's answer never arrives.
        Reply::answer(500, r#"{"success":false,"error":"gateway"}"#),
        // The confirming query cannot be answered either.
        Reply::answer(404, r#"{"success":false,"error":"not found"}"#),
        // A later reconciliation pass reads an account that is otherwise clean.
        Reply::ok(orders_page(&[], None)),
        Reply::ok(fills_page(&[], None)),
        Reply::ok(envelope(&format!("[{}]", position_json("neutral", "0.00")))),
        Reply::ok(envelope(&balance_json())),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_for_writes(&mock, 1).await;

    // A session begins: from here the machine governs new risk, and a cancel still travels.
    harness.client.begin_recovery(secs(1));

    harness
        .client
        .cancel_order(cancel_command(&order))
        .expect("the command is handled");

    let start = Instant::now();

    while harness.client.unconfirmed_cancels().is_empty() {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the cancel's outcome stays unconfirmed: {:?}",
            mock.targets(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A cancel API call is not a cancel: the confirming query is what settles it (plan §6.3).
    let targets = mock.targets();

    assert!(
        targets
            .iter()
            .any(|target| target.starts_with("/v1/perps/orders/")),
        "the cancel is confirmed by a query: {targets:?}",
    );

    // And the account it leaves behind is not one to trade on: a later pass reads everything else
    // clean and the outstanding cancel still keeps it uncertain.
    harness
        .client
        .reconcile_account(secs(2))
        .await
        .expect("pass");

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Uncertain
    );
    assert!(harness.client.refuses_new_risk());
    assert_eq!(
        harness
            .client
            .order_state(&client_order_id(CLIENT_ORDER_ID))
            .map(|state| state.status),
        Some(OndoOrderStatus::Open),
        "the order keeps the state the venue last stated, never `cancelled` because a call was made",
    );
    assert!(
        harness
            .client
            .last_judgment()
            .expect("the pass is judged")
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::UnconfirmedCancel { .. })),
    );
}

// ------------------------------------------------------------------------------------------------
// The stop sequence (plan §6.4)
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_stopping_releases_the_switch_only_after_the_runs_own_orders_are_cancelled() {
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "0.00",
    )))])
    .await;
    let harness = recovered_harness(&mock).await;

    // The switch is armed, so stopping has to release it as well as cancel this run's orders.
    harness.client.arm_dead_mans_switch(secs(1));
    harness.client.confirm_dead_mans_switch(secs(1));

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    assert_eq!(
        harness.client.stop_sequence(),
        vec![
            StopStep::CancelOwnOrders,
            StopStep::ConfirmOwnOrders,
            StopStep::ReleaseDeadMansSwitch,
            StopStep::ClosePrivateStream,
        ],
        "plan §6.4: cancel and confirm this run's orders first, release the switch, then close",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stopping_a_client_with_no_switch_has_nothing_to_release() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&api_order(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "0.00",
    )))])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    assert_eq!(
        harness.client.stop_sequence(),
        vec![
            StopStep::CancelOwnOrders,
            StopStep::ConfirmOwnOrders,
            StopStep::ClosePrivateStream,
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_the_cancel_path_still_works_while_the_account_is_not_ready() {
    let mock = MockServer::start_admitted(vec![
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "0.00",
        ))),
        Reply::ok(envelope(&api_order(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "canceled",
            "0.00",
        ))),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    // A recovery begins, and this client stops admitting new risk while cancels keep travelling.
    harness.client.begin_recovery(secs(1));

    assert!(harness.client.refuses_new_risk());

    harness
        .client
        .cancel_order(cancel_command(&order))
        .expect("the command is handled");

    // Plan §6.4: cancels and queries still go while the account is recovering; only new risk
    // waits. The order ends canceled, from the venue's own answer.
    wait_for_writes(&mock, 2).await;

    let start = Instant::now();

    while harness
        .client
        .order_state(&client_order_id(CLIENT_ORDER_ID))
        .map(|state| state.status)
        != Some(OndoOrderStatus::Canceled)
    {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "the cancel is applied from the venue's answer: {:?}",
            mock.targets(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(mock.with_method("DELETE").len(), 1, "the cancel went out");
}
