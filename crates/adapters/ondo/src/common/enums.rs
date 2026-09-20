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

//! Venue vocabulary for Ondo Perps: environment, market status, and fee-rate provenance.
//!
//! The venue's official schema leaves two things open that later layers must not silently
//! decide: whether a perps market is tradable (`test_data/conflicts.md` conflict 5, resolved by
//! observation on 2026-09-14) and which fields carry maker and taker fees
//! (`test_data/rest/markets_synthetic.json` `_fixture.field_provenance`). Both are modelled here as
//! classified values that keep the evidence they came from, so a consumer can always tell an
//! observed value from an assumption.

use nautilus_core::UnixNanos;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The Ondo Perps environment an endpoint set belongs to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.ondo",
        eq,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.ondo")
)]
pub enum OndoEnvironment {
    /// Production, the only environment with observed traffic in this phase.
    #[default]
    Production,
    /// Sandbox, documented but unverified: no authenticated session existed during the protocol
    /// freeze.
    Sandbox,
}

/// The authorization scope an authenticated Ondo Perps session is opened under.
///
/// The scope pairs the venue environment with whether the session may write, and it is the one
/// input the endpoint policy, the credential resolver and the authenticated transport share. A
/// read-only scope cannot send writes. The production trading variant additionally requires a
/// privately constructed bounded authority at the native transport constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OndoAuthenticationScope {
    /// The sandbox environment, permitted to place and cancel orders.
    SandboxTrading,
    /// The sandbox environment, restricted to authenticated reads.
    SandboxReadOnly,
    /// The production environment, restricted to authenticated reads.
    ProductionReadOnly,
    /// Production writes restricted by a privately constructed native authority.
    ProductionTrading,
}

impl OndoAuthenticationScope {
    /// Returns the environment this scope authenticates against.
    #[must_use]
    pub const fn environment(self) -> OndoEnvironment {
        match self {
            Self::SandboxTrading | Self::SandboxReadOnly => OndoEnvironment::Sandbox,
            Self::ProductionReadOnly | Self::ProductionTrading => OndoEnvironment::Production,
        }
    }

    /// Returns whether this scope refuses every signed write.
    #[must_use]
    pub const fn is_read_only(self) -> bool {
        !self.permits_writes()
    }

    /// Returns whether this scope may send a signed write (`POST` or `DELETE`).
    ///
    /// Trading scopes permit writes subject to their transport admission. Production additionally
    /// requires its bounded run authority; both read-only scopes always refuse writes.
    #[must_use]
    pub const fn permits_writes(self) -> bool {
        matches!(self, Self::SandboxTrading | Self::ProductionTrading)
    }

    /// Returns the scope's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SandboxTrading => "sandbox_trading",
            Self::SandboxReadOnly => "sandbox_read_only",
            Self::ProductionReadOnly => "production_read_only",
            Self::ProductionTrading => "production_trading",
        }
    }
}

/// The result of comparing the configured venue account id with the authenticated account.
///
/// This is deliberately three-valued: a missing configuration or an answer that carries no
/// comparable identifier is [`Self::Unknown`] and never [`Self::Matched`]. "No evidence" is not
/// evidence of a match.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OndoAccountIdentity {
    /// The authenticated account's identifier equals the configured one.
    Matched,
    /// The authenticated account's identifier differs from the configured one.
    Mismatch,
    /// No comparison was possible: no expected identifier is configured, or the account answer
    /// carried none this adapter can read.
    #[default]
    Unknown,
}

impl OndoAccountIdentity {
    /// Returns the identity's name, for a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Mismatch => "mismatch",
            Self::Unknown => "unknown",
        }
    }
}

/// Classification of an Ondo Perps market's tradability.
///
/// The official schema declares no status field on a perps trading pair, and the 2026-09-14
/// production capture confirms the venue sends none, so this is an open venue set: a raw value this
/// adapter does not recognise classifies as [`Self::Unknown`] and never as [`Self::Active`]. Never
/// infer it from WebSocket prices.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MarketStatus {
    /// The venue reports the market as tradable.
    Active,
    /// The venue reports the market as not tradable.
    Disabled,
    /// The raw status is present but is not a value this adapter recognises.
    Unknown,
}

impl MarketStatus {
    /// Maps a raw venue status string onto a classified status.
    ///
    /// The table is deliberately as small as the evidence: no `/v1/markets` response has ever been
    /// observed to carry a status string at all (0 status strings across the 81 markets of the
    /// 2026-09-14 production capture). Both arms are therefore defensive rather than grounded -
    /// `active` and `disabled` are the synthetic fixture's values, and the `disabled` the venue
    /// really sends is a boolean field, not a string this map ever sees.
    /// The synthetic fixture's `halted` is explicitly *not* an official value, so it classifies
    /// as [`Self::Unknown`] instead of being guessed into an existing state.
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        match raw {
            "active" => Self::Active,
            "disabled" => Self::Disabled,
            _ => Self::Unknown,
        }
    }

    /// Returns `true` when new orders may be sent for a market in this state.
    #[must_use]
    pub const fn is_tradable(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// The response field a market status was read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MarketStatusSource {
    /// The `status` string field of a trading pair.
    StatusString,
    /// The boolean `disabled` flag, present in the payload as a boolean.
    DisabledFlag,
    /// Neither field was present in the payload.
    ///
    /// On a perps trading pair this is the venue's way of saying "enabled" and resolves to
    /// [`MarketStatus::Active`] (see [`MarketStatusInfo::resolve`]). It stays its own variant so an
    /// absent flag can never be recorded as an explicit `false`.
    Absent,
}

/// A classified market status together with the verbatim venue value it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketStatusInfo {
    status: MarketStatus,
    source: MarketStatusSource,
    raw: Option<String>,
}

impl MarketStatusInfo {
    /// Resolves a market status from the candidate fields of one market record.
    ///
    /// Precedence, and the single place where this is decided (`test_data/conflicts.md` conflict 5,
    /// **RESOLVED BY OBSERVATION** from the 2026-09-14 production capture of `GET /v1/markets`,
    /// `reports/ondo-acceptance/20260914T142730Z-p1-build/rest-capture/markets.json` - 81 markets;
    /// the tree's `test_data/rest/markets_observed_20260914.json` is a 4-pair excerpt of it and
    /// cannot support the counts below):
    ///
    /// 1. the string `status` field when present, read verbatim through [`MarketStatus::from_raw`];
    ///    an unrecognised value stays [`MarketStatus::Unknown`];
    /// 2. otherwise the boolean `disabled` flag (`true` -> disabled, `false` -> active);
    /// 3. otherwise [`MarketStatus::Active`], with the absence recorded as
    ///    [`MarketStatusSource::Absent`]. The venue emits `disabled` only on markets that *are*
    ///    disabled (20 of 81 captured markets) and omits the key entirely on the enabled ones (61 of
    ///    81, both P1 targets included), so absence is the venue's own way of saying "enabled".
    ///
    /// The string field wins because it is the only field that can express a value the adapter does
    /// not know; classifying from the boolean first would silently turn an unrecognised raw string
    /// into `active`, which is exactly the failure mode the conflict table forbids.
    ///
    /// Rule 3 is the only inference here and it stays narrow: it fires when the record carries no
    /// status evidence *at all*. A record that carries a marker the adapter cannot read never
    /// reaches this function -- the response schema types `status` as a string and `disabled` as a
    /// boolean, so a wrong-typed member fails the whole decode closed as [`crate::http::error::OndoHttpError::Decode`]
    /// rather than being coerced into `active` (see `tests/http_contract.rs`,
    /// `test_a_wrong_typed_status_marker_fails_closed`). Absence is the only shape read as enabled,
    /// and it is recorded as absence.
    ///
    /// `isClosed` is deliberately not an input: it reports whether the *underlying* market is
    /// open, not whether the perp is tradable, and the two must never collapse into one field.
    #[must_use]
    pub fn resolve(status: Option<&str>, disabled: Option<bool>) -> Self {
        match (status, disabled) {
            (Some(raw), _) => Self {
                status: MarketStatus::from_raw(raw),
                source: MarketStatusSource::StatusString,
                raw: Some(raw.to_string()),
            },
            (None, Some(disabled)) => Self {
                status: if disabled {
                    MarketStatus::Disabled
                } else {
                    MarketStatus::Active
                },
                source: MarketStatusSource::DisabledFlag,
                raw: None,
            },
            (None, None) => Self {
                status: MarketStatus::Active,
                source: MarketStatusSource::Absent,
                raw: None,
            },
        }
    }

    /// Returns the classified status.
    #[must_use]
    pub const fn status(&self) -> MarketStatus {
        self.status
    }

    /// Returns the field the status was read from.
    #[must_use]
    pub const fn source(&self) -> MarketStatusSource {
        self.source
    }

    /// Returns the verbatim venue status string, when the status came from a string field.
    #[must_use]
    pub fn raw(&self) -> Option<&str> {
        self.raw.as_deref()
    }

    /// Returns `true` when new orders may be sent for this market.
    #[must_use]
    pub const fn is_tradable(&self) -> bool {
        self.status.is_tradable()
    }
}

/// Where a maker or taker fee rate came from.
///
/// The three variants are the three evidence classes the venue can produce. They are kept
/// distinct because only the first is account-specific, and a documentation assumption is
/// not evidence about the account at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FeeSource {
    /// A rate the authenticated account reported for itself.
    AccountReported,
    /// A rate read from public market metadata, stamped with when it was read.
    PublicMetadata,
    /// A rate assumed from dated documentation rather than observed metadata.
    DocumentationAssumption,
}

/// A fee rate as a decimal fraction of filled notional, with the provenance needed to weigh it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeRate {
    rate: Decimal,
    source: FeeSource,
    field: String,
    origin: String,
    fetched_at: Option<UnixNanos>,
}

impl FeeRate {
    /// Creates a fee rate the authenticated account reported for itself.
    #[must_use]
    pub fn account_reported(
        rate: Decimal,
        field: impl Into<String>,
        origin: impl Into<String>,
        fetched_at: UnixNanos,
    ) -> Self {
        Self {
            rate,
            source: FeeSource::AccountReported,
            field: field.into(),
            origin: origin.into(),
            fetched_at: Some(fetched_at),
        }
    }

    /// Creates a fee rate read from public market metadata.
    #[must_use]
    pub fn public_metadata(
        rate: Decimal,
        field: impl Into<String>,
        origin: impl Into<String>,
        fetched_at: UnixNanos,
    ) -> Self {
        Self {
            rate,
            source: FeeSource::PublicMetadata,
            field: field.into(),
            origin: origin.into(),
            fetched_at: Some(fetched_at),
        }
    }

    /// Creates a fee rate assumed from dated documentation.
    ///
    /// The observation time is unset by construction: an assumption has none, and stamping one
    /// on the day it was read would make it look like a fresh observation.
    #[must_use]
    pub fn documentation_assumption(
        rate: Decimal,
        field: impl Into<String>,
        origin: impl Into<String>,
    ) -> Self {
        Self {
            rate,
            source: FeeSource::DocumentationAssumption,
            field: field.into(),
            origin: origin.into(),
            fetched_at: None,
        }
    }

    /// Returns the fee rate as a decimal fraction of filled notional.
    #[must_use]
    pub const fn rate(&self) -> Decimal {
        self.rate
    }

    /// Returns where the rate came from.
    #[must_use]
    pub const fn source(&self) -> FeeSource {
        self.source
    }

    /// Returns the metadata field or documentation key the rate was read from.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the endpoint or document the rate was read from.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Returns when the rate was obtained, or `None` for an undated assumption.
    #[must_use]
    pub const fn fetched_at(&self) -> Option<UnixNanos> {
        self.fetched_at
    }
}

/// The maker and taker fee rates known for one market.
///
/// [`None`] means the payload did not carry the rate. It never means a zero fee, so a cost
/// model must treat an absent rate as unknown rather than free.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarketFees {
    maker: Option<FeeRate>,
    taker: Option<FeeRate>,
}

impl MarketFees {
    /// Creates the fee pair for a market.
    #[must_use]
    pub const fn new(maker: Option<FeeRate>, taker: Option<FeeRate>) -> Self {
        Self { maker, taker }
    }

    /// Returns the maker fee rate, when the payload carried one.
    #[must_use]
    pub const fn maker(&self) -> Option<&FeeRate> {
        self.maker.as_ref()
    }

    /// Returns the taker fee rate, when the payload carried one.
    #[must_use]
    pub const fn taker(&self) -> Option<&FeeRate> {
        self.taker.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::active("active", MarketStatus::Active)]
    #[case::disabled("disabled", MarketStatus::Disabled)]
    #[case::invented_halted("halted", MarketStatus::Unknown)]
    #[case::active_uppercase("ACTIVE", MarketStatus::Unknown)]
    #[case::empty("", MarketStatus::Unknown)]
    fn test_market_status_from_raw(#[case] raw: &str, #[case] expected: MarketStatus) {
        assert_eq!(MarketStatus::from_raw(raw), expected);
    }

    #[rstest]
    fn test_market_status_tradability() {
        assert!(MarketStatus::Active.is_tradable());
        assert!(!MarketStatus::Disabled.is_tradable());
        assert!(!MarketStatus::Unknown.is_tradable());
    }

    #[rstest]
    fn test_resolve_prefers_the_status_string() {
        let info = MarketStatusInfo::resolve(Some("halted"), Some(false));

        assert_eq!(info.status(), MarketStatus::Unknown);
        assert_eq!(info.source(), MarketStatusSource::StatusString);
        assert_eq!(info.raw(), Some("halted"));
        assert!(!info.is_tradable());
    }

    #[rstest]
    #[case::disabled_flag(true, MarketStatus::Disabled)]
    #[case::enabled_flag(false, MarketStatus::Active)]
    fn test_resolve_falls_back_to_the_disabled_flag(
        #[case] disabled: bool,
        #[case] expected: MarketStatus,
    ) {
        let info = MarketStatusInfo::resolve(None, Some(disabled));

        assert_eq!(info.status(), expected);
        assert_eq!(info.source(), MarketStatusSource::DisabledFlag);
        assert_eq!(info.raw(), None);
    }

    /// CHANGED 2026-09-14 (spec change, live observation): a record with neither field used to
    /// resolve to `Unknown` and fail the load. The 2026-09-14 capture of `GET /v1/markets` shows
    /// the venue emits `disabled` only on disabled markets (20 of 81) and omits it on the enabled
    /// ones (61 of 81, both P1 targets), so absence is the venue's "enabled". Absence is still
    /// recorded as absence, never as an explicit `false`.
    #[rstest]
    fn test_resolve_without_any_field_is_active_by_the_venue_convention() {
        let info = MarketStatusInfo::resolve(None, None);

        assert_eq!(info.status(), MarketStatus::Active);
        assert_eq!(info.source(), MarketStatusSource::Absent);
        assert_eq!(info.raw(), None);
        assert!(info.is_tradable());
    }

    /// An absent flag and an explicit `false` resolve to the same status, and stay distinguishable:
    /// the source is the only thing that records which shape the payload used.
    #[rstest]
    fn test_resolve_keeps_an_absent_flag_distinct_from_an_explicit_false() {
        let absent = MarketStatusInfo::resolve(None, None);
        let explicit = MarketStatusInfo::resolve(None, Some(false));

        assert_eq!(absent.status(), explicit.status());
        assert_eq!(explicit.status(), MarketStatus::Active);
        assert_ne!(absent.source(), explicit.source());
        assert_eq!(absent.source(), MarketStatusSource::Absent);
        assert_eq!(explicit.source(), MarketStatusSource::DisabledFlag);
    }

    #[rstest]
    fn test_fee_rate_keeps_its_provenance() {
        let fetched_at = UnixNanos::from(1_789_384_200_000_000_000);
        let rate = FeeRate::public_metadata(
            Decimal::from_str("0.00025").unwrap(),
            "takerFee",
            "/v1/markets",
            fetched_at,
        );

        assert_eq!(rate.rate(), Decimal::from_str("0.00025").unwrap());
        assert_eq!(rate.source(), FeeSource::PublicMetadata);
        assert_eq!(rate.field(), "takerFee");
        assert_eq!(rate.origin(), "/v1/markets");
        assert_eq!(rate.fetched_at(), Some(fetched_at));
    }

    #[rstest]
    fn test_documentation_assumption_has_no_observation_time() {
        let rate = FeeRate::documentation_assumption(
            Decimal::from_str("0.00025").unwrap(),
            "ONDO_TAKER_FEE_BPS",
            "https://docs.ondoperps.xyz/fees.md",
        );

        assert_eq!(rate.source(), FeeSource::DocumentationAssumption);
        assert_eq!(rate.fetched_at(), None);
    }

    #[rstest]
    fn test_market_fees_absence_is_not_zero() {
        let fees = MarketFees::default();

        assert!(fees.maker().is_none());
        assert!(fees.taker().is_none());
    }
}
