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

//! One retained identity owner for reads and guarded mutation reports.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{AccountId, ClientOrderId},
    reports::PositionStatusReport,
};

use super::private::{BackpackPrivateObservation, decode_private};
use crate::{
    account::{
        models::{BackpackFill, BackpackOrder, BackpackPosition},
        reports::{
            BackpackFillObservation, BackpackOrderObservation, BackpackReportContext, fill_report,
            order_report, position_report,
        },
    },
    execution::owner::BackpackOrderOwner,
    identity::BackpackClientIdStore,
    provider::BackpackInstrumentProvider,
};

#[derive(Debug)]
pub(crate) enum IdentityReader {
    ReadOnly(BackpackClientIdStore),
    Restricted(Arc<BackpackOrderOwner>),
}
impl IdentityReader {
    pub(crate) fn venue_id(&self, client: &ClientOrderId) -> anyhow::Result<Option<u32>> {
        match self {
            Self::ReadOnly(store) => Ok(store.venue_id(client)),
            Self::Restricted(owner) => {
                owner.with_report_identity(|store, _| Ok(store.venue_id(client)))
            }
        }
    }
    pub(crate) fn owner(&self) -> Option<&Arc<BackpackOrderOwner>> {
        match self {
            Self::ReadOnly(_) => None,
            Self::Restricted(owner) => Some(owner),
        }
    }
}

pub(crate) struct ReportReader<'a> {
    pub account_id: AccountId,
    pub instruments: &'a BackpackInstrumentProvider,
    pub identities: &'a IdentityReader,
    pub ts_init: UnixNanos,
}
impl ReportReader<'_> {
    fn parse<T>(
        &self,
        parse: impl FnOnce(&BackpackReportContext<'_>) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match self.identities {
            IdentityReader::ReadOnly(store) => parse(&BackpackReportContext {
                account_id: self.account_id,
                instruments: self.instruments,
                identities: store,
                confirmed_orders: None,
                ts_init: self.ts_init,
            }),
            IdentityReader::Restricted(owner) => owner.with_report_identity(|store, bindings| {
                parse(&BackpackReportContext {
                    account_id: self.account_id,
                    instruments: self.instruments,
                    identities: store,
                    confirmed_orders: Some(bindings),
                    ts_init: self.ts_init,
                })
            }),
        }
    }
    pub(crate) fn fill(&self, raw: BackpackFill) -> anyhow::Result<BackpackFillObservation> {
        self.parse(|context| Ok(fill_report(raw, context)?))
    }
    pub(crate) fn order(&self, raw: BackpackOrder) -> anyhow::Result<BackpackOrderObservation> {
        self.parse(|context| Ok(order_report(raw, context)?))
    }
    pub(crate) fn position(&self, raw: &BackpackPosition) -> anyhow::Result<PositionStatusReport> {
        self.parse(|context| Ok(position_report(raw, context)?))
    }
    pub(crate) fn private(&self, raw: &[u8]) -> anyhow::Result<BackpackPrivateObservation> {
        self.parse(|context| Ok(decode_private(raw, context)?))
    }
}
