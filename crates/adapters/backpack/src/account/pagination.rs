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

//! Strict bounded offset traversal; exhausted headers do not prove account completeness.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use super::{BackpackAccountError, BackpackEvidenceGap};

/// One total wall-clock and memory budget, shared by every page and retry.
#[derive(Clone, Copy, Debug)]
pub struct BackpackReadBudget {
    pub(crate) page_size: u64,
    pub(crate) max_pages: u64,
    pub(crate) max_items: u64,
    pub(crate) timeout: Duration,
}
impl BackpackReadBudget {
    /// Validates finite traversal limits. Page size is at most the official 1000.
    ///
    /// # Errors
    ///
    /// Returns an error for zero limits, more than 1000 pages/1 million rows,
    /// a page larger than the item budget, or a deadline above five minutes.
    pub fn new(
        page_size: u64,
        max_pages: u64,
        max_items: u64,
        timeout: Duration,
    ) -> Result<Self, BackpackAccountError> {
        if !(1..=1000).contains(&page_size)
            || !(1..=1000).contains(&max_pages)
            || !(page_size..=1_000_000).contains(&max_items)
            || timeout.is_zero()
            || timeout > Duration::from_secs(300)
        {
            return Err(BackpackAccountError::InvalidBudget);
        }
        Ok(Self {
            page_size,
            max_pages,
            max_items,
            timeout,
        })
    }
}

/// Explicit inclusive/exclusive millisecond history interval; never advanced per page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackHistoryWindow {
    pub from_ms: u64,
    pub to_ms: u64,
}
impl BackpackHistoryWindow {
    /// Constructs a fixed [from, to) interval for one traversal.
    ///
    /// # Errors
    ///
    /// Returns an error for reversed/empty intervals or nanosecond range overflow.
    pub fn new(from_ms: u64, to_ms: u64) -> Result<Self, BackpackAccountError> {
        if from_ms >= to_ms || to_ms.checked_mul(1_000_000).is_none() {
            return Err(BackpackAccountError::InvalidField("history window"));
        }
        Ok(Self { from_ms, to_ms })
    }
}

/// Exact observed pagination headers, including the venue's unnormalized page index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackPageHeaders {
    pub page_count: u64,
    pub current_page: u64,
    pub page_size: u64,
    pub total: u64,
}
impl BackpackPageHeaders {
    /// Decodes all four mandatory unsigned decimal headers without defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if any header is missing, noncanonical or overflows.
    pub fn parse(headers: &HashMap<String, String>) -> Result<Self, BackpackAccountError> {
        fn value(
            headers: &HashMap<String, String>,
            key: &str,
        ) -> Result<u64, BackpackAccountError> {
            let raw = headers.get(key).ok_or(BackpackAccountError::Pagination)?;
            if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
                return Err(BackpackAccountError::Pagination);
            }
            raw.parse().map_err(|_| BackpackAccountError::Pagination)
        }
        Ok(Self {
            page_count: value(headers, "x-page-count")?,
            current_page: value(headers, "x-current-page")?,
            page_size: value(headers, "x-page-size")?,
            total: value(headers, "x-total")?,
        })
    }
}

/// Successful header exhaustion within configured limits, with explicit uncertainties.
/// No method interprets this evidence as never-executed, flat or execution-ready.
#[derive(Clone, Debug)]
pub struct BackpackHistoryEvidence {
    pub window: BackpackHistoryWindow,
    pub pages: u64,
    pub rows: u64,
    pub headers: BackpackPageHeaders,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}

/// A bounded traversal result; failures return errors, never successful empty histories.
#[derive(Clone, Debug)]
pub struct BackpackHistory<T> {
    pub records: Vec<T>,
    pub evidence: BackpackHistoryEvidence,
}

pub(crate) struct PageTracker {
    budget: BackpackReadBudget,
    first: Option<BackpackPageHeaders>,
    last: Option<BackpackPageHeaders>,
    pages: u64,
    rows: u64,
    keys: BTreeSet<String>,
}
impl PageTracker {
    pub(crate) fn new(budget: BackpackReadBudget) -> Self {
        Self {
            budget,
            first: None,
            last: None,
            pages: 0,
            rows: 0,
            keys: BTreeSet::new(),
        }
    }
    pub(crate) fn next_offset(&self) -> Result<u64, BackpackAccountError> {
        if self.pages >= self.budget.max_pages {
            Err(BackpackAccountError::Budget)
        } else {
            Ok(self.rows)
        }
    }
    pub(crate) fn accept(
        &mut self,
        headers: BackpackPageHeaders,
        keys: &[String],
    ) -> Result<bool, BackpackAccountError> {
        if self.pages >= self.budget.max_pages || headers.total > self.budget.max_items {
            return Err(BackpackAccountError::Budget);
        }
        if headers.page_size != self.budget.page_size
            || headers.current_page > headers.page_count.saturating_add(1)
        {
            return Err(BackpackAccountError::Pagination);
        }
        let expected_pages = headers.total.div_ceil(headers.page_size);
        if headers.page_count != expected_pages && !(headers.total == 0 && headers.page_count == 1)
        {
            return Err(BackpackAccountError::Pagination);
        }
        if let Some(first) = self.first {
            let expected_page = first
                .current_page
                .checked_add(self.pages)
                .ok_or(BackpackAccountError::Pagination)?;
            if first.total != headers.total
                || first.page_count != headers.page_count
                || first.page_size != headers.page_size
                || headers.current_page != expected_page
            {
                return Err(BackpackAccountError::Pagination);
            }
        } else if headers.current_page > 1 {
            // Header indexing is undocumented. Preserve and validate either origin;
            // never assume a page is complete merely because its index is zero/one.
            return Err(BackpackAccountError::Pagination);
        }
        let remaining = headers
            .total
            .checked_sub(self.rows)
            .ok_or(BackpackAccountError::Pagination)?;
        let expected_rows = remaining.min(headers.page_size);
        if u64::try_from(keys.len()).ok() != Some(expected_rows) {
            return Err(BackpackAccountError::Pagination);
        }
        for key in keys {
            if !self.keys.insert(key.clone()) {
                return Err(BackpackAccountError::NoProgress);
            }
        }
        self.first.get_or_insert(headers);
        self.last = Some(headers);
        self.pages += 1;
        self.rows += expected_rows;
        Ok(self.rows == headers.total)
    }
    pub(crate) fn finish(
        self,
        window: BackpackHistoryWindow,
        server_cutoff: bool,
    ) -> Result<BackpackHistoryEvidence, BackpackAccountError> {
        let headers = self.last.ok_or(BackpackAccountError::Pagination)?;
        if self.rows != headers.total {
            return Err(BackpackAccountError::Pagination);
        }
        let mut gaps = BTreeSet::from([
            BackpackEvidenceGap::AccountIdentityUnverified,
            BackpackEvidenceGap::NonAtomicSnapshot,
            BackpackEvidenceGap::RetentionOrReplicationUnknown,
        ]);
        if !server_cutoff {
            gaps.insert(BackpackEvidenceGap::CutoffNotServerEnforced);
        }
        Ok(BackpackHistoryEvidence {
            window,
            pages: self.pages,
            rows: self.rows,
            headers,
            gaps,
        })
    }
}
