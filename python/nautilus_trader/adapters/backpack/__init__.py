# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Backpack public data and native read-only account integration."""

from nautilus_trader._fixup import fixup_module_names
from nautilus_trader._libnautilus.backpack import *  # noqa: F403


__all__ = [
    "BACKPACK",
    "BACKPACK_CLIENT_ID",
    "BACKPACK_VENUE",
    "BackpackDataClientConfig",
    "BackpackDataClientFactory",
    "BackpackInstrumentEconomics",
    "BackpackCredential",
    "BackpackQuota",
    "BackpackExecutionClientConfig",
    "BackpackExecutionClientFactory",
]

fixup_module_names(globals(), __name__)
del fixup_module_names
