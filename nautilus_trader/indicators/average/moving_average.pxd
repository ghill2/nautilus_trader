# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2022 Nautech Systems Pty Ltd. All rights reserved.
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

from nautilus_trader.indicators.base.indicator cimport Indicator
from nautilus_trader.model.c_enums.price_type cimport PriceType


cdef class MovingAverage(Indicator):
    cdef readonly int period
    """The moving average period.\n\n:returns: `PriceType`"""
    cdef readonly PriceType price_type
    """The specified price type for extracting values from quote ticks.\n\n:returns: `PriceType`"""
    cdef readonly int count
    """The count of inputs received by the indicator.\n\n:returns: `int`"""
    cdef readonly double value
    """The current output value.\n\n:returns: `double`"""

    cpdef void update_raw(self, double value) except *
    cpdef void _increment_count(self) except *
    cpdef void _reset_ma(self) except *