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

from copy import copy

import numpy as np
from libc.stdint cimport int64_t

import random

import pandas as pd

from nautilus_trader.core.correctness cimport Condition
from nautilus_trader.core.datetime cimport as_utc_index
from nautilus_trader.core.datetime cimport secs_to_nanos
from nautilus_trader.model.c_enums.aggressor_side cimport AggressorSideParser
from nautilus_trader.model.data.bar cimport Bar
from nautilus_trader.model.data.bar cimport BarType
from nautilus_trader.model.data.tick cimport QuoteTick
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity
from nautilus_trader.model.identifiers cimport InstrumentId

from datetime import datetime
from time import perf_counter

from pytower.common.date import dt_to_unix_nanos_vectorized
cdef class QuoteTickDataWrangler:
    """
    Provides a means of building lists of Nautilus `QuoteTick` objects.

    Parameters
    ----------
    instrument : Instrument
        The instrument for the data wrangler.
    """

    def __init__(self, Instrument instrument not None):
        self.instrument = instrument

    def process(
        self,
        data: pd.DataFrame,
        default_volume: float=1_000_000.0,
        ts_init_delta: int=0,
    ):
        
        Condition.false(data.empty, "data.empty")
        Condition.not_none(default_volume, "default_volume")

        as_utc_index(data)

        if "bid_size" not in data.columns:
            data["bid_size"] = float(default_volume)
        if "ask_size" not in data.columns:
            data["ask_size"] = float(default_volume)

        cdef int64_t[:] ts = np.ascontiguousarray(
            dt_to_unix_nanos_vectorized(data.index.to_series())
        )  # noqa
        
        cdef InstrumentId instrument_id = self.instrument.id
        
        cdef int i
        cdef list ticks = []
        cdef double[:] bid =  data.bid.to_numpy().astype(np.float64)
        cdef double[:] ask = data.ask.to_numpy().astype(np.float64)
        cdef int64_t[:] bid_size = data.bid_size.to_numpy().astype(np.int64)
        cdef int64_t[:] ask_size = data.ask_size.to_numpy().astype(np.int64)
        cdef QuoteTick tick
        
        for i in range(0, len(ts)):
            tick = QuoteTick.__new__(QuoteTick)
            tick.instrument_id = instrument_id
            tick.bid = bid[i]
            tick.ask = ask[i]
            tick.bid_size = bid_size[i]
            tick.ask_size = ask_size[i]
            tick.ts_event = ts[i]
            tick.ts_init = ts[i]
            ticks.append(tick)

        return ticks

   



cdef class TradeTickDataWrangler:
    """
    Provides a means of building lists of Nautilus `TradeTick` objects.

    Parameters
    ----------
    instrument : Instrument
        The instrument for the data wrangler.
    """

    def __init__(self, Instrument instrument not None):
        self.instrument = instrument

    def process(self, data: pd.DataFrame, ts_init_delta: int=0):
        """
        Process the given trade tick dataset into Nautilus `TradeTick` objects.

        Parameters
        ----------
        data : pd.DataFrame
            The data to process.
        ts_init_delta : int
            The difference in nanoseconds between the data timestamps and the
            `ts_init` value. Can be used to represent/simulate latency between
            the data source and the Nautilus system.

        Raises
        ------
        ValueError
            If `data` is empty.

        """
        Condition.not_none(data, "data")
        Condition.false(data.empty, "data.empty")

        data = as_utc_index(data)

        processed = pd.DataFrame(index=data.index)
        processed["price"] = data["price"].apply(lambda x: f'{x:.{self.instrument.price_precision}f}')
        processed["quantity"] = data["quantity"].apply(lambda x: f'{x:.{self.instrument.size_precision}f}')
        processed["aggressor_side"] = self._create_side_if_not_exist(data)
        processed["trade_id"] = data["trade_id"].apply(str)

        cdef int64_t[:] ts_events = np.ascontiguousarray([secs_to_nanos(dt.timestamp()) for dt in data.index], dtype=np.int64)  # noqa
        cdef int64_t[:] ts_inits = np.ascontiguousarray([ts_event + ts_init_delta for ts_event in ts_events], dtype=np.int64)  # noqa

        return list(map(
            self._build_tick,
            processed.values,
            ts_events,
            ts_inits,
        ))

    def _create_side_if_not_exist(self, data):
        if "side" in data.columns:
            return data["side"]
        else:
            return data["buyer_maker"].apply(lambda x: "SELL" if x is True else "BUY")

    # cpdef method for Python wrap() (called with map)
    cpdef TradeTick _build_tick(self, str[:] values, int64_t ts_event, int64_t ts_init):
        # Build a quote tick from the given values. The function expects the values to
        # be an ndarray with 4 elements [bid, ask, bid_size, ask_size] of type double.
        return TradeTick(
            instrument_id=self.instrument.id,
            price=Price(values[0], self.instrument.price_precision),
            size=Quantity(values[1], self.instrument.size_precision),
            aggressor_side=AggressorSideParser.from_str(values[2]),
            trade_id=values[3],
            ts_event=ts_event,
            ts_init=ts_init,
        )


cdef class BarDataWrangler:
    """
    Provides a means of building lists of Nautilus `Bar` objects.

    Parameters
    ----------
    bar_type : BarType
        The bar type for the wrangler.
    instrument : Instrument
        The instrument for the wrangler.
    """

    def __init__(
        self,
        BarType bar_type not None,
        Instrument instrument not None,
    ):
        Condition.not_none(bar_type, "bar_type")
        Condition.not_none(instrument, "instrument")

        self.bar_type = bar_type
        self.instrument = instrument

    def process(
        self,
        data: pd.DataFrame,
        default_volume: float=1_000_000.0,
        ts_init_delta: int=0,
    ):
        """
        Process the given bar dataset into Nautilus `Bar` objects.

        Expects columns ['open', 'high', 'low', 'close', 'volume'] with 'timestamp' index.
        Note: The 'volume' column is optional, will then use the `default_volume`.

        Parameters
        ----------
        data : pd.DataFrame
            The data to process.
        default_volume : float
            The default volume for each bar (if not provided).
        ts_init_delta : int
            The difference in nanoseconds between the data timestamps and the
            `ts_init` value. Can be used to represent/simulate latency between
            the data source and the Nautilus system.
        
        Returns
        -------
        list[Bar]

        Raises
        ------
        ValueError
            If `data` is empty.

        """
        Condition.not_none(data, "data")
        Condition.false(data.empty, "data.empty")
        Condition.not_none(default_volume, "default_volume")

        data = as_utc_index(data)

        if "volume" not in data:
            data["volume"] = float(default_volume)

        cdef int64_t[:] ts_events = np.ascontiguousarray([secs_to_nanos(dt.timestamp()) for dt in data.index], dtype=np.int64)  # noqa
        cdef int64_t[:] ts_inits = np.ascontiguousarray([ts_event + ts_init_delta for ts_event in ts_events], dtype=np.int64)  # noqa

        return list(map(
            self._build_bar,
            data.values,
            ts_events,
            ts_inits
        ))

    # cpdef method for Python wrap() (called with map)
    cpdef Bar _build_bar(self, double[:] values, int64_t ts_event, int64_t ts_init):
        # Build a bar from the given index and values. The function expects the
        # values to be an ndarray with 5 elements [open, high, low, close, volume].
        return Bar(
            bar_type=self.bar_type,
            open=Price(values[0], self.instrument.price_precision),
            high=Price(values[1], self.instrument.price_precision),
            low=Price(values[2], self.instrument.price_precision),
            close=Price(values[3], self.instrument.price_precision),
            volume=Quantity(values[4], self.instrument.size_precision),
            ts_event=ts_event,
            ts_init=ts_init,
        )
