# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
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

import pandas as pd
import pytz

from nautilus_trader.config import ActorConfig

from cpython.datetime cimport datetime
from libc.stdint cimport uint64_t

from nautilus_trader.accounting.calculators cimport RolloverInterestCalculator
from nautilus_trader.backtest.exchange cimport SimulatedExchange
from nautilus_trader.core.correctness cimport Condition
from nautilus_trader.model.currency cimport Currency
from nautilus_trader.model.enums_c cimport AssetClass
from nautilus_trader.model.enums_c cimport PriceType
from nautilus_trader.model.identifiers cimport InstrumentId
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Money
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.orderbook.book cimport OrderBook
from nautilus_trader.model.position cimport Position


class SimulationModuleConfig(ActorConfig):
    pass


cdef class SimulationModule(Actor):
    """
    The base class for all simulation modules.

    Warnings
    --------
    This class should not be used directly, but through a concrete subclass.
    """

    def __init__(self, config: SimulationModuleConfig):
        super().__init__(config)
        self.exchange = None  # Must be registered

    def __repr__(self) -> str:
        return f"{type(self).__name__}"

    cpdef void register_venue(self, SimulatedExchange exchange):
        """
        Register the given simulated exchange with the module.

        Parameters
        ----------
        exchange : SimulatedExchange
            The exchange to register.

        """
        Condition.not_none(exchange, "exchange")

        self.exchange = exchange

    cpdef void process(self, uint64_t ts_now):
        """Abstract method (implement in subclass)."""
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    cpdef void log_diagnostics(self, LoggerAdapter log):
        """Abstract method (implement in subclass)."""
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    cpdef void reset(self):
        """Abstract method (implement in subclass)."""
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover


_TZ_US_EAST = pytz.timezone("US/Eastern")


class FXRolloverInterestConfig(ActorConfig):
    """
    Provides an FX rollover interest simulation module.

    Parameters
    ----------
    rate_data : pd.DataFrame
        The interest rate data for the internal rollover interest calculator.

    """
    rate_data: pd.DataFrame


cdef class FXRolloverInterestModule(SimulationModule):
    """
    Provides an FX rollover interest simulation module.


    Parameters
    ----------
    config  : FXRolloverInterestConfig
    """

    # def __init__(self, rate_data not None: pd.DataFrame, broker_markup = None):
    #     # TODO
    #     super().__init__()
    def __init__(self, config: FXRolloverInterestConfig):
        super().__init__(config)

        self._calculator = RolloverInterestCalculator(data=config.rate_data)
        self._rollover_time = None  # Initialized at first rollover
        self._rollover_applied = False
        self._rollover_totals = {}
        self._day_number = 0

        self._broker_markup = broker_markup

    @classmethod
    def from_file(cls, path: str):

        cdef object rate_data

        if path.endswith(".csv"):
            rate_data = pd.read_csv(path)
        else:
            raise RuntimeError("Unsupported rate data file extension")

        return cls(rate_data=rate_data)

    cpdef void process(self, uint64_t ts_now):
        """
        Process the given tick through the module.

        Parameters
        ----------
        ts_now : uint64_t
            The current UNIX time (nanoseconds) in the simulated exchange.

        """
        cdef datetime now = pd.Timestamp(ts_now, tz="UTC")
        cdef datetime rollover_local
        if self._day_number != now.day:
            # Set account statistics for new day
            self._day_number = now.day
            self._rollover_applied = False

            rollover_local = now.astimezone(_TZ_US_EAST)
            self._rollover_time = _TZ_US_EAST.localize(datetime(
                rollover_local.year,
                rollover_local.month,
                rollover_local.day,
                17),
            ).astimezone(pytz.utc)

        # Check for and apply any rollover interest
        if not self._rollover_applied and now >= self._rollover_time:
            self._apply_rollover_interest(now, self._rollover_time.isoweekday())
            self._rollover_applied = True

    cdef void _apply_rollover_interest(self, datetime timestamp, int iso_week_day):
        cdef list open_positions = self.exchange.cache.positions_open()

        cdef Position position
        cdef Instrument instrument
        cdef OrderBook book
        cdef dict mid_prices = {}  # type: dict[InstrumentId, float]
        cdef Currency currency
        cdef double mid
        cdef double rollover
        cdef double xrate
        cdef Money rollover_total
        cdef double base_rate
        cdef double quote_rate
        cdef double rate_diff
        cdef double swap_charge
        cdef double close_price
        cdef double markup
        cdef double qty
        cdef double swap_charge_base
        for position in open_positions:
            instrument = self.exchange.instruments[position.instrument_id]
            if instrument.asset_class != AssetClass.FX:
                continue  # Only applicable to FX

            mid = mid_prices.get(instrument.id, 0.0)
            if mid == 0.0:
                book = self.exchange.get_book(instrument.id)
                mid = book.midpoint()
                if mid is None:
                    mid = book.best_bid_price()
                if mid is None:
                    mid = book.best_ask_price()
                if mid is None:  # pragma: no cover
                    raise RuntimeError("cannot apply rollover interest, no market prices")
                mid_prices[instrument.id] = Price(float(mid), precision=instrument.price_precision)

            close_price = float(mid_prices[instrument.id])

            base_rate, quote_rate = self._calculator.calc_overnight_rate(
                position.instrument_id,
                timestamp,
            )

            if base_rate >= quote_rate:
                rate_diff = base_rate - quote_rate
            else:
                rate_diff = quote_rate - base_rate

            swap_charge = 0
            qty = position.quantity.as_f64_c()

            if self._broker_markup:
                markup = rate_diff * self._broker_markup
            else:
                markup = 0

            if position.is_long_c() and base_rate > quote_rate \
            or position.is_short_c() and quote_rate > base_rate:
                swap_charge = (qty * (rate_diff - markup) / 100) * (close_price / 365)

            if position.is_long_c() and base_rate <= quote_rate \
            or position.is_short_c() and quote_rate <= base_rate:
                swap_charge = -(qty * (rate_diff + markup) / 100) * (close_price / 365)

            if iso_week_day == 3:  # Book triple for Wednesdays
                swap_charge *= 3
            elif iso_week_day == 5:  # Book triple for Fridays (holding over weekend)
                swap_charge *= 3

            xrate = self.exchange.cache.get_xrate(
                        venue=instrument.id.venue,
                        from_currency=instrument.quote_currency,
                        to_currency=self.exchange.base_currency,
                        price_type=PriceType.MID,
                    )

            swap_charge_home = swap_charge * xrate

            position.rollover_date.append(str(timestamp))
            position.rollover_rate.append(rate_diff)
            position.rollover_amount.append(swap_charge_home) # in home currency

            # position.rollover_total += swap_charge_home # calculate on close
            # position.rollover_total_base += swap_charge_base

            # position.rollover_data.append(
            #     {   "amount": swap_charge_base,
            #         "rate": rate_diff,
            #         "date": timestamp,
            #     }
            # )

    cpdef void log_diagnostics(self, LoggerAdapter log):
        """
        Log diagnostics out to the `BacktestEngine` logger.

        Parameters
        ----------
        log : LoggerAdapter
            The logger to log to.

        """
        account_balances_starting = ', '.join([b.to_str() for b in self.exchange.starting_balances])
        account_starting_length = len(account_balances_starting)
        rollover_totals = ', '.join([b.to_str() for b in self._rollover_totals.values()])
        log.info(f"Rollover interest (totals): {rollover_totals}")

    cpdef void reset(self):
        self._rollover_time = None  # Initialized at first rollover
        self._rollover_applied = False
        self._rollover_totals = {}
        self._day_number = 0
