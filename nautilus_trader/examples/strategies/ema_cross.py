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

from decimal import Decimal
from typing import Dict, Optional

from nautilus_trader.common.logging import LogColor
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.tick import TradeTick
from nautilus_trader.model.data.ticker import Ticker
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.orderbook.book import OrderBook
from nautilus_trader.model.orderbook.data import OrderBookData
from nautilus_trader.model.orders.market import MarketOrder
from nautilus_trader.trading.strategy import TradingStrategy
from nautilus_trader.trading.strategy import TradingStrategyConfig
from nautilus_trader.core.datetime import *

# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class EMACrossConfig(TradingStrategyConfig):
    """
    Configuration for ``EMACross`` instances.

    instrument_id : InstrumentId
        The instrument ID for the strategy.
    bar_type : BarType
        The bar type for the strategy.
    fast_ema_period : int
        The fast EMA period.
    slow_ema_period : int
        The slow EMA period.
    trade_size : str
        The position size per trade (interpreted as Decimal).
    order_id_tag : str
        The unique order ID tag for the strategy. Must be unique
        amongst all running strategies for a particular trader ID.
    oms_type : OMSType
        The order management system type for the strategy. This will determine
        how the `ExecutionEngine` handles position IDs (see docs).
    """

    instrument_id: str
    bar_type: str
    fast_ema_period: int = 10
    slow_ema_period: int = 20
    trade_size: Decimal


class EMACross(TradingStrategy):
    """
    A simple moving average cross example strategy.

    When the fast EMA crosses the slow EMA then enter a position at the market
    in that direction.

    Cancels all orders and flattens all positions on stop.

    Parameters
    ----------
    config : EMACrossConfig
        The configuration for the instance.
    """

    def __init__(self, config: EMACrossConfig):
        super().__init__(config)

        # Configuration
        self.instrument_id = InstrumentId.from_str(config.instrument_id)
        self.bar_type = BarType.from_str(config.bar_type)
        self.trade_size = Decimal(config.trade_size)

        # Create the indicators for the strategy
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)

        self.instrument: Optional[Instrument] = None  # Initialized in on_start

        self.notified = False
    def on_start(self):
        """Actions to be performed on strategy start."""
        self.instrument = self.cache.instrument(self.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.instrument_id}")
            self.stop()
            return

        # Register the indicators for updating
        self.register_indicator_for_bars(self.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.bar_type, self.slow_ema)

        # Get historical data
        self.request_bars(self.bar_type)
        # self.request_quote_ticks(self.instrument_id)
        # self.request_trade_ticks(self.instrument_id)

        # Subscribe to live data
        self.subscribe_bars(self.bar_type)
        self.subscribe_quote_ticks(self.instrument_id)
        # self.subscribe_trade_ticks(self.instrument_id)
        # self.subscribe_ticker(self.instrument_id)  # For debugging
        # self.subscribe_order_book_deltas(self.instrument_id, depth=20)  # For debugging
        # self.subscribe_order_book_snapshots(self.instrument_id, depth=20)  # For debugging

    def on_bar(self, bar: Bar):
        """
        Actions to be performed when the strategy is running and receives a bar.

        Parameters
        ----------
        bar : Bar
            The bar received.

        """
        """
        DUKA-1-005   2019-01-06 22:03:59.364999936+00:00
        DUKA-1-006      2019-01-06 22:06:29.988000+00:00
        """
        dt = unix_nanos_to_dt(bar.ts_event)
        
        
            
        print("\n")
        print(str(dt), bar.open, bar.high, bar.low, bar.close)   
        print(f"fast_ema: {self.fast_ema.value}, slow_ema: {self.slow_ema.value}")
        print("self.fast_ema.value >= self.slow_ema.value",
                self.fast_ema.value >= self.slow_ema.value)
        print("self.portfolio.is_flat(self.instrument_id)",
                self.portfolio.is_flat(self.instrument_id))
        print("self.portfolio.is_net_short(self.instrument_id)",
                self.portfolio.is_net_short(self.instrument_id))
        print("self.portfolio.is_net_long(self.instrument_id)",
                self.portfolio.is_net_long(self.instrument_id))
        print(self.cache.positions_open())
        
        # quit()
        # Check if indicators ready
        if not self.indicators_initialized():
            self.log.info(
                f"Waiting for indicators to warm up " f"[{self.cache.bar_count(self.bar_type)}]...",
                color=LogColor.BLUE,
            )
            return  # Wait for indicators to warm up...

        
        if not self.notified:
            print(f"Warming finished at bar {str(dt)}")
            self.notified = True
            # quit()
        
        # BUY LOGIC
        if self.fast_ema.value >= self.slow_ema.value:
            
            if self.portfolio.is_flat(self.instrument_id):
                self.buy()
            elif self.portfolio.is_net_short(self.instrument_id):
                print("buy flattening...")
                self.flatten_all_positions(self.instrument_id)
                
                self.buy()

       
        # SELL LOGIC
        elif self.fast_ema.value < self.slow_ema.value:
            if self.portfolio.is_flat(self.instrument_id):
                self.sell()
            elif self.portfolio.is_net_long(self.instrument_id):
                print("sell flattening...")
                self.flatten_all_positions(self.instrument_id)
                self.sell()

        # at 2019-01-05 10:00:00.000000872+00:00 this version opens a sell position from a long? why
        
        
        if str(dt).startswith("2019-01-05 10"):
            # 2019-01-06 23
            # :03:59.364999936+00:00
           
            pass
            # quit()
            
    def buy(self):
        """
        Users simple buy method (example).
        """
        print("BUY")
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.trade_size),
            # time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)

    def sell(self):
        """
        Users simple sell method (example).
        """
        print("SELL")
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(self.trade_size),
            # time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)

    def on_event(self, event: Event):
        """
        Actions to be performed when the strategy is running and receives an event.

        Parameters
        ----------
        event : Event
            The event received.

        """
        print(event)
        from nautilus_trader.model.events.position import PositionEvent
        if isinstance(event, PositionEvent):
            if str(event.position_id) == 'DUKA-1-006':
                pass
                
                # quit()
            

    def on_stop(self):
        """
        Actions to be performed when the strategy is stopped.
        """
        self.cancel_all_orders(self.instrument_id)
        self.flatten_all_positions(self.instrument_id)

        # Unsubscribe from data
        self.unsubscribe_bars(self.bar_type)
        self.unsubscribe_quote_ticks(self.instrument_id)
        # self.unsubscribe_trade_ticks(self.instrument_id)
        # self.unsubscribe_ticker(self.instrument_id)
        # self.unsubscribe_order_book_deltas(self.instrument_id)
        # self.unsubscribe_order_book_snapshots(self.instrument_id)

    def on_reset(self):
        """
        Actions to be performed when the strategy is reset.
        """
        # Reset indicators here
        self.fast_ema.reset()
        self.slow_ema.reset()

    def on_save(self) -> Dict[str, bytes]:
        """
        Actions to be performed when the strategy is saved.

        Create and return a state dictionary of values to be saved.

        Returns
        -------
        dict[str, bytes]
            The strategy state dictionary.

        """
        return {}

    def on_load(self, state: Dict[str, bytes]):
        """
        Actions to be performed when the strategy is loaded.

        Saved state values will be contained in the give state dictionary.

        Parameters
        ----------
        state : dict[str, bytes]
            The strategy state dictionary.

        """
        pass

    def on_dispose(self):
        """
        Actions to be performed when the strategy is disposed.

        Cleanup any resources used by the strategy here.

        """
        pass
