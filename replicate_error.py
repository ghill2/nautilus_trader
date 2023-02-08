import os
import sys
from decimal import Decimal

import pandas as pd
import pytest
from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events.position import PositionOpened
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders.market import MarketOrder

from nautilus_trader.backtest.data.providers import TestDataProvider
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from nautilus_trader.backtest.data.wranglers import QuoteTickDataWrangler
from nautilus_trader.backtest.data.wranglers import TradeTickDataWrangler
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.engine import ExecEngineConfig
from nautilus_trader.backtest.engine import RiskEngineConfig
from nautilus_trader.backtest.modules import FXRolloverInterestConfig
from nautilus_trader.backtest.modules import FXRolloverInterestModule

from nautilus_trader.config import StrategyConfig
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig
from nautilus_trader.examples.strategies.ema_cross_stop_entry import EMACrossStopEntry
from nautilus_trader.examples.strategies.ema_cross_stop_entry import EMACrossStopEntryConfig
from nautilus_trader.examples.strategies.ema_cross_trailing_stop import EMACrossTrailingStop
from nautilus_trader.examples.strategies.ema_cross_trailing_stop import EMACrossTrailingStopConfig
from nautilus_trader.examples.strategies.market_maker import MarketMaker
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalance
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalanceConfig
from nautilus_trader.model.currencies import AUD
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import GBP
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.tick import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.betting import BettingInstrument
from nautilus_trader.model.objects import Money
from nautilus_trader.model.orderbook.data import OrderBookData

from nautilus_trader.model.data.bar import Bar
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.core.message import Event
from tests import TEST_DATA_DIR
from tests.integration_tests.adapters.betfair.test_kit import BetfairDataProvider


ASK_BAR_TYPE_EXTERNAL = BarType.from_str("GBP/USD.SIM-1-MINUTE-ASK-EXTERNAL")
BID_BAR_TYPE_EXTERNAL = BarType.from_str("GBP/USD.SIM-1-MINUTE-BID-EXTERNAL")

ASK_BAR_TYPE_INTERNAL = BarType.from_str("GBP/USD.SIM-1-MINUTE-ASK-INTERNAL")
BID_BAR_TYPE_INTERNAL = BarType.from_str("GBP/USD.SIM-1-MINUTE-BID-INTERNAL")

class TestStrategy(Strategy):
    def __init__(self, bar_type: BarType):
        super().__init__()
        self.i = 0
        self._bar_type = bar_type
        self.open_price = None # saved when the strategy opens the position

    def on_event(self, event: Event):
        if isinstance(event, PositionOpened):
            self.open_price = event.avg_px_open

    def on_start(self):
        self.subscribe_bars(self._bar_type)

    def on_bar(self, bar: Bar):
        if self.i == 2:
            self.log.info(repr(bar), LogColor.CYAN)
            self.buy()
        self.i += 1

    def buy(self):
        order: MarketOrder = self.order_factory.market(
            instrument_id=self._bar_type.instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str("100000.0")
        )

        self.submit_order(order)

class TestInternalBars:

    def __init__(self):
        self.instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD")

    def setup_engine(self) -> BacktestEngine:
        # Fixture Setup
        config = BacktestEngineConfig(
            bypass_logging=False,
            run_analysis=False,
            risk_engine=RiskEngineConfig(
                bypass=True,  # Example of bypassing pre-trade risk checks for backtests
                max_notional_per_order={"GBP/USD.SIM": 2_000_000},
            ),
        )
        engine = BacktestEngine(config=config)

        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
        )

        return engine

    def setup_engine_with_internal_data(self) -> BacktestEngine:
        engine = self.setup_engine()

        # Setup data
        wrangler = QuoteTickDataWrangler(self.instrument)
        provider = TestDataProvider()
        ticks = wrangler.process_bar_data(
            bid_data=provider.read_csv_bars("fxcm-gbpusd-m1-bid-2012.csv"),
            ask_data=provider.read_csv_bars("fxcm-gbpusd-m1-ask-2012.csv"),
        )
        engine.add_instrument(self.instrument)
        engine.add_data(ticks)
        return engine

    def setup_engine_with_external_data(self)  -> BacktestEngine:
        engine = self.setup_engine()

        # Setup wranglers
        bid_wrangler = BarDataWrangler(bar_type=BID_BAR_TYPE_EXTERNAL, instrument=self.instrument)

        ask_wrangler = BarDataWrangler(bar_type=ASK_BAR_TYPE_EXTERNAL, instrument=self.instrument)

        # Setup data
        provider = TestDataProvider()

        # Build externally aggregated bars
        bid_bars = bid_wrangler.process(
            data=provider.read_csv_bars("fxcm-gbpusd-m1-bid-2012.csv"),
        )
        ask_bars = ask_wrangler.process(
            data=provider.read_csv_bars("fxcm-gbpusd-m1-ask-2012.csv"),
        )

        engine.add_instrument(self.instrument)
        engine.add_data(bid_bars)
        engine.add_data(ask_bars)

        return engine

    def test_internal_bars_has_same_open_price_as_external_bars(self):
        # Arrange

        # Get the open price of the order created using INTERNAL bars
        strategy = TestStrategy(bar_type=ASK_BAR_TYPE_INTERNAL)
        engine = self.setup_engine_with_internal_data()
        engine.add_strategy(strategy)
        engine.run()
        open_price_internal = strategy.open_price

        # Get the open price of the order created using INTERNAL bars
        strategy = TestStrategy(bar_type=ASK_BAR_TYPE_EXTERNAL)
        engine = self.setup_engine_with_external_data()
        engine.add_strategy(strategy)
        engine.run()
        open_price_external = strategy.open_price

        print(open_price_internal, open_price_external)

        assert open_price_internal == open_price_external


if __name__ == "__main__":
    mod = TestInternalBars()
    mod.test_internal_bars_has_same_open_price_as_external_bars()











































