from nautilus_trader.backtest.data.providers import TestDataProvider
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.data.wranglers import QuoteTickDataWrangler
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.modules import FXRolloverInterestModule
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OMSType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.backtest.config import BacktestRunConfig, BacktestVenueConfig, BacktestDataConfig, BacktestEngineConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.bar import BarSpecification, BarType
from nautilus_trader.model.enums import BarAggregation, PriceType, AggregationSource
import pandas as pd
import os, sys
from nautilus_trader.persistence.catalog import DataCatalog
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.cache.cache import CacheConfig
from decimal import Decimal
from nautilus_trader.trading.config import ImportableStrategyConfig

from pytower.actors.data_actor import DataActor, DataActorConfig
from nautilus_trader.common.config import ImportableActorConfig
from pytower.examples.strategies.ema_cross import EMACrossConfig, EMACross
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.model.identifiers import Venue

from nautilus_trader.trading.strategy import TradingStrategy
from distributed import Client
import inspect
import datetime
import dill as pickle
import itertools
from pytower import CATALOG_DIR
from pytower.data.results.collection import ResultsCollection
import nautilus_trader
from time import perf_counter
class TestStrategy(TradingStrategy):

    def __init__(self, instrument):
        super().__init__()
        self.instrument = instrument
        self.i = 0
    def on_start(self):
        self.subscribe_quote_ticks(self.instrument.id)
    def on_quote_tick(self, tick):
        # if self.i == 0:
        #     self.buy()
        if self.i == 2000:
            self.buy()
        if self.i == 4000:
            self.buy()
        if self.i == 6000:
            self.buy()
        if self.i == 8000:
            self.buy()
        if self.i == 10_000:
            self.sell()
        if self.i == 12_000:
            self.sell()
        if self.i == 14_000:
            self.sell()
        self.i += 1
        
    def buy(self):
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(1_000_000),
        )

        self.submit_order(order)
    def sell(self):
        """
        Users simple sell method (example).
        """
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(1_000_000),
            # time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)


if __name__ == "__main__":
    CATALOG_PATH = f"{CATALOG_DIR}/DUKA/EURUSD"
    catalog = DataCatalog(CATALOG_PATH)

    
    start_date = dt_to_unix_nanos(pd.Timestamp('2010-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2011-01-01', tz='UTC'))
    
    ticks_df = pd.read_feather('/data/EURUSD-2019-T1.feather')
    # ticks_df = ticks_df[:30_000]    
    ticks_df.set_index('date',inplace=True)
    
    instrument = catalog.instruments(as_nautilus=True)[0] #instrument.id is None
    wrangler = QuoteTickDataWrangler(instrument=instrument)
    start = perf_counter()
    ticks= wrangler.process(ticks_df)
    # ticks = catalog.quote_ticks(start=start_date, end=end_date, as_nautilus=True)
    stop = perf_counter()
    tick_creation_time = stop-start
    

    venue = Venue("DUKA")
    # this failed on update
    
    bar_type =  BarType(
        instrument.id,
        BarSpecification(1, BarAggregation.HOUR, PriceType.BID),
        aggregation_source=AggregationSource.INTERNAL #IMPORTANT
    )

    config = BacktestEngineConfig(
        trader_id="BACKTESTER-001",
    )
    # Build the backtest engine
    engine = BacktestEngine(config=config)
    engine.add_instrument(instrument)
    engine.add_ticks(ticks)
    
    
    engine.add_venue(
        venue=venue,
        oms_type=OMSType.HEDGING,  # Venue will generate position IDs
        account_type=AccountType.MARGIN,
        base_currency=USD,  # Standard single-currency account
        starting_balances=[Money(1_000_000, USD)]
    )
    strategy = TestStrategy(instrument=instrument)
    engine.add_strategy(strategy=strategy)
    
    start = perf_counter()
    engine.run()
    stop = perf_counter()
    test_time = stop-start
    print(f"Tick creation time: {tick_creation_time} secs")
    print(f"Test time: {test_time} secs")
    
    import pytower
    print(dict(pytower.common.util.function_counts))
    print(f"iteration_count: {len(ticks)}")