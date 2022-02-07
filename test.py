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
import pytower
events = []
class TestStrategy(TradingStrategy):

    def __init__(self, instrument):
        super().__init__()
        self.instrument = instrument
        self.i = 0
        self.trade_size = 1_000_000
    def on_start(self):
        self.subscribe_quote_ticks(self.instrument.id)
    def on_quote_tick(self, tick):
        
        pytower.tick = tick.bid
        self.i += 1
        # if self.i == 1:
        #     self.sell()
        # if self.i == 2000:
        #     self.buy()
        if self.i == 1000:
            self.buy()
        if self.i == 2000:
            self.flatten_all_positions(self.instrument.id)
        if self.i == 3000:
            self.buy()
        if self.i == 4000:
            self.flatten_all_positions(self.instrument.id)
        if self.i == 5000:
            self.buy()
        if self.i == 6000:
            self.flatten_all_positions(self.instrument.id)
        if self.i == 7000:
            self.buy()
        if self.i == 8000:
            self.flatten_all_positions(self.instrument.id)
        # if self.i == 50:
        #     self.flatten_all_positions(self.instrument.id)
        # if self.i == 4000:
        #     self.buy()
        # if self.i == 6000:
        #     self.buy()
        # if self.i == 8000:
        #     self.buy()
        # if self.i == 10_000:
        #     self.sell()
        # if self.i == 12_000:
        #     self.sell()
        # if self.i == 14_000:
        #     self.sell()
        
    def on_event(self, event):
        events.append(str(type(event)))
        
    def buy(self):
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.trade_size),
        )

        self.submit_order(order)
    def sell(self):
        """
        Users simple sell method (example).
        """
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(self.trade_size),
            # time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)

if __name__ == "__main__":
    CATALOG_PATH = f"{CATALOG_DIR}/DUKA/EURUSD"
    catalog = DataCatalog(CATALOG_PATH)

    
    start_date = dt_to_unix_nanos(pd.Timestamp('2010-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2010-01-02', tz='UTC'))
    
    ticks_df = pd.read_feather('/data/EURUSD-2019-T1.feather')
    
    
    
    len_ = int(len(ticks_df)/10)
    len_ = 20_000
    ticks_df = ticks_df[:len_]
    assert len(ticks_df) > 18_000
    ticks_df.set_index('date', inplace=True)
     
    
    instrument = catalog.instruments(as_nautilus=True)[0] #instrument.id is None

    wrangler = QuoteTickDataWrangler(instrument=instrument)
    start = perf_counter()
    ticks= wrangler.process(ticks_df)
    # ticks = catalog.quote_ticks(start=start_date, end=end_date, as_nautilus=True)
    stop = perf_counter()
    tick_creation_time = stop-start
    # with old wrangler: Tick creation time: 13.708742852999421 secs
    # 

    venue = Venue("DUKA")
    # this failed on update
    
    bar_type =  BarType(
        instrument.id,
        BarSpecification(1, BarAggregation.HOUR, PriceType.BID),
        aggregation_source=AggregationSource.INTERNAL #IMPORTANT
    )

    config = BacktestEngineConfig(
        trader_id="BACKTESTER-001",
        risk_engine={
            "bypass":True,
        }
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
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(engine.trader.generate_orders_report())
        print(engine.trader.generate_positions_report())
        
        print(set(events))
        # print(pytower.common.values.dataframe())
        path = path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "floats.csv"
        )
        pytower.common.values.write(path=path)
        from pytower.common.util import zip
        zip(path)
