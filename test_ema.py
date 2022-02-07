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

events = []

if __name__ == "__main__":
    CATALOG_PATH = f"{CATALOG_DIR}/DUKA/EURUSD"
    catalog = DataCatalog(CATALOG_PATH)

    start_date = dt_to_unix_nanos(pd.Timestamp('2010-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2010-01-02', tz='UTC'))
    
    ticks_df = pd.read_feather('/data/EURUSD-2019-T1.feather')
    ticks_df['bid_size'] = ticks_df['bid_size'] * 4_000_000
    ticks_df['ask_size'] = ticks_df['ask_size'] * 4_000_000
    
    df_new = pd.DataFrame()
    df_new['date'] = ticks_df['date']
    df_new['bid'] = ticks_df['bid']
    df_new['ask'] = ticks_df['ask']
    df_new['bid_size'] = ticks_df['bid_size']
    df_new['ask_size'] = ticks_df['ask_size']
    ticks_df = df_new
    ticks_df.set_index("date", inplace=True)
    
    len_ = int(len(ticks_df)/2)
    # len_ = 800_000
    ticks_df = ticks_df[:len_]
    assert len(ticks_df) > 18_000

    
    
    instrument = catalog.instruments(as_nautilus=True)[0] #instrument.id is None
    
    
    wrangler = QuoteTickDataWrangler(instrument=instrument)
    start = perf_counter()
    ticks= wrangler.process(ticks_df)
    # ticks = catalog.quote_ticks(start=start_date, end=end_date, as_nautilus=True)
    stop = perf_counter()
    tick_creation_time = stop-start
    # with old wrangler: Tick creation time: 13.708742852999421 secs
    

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
            "bypass":True
        },
        log_level="CRITICAL",
        bypass_logging=True
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
    
    config = EMACrossConfig(
        instrument_id=str(instrument.id),
        bar_type=str(bar_type),
        fast_ema_period=10,
        slow_ema_period=20,
        trade_size=1_000_000,
        order_id_tag="001",
    )
    
    
    strategy = EMACross(config=config)
    engine.add_strategy(strategy=strategy)
    
    
    print("Creating ticks....")
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
        100,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(engine.trader.generate_orders_report())
        print(engine.trader.generate_positions_report())
        print(engine.trader.generate_positions_report().ts_opened)
    import pytower

    
    
    
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(pytower.common.values.print_())
    
    
    print(engine.trader.analyzer.returns())
    # for event in strategy.events:
    #     print(event)
    