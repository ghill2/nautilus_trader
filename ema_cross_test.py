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
from nautilus_trader.backtest.node import BacktestNode
from distributed import Client
import inspect
import datetime
import dill as pickle
import itertools
from pytower import CATALOG_DIR
from pytower.data.results.collection import ResultsCollection
def on_save(data):
    import pandas as pd
    from pytower.plot.objects import Plot, Line, BarPlot, LinePlot
    from pytower.plot.window import Window
    from nautilus_trader.model.enums import PriceType
    
    print("\n", data.folder, data.params)
    df = data.dataframe(
        price_type=PriceType.BID,
        #offset=pd.Timedelta(hours=1)
    )
    with pd.option_context(
        "display.max_rows",
        100,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):  
        print(df)

    window = Window(
        index=df.index
        #range_start=datetime(2010, 9, 6, 0,0,0),
        #range_end=datetime(2012, 9, 6, 0,0,0)
    )
    
    window.add(
        BarPlot(
            lines=[
                Line(
                    data=df["net_position"],
                    color="red",
                    width=1,
                    name='net_position'
                ),
            ],
            data=df)
        )
    window.add(
        LinePlot(lines=[
                Line(
                    data=df.net_position,
                    color="purple",
                    width=1,
                    name='net_position'
                ),
            ]
        )
    )

    data.write(window=window)
if __name__ == "__main__":
    CATALOG_PATH = f"{CATALOG_DIR}/DUKA/EURUSD"
    catalog = DataCatalog(CATALOG_PATH)

    start_date = dt_to_unix_nanos(pd.Timestamp('2010-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2010-01-07', tz='UTC'))

    ticks = catalog.quote_ticks(start=start_date, end=end_date)
    # ticks = catalog.quote_ticks(start=start_date, end=end_date)
    print(ticks)
    exit()
    venue = Venue("DUKA")
    # this failed on update
    instrument = catalog.instruments(as_nautilus=True)[0] #instrument.id is None
    bar_type =  BarType(
        instrument.id,
        BarSpecification(1, BarAggregation.HOUR, PriceType.BID),
        aggregation_source=AggregationSource.INTERNAL #IMPORTANT
    )

    RESULTS_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.path.basename(os.path.abspath(__file__).split(".")[0])
    )
    
    
    #instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    
    # Create a `base` config object to be shared with all backtests
    config = BacktestRunConfig(
        venues=[
            BacktestVenueConfig(
                name="DUKA",
                #venue_type="ECN",
                oms_type="HEDGING",
                account_type="MARGIN",
                base_currency="USD",
                starting_balances=["1000000 USD"],
            )
        ],
        engine=BacktestEngineConfig(
            risk_engine={
                "bypass": True,  # Example of bypassing pre-trade risk checks for backtests
                "max_notional_per_order": {"GBP/USD.SIM": 2_000_000},
            },
            cache=CacheConfig(
                bar_capacity = 2147483647,
                tick_capacity = 1
            ),
            bypass_logging=True,
            #log_level='DBG',
            #run_analysis=False
        )
        

    )

    PARAM_SET = [
        {"fast_ema_period": x, "slow_ema_period": y}
        for x, y in itertools.product(
            list(range(10, 94 + 1, 2)),
            list(range(20, 94 + 1, 2))
        )
    ]
    PARAM_SET = PARAM_SET[0]
    
    configs = []
    save_func = pickle.dumps(on_save)
    
    
    collection = ResultsCollection.create(RESULTS_DIR, PARAM_SET)

    
    for params in PARAM_SET:
        
        #func = '{}.{}'.format(done.__module__, done.__qualname__)
        
        data_config = DataActorConfig(
            venue=str(venue),
            bar_type=str(bar_type),
            start_date=start_date,
            end_date=end_date,
            collection=str(collection),
            params=params,
            attrs='ladder_count'.split(),
            func=save_func
        )
        
        strategy_config = EMACrossConfig(
                    instrument_id=str(instrument.id),
                    bar_type=str(bar_type),
                    trade_size=str(Quantity(10000, 0)),
                    data_config=data_config,
                    **params
                )
   
        strategies = [
            ImportableStrategyConfig(
                path=f"{EMACross.__module__}:{EMACross.__name__}",
                config=strategy_config,
            ),
        ]
        data=[
            BacktestDataConfig(
                catalog_path=CATALOG_PATH,
                data_cls_path='nautilus_trader.model.data.tick.QuoteTick', # "nautilus_trader.model.data.tick:QuoteTick" f"{QuoteTick.__module__}.{QuoteTick.__name__}"
                instrument_id=str(instrument.id), # instrument_id=instrument.id.value, str(instrument.id)
                start_time=start_date,
                end_time=end_date,
            )
        ]
     
        # Create the final config
        new = config.replace(strategies=strategies, data=data)
        
        
      
        configs.append(new)

    
    print("\n\n".join(map(str, configs)))

    
    node = BacktestNode()
    
    # from dask.distributed import Client, LocalCluster
    # cluster = LocalCluster(
    #                     n_workers=64, 
    #                    threads_per_worker=1,
    #                    memory_limit='64GB')
    # client = Client(cluster)
    
    # client.close()
    # cluster.close()
    if len(PARAM_SET) == 1:
        node.run_sync(configs)
    else:
        task = node.build_graph(run_configs=configs)
        client = Client(n_workers=64, memory_limit='3GB')
        results = task.compute()[0]

    collection.write_csv()
    collection.save_heatmap("fast_ema_period", "slow_ema_period", "expectancy")