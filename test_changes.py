import sys, os
import pandas as pd
from nautilus_trader.persistence.catalog import DataCatalog
import datetime
import pandas as pd

from nautilus_trader.persistence.catalog import DataCatalog
from nautilus_trader.persistence.external.core import process_files, write_objects
from nautilus_trader.persistence.external.readers import TextReader, CSVReader

from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.core.datetime import dt_to_unix_nanos

from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from pytower import TEST_CSV_DIR
from pytower import DATA_DIR, CATALOG_DIR, CSV_DIR, DATA_DIR
from pytower.common.util import Timer
from pytower.data.export.format import Format
from nautilus_trader.core.datetime import *
import os, shutil
from time import perf_counter

def _write_catalog(file, catalog_path, overwrite=False):
    
    if not overwrite and os.path.exists(catalog_path):
        print("No catalog written because catalog exists.")
        return
    AUDUSD = TestInstrumentProvider.default_fx_ccy("AUD/USD")
    def parser(df):
        
        format_ = Format.from_csv(file)
        df.columns = [x.replace("\r", "") for x in df.columns.to_list()]
        df = format_.rename_dataframe(df)
        
        for row in df.itertuples():
            yield QuoteTick(
                instrument_id=AUDUSD.id,
                bid=row.bid,
                ask=row.ask,
                bid_size=row.bid_size,
                ask_size=row.ask_size,
                ts_event=dt_to_unix_nanos(row.date),
                ts_init=dt_to_unix_nanos(row.date),
            )

    # Clear if it already exists, then create fresh
    if overwrite and os.path.exists(catalog_path):
        shutil.rmtree(catalog_path)
            
    if not os.path.exists(catalog_path):
        os.mkdir(catalog_path)

    catalog = DataCatalog(catalog_path)

    process_files(
        glob_path=file,
        reader=CSVReader(block_parser=parser),
        catalog=catalog,
    )
    write_objects(catalog, [AUDUSD])
def test_tick_deserialize_types():
    catalog_path = os.getcwd() + "/ignored/catalog"
    file = f"{TEST_CSV_DIR}/DUKA/EURUSD_2019.csv"
    _write_catalog(file, catalog_path, overwrite=True)

    start_date = dt_to_unix_nanos(pd.Timestamp('2019-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2020-01-01', tz='UTC'))
    
    
    catalog = DataCatalog(catalog_path)
    
    ticks = catalog.quote_ticks(start=start_date, end=end_date, as_nautilus=True)
    tick = ticks[0]
    assert type(tick.bid) == float
    assert type(tick.ask) == float
    assert type(tick.bid_size) == int
    assert type(tick.ask_size) == int
    
def test_tick_deserialize_performance():
    catalog_path = os.getcwd() + "/ignored/catalog/EURUSD_2019_FULL.csv"
    file = f"{CSV_DIR}/DUKA/EURUSD_2019.csv" # entire year
    _write_catalog(file, catalog_path)

    catalog = DataCatalog(catalog_path)
    
    start_date = dt_to_unix_nanos(pd.Timestamp('2019-01-01', tz='UTC'))
    end_date =  dt_to_unix_nanos(pd.Timestamp('2020-01-01', tz='UTC'))
    
    print("Getting quote ticks")
    start = perf_counter()
    ticks = catalog.quote_ticks(start=start_date, end=end_date, as_nautilus=True)
    stop = perf_counter()
    elapsed = (stop - start)
    print(unix_nanos_to_dt(ticks[-1].ts_event))
    print(elapsed)


from pytower.tests.providers import TestBacktestConfigProvider
from pytower.optimizer.optimizer import Optimizer
import itertools
from time import perf_counter
from pytower import TEST_TEMP_DIR
def test_tick_ema_cross_1():
    
    start = perf_counter()
    param_set = [{"fast_ema_period": 4, "slow_ema_period": 16}]
    configs = TestBacktestConfigProvider.ema_cross(
        param_set=param_set
    )
    optimizer = Optimizer(  configs=configs,
                            param_set=param_set,
                        #    batch_size_bytes=1
                        )
    optimizer.run()
    stop = perf_counter()
    print(f"Elapsed = {stop-start} seconds.")
def test_tick_ema_cross_multi_core():
    start = perf_counter()
    param_set = list(
        {'fast_ema_period': x, "slow_ema_period": y}
        for x, y in itertools.product(
            list(range(4, 24 + 1, 4)),
            list(range(16, 80 + 1, 4))
    ))
    
    configs = TestBacktestConfigProvider.ema_cross(
        param_set=param_set
    )
    optimizer = Optimizer(  configs=configs,
                            param_set=param_set,
                        #    batch_size_bytes=1
                        )
    optimizer.run()
    stop = perf_counter()
    print(f"Elapsed = {stop-start} seconds.")
    
def test_tick_ema_cross_writes_results():
    start = perf_counter()
    param_set = list(
        {'fast_ema_period': x, "slow_ema_period": y}
        for x, y in itertools.product(
            list(range(4, 24 + 1, 4)),
            list(range(16, 80 + 1, 4))
    ))
    results_dir = os.path.join(
        TEST_TEMP_DIR,
        os.path.basename(os.path.abspath(__file__).split(".")[0])
    )
    
    configs = TestBacktestConfigProvider.ema_cross(
        param_set=param_set,
        results_dir=results_dir
    )
    optimizer = Optimizer(  configs=configs,
                            param_set=param_set,
                        #    batch_size_bytes=1
                        )
    optimizer.run()
    stop = perf_counter()
    print(f"Elapsed = {stop-start} seconds.")
# from pytower import TEST_FEATHER_DIR
# 
# ticks = pd.read_feather(
#     os.path.join(TEST_FEATHER_DIR, 'DUKA', 'EURUSD_2020.feather')
# )
if __name__ == "__main__":
    test_tick_ema_cross_writes_results()