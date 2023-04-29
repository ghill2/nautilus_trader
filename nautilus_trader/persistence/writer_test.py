import os
import tempfile

import pandas as pd
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetType
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data.tick import QuoteTick

from nautilus_trader.persistence.writer import float_to_int_dataframe
from nautilus_trader.persistence.writer import write_dataframe
from nautilus_trader.persistence.writer import write_objects
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.core.nautilus_pyo3.persistence import PythonCatalog
from nautilus_trader.persistence.wranglers import list_from_capsule
import pyarrow.parquet as pq

def test_writer_writes_quote_ticks_object():
    instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD")
    quotes = [
        QuoteTick(
            instrument_id=instrument.id,
            ask=Price.from_str("2.0"),
            bid=Price.from_str("2.1"),
            bid_size=Quantity.from_int(10),
            ask_size=Quantity.from_int(10),
            ts_event=0,
            ts_init=0,
        ),
        QuoteTick(
            instrument_id=instrument.id,
            ask=Price.from_str("2.0"),
            bid=Price.from_str("2.1"),
            bid_size=Quantity.from_int(10),
            ask_size=Quantity.from_int(10),
            ts_event=1,
            ts_init=1,
        )
    ]

    with tempfile.TemporaryDirectory() as tempdir:
        file = os.path.join(tempdir, "test_parquet_file.parquet")
        write_objects(quotes, file)

        session = PythonCatalog()
        session.add_file_with_query(
            "quotes",
            file,
            "SELECT * FROM quotes;",
            ParquetType.QuoteTick
        )

        for chunk in session.to_query_result():
            written_quotes = list_from_capsule(chunk)
            assert written_quotes == quotes
            return

def test_writer_writes_quote_tick_dataframe():
    instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD")
    df = pd.DataFrame.from_dict(
        {
            "instrument_id": ['GBP/USD.SIM', 'GBP/USD.SIM'],
            "bid": [2.1, 2.1],
            "ask": [2.0, 2.0],
            "bid_size": [10.0, 10.0],
            "ask_size": [10.0, 10.0],
            "ts_event": [0, 1],
            "ts_init": [0, 1],

        }
    )

    df = float_to_int_dataframe(df)

    with tempfile.TemporaryDirectory() as tempdir:
        file = os.path.join(tempdir, "test_parquet_file.parquet")
        write_dataframe(df, instrument, file)

        session = PythonCatalog()
        session.add_file_with_query(
            "quotes",
            file,
            "SELECT * FROM quotes;",
            ParquetType.QuoteTick
        )
        for chunk in session.to_query_result():
            written_quotes = list_from_capsule(chunk)
            written_df = pd.DataFrame.from_records([QuoteTick.to_raw(x) for x in written_quotes])
            assert written_df.equals(df)
            return

if __name__ == "__main__":
    test_writer_writes_quote_tick_dataframe()