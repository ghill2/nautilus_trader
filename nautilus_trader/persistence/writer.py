from pathlib import Path

import pandas as pd
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.instruments.base import Instrument
import pyarrow as pa
import pyarrow.parquet as pq

from nautilus_trader.model.objects import FIXED_SCALAR

"""
An example to write QuoteTicks to a Parquet file
"""

def float_to_int_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for name in df.columns:
        if df[name].dtype == float:
            df[name] = df[name].multiply(FIXED_SCALAR).astype('int64')
    return df

def write_objects(data: list[QuoteTick], path: str):
    assert all(type(x) is QuoteTick for x in data)  # same type
    assert all(x.instrument_id == data[0].instrument_id for x in data)  # same instrument_id
    assert len(data) > 0

    # Extract raw from ticks
    df = pd.DataFrame.from_records([QuoteTick.to_raw(x) for x in data])

    metadata = {
        "instrument_id": str(data[0].instrument_id),
        "price_precision": str(data[0].bid.precision),
        "size_precision": str(data[0].bid_size.precision),
    }

    _write_dataframe(df, metadata, path)

def write_dataframe(df: pd.DataFrame, instrument: Instrument, path: str):
    metadata = {
        "instrument_id": str(instrument.id),
        "price_precision": str(instrument.price_precision),
        "size_precision": str(instrument.size_precision),
    }
    df = float_to_int_dataframe(df)
    _write_dataframe(df, metadata, path)

def _write_dataframe(df: pd.DataFrame, metadata: dict, path: str) -> None:

    table = pa.Table.from_pandas(df)
    _write_parquet(table, metadata, path)

def _write_parquet(table: pa.Table, metadata: dict, path: str) -> None:
    # TODO use schema in schema.py
    schema = pa.schema([
        ("bid", pa.int64()),
        ("ask", pa.int64()),
        ("bid_size", pa.uint64()),
        ("ask_size", pa.uint64()),
        ("ts_event", pa.uint64()),
        ("ts_init", pa.uint64()),
    ])

    # Check columns exists
    for name in schema.names:
        assert name in table.schema.names, f"{name} not in {table.schema.names}"

    # Drop unused columns
    table = table.select(schema.names)

    # Convert integer dtypes if safe - checking for overflows or other unsafe conversions.
    table = table.cast(schema, safe=True)

    # Assert dataframe schema
    assert table.schema == schema

    # Write the table
    schema = schema.with_metadata(metadata)

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(table)









