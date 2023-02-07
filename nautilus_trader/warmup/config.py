from __future__ import annotations

import numpy as np
import pandas as pd
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AggregationSource
from pyarrow import dataset as ds

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.warmup.tree import WarmupStart


class WarmupConfig(WarmupStart):
    def __init__(
            self,
            count: int,
            bar_type: BarType,
            unstable: int = 0,
            children: list[Indicator] = None
    ):
        PyCondition.positive_int(count, "count")
        # PyCondition.not_negative(unstable, "unstable")
        PyCondition.type(bar_type, BarType, "bar_type")

        self.parents = []

        if children is None:
            children = []
        PyCondition.list_type(children, Indicator, "children")
        for child in children:
            child.parent = self

        self.children = children

        assert bar_type.spec.is_time_aggregated(), "Only TIME aggregated BarTypes allowed"
        self.bar_type = bar_type.with_aggregation_source(AggregationSource.EXTERNAL)

        self.count = count

    def __str__(self):
        return f"{self.__class__.__name__}({self.count}-{self.bar_type})"

    def __repr__(self):
        return str(self)

    def to_timedelta(self) -> pd.Timedelta:
        return self.bar_type.spec.to_timedelta() * self.count

    def start_date(self, end_date: pd.Timestamp, catalog: ParquetDataCatalog) -> pd.Timestamp:
        """
        Calculate the start date based on the catalog's data.
        TODO: improve performance by querying the num_rows metadata directly in the parquet files
        """
        warmup_length = self.to_timedelta()
        stop_date = (end_date - pd.Timedelta(weeks=52)) - (warmup_length / 2)
        start_date = end_date - warmup_length * 1.25
        end_date -= pd.Timedelta(milliseconds=1)  # exclusive range end

        filter_expr = ds.field("bar_type").cast("string").isin([str(self.bar_type)])
        while start_date > stop_date:
            df = catalog.query(
                cls=Bar,
                filter_expr=filter_expr,
                start=start_date,
                end=end_date,
                as_nautilus=False,
                instrument_ids=[str(self.bar_type.instrument_id)],
                raise_on_empty=False,
                clean_instrument_keys=True,
                table_kwargs={"columns": "ts_event instrument_id".split()}
            )

            if df.empty:
                break

            timestamps = pd.Index(df.ts_event, dtype=np.uint64)
            if len(timestamps) >= self.count:
                return unix_nanos_to_dt(timestamps[-self.count])

            start_date -= warmup_length

        raise RuntimeError(
            f"Not enough data for {self} in catalog {catalog.path}"
        )

    def __eq__(self, other):
        return  self.count == other.count \
                and self.bar_type == other.bar_type \
                and self.children == other.children
