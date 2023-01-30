import numpy as np
import pandas as pd
from pyarrow import dataset as ds

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.persistence.catalog import ParquetDataCatalog


class WarmupDataProvider:
    def __init__(
        self,
        catalog: ParquetDataCatalog,
        end_date: pd.Timestamp,
        grow_size: pd.Timedelta,
        bar_type: BarType,
        stop_date: pd.Timestamp,
    ):
        self._catalog = catalog
        self._grow_size = grow_size
        self._bar_type = bar_type.with_aggregation_source(AggregationSource.EXTERNAL)
        self._stop_date = stop_date
        self._end_date = end_date

    def __iter__(self) -> pd.Index:
        start = self._end_date
        stop_nanos = dt_to_unix_nanos(self._stop_date)

        while True:
            start -= self._grow_size
            timestamps = self._load_timestamps(start)
            if start <= self._stop_date:
                mask = timestamps >= stop_nanos
                yield timestamps.array[mask]
                return
            yield timestamps

    def _load_timestamps(self, start: pd.Timestamp) -> pd.Index:
        filter_expr = ds.field("bar_type").cast("string").isin([str(self._bar_type)])
        end = self._end_date - pd.Timedelta(milliseconds=1)  # exclusive range end
        df = self._catalog.query(
            cls=Bar,
            filter_expr=filter_expr,
            start=start,
            end=end,
            as_nautilus=False,
            instrument_ids=[str(self._bar_type.instrument_id)],
            raise_on_empty=False,
            clean_instrument_keys=True,
            table_kwargs={"columns": "ts_event instrument_id".split()},
        )
        if df.empty:
            return pd.Index([], dtype=np.int64)
        return pd.Index(df.ts_event, dtype=np.int64)


class WarmupRange:
    def __init__(self, count: int, bar_type: BarType):
        PyCondition.positive_int(count, "count")
        PyCondition.type(bar_type, BarType, "bar_type")

        self.count = count
        self.bar_type = bar_type.with_aggregation_source(AggregationSource.EXTERNAL)

    def to_timedelta(self) -> pd.Timedelta:
        return self.bar_type.spec.to_timedelta() * self.count

    def start_date(self, catalog: ParquetDataCatalog, end_date: pd.Timestamp) -> pd.Timestamp:
        """
        Calculate the warmup start date for the lookback based on the data in the ParquetDataCatalog

        To be implemented in a concrete sub-class
        """
        raise NotImplementedError()

    def __str__(self):
        return f"{self.__class__.__name__}({self.count}-{self.bar_type})"

    def __repr__(self):
        return str(self)

    def __eq__(self, other: "WarmupRange"):
        return self.count == other.count and self.bar_type == other.bar_type


class StaticWarmupRange(WarmupRange):
    def start_date(self, catalog: ParquetDataCatalog, end_date: pd.Timestamp) -> pd.Timestamp:

        PyCondition.type(end_date, pd.Timestamp, "end_date")
        PyCondition.type(catalog, ParquetDataCatalog, "catalog")

        warmup_length = self.to_timedelta()
        stop_date = (end_date - pd.Timedelta(weeks=52)) - (warmup_length / 2)

        timestamps_gen = WarmupDataProvider(
            catalog=catalog,
            end_date=end_date,
            grow_size=warmup_length,
            bar_type=self.bar_type,
            stop_date=stop_date,
        )

        for timestamps in timestamps_gen:

            if len(timestamps) >= self.count:
                # TODO timestamps[(self._count * -1) - 1]
                timestamps = timestamps[len(timestamps) - self.count :]
                assert len(timestamps) == self.count
                return unix_nanos_to_dt(timestamps[0])

        raise RuntimeError(
                f"Not enough data for {self} in catalog {catalog.path}"
        )


class DynamicWarmupRange(WarmupRange):
    def __init__(self, count: int, bar_type: BarType, indicator: Indicator):
        super().__init__(count=count, bar_type=bar_type)

        self._indicator = indicator

    def start_date(self, catalog: ParquetDataCatalog, end_date: pd.Timestamp) -> pd.Timestamp:
        """
        fisher indi sets warmup_len dynamically calculated from DC_HHT and multiplier values
        do warmup run to sample the warmup_len
        work out exact amount to add where indicator won't fail
        + 20% and do warmup again
        """
        pass


# def _prepend_next(self, start: pd.Timestamp) -> None:
#     data_new = self._load_timestamps(start)
#     return pd.Index(
#         pd.concat([data_new.to_series(), self._data.to_series()], ignore_index=True),
#         dtype=np.int64,
#     )
# for multiplier in np.arange(1.25, 5.0, 0.15):
#     start = end - (warmup_length * multiplier)
#     print(start, end)
#     timestamps = self._load_timestamps(catalog, self.bar_type, start, end)
#     print(unix_nanos_to_dt_vectorized(timestamps))
#     if len(timestamps) >= self.count:
#         timestamps = timestamps[len(timestamps) - self.count :]
#         assert len(timestamps) == self.count
#         return unix_nanos_to_dt(timestamps[0])
# TODO, concat instead of loading each time from end_date
#
#
#
#     def __iter__2(self) -> pd.Index:
#         start = self._start_date - self._window_size
#         end = self._start_date - pd.Timedelta(milliseconds=1)  # exclusive range end
#
#         while start > self._stop_date:
#             data = self._load_timestamps(start, end)
#             if data.empty:
#                 start -= self._window_size
#                 end -= self._window_size
#                 continue
#             self._prepend(data)
#             yield self._data
#             start -= self._window_size
#             end -= self._window_size
#
#         # last batch
#         data = self._load_timestamps(start, end)
#         self._prepend(data)
#         return self._data[self._stop_nanos:]
#
#     def _prepend(self, data: pd.Index) -> None:
#         self._data = pd.Index(
#             pd.concat([data.to_series(), self._data.to_series()], ignore_index=True),
#             dtype=np.int64,
#         )
