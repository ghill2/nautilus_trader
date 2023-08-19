from datetime import datetime

import pandas as pd
import pyarrow.dataset as ds

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.trading.warmup.range import WarmupRange


def unix_nanos_to_dt_vectorized(values: pd.Series):
    return pd.to_datetime(values, unit="ns", utc=True)


class WarmupEngine:
    def __init__(
        self,
        ranges: list[WarmupRange],
        end_date: datetime,
        catalog: ParquetDataCatalog,
    ):

        PyCondition.type(end_date, datetime, "strategy_start_date")
        PyCondition.type(catalog, ParquetDataCatalog, "catalog")
        PyCondition.list_type(ranges, WarmupRange, "warmup_ranges")

        self._ranges = _keep_largest_warmup_range_for_each_bar_type(ranges)
        assert all(
            r.bar_type.spec.is_time_aggregated() for r in self._ranges
        ), "BarTypes are not all TIME aggregated"

        self._end_date = end_date
        self._catalog = catalog

    @property
    def start_date(self) -> pd.Timestamp:
        return min(
            r.start_date(catalog=self._catalog, end_date=self._end_date) for r in self._ranges
        )

    def request_bars(self, as_nautilus=True) -> list[Bar]:
        bar_types = [x.bar_type for x in self._ranges]

        filter_expr = ds.field("bar_type").cast("string").isin([str(bt) for bt in bar_types])

        bars: pd.DataFrame = self._catalog.query(
            cls=Bar,
            filter_expr=filter_expr,
            start=self.start_date,
            end=self._end_date - pd.Timedelta(milliseconds=1),  # exclusive range end
            instrument_ids=[str(bt.instrument_id) for bt in bar_types],
            clean_instrument_keys=True,
            as_nautilus=as_nautilus,
        )

        if as_nautilus:
            return _sort_nautilus_bars(bars)
        else:
            return sort_dataframe_bars(bars)

    def request_bars_dict(self) -> dict[BarType, list[Bar]]:
        bars = self.request_bars(as_nautilus=True)
        result = {}
        for bar in bars:
            result.setdefault(bar.bar_type, []).append(bar)
        return result


def _keep_largest_warmup_range_for_each_bar_type(
    ranges: list[WarmupRange],
) -> list[WarmupRange]:
    return list({r.bar_type: r for r in sorted(ranges, key=lambda r: r.count)}.values())


def _sort_nautilus_bars(bars):
    bars.sort(key=lambda x: (x.bar_type.spec.aggregation * -1, x.bar_type.spec.step * -1))
    return bars


def sort_dataframe_bars(df):
    # TODO improvement performance
    if type(df.bar_type.iloc[0]) is not BarType:
        df["bar_type"] = df.bar_type.apply(BarType.from_str)
    df["aggregation"] = df.bar_type.apply(lambda x: x.spec.aggregation * -1)
    df["step"] = df.bar_type.apply(lambda x: x.spec.step * -1)
    df.sort_values(["ts_event", "aggregation", "step"], inplace=True)
    df.drop(["aggregation", "step"], axis=1, inplace=True)
    # df["bar_type"] = df.bar_type.apply(str)
    # assert type(df.bar_type.iloc[0]) is str
    return df


# @classmethod
# def from_indicators(
#     cls,
#     strategy_start_date: datetime,
#     indicators: Dict[BarType, List[Indicator]],
#     catalog: ParquetDataCatalog,
# ):
#     # Create calculator from a dict with format BarType:[Indicator]
#     PyCondition.type(indicators, dict, "indicators")
#
#     lookbacks = []
#     for bar_type, _indicators in indicators.items():
#         if not isinstance(_indicators, list):
#             _indicators = [_indicators]
#         PyCondition.list_type(_indicators, Indicator, "indicators")
#         for indicator in _indicators:
#             count = indicator.get_warmup_value()
#             if not count:
#                 raise ValueError(f"Warmup count not found for indicator: {indicator}.")
#             lookbacks.append(WarmupRange(count, bar_type))
#
#     return cls(strategy_start_date, lookbacks, catalog)


# assert all(
#     x.is_externally_aggregated() for x in self._bar_types
# ), "Strategy warmup failed, BarTypes are not all AggregationSource.EXTERNAL."
