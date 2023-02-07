from typing import Optional
from typing import Union

import pandas as pd

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from pyarrow import dataset as ds

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.warmup.tree import WarmupTree


class WarmupEngine:
    def __init__(
            self,
            indicators: list[Indicator],
            catalog: ParquetDataCatalog,
            end_date: pd.Timestamp,
    ):

        PyCondition.list_type(indicators, Indicator, "indicators")
        PyCondition.type(catalog, ParquetDataCatalog, "catalog")
        PyCondition.type(end_date, pd.Timestamp, "end_date")

        self._indicators = indicators
        self._catalog = catalog
        self._end_date = end_date

    def process(self) -> None:
        # Warmup the trees
        bars_dict = self._request_bars_dict()

        for tree in self.trees:

            # Get the bars related to the tree
            bars = []
            for bar_type in tree.bar_types:
                bars.extend(bars_dict[bar_type])

            bars = self._sort_bars(bars)

            tree.handle_bars(bars)

    @property
    def start_date(self) -> pd.Timestamp:
        return min(tree.start_date(catalog=self._catalog, end_date=self._end_date) for tree in self.trees)

    @property
    def trees(self) -> list[WarmupTree]:
        trees = []
        for indicator in self._indicators:

            config = indicator.warmup_config
            if config is None:
                continue

            if config.parents != []:
                continue  # filter root indicators

            trees.append(
                WarmupTree(indicator)
            )
        return trees

    def _request_bars_dict(self) -> dict[BarType, list[Bar]]:
        bars = self._request_bars()

        # Split bars by bar_type
        bars_dict = {}
        for bar in bars:
            bars_dict.setdefault(bar.bar_type, []).append(bar)
        return bars_dict

    def _request_bars(self) -> list[Bar]:
        """Requests the bars needed to satisfy the warmup ranges sorted by their BarTypes"""
        bar_types = [indicator.warmup_config.bar_type for indicator in self._indicators]
        filter_expr = ds.field("bar_type").cast("string").isin([str(bt) for bt in bar_types])
        bars: pd.DataFrame = self._catalog.query(
            cls=Bar,
            filter_expr=filter_expr,
            start=self.start_date,
            end=self._end_date - pd.Timedelta(milliseconds=1),  # exclusive range end
            instrument_ids=[str(bt.instrument_id) for bt in bar_types],
            clean_instrument_keys=True,
            as_nautilus=True,
        )
        # Sort bars large aggregations first. H4 > H1
        return bars

    @staticmethod
    def _sort_bars(bars):
        bars.sort(key=lambda x: (x.bar_type.spec.aggregation * -1, x.bar_type.spec.step * -1))
        return bars
