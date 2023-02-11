from __future__ import annotations
from __future__ import annotations
from __future__ import annotations
from __future__ import annotations
from __future__ import annotations

import itertools
from collections import defaultdict

import numpy as np
import pandas as pd
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggregationSource
from pyarrow import dataset as ds

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.catalog import ParquetDataCatalog


class WarmupStart:
    @property
    def start_date(self) -> pd.Timestamp:
        raise NotImplementedError()


class WarmupConfig(WarmupStart):
    def __init__(
            self,
            count: int,
            bar_type: BarType,
            children: list[Indicator] = None
            # unstable: int = 0,
    ):
        PyCondition.positive_int(count, "count")
        # PyCondition.not_negative(unstable, "unstable")
        PyCondition.type(bar_type, BarType, "bar_type")

        self.parents = []
        self.children = []
        if children is not None:
            PyCondition.list_type(children, Indicator, "children")
            for child in children:
                self.add_child(child)

        assert bar_type.spec.is_time_aggregated(), "Only TIME aggregated BarTypes allowed"
        self.bar_type = bar_type.with_aggregation_source(AggregationSource.EXTERNAL)

        self.count = count

    def add_child(self, indicator: Indicator) -> None:
        PyCondition.type(indicator, Indicator, "indicator")
        indicator.warmup_config.add_parent(self)
        self.children.append(indicator)

    def add_parent(self, indicator: Indicator) -> None:
        self.parents.append(indicator)

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

class WarmupTree(WarmupStart):
    def __init__(self, indicator: Indicator):
        self._indicator = indicator

        self._indicators_for_bar_type = self._get_indicator_map

        self.bar_types = self._indicators_for_bar_type.keys()

    def start_date(self, end_date: pd.Timestamp, catalog: ParquetDataCatalog) -> pd.Timestamp:
        start_date = end_date
        for configs in self._configs_on_level(self._indicator):
            start_date = min(config.start_date(end_date=start_date, catalog=catalog)
                             for config in self._filter_max(configs)
                                )
        return start_date

    def handle_bars(self, bars: list[Bar]):
        for bar in bars:
            self.handle_bar(bar)

    def handle_bar(self, bar: Bar):
        indicators = self._indicators_for_bar_type[bar.bar_type]
        for indicator in indicators:
            indicator.handle_bar(bar)

    @property
    def _indicators(self) -> list[Indicator]:
        return list(itertools.chain.from_iterable(
            self._indicators_on_level(self._indicator)
        ))

    @property
    def _get_indicator_map(self) -> dict[BarType, list[Indicator]]:
        """
        Create a BarType > Indicator map for efficient access to Indicator's during warmup

        """
        # Create a BarType > Indicator map
        _map = defaultdict(list)  # dict[BarType, list[Indicator]]
        for indicator in self._indicators:

            bar_type = indicator.warmup_config.bar_type

            # INTERNAL bar types are warmed using EXTERNAL bar sources
            if bar_type.aggregation_source == AggregationSource.INTERNAL:
                bar_type = bar_type.with_aggregation_source(AggregationSource.EXTERNAL)

            _map[bar_type].append(indicator)

        return _map

    @staticmethod
    def _filter_max(configs: list[WarmupConfig]) -> list[WarmupConfig]:
        """
        Returns WarmupConfigs with the maximum counts for each BarType
        """
        return list({r.bar_type: r for r in sorted(configs, key=lambda r: r.count)}.values())

    @staticmethod
    def _configs_on_level(indicator: Indicator) -> list[list[WarmupConfig]]:
        return [
            [i.warmup_config for i in indicators]
            for indicators in WarmupTree._indicators_on_level(indicator)
        ]

    @staticmethod
    def _indicators_on_level(indicator: Indicator) -> list[list[Indicator]]:
        indicators_on_level = []
        indicators = [indicator]

        i = 1
        while len(indicators) != 0:

            indicators_on_level.append(indicators)

            children = []
            for indicator in indicators:
                children += indicator.warmup_config.children
            indicators = children

            i += 1

        return indicators_on_level

    # @property
    # def levels(self) -> list[_WarmupLevel]:
    #     """
    #     Parse the warmup levels of the tree
    #     """
    #     return list(map(_WarmupLevel, self._configs_on_level(self._indicator)))

        # self.indicators = defaultdict(list)  # dict[BarType, list[Indicator]]

    # list(set(itertools.chain.from_iterable(
    #                     level.bar_types for level in self._levels
    #         ))
    #
    #             @ staticmethod

    # @classmethod
    # def from_config(cls, config: WarmupConfig):
    #     tree = cls()
    #
    #     # Add configs to tree
    #
    #     tree.indicators
    #
    #     return tree

    # @property
    # def bar_types(self) -> list[BarType]:
    #     return self.indicators_for_bar_type.keys()

    # @property
    # def configs(self) -> list[IndicatorConfig]:
    #     return list(set(itertools.chain.from_iterable(self._levels.values()))

    # an indexing method that returns the indicator(s) related to the BarType
    # def __getitem__(self, item):

    # @property
    # def get_indicators(self) -> dict[BarType, list[Indicator]]:
    # map = {}
    #
    # result = {}
    # for bar in bars:
    #     result.setdefault(bar.bar_type, []).append(bar)
    #
    # for config in self.configs:
    #     {config.bar_type}

    # @property
    # def indicators(self):
    # @classmethod
    # def from_indicators(cls, indicators: list[Indicators]) -> list[WarmupTree]:

    # self._tree = Tree()

    # def add_range(self, level: int, range: WarmupRange):
    #     self._levels[level].append(range)

    # def start_date(self, end_date: pd.Timestamp, catalog: ParquetDataCatalog) -> pd.Timestamp:
    #     assert len(self._ranges) != 0
    #     if len(self._ranges) == 1:
    #         return self._ranges[0].start_date(end_date=end_date)
    #     else:
    #         # sum length of ranges
    #         start_date = None
    #         end_date = self._end_date
    #
    #         for i in range(self._ranges) - 1:
    #             start_date = self._ranges[i].start_date(end_date=end_date)
    #             end_date = start_date
    #
    #         return start_date

    # def add_level(self, index: int, data: list[IndicatorConfig]) -> None:
    #      self._levels[index] = data
    # assert len(self._ranges) != 0
    # if len(self._ranges) == 1:
    #     return self._ranges[0].start_date(end_date=end_date)
    # else:
    #     # sum length of ranges
    #     start_date = None
    #     end_date = self._end_date
    #
    #     for i in range(self._ranges) - 1:
    #         start_date = self._ranges[i].start_date(end_date=end_date)
    #         end_date = start_date
    #
    #     return start_date

    # @classmethod
    # def _get_children_at_level(cls, i, config):
    #     configs = [config]
    #     for _ in range(i):
    #         configs = cls._get_children(configs)
    #     return configs

    # @staticmethod
    # def _get_children(configs) -> list[WarmupConfig]:
    #     children = []
    #     for config in configs:
    #         children += config.children
    #     return children

    # children = cls._get_children(configs)

    # print(i, children)
    # i += 1

    # configs = children

    # root = roots[0]

    # for config in roots:
    # config = roots[0]
    # go to top

    # get parent

    #

    # config = config.children[0]
    # y = 0
    # configs = []
    # while True:
    # if i == y:
    #         break
    # print(config)

    #     # find configs where root in dependents
    #
    #     children = []
    #     for config in configs:
    #
    #         print(configs)
    #         l = []
    #         if config.dependents is not None and root in config.dependents:
    #             children.append(config)

    # config_by_dependents =
    # for config in configs:
    #     if config is None:
    #         continue
    #     for dependent in config.dependents:
    #         indicator_by_dependents[dependent] = config

    # how to know the next
    # for config in configs:

    """Filter out the root indicators from the registered indicators"""

    # self._tree.create_node("Harry", "harry")  # root node
    # for bar_type, indicator in indicator_dict.items():
    # start at top of tree
    # traverse down, when you get to

    # Each indicator, create a Double Linked List
#    pass


# @classmethod
# def from_indicator(cls, bar_type: BarType, indicator: Indicator, indicator_dict: dict[BarType, list[Indicator]]):
    """
    Construct a warmup tree from an indicator

    Iterate over the length of the tree, computing the start date of each node.

    Take the max of the start dates and keep going

    """
    # tree = cls()

    # config = indicator.config
    # if config.dependents is None:
    # tree.add_range(WarmupRange())
    # return tree


# def start_date(self, end_date: pd.Timestamp) -> pd.Timestamp:
#     pass

# def __add__(self, other: WarmupRange):
#     return WarmupRange(other._spec,
#                         end_date=self.get_start_date(),
#                         catalog=other._catalog
#                         ).start_date
# self.unstable = unstable

        # def add_parent(self, config: IndicatorConfig):
        #     PyCondition.type(config, IndicatorConfig, "config")
        #     self._parents.append(config)

