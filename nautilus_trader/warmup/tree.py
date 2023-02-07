from __future__ import annotations
from __future__ import annotations
from __future__ import annotations
from __future__ import annotations

import itertools
from collections import defaultdict

import pandas as pd
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.warmup.config import WarmupConfig


class WarmupStart:
    @property
    def start_date(self) -> pd.Timestamp:
        raise NotImplementedError()


class WarmupTree(WarmupStart):
    def __init__(self, indicator: Indicator):
        self._indicator = indicator

        # Create a BarType > Indicator map
        self._indicators_for_bar_type = defaultdict(list)  # dict[BarType, list[Indicator]]
        for indicator in self.indicators:
            bar_type = indicator.config.bar_type
            self._indicators_for_bar_type[bar_type].append(indicator)

        self.bar_types = self._indicators_for_bar_type.keys()

    def start_date(self, end_date: pd.Timestamp, catalog: ParquetDataCatalog) -> pd.Timestamp:
        start_date = end_date
        for configs in self._configs_on_level(self._indicator):
            start_date = min(config.start_date(end_date=start_date, catalog=catalog)
                             for config in self._filter_max(configs)
                                )
        return start_date

    @staticmethod
    def _filter_max(configs: list[WarmupConfig]) -> list[WarmupConfig]:
        """
        Returns WarmupConfigs with the maximum counts for each BarType
        """
        return list({r.bar_type: r for r in sorted(configs, key=lambda r: r.count)}.values())

    def handle_bars(self, bars: list[Bar]):
        for bar in bars:
            self.handle_bar(bar)

    def handle_bar(self, bar: Bar):
        indicators = self._indicators_for_bar_type[bar.bar_type]
        for indicator in indicators:
            indicator.handle_bar(bar)

    @property
    def indicators(self) -> list[Indicator]:
        return list(itertools.chain.from_iterable(
            self._indicators_on_level(self._indicator)
        ))

    @staticmethod
    def _configs_on_level(indicator: Indicator) -> list[list[WarmupConfig]]:
        return [
            [i.config for i in indicators]
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
                children += indicator.config.children
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
