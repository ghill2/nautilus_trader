from typing import Callable

import pandas as pd
import pytest
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from nautilus_trader.test_kit.mocks.data import data_catalog_setup

from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.external.core import process_files
from nautilus_trader.persistence.external.readers import ParquetReader
from nautilus_trader.warmup.engine import WarmupEngine
from nautilus_trader.warmup.tree import WarmupConfig
from tests import TEST_DATA_DIR
from tests.unit_tests.warmup.warmup_mocks import MockWarmupIndicator


def load_warmup_bars_into_catalog(catalog: ParquetDataCatalog, test_bar_types: list[BarType]):
    test_bar_types = [
        bar_type.with_aggregation_source(AggregationSource.EXTERNAL) for bar_type in test_bar_types
    ]

    for bar_type in test_bar_types:
        filename = (
                str(bar_type).replace("/", "").replace(".", "-") + "-2019.parquet"
        )  # USDJPY-SIM-1-DAY-BID-EXTERNAL-2019.parquet

        def bar_parser(df):
            symbol = str(bar_type.instrument_id.symbol)
            instrument = TestInstrumentProvider.default_fx_ccy(symbol)
            wrangler = BarDataWrangler(instrument=instrument, bar_type=bar_type)
            bars = wrangler.process(data=df)
            yield from bars

        import os
        assert os.path.exists(TEST_DATA_DIR + "/" + filename)

        process_files(
            glob_path=TEST_DATA_DIR + "/" + filename,
            catalog=catalog,
            reader=ParquetReader(parser=bar_parser),
        )



def warmup_indicators_test_case_1() -> list[Indicator]:
    bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
    indicator3 = MockWarmupIndicator(WarmupConfig(5, bar_type))
    indicator2 = MockWarmupIndicator(WarmupConfig(10, bar_type, children=[indicator3]))
    indicator1 = MockWarmupIndicator(WarmupConfig(20, bar_type, children=[indicator2]))
    return [
        indicator1, indicator2, indicator3
    ]

def warmup_indicators_test_case_2() -> list[Indicator]:
    EURUSD_H1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
    EURUSD_H4 = BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")
    indicator4 = MockWarmupIndicator(WarmupConfig(5, EURUSD_H1))
    indicator3 = MockWarmupIndicator(WarmupConfig(8, EURUSD_H1, children=[indicator4]))
    indicator2 = MockWarmupIndicator(WarmupConfig(10, EURUSD_H4, children=[indicator4]))
    indicator1 = MockWarmupIndicator(WarmupConfig(20, EURUSD_H1, children=[indicator2, indicator3]))
    return [
        indicator1, indicator2, indicator3, indicator4
    ]

def warmup_indicators_test_case_3() -> list[Indicator]:
    EURUSD_H1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
    USDJPY_H4 = BarType.from_str("USD/JPY.SIM-4-HOUR-BID-EXTERNAL")
    indicator4 = MockWarmupIndicator(WarmupConfig(5, USDJPY_H4))
    indicator3 = MockWarmupIndicator(WarmupConfig(8, EURUSD_H1, children=[indicator4]))
    indicator2 = MockWarmupIndicator(WarmupConfig(10, USDJPY_H4, children=[indicator3]))
    indicator1 = MockWarmupIndicator(WarmupConfig(20, EURUSD_H1, children=[indicator2]))
    return [
        indicator1, indicator2, indicator3, indicator4
    ]

def warmup_internal_indicators_test_case_1() -> list[Indicator]:
    EURUSD_H1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-INTERNAL")
    USDJPY_H4 = BarType.from_str("USD/JPY.SIM-4-HOUR-BID-INTERNAL")
    indicator4 = MockWarmupIndicator(WarmupConfig(5, USDJPY_H4))
    indicator3 = MockWarmupIndicator(WarmupConfig(8, EURUSD_H1, children=[indicator4]))
    indicator2 = MockWarmupIndicator(WarmupConfig(4, USDJPY_H4, children=[indicator3]))
    indicator1 = MockWarmupIndicator(WarmupConfig(10, EURUSD_H1, children=[indicator2]))
    return [
        indicator1, indicator2, indicator3, indicator4
    ]

class TestWarmupEngine:
    # def test_get_start_date_returns_expected_date_with_multiple_bar_types(self):
    #     pass # TODO

    @pytest.mark.parametrize(
        "indicators", [
            warmup_indicators_test_case_1,
            warmup_indicators_test_case_2,
            warmup_indicators_test_case_3,
        ]
    )
    @pytest.mark.parametrize(
        "end_date",
        [
            pd.Timestamp("2019-12-31 10:00:00", tz="UTC"),
            pd.Timestamp("2019-10-01 02:00:00", tz="UTC"),
            pd.Timestamp("2019-09-10 02:00:00", tz="UTC"),
            pd.Timestamp("2019-07-10 02:00:00", tz="UTC"),
            pd.Timestamp("2019-06-10 00:00:00", tz="UTC"),
        ]
    )
    def test_request_bars_returns_expected_bar_count_for_each_bar_type_with_various_end_dates(
            self,
            indicators: Callable,
            end_date: pd.Timestamp,
    ):

        indicators = indicators()

        catalog = data_catalog_setup(protocol="file")

        bar_types = [indicator.warmup_config.bar_type for indicator in indicators]

        load_warmup_bars_into_catalog(catalog, bar_types)

        engine = WarmupEngine(indicators=indicators, catalog=catalog, end_date=end_date)

        bars = engine._request_bars()
        for indicator in indicators:
            count = indicator.warmup_config.count
            bar_type = indicator.warmup_config.bar_type
            assert len([bar for bar in bars if bar.bar_type == bar_type]) >= count

    @pytest.mark.parametrize(
        "indicators",
        [
            warmup_internal_indicators_test_case_1,
            warmup_indicators_test_case_1,
            warmup_indicators_test_case_2,
            warmup_indicators_test_case_3,
        ],
    )
    @pytest.mark.parametrize(
        "end_date",
        [
            pd.Timestamp("2020-01-01 00:00:00", tz="UTC"),
            pd.Timestamp("2019-10-01 02:00:00", tz="UTC"),
            pd.Timestamp("2019-09-10 02:00:00", tz="UTC"),
        ],
    )
    def test_various_indicators_warmup_completes_before_end_date(
            self,
            indicators: Callable,
            end_date: pd.Timestamp
    ):

        indicators = indicators()

        bar_types = [indicator.warmup_config.bar_type for indicator in indicators]

        # Arrange
        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(catalog, bar_types)
        engine = WarmupEngine(indicators=indicators, catalog=catalog, end_date=end_date)

        # Act
        engine.process()

        # Assert
        for indicator in indicators:
            assert indicator.initialized

    def test_sort_bar_obj_list_by_step_returns_expected(self):
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")

        bars = [self._create_bar(bar_type_1), self._create_bar(bar_type_2)]
        bars = WarmupEngine._sort_bars(bars)

        expected = [bar_type_2, bar_type_1]
        assert [bar.bar_type for bar in bars] == expected

    def test_sort_bar_obj_list_by_aggregation_returns_expected(self):
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-2-DAY-BID-EXTERNAL")

        bars = [self._create_bar(bar_type_1), self._create_bar(bar_type_2)]
        bars = WarmupEngine._sort_bars(bars)

        expected = [bar_type_2, bar_type_1]
        assert [bar.bar_type for bar in bars] == expected

    @staticmethod
    def _create_bar(bar_type: BarType) -> Bar:
        timestamp = dt_to_unix_nanos(pd.Timestamp("2019-12-30 14:00:00+00:00"))
        return Bar(
            bar_type=bar_type,
            open=Price.from_str("1.00001"),
            high=Price.from_str("1.00010"),
            low=Price.from_str("1.00000"),
            close=Price.from_str("1.00002"),
            volume=Quantity.from_str("1"),
            ts_event=timestamp,
            ts_init=timestamp,
        )

# class TestWarmupWindow:
#     @pytest.mark.parametrize(
#         "ranges, end_date",
#         [
#             [
#                 [
#                     WarmupRange(6, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
#                     WarmupRange(2, BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")),
#                 ],
#                 pd.Timestamp("2019-12-31 10:00:00", tz="UTC"),
#             ],
#             [
#                 [
#                     WarmupRange(46, BarType.from_str("EUR/USD.SIM-15-MINUTE-BID-EXTERNAL")),
#                     WarmupRange(10, BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")),
#                 ],
#                 pd.Timestamp("2019-11-05 10:00:00", tz="UTC"),
#             ],
#             [
#                 [
#                     WarmupRange(1440 * 2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
#                     WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
#                 ],
#                 pd.Timestamp("2019-11-05 10:00:00", tz="UTC"),
#             ],
#         ],
#     )

#

#

#

#
#
# class TestWarmupEngine:

