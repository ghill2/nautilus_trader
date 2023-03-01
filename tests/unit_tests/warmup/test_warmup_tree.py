import pandas as pd
import pytest
from nautilus_trader.core.datetime import as_utc_index

from nautilus_trader.persistence.external.core import process_files

from nautilus_trader.persistence.external.readers import ParquetReader

from tests import TEST_DATA_DIR

from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from nautilus_trader.model.data.bar import BarType

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.warmup.tree import WarmupConfig
from nautilus_trader.warmup.tree import WarmupTree
from tests.unit_tests.warmup.test_warmup_engine import load_warmup_bars_into_catalog

from pathlib import Path

from tests.unit_tests.warmup.warmup_mocks import MockWarmupIndicator


def data_filepath(bar_type: BarType) -> Path:
    filename = (
            str(bar_type).replace("/", "").replace(".", "-") + "-2019.parquet"
    )  # USDJPY-SIM-1-DAY-BID-EXTERNAL-2019.parquet
    return Path(TEST_DATA_DIR) / filename


def load_warmup_bars_into_catalog(catalog: ParquetDataCatalog, bar_types: list[BarType]):
    for bar_type in bar_types:
        def bar_parser(df):
            symbol = str(bar_type.instrument_id.symbol)
            instrument = TestInstrumentProvider.default_fx_ccy(symbol)
            wrangler = BarDataWrangler(instrument=instrument, bar_type=bar_type)
            bars = wrangler.process(data=df)
            yield from bars

        process_files(
            glob_path=data_filepath(bar_type).as_posix(),
            catalog=catalog,
            reader=ParquetReader(parser=bar_parser),
        )


class TestWarmupTree:

    def test_configs_on_level(self):
        bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")

        indicator3 = MockWarmupIndicator(WarmupConfig(5, bar_type))
        indicator2 = MockWarmupIndicator(WarmupConfig(10, bar_type, children=[indicator3]))
        indicator1 = MockWarmupIndicator(WarmupConfig(20, bar_type, children=[indicator2]))

        levels = WarmupTree._configs_on_level(indicator1)

        assert levels == [
            [indicator1.warmup_config],
            [indicator2.warmup_config],
            [indicator3.warmup_config]
        ]

    def test_indicators_on_level(self):
        bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")

        indicator3 = MockWarmupIndicator(WarmupConfig(5, bar_type))
        indicator2 = MockWarmupIndicator(WarmupConfig(10, bar_type, children=[indicator3]))
        indicator1 = MockWarmupIndicator(WarmupConfig(20, bar_type, children=[indicator2]))

        assert WarmupTree._indicators_on_level(indicator1) == [
            [indicator1],
            [indicator2],
            [indicator3]
        ]

    def test_start_date_returns_expected_date(self):
        bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")

        indicator3 = MockWarmupIndicator(WarmupConfig(5, bar_type))
        indicator2 = MockWarmupIndicator(WarmupConfig(10, bar_type, children=[indicator3]))
        indicator1 = MockWarmupIndicator(WarmupConfig(20, bar_type, children=[indicator2]))

        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(catalog, [bar_type])

        tree = WarmupTree(indicator=indicator1)

        data = as_utc_index(pd.read_parquet(data_filepath(bar_type))).index

        start_date = tree.start_date(end_date=data[-1], catalog=catalog)

        expected_index = indicator1.warmup_config.count \
                         + indicator2.warmup_config.count \
                         + indicator3.warmup_config.count \

        expected = data[-expected_index - 1]

        assert start_date == expected

    def test_get_start_date_returns_expected_date_with_ignored_config(self):
        bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")

        indicator4 = MockWarmupIndicator(WarmupConfig(5, bar_type))
        indicator3 = MockWarmupIndicator(WarmupConfig(8, bar_type))
        indicator2 = MockWarmupIndicator(WarmupConfig(10, bar_type, children=[indicator4]))
        indicator1 = MockWarmupIndicator(WarmupConfig(20, bar_type, children=[indicator2, indicator3]))

        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(catalog, [bar_type])

        data = as_utc_index(pd.read_parquet(data_filepath(bar_type))).index

        expected_index = indicator1.warmup_config.count \
                         + indicator2.warmup_config.count \
                         + indicator4.warmup_config.count

        expected = data[-expected_index - 1]

        start_date = WarmupTree(indicator1).start_date(end_date=data[-1], catalog=catalog)

        assert start_date == expected

    @pytest.mark.parametrize(
        "configs, expected",
        [
            [
                [
                    WarmupConfig(1, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
                    WarmupConfig(10, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                ],
                [
                    WarmupConfig(1, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
                    WarmupConfig(10, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                ],
            ],
            [
                [
                    WarmupConfig(1, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                    WarmupConfig(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                    WarmupConfig(3, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                ],
                [WarmupConfig(3, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL"))],
            ],
            [
                [
                    WarmupConfig(1, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                    WarmupConfig(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                    WarmupConfig(2, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
                ],
                [
                    WarmupConfig(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                    WarmupConfig(2, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
                ],
            ],
        ],
    )
    def test_filter_max_configs_returns_expected_configs(
            self,
            configs: list[WarmupConfig],
            expected: list[WarmupConfig],
    ):
        assert WarmupTree._filter_max(configs) == expected
# class TestWarmupRange:

#
#     def test_add_ranges_returns_expected_date(self):
#         # Create IndicatorWarmupConfig
#         bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
#         range1 = WarmupRange(20, bar_type)
#         range2 = WarmupRange(40, bar_type)
#
#         # Create catalog
#         catalog = data_catalog_setup(protocol="file")
#         load_warmup_bars_into_catalog(catalog, [bar_type])
#
#         end_date = pd.Timestamp("2019-12-31 10:00:00", tz="UTC")
#
#         # Act
#         start_date = range1.add(range2, catalog=catalog, end_date=end_date)
#
#         # Assert
#         assert start_date == pd.Timestamp("2019-12-30 14:00:00+00:00")


# class TestWarmupChain:
#     # TODO test from_config
#     # TODO test get_start_date
#     # TODO test a single chain where each indicator
#     pass
#
#     def test_get_start_date_returns_expected_date(self):
#         # Create IndicatorWarmupConfig
#         bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
#         range = WarmupRange(20, bar_type)
#
#         # Create catalog
#         catalog = data_catalog_setup(protocol="file")
#         load_warmup_bars_into_catalog(catalog, [bar_type])
#
#         end_date = pd.Timestamp("2019-12-31 10:00:00", tz="UTC")
#
#         # Act
#         start_date = range.get_start_date(catalog=catalog, end_date=end_date)
#
#         # Assert
#         assert start_date == pd.Timestamp("2019-12-30 14:00:00+00:00")
