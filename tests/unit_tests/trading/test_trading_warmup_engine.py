import pandas as pd
import pytest

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.trading.warmup.engine import WarmupEngine
from nautilus_trader.trading.warmup.engine import _keep_largest_warmup_range_for_each_bar_type
from nautilus_trader.trading.warmup.engine import _sort_nautilus_bars
from nautilus_trader.trading.warmup.engine import sort_dataframe_bars
from nautilus_trader.trading.warmup.range import StaticWarmupRange
from nautilus_trader.trading.warmup.range import WarmupRange
from tests.unit_tests.trading.test_trading_warmup import load_warmup_bars_into_catalog


@pytest.mark.parametrize(
    "ranges, end_date",
    [
        [
            [
                StaticWarmupRange(6, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
                StaticWarmupRange(2, BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")),
            ],
            pd.Timestamp("2019-12-31 10:00:00", tz="UTC"),
        ],
        [
            [
                StaticWarmupRange(46, BarType.from_str("EUR/USD.SIM-15-MINUTE-BID-EXTERNAL")),
                StaticWarmupRange(10, BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")),
            ],
            pd.Timestamp("2019-11-05 10:00:00", tz="UTC"),
        ],
        [
            [
                StaticWarmupRange(1440 * 2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                StaticWarmupRange(2, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
            ],
            pd.Timestamp("2019-11-05 10:00:00", tz="UTC"),
        ],
    ],
)
class TestWarmupEngine:
    def test_request_bars_returns_expected_as_dataframe(
        self,
        ranges: list[WarmupRange],
        end_date: pd.Timestamp,
    ):
        catalog = data_catalog_setup(protocol="file")
        bar_types = [r.bar_type for r in ranges]
        load_warmup_bars_into_catalog(catalog, bar_types)

        engine = WarmupEngine(ranges=ranges, catalog=catalog, end_date=end_date)
        bars = engine.request_bars(as_nautilus=False)
        for r in ranges:
            assert len(bars[bars.bar_type == r.bar_type]) >= r.count

    def test_request_bars_returns_expected_as_nautilus(
        self,
        ranges: list[WarmupRange],
        end_date: pd.Timestamp,
    ):
        catalog = data_catalog_setup(protocol="file")
        bar_types = [r.bar_type for r in ranges]
        load_warmup_bars_into_catalog(catalog, bar_types)

        engine = WarmupEngine(ranges=ranges, catalog=catalog, end_date=end_date)
        bars = engine.request_bars()
        for r in ranges:
            assert len([bar for bar in bars if bar.bar_type == r.bar_type]) >= r.count


@pytest.mark.parametrize(
    "ranges, expected",
    [
        [
            [
                WarmupRange(1, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
                WarmupRange(10, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
            ],
            [
                WarmupRange(1, BarType.from_str("EUR/USD.SIM-1-DAY-BID-EXTERNAL")),
                WarmupRange(10, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
            ],
        ],
        [
            [
                WarmupRange(1, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                WarmupRange(3, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
            ],
            [WarmupRange(3, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL"))],
        ],
        [
            [
                WarmupRange(1, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
            ],
            [
                WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")),
                WarmupRange(2, BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")),
            ],
        ],
    ],
)
def test_use_max_lookback_per_bar_type(
    ranges: list[WarmupRange],
    expected: list[WarmupRange],
):
    assert _keep_largest_warmup_range_for_each_bar_type(ranges) == expected


class TestSortBars:
    def test_sort_bars_by_step_returns_expected(self):
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")

        bars = [self._create_bar(bar_type_1), self._create_bar(bar_type_2)]
        bars = _sort_nautilus_bars(bars)

        expected = [bar_type_2, bar_type_1]
        assert [bar.bar_type for bar in bars] == expected

    def test_sort_bars_by_aggregation_returns_expected(self):
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-2-DAY-BID-EXTERNAL")

        bars = [self._create_bar(bar_type_1), self._create_bar(bar_type_2)]
        bars = _sort_nautilus_bars(bars)

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


class TestSortDataFrame:
    def test_sort_dataframe_by_step_returns_expected(self):
        timestamp = pd.Timestamp("2019-12-30 14:00:00+00:00")
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL")
        df = pd.DataFrame.from_dict(
            {"ts_event": [timestamp, timestamp], "bar_type": [bar_type_1, bar_type_2]},
        )
        df = sort_dataframe_bars(df)
        expected = [bar_type_2, bar_type_1]
        assert list(df.bar_type) == expected

    def test_sort_dataframe_by_aggregation_returns_expected(self):
        timestamp = pd.Timestamp("2019-12-30 14:00:00+00:00")
        bar_type_1 = BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL")
        bar_type_2 = BarType.from_str("EUR/USD.SIM-2-DAY-BID-EXTERNAL")
        df = pd.DataFrame.from_dict(
            {
                "ts_event": [timestamp, timestamp],
                "bar_type": [bar_type_1, bar_type_2],
            },
        )
        df = sort_dataframe_bars(df)
        expected = [bar_type_2, bar_type_1]
        assert list(df.bar_type) == expected
