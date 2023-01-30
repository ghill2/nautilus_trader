import pandas as pd

from nautilus_trader.model.data.bar import BarType
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.trading.warmup.range import StaticWarmupRange
from nautilus_trader.trading.warmup.range import WarmupDataProvider
from tests.unit_tests.trading.test_trading_warmup import load_warmup_bars_into_catalog


class TestWarmupDataProvider:
    def test_output_bars_identical_to_normal_load(self):
        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(
            catalog, [BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")]
        )

        end_date = pd.Timestamp("2019-12-31 10:00:00", tz="UTC")
        stop_date = pd.Timestamp("2019-12-30 14:00:00+00:00", tz="UTC")
        timestamps_gen = WarmupDataProvider(
            catalog=catalog,
            end_date=end_date,
            grow_size=pd.Timedelta(hours=4),
            bar_type=BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"),
            stop_date=stop_date,
        )

        actual = None
        for actual in timestamps_gen:
            pass

        expected = timestamps_gen._load_timestamps(stop_date)
        assert list(actual) == list(expected)

    def test_output_bars_identical_to_normal_load_stop_date_not_exist(self):
        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(
            catalog, [BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")]
        )

        end_date = pd.Timestamp("2019-12-31 10:00:00", tz="UTC")
        stop_date = pd.Timestamp("2019-12-30 13:59:00+00:00", tz="UTC")
        timestamps_gen = WarmupDataProvider(
            catalog=catalog,
            end_date=end_date,
            grow_size=pd.Timedelta(hours=4),
            bar_type=BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"),
            stop_date=stop_date,
        )

        actual = None
        for actual in timestamps_gen:
            pass

        expected = timestamps_gen._load_timestamps(stop_date)
        assert list(actual) == list(expected)


class TestWarmupRange:
    def test_get_warmup_start_date_returns_expected(self):
        # TODO, paramaterize test

        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(
            catalog, [BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")]
        )

        bar_type = BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL")
        end_date = pd.Timestamp("2019-12-31 10:00:00", tz="UTC")

        warmup_range = StaticWarmupRange(20, bar_type)
        start_date = warmup_range.start_date(catalog=catalog, end_date=end_date)
        assert start_date == pd.Timestamp("2019-12-30 14:00:00+00:00")


if __name__ == "__main__":
    TestWarmupDataProvider().test_output_bars_identical_to_normal_load()
