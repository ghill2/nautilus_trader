import itertools

import pandas as pd
import pytest

from nautilus_trader.backtest.data.providers import TestDataProvider
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config.common import StrategyConfig
from nautilus_trader.config.common import WarmupConfig
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.external.core import process_files
from nautilus_trader.persistence.external.readers import ParquetReader
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.trading.strategy import Strategy
from tests import TEST_DATA_DIR


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




class MockWarmupIndicator(Indicator):
    """
    A test indicator that completes warmup after a specified amount of bars
    """

    def __init__(self, i):
        self.warmup_value = i
        super().__init__([])
        self.i = 0

    def handle_bar(self, bar):
        self.i += 1
        if self.warmup_value == self.i:
            self._set_initialized(True)
            assert self.initialized


class TestMockWarmupIndicator:
    def test_indicator_completes_warmup_on_expected_bar(self):
        n = 10
        bars = [
            Bar(
                BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL"),
                Price(1.234, 4),
                Price(1.234, 4),
                Price(1.234, 4),
                Price(1.234, 4),
                Quantity(5, 0),
                0,
                0,
            ),
        ] * n
        indicator = MockWarmupIndicator(n)
        for i, bar in enumerate(bars):
            indicator.handle_bar(bar)
            if i == n - 2:
                assert not indicator.initialized
            if i == n - 1:
                assert indicator.initialized


class MockWarmupStrategy(Strategy):
    """
    A test strategy to test the warmup module.
    """

    def __init__(self, config: StrategyConfig, indicators_dict: dict[BarType, list[Indicator]]):
        super().__init__(config=config)
        self._indicators_dict = indicators_dict

    def on_start(self):
        for bar_type, indicators in self._indicators_dict.items():
            for indicator in indicators:
                self.register_indicator_for_bars(bar_type, indicator)

        if self.config.warmup_config:
            self.warmup()


class TestStrategyWarmup:
    def setup(self):
        # Fixture
        config = BacktestEngineConfig(
            bypass_logging=True,
            run_analysis=False,
        )
        self.engine = BacktestEngine(config=config)
        self.engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
        )
        instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
        self.engine.add_instrument(instrument)

        # Setup data
        wrangler = QuoteTickDataWrangler(instrument=instrument)
        provider = TestDataProvider()
        ticks = wrangler.process_bar_data(
            bid_data=provider.read_csv_bars("fxcm-usdjpy-m1-bid-2013.csv"),
            ask_data=provider.read_csv_bars("fxcm-usdjpy-m1-ask-2013.csv"),
        )
        self.engine.add_data(ticks)

    def teardown(self):
        self.engine.dispose()

    def test_mock_warmup_strategy_indicators_are_added_to_strategy_for_both_aggregation_sources(
        self,
    ):
        indicators_dict: dict[BarType, list[Indicator]] = {
            BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(10)],
            BarType.from_str("EUR/USD.SIM-1-HOUR-BID-INTERNAL"): [MockWarmupIndicator(10)],
        }

        strategy = MockWarmupStrategy(config=None, indicators_dict=indicators_dict)

        self.engine.add_strategy(strategy)
        self.engine.run()  # register the indicators

        indicators = list(itertools.chain(*indicators_dict.values()))
        registered_strategy_indicators = list(
            itertools.chain(*strategy._indicators_for_bars.values()),
        )

        for indicator in indicators:
            assert indicator in registered_strategy_indicators

    @pytest.mark.parametrize(
        "indicators_dict",
        [
            {
                BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(10)],
                BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(3)],
            },
            {
                BarType.from_str("USD/JPY.SIM-1-MINUTE-BID-EXTERNAL"): [
                    MockWarmupIndicator(1),
                    MockWarmupIndicator(5),
                ],
                BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(2)],
            },
            {
                BarType.from_str("EUR/USD.SIM-1-HOUR-BID-INTERNAL"): [MockWarmupIndicator(10)],
                BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(3)],
            },
            {
                BarType.from_str("USD/JPY.SIM-1-HOUR-BID-INTERNAL"): [MockWarmupIndicator(20)],
                BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL"): [
                    MockWarmupIndicator(13),
                    MockWarmupIndicator(12),
                ],
            },
            {
                BarType.from_str("USD/JPY.SIM-1-HOUR-BID-INTERNAL"): [MockWarmupIndicator(10)],
                BarType.from_str("EUR/USD.SIM-4-HOUR-BID-EXTERNAL"): [MockWarmupIndicator(3)],
            },
        ],
    )
    def test_various_indicators_warmup_completes_before_strategy_start(
        self,
        indicators_dict: dict[BarType, list[Indicator]],
    ):
        # Load data into catalog
        catalog = data_catalog_setup(protocol="file")
        load_warmup_bars_into_catalog(catalog, indicators_dict.keys())

        # Create configs
        warmup_config = WarmupConfig(
            catalog_path=catalog.path,
            end_time=pd.Timestamp("2020-01-01 00:00:00+00:00"),
        )
        strategy_config = StrategyConfig(warmup_config=warmup_config)

        strategy = MockWarmupStrategy(config=strategy_config, indicators_dict=indicators_dict)

        self.engine.add_strategy(strategy)

        # Act
        self.engine.run()

        # Assert
        assert strategy.registered_indicators
        assert strategy.indicators_initialized
