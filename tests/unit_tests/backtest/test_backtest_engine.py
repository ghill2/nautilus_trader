# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import tempfile
from decimal import Decimal
from typing import Optional

import pandas as pd
import pytest

from nautilus_trader.backtest.data.providers import TestDataProvider
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from nautilus_trader.backtest.data.wranglers import QuoteTickDataWrangler
from nautilus_trader.backtest.data.wranglers import TradeTickDataWrangler
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.config import StreamingConfig
from nautilus_trader.config.error import InvalidConfiguration
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig
from nautilus_trader.examples.strategies.signal_strategy import SignalStrategy
from nautilus_trader.examples.strategies.signal_strategy import SignalStrategyConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data.bar import BarSpecification
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.base import DataType
from nautilus_trader.model.data.base import GenericData
from nautilus_trader.model.data.venue import InstrumentStatusUpdate
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import MarketStatus
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orderbook.data import BookOrder
from nautilus_trader.model.orderbook.data import OrderBookDelta
from nautilus_trader.model.orderbook.data import OrderBookDeltas
from nautilus_trader.model.orderbook.data import OrderBookSnapshot
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.test_kit.stubs import MyData
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.config import TestConfigStubs
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.trading.strategy import Strategy


ETHUSDT_BINANCE = TestInstrumentProvider.ethusdt_binance()
AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")
GBPUSD_SIM = TestInstrumentProvider.default_fx_ccy("GBP/USD")
USDJPY_SIM = TestInstrumentProvider.default_fx_ccy("USD/JPY")


class TestBacktestEngine:
    def setup(self):
        # Fixture Setup
        self.usdjpy = TestInstrumentProvider.default_fx_ccy("USD/JPY")
        self.engine = self.create_engine()

    def create_engine(self, config: Optional[BacktestEngineConfig] = None):
        engine = BacktestEngine(config)
        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
            fill_model=FillModel(),
        )

        # Setup data
        wrangler = QuoteTickDataWrangler(self.usdjpy)
        provider = TestDataProvider()
        ticks = wrangler.process_bar_data(
            bid_data=provider.read_csv_bars("fxcm-usdjpy-m1-bid-2013.csv")[:2000],
            ask_data=provider.read_csv_bars("fxcm-usdjpy-m1-ask-2013.csv")[:2000],
        )
        engine.add_instrument(USDJPY_SIM)
        engine.add_data(ticks)
        return engine

    def teardown(self):
        self.engine.reset()
        self.engine.dispose()

    def test_initialization(self):
        engine = BacktestEngine(BacktestEngineConfig(bypass_logging=True))

        # Arrange, Act, Assert
        assert engine.run_id is None
        assert engine.run_started is None
        assert engine.run_finished is None
        assert engine.backtest_start is None
        assert engine.backtest_end is None
        assert engine.iteration == 0

    def test_reset_engine(self):
        # Arrange
        self.engine.run()

        # Act
        self.engine.reset()

        # Assert
        assert self.engine.run_id is None
        assert self.engine.run_started is None
        assert self.engine.run_finished is None
        assert self.engine.backtest_start is None
        assert self.engine.backtest_end is None
        assert self.engine.iteration == 0  # No exceptions raised

    def test_run_with_no_strategies(self):
        # Arrange, Act
        self.engine.run()

        # Assert
        assert self.engine.iteration == 8000

    def test_run(self):
        # Arrange, Act
        self.engine.add_strategy(Strategy())
        self.engine.run()

        # Assert
        assert len(self.engine.trader.strategy_states()) == 1

    def test_change_fill_model(self):
        # Arrange, Act
        self.engine.change_fill_model(Venue("SIM"), FillModel())

        # Assert
        assert True  # No exceptions raised

    def test_account_state_timestamp(self):
        # Arrange
        start = pd.Timestamp("2013-01-31 23:59:59.700000+00:00")
        self.engine.run(start=start)

        # Act
        report = self.engine.trader.generate_account_report(Venue("SIM"))

        # Assert
        assert len(report) == 1
        assert report.index[0] == start

    def test_persistence_files_cleaned_up(self):
        # Arrange
        temp_dir = tempfile.mkdtemp()
        catalog = ParquetDataCatalog(
            path=str(temp_dir),
            fs_protocol="file",
        )
        config = TestConfigStubs.backtest_engine_config(persist=True, catalog=catalog)
        engine = TestComponentStubs.backtest_engine(
            config=config,
            instrument=self.usdjpy,
            ticks=TestDataStubs.quote_ticks_usdjpy(),
        )
        engine.run()
        engine.dispose()

        assert all([f.closed for f in engine.kernel.writer._files.values()])

    def test_backtest_engine_multiple_runs(self):
        for _ in range(2):
            config = SignalStrategyConfig(instrument_id=USDJPY_SIM.id.value)
            strategy = SignalStrategy(config)
            engine = self.create_engine(
                config=BacktestEngineConfig(
                    streaming=StreamingConfig(catalog_path="/", fs_protocol="memory"),
                    bypass_logging=True,
                ),
            )
            engine.add_strategy(strategy)
            engine.run()
            engine.dispose()

    def test_backtest_engine_strategy_timestamps(self):
        # Arrange
        config = SignalStrategyConfig(instrument_id=USDJPY_SIM.id.value)
        strategy = SignalStrategy(config)
        engine = self.create_engine(
            config=BacktestEngineConfig(
                streaming=StreamingConfig(catalog_path="/", fs_protocol="memory"),
                bypass_logging=True,
            ),
        )
        engine.add_strategy(strategy)
        messages = []
        strategy.msgbus.subscribe("*", handler=messages.append)

        # Act
        engine.run()

        # Assert
        msg = messages[1]
        assert msg.__class__.__name__ == "SignalCounter"
        assert msg.ts_init == 1359676799700000000
        assert msg.ts_event == 1359676799700000000

    def test_set_instance_id(self):
        # Arrange
        instance_id = UUID4().value

        # Act
        engine = self.create_engine(
            config=BacktestEngineConfig(instance_id=instance_id, bypass_logging=True),
        )
        engine2 = self.create_engine(
            config=BacktestEngineConfig(bypass_logging=True),
        )  # Engine sets instance id

        # Assert
        assert engine.kernel.instance_id.value == instance_id
        assert engine2.kernel.instance_id.value != instance_id


class TestBacktestEngineData:
    def setup(self):
        # Fixture Setup
        self.engine = BacktestEngine(BacktestEngineConfig(bypass_logging=True))
        self.engine.add_venue(
            venue=Venue("BINANCE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USDT)],
        )
        self.engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
            fill_model=FillModel(),
        )

    def test_add_generic_data_adds_to_engine(self, capsys):
        # Arrange
        data_type = DataType(MyData, metadata={"news_wire": "hacks"})

        generic_data1 = [
            GenericData(data_type, MyData("AAPL hacked")),
            GenericData(
                data_type,
                MyData("AMZN hacked", 1000, 1000),
            ),
            GenericData(
                data_type,
                MyData("NFLX hacked", 3000, 3000),
            ),
            GenericData(
                data_type,
                MyData("MSFT hacked", 2000, 2000),
            ),
        ]

        generic_data2 = [
            GenericData(
                data_type,
                MyData("FB hacked", 1500, 1500),
            ),
        ]

        # Act
        self.engine.add_data(generic_data1, ClientId("NEWS_CLIENT"))
        self.engine.add_data(generic_data2, ClientId("NEWS_CLIENT"))

        # Assert
        assert len(self.engine.data) == 5

    def test_add_instrument_when_no_venue_raises_exception(self):
        # Arrange
        engine = BacktestEngine(BacktestEngineConfig(bypass_logging=True))

        # Act, Assert
        with pytest.raises(InvalidConfiguration):
            engine.add_instrument(ETHUSDT_BINANCE)

    def test_add_order_book_snapshots_adds_to_engine(self, capsys):
        # Arrange
        self.engine.add_instrument(ETHUSDT_BINANCE)

        snapshot1 = OrderBookSnapshot(
            instrument_id=ETHUSDT_BINANCE.id,
            book_type=BookType.L2_MBP,
            bids=[[1550.15, 0.51], [1580.00, 1.20]],
            asks=[[1552.15, 1.51], [1582.00, 2.20]],
            ts_event=0,
            ts_init=0,
        )

        snapshot2 = OrderBookSnapshot(
            instrument_id=ETHUSDT_BINANCE.id,
            book_type=BookType.L2_MBP,
            bids=[[1551.15, 0.51], [1581.00, 1.20]],
            asks=[[1553.15, 1.51], [1583.00, 2.20]],
            ts_event=1_000_000_000,
            ts_init=1_000_000_000,
        )

        # Act
        self.engine.add_data([snapshot2, snapshot1])  # <-- reverse order

        # Assert
        assert len(self.engine.data) == 2
        assert self.engine.data[0] == snapshot1
        assert self.engine.data[1] == snapshot2

    def test_add_order_book_deltas_adds_to_engine(self, capsys):
        # Arrange
        self.engine.add_instrument(AUDUSD_SIM)
        self.engine.add_instrument(ETHUSDT_BINANCE)

        deltas = [
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("13.0"),
                    size=Quantity.from_str("40"),
                    side=OrderSide.SELL,
                ),
                ts_event=0,
                ts_init=0,
            ),
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("12.0"),
                    size=Quantity.from_str("30"),
                    side=OrderSide.SELL,
                ),
                ts_event=0,
                ts_init=0,
            ),
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("11.0"),
                    size=Quantity.from_str("20"),
                    side=OrderSide.SELL,
                ),
                ts_event=0,
                ts_init=0,
            ),
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("10.0"),
                    size=Quantity.from_str("20"),
                    side=OrderSide.BUY,
                ),
                ts_event=0,
                ts_init=0,
            ),
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("9.0"),
                    size=Quantity.from_str("30"),
                    side=OrderSide.BUY,
                ),
                ts_event=0,
                ts_init=0,
            ),
            OrderBookDelta(
                instrument_id=AUDUSD_SIM.id,
                book_type=BookType.L2_MBP,
                action=BookAction.ADD,
                order=BookOrder(
                    price=Price.from_str("0.0"),
                    size=Quantity.from_str("40"),
                    side=OrderSide.BUY,
                ),
                ts_event=0,
                ts_init=0,
            ),
        ]

        operations1 = OrderBookDeltas(
            instrument_id=ETHUSDT_BINANCE.id,
            book_type=BookType.L2_MBP,
            deltas=deltas,
            ts_event=0,
            ts_init=0,
        )

        operations2 = OrderBookDeltas(
            instrument_id=ETHUSDT_BINANCE.id,
            book_type=BookType.L2_MBP,
            deltas=deltas,
            ts_event=1000,
            ts_init=1000,
        )

        # Act
        self.engine.add_data([operations2, operations1])  # <-- not sorted

        # Assert
        assert len(self.engine.data) == 2
        assert self.engine.data[0] == operations1
        assert self.engine.data[1] == operations2

    def test_add_quote_ticks_adds_to_engine(self, capsys):
        # Arrange, Setup data
        self.engine.add_instrument(AUDUSD_SIM)
        wrangler = QuoteTickDataWrangler(AUDUSD_SIM)
        provider = TestDataProvider()
        ticks = wrangler.process(provider.read_csv_ticks("truefx-audusd-ticks.csv"))

        # Act
        self.engine.add_data(ticks)

        # Assert
        assert len(self.engine.data) == 100000

    def test_add_trade_ticks_adds_to_engine(self, capsys):
        # Arrange
        self.engine.add_instrument(ETHUSDT_BINANCE)

        wrangler = TradeTickDataWrangler(ETHUSDT_BINANCE)
        provider = TestDataProvider()
        ticks = wrangler.process(provider.read_csv_ticks("binance-ethusdt-trades.csv"))

        # Act
        self.engine.add_data(ticks)

        # Assert
        assert len(self.engine.data) == 69806

    def test_add_bars_adds_to_engine(self, capsys):
        # Arrange
        bar_spec = BarSpecification(
            step=1,
            aggregation=BarAggregation.MINUTE,
            price_type=PriceType.BID,
        )

        bar_type = BarType(
            instrument_id=USDJPY_SIM.id,
            bar_spec=bar_spec,
            aggregation_source=AggregationSource.EXTERNAL,  # <-- important
        )

        wrangler = BarDataWrangler(
            bar_type=bar_type,
            instrument=USDJPY_SIM,
        )
        provider = TestDataProvider()
        bars = wrangler.process(provider.read_csv_bars("fxcm-usdjpy-m1-bid-2013.csv")[:2000])

        # Act
        self.engine.add_instrument(USDJPY_SIM)
        self.engine.add_data(data=bars)

        # Assert
        assert len(self.engine.data) == 2000

    def test_add_instrument_status_to_engine(self, capsys):
        # Arrange
        data = [
            InstrumentStatusUpdate(
                instrument_id=USDJPY_SIM.id,
                status=MarketStatus.CLOSED,
                ts_init=0,
                ts_event=0,
            ),
            InstrumentStatusUpdate(
                instrument_id=USDJPY_SIM.id,
                status=MarketStatus.OPEN,
                ts_init=0,
                ts_event=0,
            ),
        ]

        # Act
        self.engine.add_instrument(USDJPY_SIM)
        self.engine.add_data(data=data)

        # Assert
        assert len(self.engine.data) == 2
        assert self.engine.data == data


class TestBacktestWithAddedBars:
    def setup(self):
        # Fixture Setup
        config = BacktestEngineConfig(
            bypass_logging=True,
            run_analysis=False,
        )
        self.engine = BacktestEngine(config=config)
        self.venue = Venue("SIM")

        # Setup venue
        self.engine.add_venue(
            venue=self.venue,
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
        )

        # Setup data
        bid_bar_type = BarType(
            instrument_id=GBPUSD_SIM.id,
            bar_spec=TestDataStubs.bar_spec_1min_bid(),
            aggregation_source=AggregationSource.EXTERNAL,  # <-- important
        )

        ask_bar_type = BarType(
            instrument_id=GBPUSD_SIM.id,
            bar_spec=TestDataStubs.bar_spec_1min_ask(),
            aggregation_source=AggregationSource.EXTERNAL,  # <-- important
        )

        bid_wrangler = BarDataWrangler(
            bar_type=bid_bar_type,
            instrument=GBPUSD_SIM,
        )

        ask_wrangler = BarDataWrangler(
            bar_type=ask_bar_type,
            instrument=GBPUSD_SIM,
        )

        provider = TestDataProvider()
        bid_bars = bid_wrangler.process(provider.read_csv_bars("fxcm-gbpusd-m1-bid-2012.csv"))
        ask_bars = ask_wrangler.process(provider.read_csv_bars("fxcm-gbpusd-m1-ask-2012.csv"))

        # Add data
        self.engine.add_instrument(GBPUSD_SIM)
        self.engine.add_data(bid_bars)
        self.engine.add_data(ask_bars)

    def teardown(self):
        self.engine.dispose()

    def test_run_ema_cross_with_added_bars(self):
        # Arrange
        bar_type = BarType(
            instrument_id=GBPUSD_SIM.id,
            bar_spec=TestDataStubs.bar_spec_1min_bid(),
            aggregation_source=AggregationSource.EXTERNAL,  # <-- important
        )
        config = EMACrossConfig(
            instrument_id=str(GBPUSD_SIM.id),
            bar_type=str(bar_type),
            trade_size=Decimal(100_000),
            fast_ema_period=10,
            slow_ema_period=20,
        )
        strategy = EMACross(config=config)
        self.engine.add_strategy(strategy)

        # Act
        self.engine.run()

        # Assert
        assert strategy.fast_ema.count == 30117
        assert self.engine.iteration == 60234
        assert self.engine.portfolio.account(self.venue).balance_total(USD) == Money(
            1011166.89,
            USD,
        )

    def test_dump_pickled_data(self):
        # Arrange, # Act, # Assert
        assert len(self.engine.dump_pickled_data()) == 5060524

    def test_load_pickled_data(self):
        # Arrange
        bar_type = BarType(
            instrument_id=GBPUSD_SIM.id,
            bar_spec=TestDataStubs.bar_spec_1min_bid(),
            aggregation_source=AggregationSource.EXTERNAL,  # <-- important
        )
        config = EMACrossConfig(
            instrument_id=str(GBPUSD_SIM.id),
            bar_type=str(bar_type),
            trade_size=Decimal(100_000),
            fast_ema_period=10,
            slow_ema_period=20,
        )
        strategy = EMACross(config=config)
        self.engine.add_strategy(strategy)

        data = self.engine.dump_pickled_data()

        # Act
        self.engine.load_pickled_data(data)
        self.engine.run()

        # Assert
        assert strategy.fast_ema.count == 30117
        assert self.engine.iteration == 60234
        assert self.engine.portfolio.account(self.venue).balance_total(USD) == Money(
            1011166.89,
            USD,
        )


####################################################################################################
from pytower import CATALOG_DIR
import gc
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.persistence.batching import batch_configs
from nautilus_trader.persistence.funcs import parse_bytes
from pytower.instruments.provider import InstrumentProvider
from setproctitle import setproctitle
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.model.data.bar import Bar
setproctitle("nau")

instrument_id = "USD/JPY.DUKA"
start = str(pd.Timestamp("2012-01-01", tz="UTC"))
end = str(pd.Timestamp("2012-03-01", tz="UTC"))
tick_config = BacktestDataConfig(  # Strategy ticks
            catalog_path=str(CATALOG_DIR),
            data_cls=QuoteTick,
            instrument_id=str(instrument_id),
            start_time=start,
            end_time=end,
            use_rust=True,
    )
bar_config = BacktestDataConfig(  # Strategy bars
                catalog_path=str(CATALOG_DIR),
                data_cls=Bar,
                instrument_id=str(instrument_id),
                start_time=start,
                end_time=end,
                bar_spec="1-HOUR-ASK",
            )
def _create_engine():
    
    
    bar_spec = "1-HOUR-ASK"
    
    bar_type = BarType.from_str(f"{instrument_id}-{bar_spec}-EXTERNAL")
    instrument = InstrumentProvider.get(instrument_id)
    # Arrange
    engine = BacktestEngine(
                config=BacktestEngineConfig(
                    risk_engine=RiskEngineConfig(bypass=True),
                    cache=CacheConfig(bar_capacity=1, tick_capacity=1),
                    log_level="WRN"
                )
            )
    strategy = EMACross(EMACrossConfig(
                                    instrument_id=str(GBPUSD_SIM.id),
                                    bar_type=str(bar_type),
                                    trade_size=Decimal(100_000),
                                    fast_ema_period=10,
                                    slow_ema_period=20,
                    ))
    engine.add_strategy(strategy)
                        
    engine.add_venue(
        venue=Venue("DUKA"),
        oms_type=OMSType.HEDGING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(1_000_000, USD)],
        fill_model=FillModel(),
    )
    engine.add_instrument(instrument)
    return engine

def get_peak_memory_usage_gb():
    import platform

    BYTES_IN_GIGABYTE = 1e9
    if platform.system() == "Darwin" or platform.system() == "Linux":
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / BYTES_IN_GIGABYTE
    elif platform.system() == "Windows":
        import psutil

        return psutil.Process().memory_info().peak_wset / BYTES_IN_GIGABYTE
    else:
        raise RuntimeError("Unsupported OS.")

def get_memory_usage_gb():
    import psutil
    import os
    BYTES_IN_GIGABYTE = 1e9
    return psutil.Process(os.getpid()).memory_info().rss / BYTES_IN_GIGABYTE



def test_venue_to_str_frees():
    from nautilus_trader.model.identifiers import Venue
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")
    strings = [str(Venue("DUKA")) for _ in range(3_000_000)]
    del strings # disabling this line makes no difference
    gc.collect()
    # f"{_}, {str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}"
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")

def test_topic_to_str_frees():
    from nautilus_trader.model.identifiers import InstrumentId
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")
    n = 1_000_000
    ticks = [
        QuoteTick(
            instrument_id=InstrumentId.from_str(instrument_id),
            bid=Price(1.123, 1),
            ask=Price(1.123, 1),
            bid_size=Quantity(1.123, 1),
            ask_size=Quantity(1.123, 1),
            ts_event=0,
            ts_init=0,
        )
        for _ in range(n)
    ]
    tick_strings = [f"data.quotes"f".{tick.instrument_id.venue}"f".{tick.instrument_id.symbol}" for tick in ticks]
    del tick_strings # disabling this line makes no difference
    del ticks
    gc.collect()
    # f"{_}, {str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}"
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")
def test_quote_tick_repr_frees():
    from nautilus_trader.model.identifiers import InstrumentId
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")
    n = 1_000_000
    ticks = [
        QuoteTick(
            instrument_id=InstrumentId.from_str(instrument_id),
            bid=Price(1.123, 1),
            ask=Price(1.123, 1),
            bid_size=Quantity(1.123, 1),
            ask_size=Quantity(1.123, 1),
            ts_event=0,
            ts_init=0,
        )
        for _ in range(n)
    ]
    tick_strings = [f"{repr(tick)}" for tick in ticks]

    # del tick_strings # disabling this line makes no difference
    del ticks
    gc.collect()
    # f"{_}, {str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}"
    print(f"{str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")

def test_quote_tick_frees(i):
    from nautilus_trader.model.identifiers import InstrumentId
    print(f"{get_peak_memory_usage_gb():2f}")
    for _ in range(i):
        
        data = [
            QuoteTick(
                instrument_id=InstrumentId.from_str(instrument_id),
                bid=Price(1.123, 1),
                ask=Price(1.123, 1),
                bid_size=Quantity(1.123, 1),
                ask_size=Quantity(1.123, 1),
                ts_event=0,
                ts_init=0,
            )
            for _ in range(10_000)
        ]
        del data
        gc.collect()
        print(_, f"{get_memory_usage_gb():2f}")
    gc.collect()
    print(f"{get_peak_memory_usage_gb():2f}")

def test_quote_tick_frees_reader(i, data_config):
    print(f"{get_peak_memory_usage_gb():2f}")
    from nautilus_trader.persistence.catalog.rust.reader import ParquetFileReader
    import itertools
    file_path = data_config.get_files()[0]
    
    for _ in range(i):
        for __ in range(1000):
            reader = ParquetFileReader(QuoteTick, file_path)
            data = list(itertools.chain.from_iterable(reader))
            # del data
        gc.collect()
        print(_, f"{(get_memory_usage_gb()):2f}", f"{(get_peak_memory_usage_gb()):2f}")
    gc.collect()
    print(f"{get_peak_memory_usage_gb():2f}")
    
def test_loading_data_frees(i, data_config):
    """
    tick_config = True
    """
    print(f"{get_peak_memory_usage_gb():2f}")
    for _ in range(i):
        print(_, f"{(get_memory_usage_gb()):2f}", f"{(get_peak_memory_usage_gb()):2f}")
        data = data_config.load()['data']
        del data
        gc.collect()
    gc.collect()
    print(f"{get_peak_memory_usage_gb():2f}")
    
def test_loading_to_engine_frees(i, data_config):
    print(f"{get_peak_memory_usage_gb():2f}")
    for _ in range(i):
        engine = _create_engine()
        data = data_config.load()['data']
        engine.add_data(data)
        engine.dispose()
        del data
        del engine
        gc.collect()
        print(_, f"{(get_memory_usage_gb()):2f}", f"{(get_peak_memory_usage_gb()):2f}")
    gc.collect()
    print(f"{get_peak_memory_usage_gb():2f}")

def test_engine_run_streaming_frees(i, data_config):

    data = []
    data.append(get_peak_memory_usage_gb())
    for _ in range(i):
        engine = _create_engine()
        for j, batch in enumerate(batch_configs(
            data_configs=[data_config],
            read_num_rows=10_000,
            target_batch_size_bytes=parse_bytes("10mb"),
        )):
            engine.clear_data()
            engine.add_data(data=batch)
            engine.run_streaming()
            engine.clear_data()
            del batch
            gc.collect()
            data.append(f"{j}, {str(round(get_memory_usage_gb(), 2))}, {str(round(get_peak_memory_usage_gb(), 2))}")
        engine.end_streaming()
        engine.dispose()
        del engine
        gc.collect()
        
    gc.collect()
    data.append(get_peak_memory_usage_gb())
    for x in data:
        print(x)

    

if __name__ == "__main__":
    test_venue_to_str_frees()
    # test_topic_to_str_frees()
    # test_engine_run_streaming_frees(1, tick_config)
    # test_quote_tick_repr_frees()
    # test_loading_data_frees(10, tick_config)
    # test_loading_to_engine_frees(10, tick_config)
    # test_loading_to_engine_frees(100_000, bar_config)
    

    # test_quote_tick_frees_reader(1_000, bar_config)
    # test_engine_memory_ticks()
    # test_quote_tick_frees_from_init_func()
    
    # _run_memory_streaming_test(10, data_config)
    # test_engine_memory_bars()
    
    # test_loading_data_frees(1_000, data_config)
    # _run_memory_test_one_shot(10_000, data_config)
    # _run_memory_streaming_test(10, data_configs)
    # test_quote_tick_frees(100)
        

        
