# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2022 Nautech Systems Pty Ltd. All rights reserved.
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

import heapq
import itertools
import os
import sys

import fsspec
import pandas as pd
import pytest

from nautilus_trader.adapters.betfair.providers import BetfairInstrumentProvider
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from nautilus_trader.backtest.data.wranglers import QuoteTickDataWrangler
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import BacktestRunConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.venue import InstrumentStatusUpdate
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orderbook.data import OrderBookData
from nautilus_trader.persistence.batching import Buffer
from nautilus_trader.persistence.batching import _iterate_buffers
from nautilus_trader.persistence.batching import batch_configs
from nautilus_trader.persistence.batching import generate_batches
from nautilus_trader.persistence.catalog.rust.reader import ParquetFileReader
from nautilus_trader.persistence.catalog.rust.writer import ParquetWriter
from nautilus_trader.persistence.external.core import process_files
from nautilus_trader.persistence.external.readers import CSVReader
from nautilus_trader.persistence.external.readers import ParquetReader
from nautilus_trader.persistence.funcs import parse_bytes
from nautilus_trader.test_kit.mocks.data import NewsEventData
from nautilus_trader.test_kit.mocks.data import data_catalog_setup
from nautilus_trader.test_kit.stubs.persistence import TestPersistenceStubs
from tests import TEST_DATA_DIR
from tests.integration_tests.adapters.betfair.test_kit import BetfairTestStubs


class TestBatchingData:

    test_parquet_files = [
        os.path.join(TEST_DATA_DIR, "quote_tick_eurusd_2019_sim_rust.parquet"),
        os.path.join(TEST_DATA_DIR, "quote_tick_usdjpy_2019_sim_rust.parquet"),
        os.path.join(TEST_DATA_DIR, "bars_eurusd_2019_sim.parquet"),
    ]

    test_instruments = [
        TestInstrumentProvider.default_fx_ccy("EUR/USD", venue=Venue("SIM")),
        TestInstrumentProvider.default_fx_ccy("USD/JPY", venue=Venue("SIM")),
        TestInstrumentProvider.default_fx_ccy("EUR/USD", venue=Venue("SIM")),
    ]
    test_instrument_ids = [x.id for x in test_instruments]


class TestBuffer(TestBatchingData):
    def test_buffer_pop_returns_returns_valid_data(self):
        ######################
        # Arrange
        buffer = Buffer(
            generate_batches(
                files=[self.test_parquet_files[0]],
                cls=QuoteTick,
                use_rust=True,
                n_rows=10,
            ),
        )
        pop_timestamp = 1546383600588999936  # index 4
        next_timestamp = 1546383600691000064  # index 5
        # Act

        buffer.update()
        removed = buffer.pop(pop_timestamp)  # timestamp exists

        # Assert
        assert removed[-1].ts_init == pop_timestamp
        assert buffer._buffer[0].ts_init == next_timestamp
        # assert buffer._index[0] == next_timestamp

        ######################
        # Arrange
        buffer = Buffer(
            generate_batches(
                files=[self.test_parquet_files[0]],
                cls=QuoteTick,
                use_rust=True,
                n_rows=10,
            ),
        )
        pop_timestamp = 1546383600588999936  # index 4
        next_timestamp = 1546383600691000064  # index 5
        # Act

        buffer.update()
        removed = buffer.pop(pop_timestamp + 1)  # timestamp !exists

        # Assert
        assert removed[-1].ts_init == pop_timestamp
        assert buffer._buffer[0].ts_init == next_timestamp
        # assert buffer._index[0] == next_timestamp

        ######################
        # Timestamp exists
        # Arrange
        buffer = Buffer(
            generate_batches(
                files=[self.test_parquet_files[0]],
                cls=QuoteTick,
                use_rust=True,
                n_rows=10,
            ),
        )
        prev_timestamp = 1546383600487000064  # index 3
        pop_timestamp = 1546383600588999936  # index 4
        # next_timestamp = 1546383600691000064 # index 5

        # Act
        buffer.update()
        removed = buffer.pop(pop_timestamp - 1)  # timestamp !exist

        # Assert
        assert removed[-1].ts_init == prev_timestamp
        assert buffer._buffer[0].ts_init == pop_timestamp
        # assert buffer._index[0] == pop_timestamp

        ######################
        # Timestamp NOT exists
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=10,
        )
        buffer = Buffer(batch_gen)

        # Act
        timestamp_ns = 1546383600588999936  # index 4
        buffer.update()
        removed = buffer.pop(timestamp_ns + 1)

        # Assert
        last_timestamp = removed[-1].ts_init
        assert last_timestamp == timestamp_ns


class TestGenerateBatches(TestBatchingData):
    def test_generate_batches_returns_empty_list_before_start_timestamp_with_end_timestamp(self):

        start_timestamp = 1546389021944999936
        batch_gen = generate_batches(
            files=[self.test_parquet_files[1]],
            cls=QuoteTick,
            n_rows=1000,
            use_rust=True,
            start_time=start_timestamp,
            end_time=1546394394948999936,
        )
        batches = list(batch_gen)
        assert [len(x) for x in batches] == [0, 0, 0, 0, 172, 1000, 1000, 1000, 1000, 887]
        assert batches[4][0].ts_init == start_timestamp

        #################################
        batch_gen = generate_batches(
            files=[self.test_parquet_files[1]],
            cls=QuoteTick,
            n_rows=1000,
            use_rust=True,
            start_time=start_timestamp - 1,
            end_time=1546394394948999936,
        )
        batches = list(batch_gen)
        assert [len(x) for x in batches] == [0, 0, 0, 0, 172, 1000, 1000, 1000, 1000, 887]
        assert batches[4][0].ts_init == start_timestamp

    def test_generate_batches_returns_empty_list_before_start_timestamp(self):

        #############################################################################
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        start_timestamp = 1546383601403000064  # index 10 (1st item in batch)
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=10,
            start_time=start_timestamp,
        )

        # Act
        batch = next(batch_gen, None)

        # Assert
        assert batch == []

        #############################################
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        start_timestamp = 1546383601862999808  # index 18 (last item in batch)
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=10,
            start_time=start_timestamp,
        )
        # Act
        batch = next(batch_gen, None)

        # Assert
        assert batch == []

        ###################################################
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        start_timestamp = 1546383601352000000  # index 9
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=10,
            start_time=start_timestamp,
        )

        # Act
        batch = next(batch_gen, None)

        # Assert
        assert batch != []

    def test_generate_batches_trims_first_batch_by_start_timestamp(self):
        def create_test_batch_gen(start_timestamp):
            parquet_data_path = self.test_parquet_files[0]
            return generate_batches(
                files=[parquet_data_path],
                cls=QuoteTick,
                use_rust=True,
                n_rows=10,
                start_time=start_timestamp,
            )

        start_timestamp = 1546383605776999936
        batches = list(
            generate_batches(
                files=[self.test_parquet_files[0]],
                cls=QuoteTick,
                use_rust=True,
                n_rows=300,
                start_time=start_timestamp,
            ),
        )

        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == start_timestamp

        ###############################################################
        # Timestamp, index -1, exists
        start_timestamp = 1546383601301000192  # index 8
        batch_gen = create_test_batch_gen(start_timestamp)

        # Act
        batches = list(batch_gen)

        # Assert
        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == start_timestamp

        ###############################################################
        # Timestamp, index 0, exists
        start_timestamp = 1546383600078000128  # index 0
        batch_gen = create_test_batch_gen(start_timestamp)

        # Act
        batches = list(batch_gen)

        # Assert
        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == start_timestamp

        ###############################################################
        # Timestamp, index 0, NOT exists
        start_timestamp = 1546383600078000128  # index 0
        batch_gen = create_test_batch_gen(start_timestamp - 1)

        # Act
        batches = list(batch_gen)

        # Assert
        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == start_timestamp

        ###############################################################
        # Timestamp, index -1, NOT exists
        start_timestamp = 1546383601301000192  # index 8
        batch_gen = create_test_batch_gen(start_timestamp - 1)

        # Act
        batches = list(batch_gen)

        # Assert
        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == start_timestamp
        ###############################################################
        # Arrange

        start_timestamp = 1546383600691000064
        batch_gen = create_test_batch_gen(start_timestamp)

        # Act
        batches = list(batch_gen)

        # Assert
        first_batch = batches[0]
        print(len(first_batch))
        assert len(first_batch) == 5

        first_timestamp = first_batch[0].ts_init
        assert first_timestamp == start_timestamp
        ###############################################################
        # Starts on next timestamp if start_timestamp NOT exists
        # Arrange
        start_timestamp = 1546383600078000128  # index 0
        next_timestamp = 1546383600180000000  # index 1
        batch_gen = create_test_batch_gen(start_timestamp + 1)

        # Act
        batches = list(batch_gen)

        # Assert
        first_timestamp = batches[0][0].ts_init
        assert first_timestamp == next_timestamp

    def test_generate_batches_trims_end_batch_returns_no_empty_batch(self):
        parquet_data_path = self.test_parquet_files[0]

        # Timestamp, index -1, NOT exists
        # Arrange
        end_timestamp = 1546383601914000128  # index 19
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=10,
            end_time=end_timestamp,
        )

        # Act
        batches = list(batch_gen)

        # Assert
        last_batch = batches[-1]
        assert last_batch != []

    def test_generate_batches_trims_end_batch_by_end_timestamp(self):
        def create_test_batch_gen(end_timestamp):
            parquet_data_path = self.test_parquet_files[0]
            return generate_batches(
                files=[parquet_data_path],
                cls=QuoteTick,
                use_rust=True,
                n_rows=10,
                end_time=end_timestamp,
            )

        ###############################################################
        # Timestamp, index 0
        end_timestamp = 1546383601403000064  # index 10
        batches = list(create_test_batch_gen(end_timestamp))
        last_timestamp = batches[-1][-1].ts_init
        assert last_timestamp == end_timestamp

        batches = list(create_test_batch_gen(end_timestamp + 1))
        last_timestamp = batches[-1][-1].ts_init
        assert last_timestamp == end_timestamp

        ###############################################################
        # Timestamp index -1
        end_timestamp = 1546383601914000128  # index 19

        batches = list(create_test_batch_gen(end_timestamp))
        last_timestamp = batches[-1][-1].ts_init
        assert last_timestamp == end_timestamp

        batches = list(create_test_batch_gen(end_timestamp + 1))
        last_timestamp = batches[-1][-1].ts_init
        assert last_timestamp == end_timestamp

        ###############################################################
        # Ends on prev timestamp

        end_timestamp = 1546383601301000192  # index 8
        prev_timestamp = 1546383601197999872  # index 7
        batches = list(create_test_batch_gen(end_timestamp - 1))
        last_timestamp = batches[-1][-1].ts_init
        assert last_timestamp == prev_timestamp

    def test_generate_batches_returns_valid_data(self):
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=300,
        )
        reader = ParquetFileReader(QuoteTick, parquet_data_path)
        expected = list(itertools.chain(*list(reader)))

        # Act
        results = []
        for batch in batch_gen:
            results.extend(batch)

        # Assert
        assert len(results) == len(expected)
        assert pd.Series([x.ts_init for x in results]).equals(
            pd.Series([x.ts_init for x in expected]),
        )

    def test_generate_batches_returns_has_inclusive_start_and_end(self):
        # Arrange
        parquet_data_path = self.test_parquet_files[0]
        reader = ParquetFileReader(QuoteTick, parquet_data_path)
        expected = list(itertools.chain(*list(reader)))
        batch_gen = generate_batches(
            files=[parquet_data_path],
            cls=QuoteTick,
            use_rust=True,
            n_rows=500,
            start_time=expected[0].ts_init,
            end_time=expected[-1].ts_init,
        )
        # Act
        results = []
        for batch in batch_gen:
            results.extend(batch)

        # Assert
        assert len(results) == len(expected)
        assert pd.Series([x.ts_init for x in results]).equals(
            pd.Series([x.ts_init for x in expected]),
        )


class TestIterateBuffersRust(TestBatchingData):
    def test_iterate_single_buffer_returns_valid_timestamps_rust(self):
        # Arrange

        buffers = [
            Buffer(
                generate_batches(
                    [self.test_parquet_files[0]],
                    QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                ),
            ),
        ]

        iter_batches = _iterate_buffers(
            buffers=buffers,
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=1000,
        )

        # Act
        results = []
        for batch in iter_batches:
            results.extend(batch)

        # Assert
        timestamps = [x.ts_init for x in results]
        assert timestamps == sorted(timestamps)

    def test_iterate_multiple_buffers_returns_valid_timestamps_rust(self):
        # Arrange
        readers = [
            ParquetFileReader(QuoteTick, self.test_parquet_files[0], sys.maxsize),
            ParquetFileReader(QuoteTick, self.test_parquet_files[1], sys.maxsize),
        ]
        expected = list(
            heapq.merge(*itertools.chain.from_iterable(readers), key=lambda x: x.ts_init),
        )

        buffers = [
            Buffer(
                generate_batches(
                    [self.test_parquet_files[0]],
                    QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                ),
            ),
            Buffer(
                generate_batches(
                    [self.test_parquet_files[1]],
                    QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                ),
            ),
        ]

        iter_batches = _iterate_buffers(
            buffers=buffers,
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=1000,
        )

        # Act
        timestamps = []
        timestamp_chunks = []
        for batch in iter_batches:
            timestamp_chunks.append([x.ts_init for x in batch])
            timestamps.extend([x.ts_init for x in batch])

        # Assert
        assert len(timestamps) == len(expected)
        assert pd.Series(timestamps).equals(pd.Series([x.ts_init for x in expected]))

        latest_timestamp = 0
        for timestamps in timestamp_chunks:
            assert max(timestamps) > latest_timestamp
            latest_timestamp = max(timestamps)
            assert timestamps == sorted(timestamps)

    def test_iterate_multiple_buffers_returns_valid_timestamps_with_start_end_range_rust(self):
        # Arrange
        start_timestamps = (1546383605776999936, 1546389021944999936)
        end_timestamps = (1546390125908000000, 1546394394948999936)
        buffers = [
            Buffer(
                generate_batches(
                    files=[self.test_parquet_files[0]],
                    cls=QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                    start_time=start_timestamps[0],
                    end_time=end_timestamps[0],
                ),
            ),
            Buffer(
                generate_batches(
                    files=[self.test_parquet_files[1]],
                    cls=QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                    start_time=start_timestamps[1],
                    end_time=end_timestamps[1],
                ),
            ),
        ]

        iter_batches = _iterate_buffers(
            buffers=buffers,
            read_num_rows=1000,
            target_batch_size_bytes=parse_bytes("10kib"),
        )

        # Act
        results = []
        for batch in iter_batches:
            results.extend(batch)

        instrument_1_timestamps = [
            x.ts_init for x in results if x.instrument_id == self.test_instrument_ids[0]
        ]
        instrument_2_timestamps = [
            x.ts_init for x in results if x.instrument_id == self.test_instrument_ids[1]
        ]

        # Assert
        assert instrument_1_timestamps[0] == start_timestamps[0]
        assert instrument_1_timestamps[-1] == end_timestamps[0]

        assert instrument_2_timestamps[0] == start_timestamps[1]
        assert instrument_2_timestamps[-1] == end_timestamps[1]

        timestamps = [x.ts_init for x in results]
        assert timestamps == sorted(timestamps)

    def test_iterate_multiple_buffers_returns_valid_timestamps_with_start_end_range_with_bars_rust(
        self,
    ):
        # Arrange
        start_timestamps = (1546383605776999936, 1546389021944999936, 1559224800000000000)
        end_timestamps = (1546390125908000000, 1546394394948999936, 1577710800000000000)

        buffers = [
            Buffer(
                generate_batches(
                    files=[self.test_parquet_files[0]],
                    cls=QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                    start_time=start_timestamps[0],
                    end_time=end_timestamps[0],
                ),
            ),
            Buffer(
                generate_batches(
                    files=[self.test_parquet_files[1]],
                    cls=QuoteTick,
                    n_rows=1000,
                    use_rust=True,
                    start_time=start_timestamps[1],
                    end_time=end_timestamps[1],
                ),
            ),
            Buffer(
                generate_batches(
                    files=[self.test_parquet_files[2]],
                    cls=Bar,
                    n_rows=1000,
                    use_rust=False,
                    start_time=start_timestamps[2],
                    end_time=end_timestamps[2],
                ),
            ),
        ]

        iter_batches = _iterate_buffers(
            buffers=buffers,
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=1000,
        )

        # Act
        results = []
        [results.extend(x) for x in iter_batches]

        bars = [x for x in results if isinstance(x, Bar)]

        quote_ticks = [x for x in results if isinstance(x, QuoteTick)]

        instrument_1_timestamps = [
            x.ts_init for x in quote_ticks if x.instrument_id == self.test_instrument_ids[0]
        ]
        instrument_2_timestamps = [
            x.ts_init for x in quote_ticks if x.instrument_id == self.test_instrument_ids[1]
        ]
        instrument_3_timestamps = [
            x.ts_init for x in bars if x.bar_type.instrument_id == self.test_instrument_ids[2]
        ]

        # Assert
        assert instrument_1_timestamps[0] == start_timestamps[0]
        assert instrument_1_timestamps[-1] == end_timestamps[0]

        assert instrument_2_timestamps[0] == start_timestamps[1]
        assert instrument_2_timestamps[-1] == end_timestamps[1]

        assert instrument_3_timestamps[0] == start_timestamps[2]
        assert instrument_3_timestamps[-1] == end_timestamps[2]

        timestamps = [x.ts_init for x in results]
        assert timestamps == sorted(timestamps)

    def test_iterate_multiple_buffers_returns_larger_bar_specifications_first(self):
        # TODO
        pass

    def test_iterate_batchers_yields_last_batch(self):
        #
        # TODO
        pass


class TestBatchConfigsRust(TestBatchingData):
    def setup(self):
        self.catalog = data_catalog_setup(protocol="file")
        self._load_bars_into_catalog_rust()
        self._load_quote_ticks_into_catalog_rust()

    def _load_bars_into_catalog_rust(self):
        instrument = self.test_instruments[2]
        parquet_data_path = self.test_parquet_files[2]

        def parser(df, instrument):
            df.index = df["ts_init"].apply(unix_nanos_to_dt)
            df = df["open high low close".split()]
            for col in df:
                df[col] = df[col].astype(float)
            objs = BarDataWrangler(
                bar_type=BarType.from_str("EUR/USD.SIM-1-HOUR-BID-EXTERNAL"),
                instrument=instrument,
            ).process(df)
            yield from objs

        process_files(
            glob_path=parquet_data_path,
            reader=ParquetReader(parser=lambda x: parser(x, instrument)),
            catalog=self.catalog,
            use_rust=False,
        )

    def _load_quote_ticks_into_catalog_rust(self):

        for instrument, parquet_data_path in zip(
            self.test_instruments[:2],
            self.test_parquet_files[:2],
        ):

            def parser(df, instrument):
                df.index = df["ts_init"].apply(unix_nanos_to_dt)
                df = df["bid ask bid_size ask_size".split()]
                for col in df:
                    df[col] = df[col].astype(float)
                objs = QuoteTickDataWrangler(instrument=instrument).process(df)
                yield from objs

            process_files(
                glob_path=parquet_data_path,
                reader=ParquetReader(parser=lambda x: parser(x, instrument)),  # noqa: B023
                catalog=self.catalog,
                use_rust=True,
                instrument=instrument,
            )

    def test_batch_data_configs_single_config_returns_valid_timestamps_rust(self):
        # Arrange
        config = BacktestDataConfig(
            catalog_path=str(self.catalog.path),
            instrument_id=str(self.test_instrument_ids[0]),
            data_cls=QuoteTick,
            use_rust=True,
        )
        reader = ParquetFileReader(QuoteTick, self.test_parquet_files[0])
        expected = list(itertools.chain(*list(reader)))

        iter_batches = batch_configs(
            data_configs=[config],
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=300,
        )

        # Act
        results = []
        for batch in iter_batches:
            results.extend(batch)

        # Assert
        assert len(results) == len(expected)

        expected_timestamps = [x.ts_init for x in expected]
        timestamps = [x.ts_init for x in results]
        assert timestamps == expected_timestamps

    def test_batch_data_configs_multiple_configs_returns_valid_timestamps_rust(self):
        # Arrange
        base = BacktestDataConfig(
            catalog_path=str(self.catalog.path),
            data_cls=QuoteTick,
            use_rust=True,
        )

        import sys

        readers = [
            ParquetFileReader(QuoteTick, self.test_parquet_files[0], sys.maxsize),
            ParquetFileReader(QuoteTick, self.test_parquet_files[1], sys.maxsize),
        ]
        expected = list(
            heapq.merge(*itertools.chain.from_iterable(readers), key=lambda x: x.ts_init),
        )

        iter_batches = batch_configs(
            data_configs=[
                base.replace(instrument_id=str(self.test_instrument_ids[0])),
                base.replace(instrument_id=str(self.test_instrument_ids[1])),
            ],
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=300,
        )

        # Act
        timestamps = []
        timestamp_chunks = []
        for batch in iter_batches:
            timestamp_chunks.append([x.ts_init for x in batch])
            timestamps.extend([x.ts_init for x in batch])

        # Assert
        assert len(timestamps) == len(expected)

        expected_timestamps = [x.ts_init for x in expected]
        assert timestamps == expected_timestamps

        latest_timestamp = 0
        for timestamps in timestamp_chunks:
            assert max(timestamps) > latest_timestamp
            latest_timestamp = max(timestamps)
            assert timestamps == sorted(timestamps)

    def test_batch_data_configs_multiple_configs_returns_valid_timestamps_start_end_range_rust(
        self,
    ):
        # Arrange
        base = BacktestDataConfig(
            catalog_path=str(self.catalog.path),
            data_cls=QuoteTick,
            use_rust=True,
        )

        start_timestamps = (1546383605776999936, 1546389021944999936)
        end_timestamps = (1546390125908000000, 1546394394948999936)
        iter_batches = batch_configs(
            data_configs=[
                base.replace(
                    instrument_id=str(self.test_instrument_ids[0]),
                    start_time=unix_nanos_to_dt(start_timestamps[0]),
                    end_time=unix_nanos_to_dt(end_timestamps[0]),
                ),
                base.replace(
                    instrument_id=str(self.test_instrument_ids[1]),
                    start_time=unix_nanos_to_dt(start_timestamps[1]),
                    end_time=unix_nanos_to_dt(end_timestamps[1]),
                ),
            ],
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=300,
        )

        # Act
        results = []
        [results.extend(x) for x in iter_batches]

        # Assert
        instrument_1_timestamps = [
            x.ts_init for x in results if x.instrument_id == self.test_instrument_ids[0]
        ]
        instrument_2_timestamps = [
            x.ts_init for x in results if x.instrument_id == self.test_instrument_ids[1]
        ]
        assert instrument_1_timestamps[0] == start_timestamps[0]
        assert instrument_1_timestamps[-1] == end_timestamps[0]

        assert instrument_2_timestamps[0] == start_timestamps[1]
        assert instrument_2_timestamps[-1] == end_timestamps[1]

    def test_batch_data_configs_multiple_configs_returns_valid_timestamps_start_end_range_with_bars_rust(
        self,
    ):
        # Arrange
        start_timestamps = (1546383605776999936, 1546389021944999936, 1577725200000000000)
        end_timestamps = (1546390125908000000, 1546394394948999936, 1577826000000000000)

        iter_batches = batch_configs(
            data_configs=[
                BacktestDataConfig(
                    catalog_path=str(self.catalog.path),
                    instrument_id=str(self.test_instrument_ids[0]),
                    data_cls=QuoteTick,
                    start_time=unix_nanos_to_dt(start_timestamps[0]),
                    end_time=unix_nanos_to_dt(end_timestamps[0]),
                    use_rust=True,
                ),
                BacktestDataConfig(
                    catalog_path=str(self.catalog.path),
                    instrument_id=str(self.test_instrument_ids[1]),
                    data_cls=QuoteTick,
                    start_time=unix_nanos_to_dt(start_timestamps[1]),
                    end_time=unix_nanos_to_dt(end_timestamps[1]),
                    use_rust=True,
                ),
                BacktestDataConfig(
                    catalog_path=str(self.catalog.path),
                    instrument_id=str(self.test_instrument_ids[2]),
                    data_cls=Bar,
                    start_time=unix_nanos_to_dt(start_timestamps[2]),
                    end_time=unix_nanos_to_dt(end_timestamps[2]),
                    bar_spec="1-HOUR-BID",
                    use_rust=False,
                ),
            ],
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=10,
        )

        # Act
        results = []
        [results.extend(x) for x in iter_batches]

        bars = [x for x in results if isinstance(x, Bar)]
        quote_ticks = [x for x in results if isinstance(x, QuoteTick)]

        instrument_1_timestamps = [
            x.ts_init for x in quote_ticks if x.instrument_id == self.test_instrument_ids[0]
        ]
        instrument_2_timestamps = [
            x.ts_init for x in quote_ticks if x.instrument_id == self.test_instrument_ids[1]
        ]
        instrument_3_timestamps = [
            x.ts_init for x in bars if x.bar_type.instrument_id == self.test_instrument_ids[2]
        ]

        # Assert
        assert instrument_1_timestamps[0] == start_timestamps[0]
        assert instrument_1_timestamps[-1] == end_timestamps[0]

        assert instrument_2_timestamps[0] == start_timestamps[1]
        assert instrument_2_timestamps[-1] == end_timestamps[1]

        assert instrument_3_timestamps[0] == start_timestamps[2]
        assert instrument_3_timestamps[-1] == end_timestamps[2]

        timestamps = [x.ts_init for x in results]
        assert timestamps == sorted(timestamps)

    def test_batch_configs_memory_usage(self):
        """
        The peak memory usage should not increase much more than the batch size
        when iterating the batches.
        """
        import gc

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

        # Arrange
        # data_configs=[
        #     BacktestDataConfig(
        #         catalog_path=self.catalog.path,
        #         instrument_id=self.test_instrument_ids[0],
        #         data_cls=QuoteTick,
        #         use_rust=True,
        #     ),
        #     BacktestDataConfig(
        #         catalog_path=self.catalog.path,
        #         instrument_id=self.test_instrument_ids[1],
        #         data_cls=QuoteTick,
        #         use_rust=True,
        #     ),
        #     BacktestDataConfig(
        #         catalog_path=str(self.catalog.path),
        #         instrument_id=self.test_instrument_ids[2],
        #         data_cls=Bar,
        #         bar_spec="1-HOUR-BID",
        #         use_rust=False,
        #     ),
        # ]

        start = str(pd.Timestamp("2012-01-01", tz="UTC"))
        end = str(pd.Timestamp("2014-01-01", tz="UTC"))

        from pytower import CATALOG_DIR

        instrument_id = "USD/JPY.DUKA"
        strategy_bar_spec = "1-HOUR-ASK"
        from pytower.instruments.provider import InstrumentProvider

        instrument_qh = InstrumentProvider.get_home_instrument(instrument_id)
        strategy_bar_type_qh = BarType.from_str(f"{instrument_qh}-1-DAY-BID-EXTERNAL")

        data_configs = [
            BacktestDataConfig(  # Strategy ticks
                catalog_path=str(CATALOG_DIR),
                data_cls=QuoteTick,
                instrument_id=instrument_id,
                start_time=start,
                end_time=end,
                use_rust=True,
            ),
            BacktestDataConfig(  # Strategy bars
                catalog_path=str(CATALOG_DIR),
                data_cls=Bar,
                instrument_id=str(instrument_id),
                start_time=start,
                end_time=end,
                bar_spec=strategy_bar_spec,
            ),
            *(
                BacktestDataConfig(  # QH Bars
                    catalog_path=str(CATALOG_DIR),
                    data_cls=Bar,
                    instrument_id=str(instrument_qh.id),
                    start_time=start,
                    end_time=end,
                    bar_spec=f"1-DAY-{x}",
                )
                for x in "BID ASK".split()
            ),
        ]

        # Act
        start_memory = get_peak_memory_usage_gb()
        print(f"{start_memory:2f}")

        for i in range(5):

            batch_gen = batch_configs(data_configs, target_batch_size_bytes=parse_bytes("200mb"))
            for _ in batch_gen:
                pass
                # gc.collect()
            print(i, get_peak_memory_usage_gb())
            gc.collect()

        gc.collect()

        end_memory = get_peak_memory_usage_gb()
        print(f"{end_memory:2f}")

        # Assert
        tolerance = 0.15
        assert start_memory - tolerance <= end_memory <= start_memory + tolerance


class TestPersistenceBatching:
    def setup(self):
        self.catalog = data_catalog_setup(protocol="memory")

        self.fs: fsspec.AbstractFileSystem = self.catalog.fs

        self._load_data_into_catalog()

    def teardown(self):
        # Cleanup
        path = self.catalog.path
        fs = self.catalog.fs
        if fs.exists(path):
            fs.rm(path, recursive=True)

    def _load_data_into_catalog(self):
        self.instrument_provider = BetfairInstrumentProvider.from_instruments([])
        process_files(
            glob_path=TEST_DATA_DIR + "/1.166564490.bz2",
            reader=BetfairTestStubs.betfair_reader(instrument_provider=self.instrument_provider),
            instrument_provider=self.instrument_provider,
            catalog=self.catalog,
        )

    def test_batch_configs_single(self):

        # Arrange
        instrument_ids = self.catalog.instruments()["id"].unique().tolist()
        shared_kw = dict(
            catalog_path=str(self.catalog.path),
            catalog_fs_protocol=self.catalog.fs.protocol,
            data_cls=OrderBookData,
        )

        iter_batches = batch_configs(
            catalog=self.catalog,
            data_configs=[
                BacktestDataConfig(**shared_kw, instrument_id=instrument_ids[0]),
                BacktestDataConfig(**shared_kw, instrument_id=instrument_ids[1]),
            ],
            target_batch_size_bytes=parse_bytes("10kib"),
            read_num_rows=300,
        )

        # Act
        timestamp_chunks = []
        for batch in iter_batches:
            timestamp_chunks.append([b.ts_init for b in batch])

        # Assert
        latest_timestamp = 0
        for timestamps in timestamp_chunks:
            assert max(timestamps) > latest_timestamp
            latest_timestamp = max(timestamps)
            assert timestamps == sorted(timestamps)

    def test_batch_generic_data(self):
        # Arrange
        TestPersistenceStubs.setup_news_event_persistence()
        process_files(
            glob_path=f"{TEST_DATA_DIR}/news_events.csv",
            reader=CSVReader(block_parser=TestPersistenceStubs.news_event_parser),
            catalog=self.catalog,
        )
        data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol="memory",
            data_cls=NewsEventData,
            client_id="NewsClient",
        )
        # Add some arbitrary instrument data to appease BacktestEngine
        instrument_data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol="memory",
            instrument_id=self.catalog.instruments(as_nautilus=True)[0].id.value,
            data_cls=InstrumentStatusUpdate,
        )

        streaming = BetfairTestStubs.streaming_config(
            catalog_path=self.catalog.path,
        )
        engine = BacktestEngineConfig(streaming=streaming)
        run_config = BacktestRunConfig(
            engine=engine,
            data=[data_config, instrument_data_config],
            venues=[BetfairTestStubs.betfair_venue_config()],
            batch_size_bytes=parse_bytes("1mib"),
        )

        # Act
        node = BacktestNode(configs=[run_config])
        node.run()

        # Assert
        assert node


if __name__ == "__main__":
    mod = TestBatchConfigsRust()  # type:ignore
    mod.setup()  # type:ignore
    mod.test_batch_configs_memory_usage()

    # mod = TestGenerateBatches()  # type:ignore
    # mod.test_generate_batches_returns_empty_list_before_start_timestamp_with_end_timestamp()  # type:ignore
    # mod.test_generate_batches_returns_empty_list_before_start_timestamp()  # type:ignore
    # mod.test_generate_batches_trims_first_batch_by_start_timestamp()  # type:ignore
    # mod.test_generate_batches_trims_end_batch_by_end_timestamp()  # type:ignore
    # mod.test_generate_batches_trims_end_batch_returns_no_empty_batch()  # type:ignore
    # mod.test_generate_batches_returns_valid_data()  # type:ignore
    # mod.test_generate_batches_returns_has_inclusive_start_and_end()  # type:ignore

    # mod = TestIterateBuffersRust()  # type:ignore
    # mod.test_iterate_single_buffer_returns_valid_timestamps_rust()  # type:ignore
    # mod.test_iterate_multiple_buffers_returns_valid_timestamps_rust()  # type:ignore
    # mod.test_iterate_multiple_buffers_returns_valid_timestamps_with_start_end_range_rust()  # type:ignore
    # mod.test_iterate_multiple_buffers_returns_valid_timestamps_with_start_end_range_with_bars_rust()  # type:ignore

    # mod = TestBatchConfigsRust()  # type:ignore
    # mod.setup()  # type:ignore
    # mod.test_batch_data_configs_single_config_returns_valid_timestamps_rust()  # type:ignore
    # mod.test_batch_data_configs_multiple_configs_returns_valid_timestamps_rust()  # type:ignore
    # mod.test_batch_data_configs_multiple_configs_returns_valid_timestamps_start_end_range_rust()  # type:ignore
    # mod.test_batch_data_configs_multiple_configs_returns_valid_timestamps_start_end_range_with_bars_rust()  # type:ignore

    # mod = TestBuffer()  # type:ignore
    # mod.test_buffer_pop_returns_returns_valid_data()  # type:ignore
