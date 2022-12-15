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
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Optional

import fsspec
import numpy as np
import pyarrow.parquet as pq

from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.tick import TradeTick
from nautilus_trader.persistence.catalog.rust.reader import ParquetFileReader
from nautilus_trader.persistence.funcs import parse_bytes
from nautilus_trader.serialization.arrow.serializer import ParquetSerializer


def _generate_batches(
    files: list[str],
    cls: type,
    fs: fsspec.AbstractFileSystem = None,
    use_rust: bool = False,
    n_rows: int = 10_000,
):

    if fs is None:
        fs = fsspec.filesystem("file")

    # Check for valid files
    assert isinstance(files, list)
    assert all(Path(file).exists() for file in files)
    same_instrument_id = all(Path(f).parent == Path(files[0]).parent for f in files)
    assert same_instrument_id
    use_rust = use_rust and cls in (QuoteTick, TradeTick)
    if use_rust:
        for file in files:
            f = pq.ParquetFile(fs.open(file))
            valid_rust_file = all(x.physical_type == "INT64" for x in list(f.metadata.schema))
            assert valid_rust_file

    files = sorted(files, key=lambda x: Path(x).stem)
    for file in files:
        f = pq.ParquetFile(fs.open(file))
        if use_rust:
            for objs in ParquetFileReader(parquet_type=cls, file_path=file, chunk_size=n_rows):
                yield objs
        else:
            for batch in f.iter_batches(batch_size=n_rows):
                if batch.num_rows == 0:
                    break
                objs = ParquetSerializer.deserialize(cls=cls, chunk=batch.to_pylist())
                yield objs


def generate_batches(
    files: list[str],
    cls: type,
    fs: fsspec.AbstractFileSystem = None,
    use_rust: bool = False,
    n_rows: int = 10_000,
    start_time: int = 0,
    end_time: int = sys.maxsize,
):

    batches = _generate_batches(files, cls, fs, use_rust=use_rust, n_rows=n_rows)

    start = start_time
    end = end_time
    started = False
    for batch in batches:

        min = batch[0].ts_init
        max = batch[-1].ts_init
        if min < start and max < start:
            batch = []  # not started yet

        if max >= start and not started:
            timestamps = np.array([x.ts_init for x in batch])
            mask = timestamps >= start
            masked = list(itertools.compress(batch, mask))
            batch = masked
            started = True

        if max > end:
            timestamps = np.array([x.ts_init for x in batch])
            mask = timestamps <= end
            masked = list(itertools.compress(batch, mask))
            batch = masked
            if batch:
                yield batch
            return  # stop iterating

        yield batch


class Buffer:
    """yields batches of nautilus objects, supports trimming from the front by timestamp"""

    def __init__(self, batches: Generator, config=None):
        self._buffer: list = []
        self._is_complete = False
        self._batches = batches

    @property
    def is_complete(self) -> bool:
        return self._is_complete and len(self._buffer) == 0

    def update(self) -> None:
        objs = next(self._batches, None)
        if objs is None:
            self._is_complete = True
        else:
            self._buffer.extend(objs)

    def pop(self, timestamp_ns: int) -> list:

        if timestamp_ns < self._buffer[0].ts_init:
            return []  # nothing to pop

        timestamps = np.array([x.ts_init for x in self._buffer])
        mask = timestamps <= timestamp_ns
        i = len(self._buffer)
        masked = list(itertools.compress(self._buffer, mask))
        assert len(self._buffer) == i
        removed = masked
        self._buffer = list(itertools.compress(self._buffer, np.invert(mask)))

        return removed

    @property
    def max_timestamp(self) -> Optional[int]:
        return self._buffer[-1].ts_init if self._buffer else None

    def __len__(self) -> int:
        return len(self._buffer)


def batch_configs(  # noqa: C901
    data_configs: list[BacktestDataConfig],
    read_num_rows: int = 10_000,
    target_batch_size_bytes: int = parse_bytes("100mb"),  # noqa: B008,
):

    # Validate configs
    for config in data_configs:
        assert config.instrument_id
        assert config.data_cls
        if config.data_type is Bar:
            assert config.bar_spec

    # Sort configs (larger time_aggregated bar specifications first)
    def sort_by_large_time_aggregated_specifications(config) -> tuple[int, int]:
        spec = config.bar_specification
        if config.data_type is Bar and spec.is_time_aggregated():
            return (spec.aggregation, spec.step)
        else:
            return (sys.maxsize, sys.maxsize)  # last

    data_configs = sorted(data_configs, key=sort_by_large_time_aggregated_specifications)
    data_configs.reverse()

    # Setup buffers
    buffers = []
    for config in data_configs:
        files = config.get_files()

        assert files, f"No files found for {config}"
        batch_gen = generate_batches(
            files=files,
            cls=config.data_type,
            fs=fsspec.filesystem(config.catalog_fs_protocol or "file"),
            use_rust=config.use_rust,
            n_rows=read_num_rows,
            start_time=config.start_time_nanos,
            end_time=config.end_time_nanos,
        )
        buffer = Buffer(batch_gen, config=config)
        buffers.append(buffer)

    yield from _iterate_buffers(
        buffers,
        read_num_rows=read_num_rows,
        target_batch_size_bytes=target_batch_size_bytes,
    )


def _iterate_buffers(
    buffers: list[Buffer],
    read_num_rows: int = 10_000,
    target_batch_size_bytes: int = parse_bytes("100mb"),  # noqa: B008,
):

    bytes_read = 0
    values = []
    while len(buffers) != 0:

        # Fill buffer (if required)
        for buffer in buffers:
            if len(buffer) < read_num_rows:
                buffer.update()

        # Remove completed buffers
        buffers = [b for b in buffers if not b.is_complete]

        # Find pop timestamp
        max_timestamps = list(filter(None, [buffer.max_timestamp for buffer in buffers]))
        if not max_timestamps:
            continue
        min_timestamp = min(max_timestamps)

        # Trim the buffers
        batches = list(filter(len, [b.pop(min_timestamp) for b in buffers if len(b)]))
        if not batches:
            continue

        # Merge
        values.extend(list(heapq.merge(*batches, key=lambda x: x.ts_init)))

        bytes_read += sum([sys.getsizeof(x) for x in values])

        if bytes_read > target_batch_size_bytes:
            yield values
            bytes_read = 0
            values = []

    if values:  # yield remaining values
        yield values


def groupby_datatype(data):
    def _groupby_key(x):
        return type(x).__name__

    return [
        {"type": type(v[0]), "data": v}
        for v in [
            list(v) for _, v in itertools.groupby(sorted(data, key=_groupby_key), key=_groupby_key)
        ]
    ]


def extract_generic_data_client_ids(data_configs: list[BacktestDataConfig]) -> dict:
    """
    Extract a mapping of data_type : client_id from the list of `data_configs`.
    In the process of merging the streaming data, we lose the `client_id` for
    generic data, we need to inject this back in so the backtest engine can be
    correctly loaded.
    """
    data_client_ids = [
        (config.data_type, config.client_id) for config in data_configs if config.client_id
    ]
    assert len(set(data_client_ids)) == len(
        dict(data_client_ids),
    ), "data_type found with multiple client_ids"
    return dict(data_client_ids)


def _dbg_batches(batches):
    for batch in batches:
        item = batch[0]
        if isinstance(item, Bar):
            instrument_id = str(item.bar_type.instrument_id)
        elif isinstance(item, QuoteTick):
            instrument_id = str(item.instrument_id)

        print(
            type(item).__name__,
            instrument_id,
            len(batch),
            unix_nanos_to_dt(batch[0].ts_init),
            " > ",
            unix_nanos_to_dt(batch[-1].ts_init),
            sep="|",
        )
