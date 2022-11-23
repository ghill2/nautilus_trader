from cpython.object cimport PyObject
import gc
import psutil
import itertools
from nautilus_trader.model.data.tick import QuoteTick
import os
from tests.test_kit import PACKAGE_ROOT
from nautilus_trader.core.rust.persistence cimport parquet_reader_drop_chunk
from nautilus_trader.core.rust.core cimport CVec
from nautilus_trader.persistence.catalog.rust.common import py_type_to_parquet_type
from nautilus_trader.core.rust.persistence cimport ParquetReaderType
from nautilus_trader.core.rust.persistence cimport ParquetType
from nautilus_trader.persistence.catalog.rust.reader cimport _parse_quote_tick_chunk
from nautilus_trader.core.rust.persistence cimport parquet_reader_free
from nautilus_trader.core.rust.persistence cimport parquet_reader_file_new
from nautilus_trader.core.rust.persistence cimport parquet_reader_next_chunk
from nautilus_trader.core.rust.persistence cimport create_test_cvec
BYTES_IN_GIGABYTE = 1e9
class TestParquetFileReader:
    def test_parquet_reader_frees(self):
        n = 10_000
        parquet_data_path = os.path.join(PACKAGE_ROOT, "data/quote_tick_data.parquet")
        cdef void *reader
        cdef CVec cvec
        ram_used_gb = psutil.Process(os.getpid()).memory_info().rss / BYTES_IN_GIGABYTE
        print(ram_used_gb)
        for _ in range(n):
            reader = parquet_reader_file_new(
                file_path=<PyObject *>parquet_data_path,
                parquet_type=ParquetType.QuoteTick,
                chunk_size=1_000,
            )
            
            cvec = parquet_reader_next_chunk(
                reader=reader,
                parquet_type=ParquetType.QuoteTick,
                reader_type=ParquetReaderType.File,
            )
            while cvec.len != 0:
                cvec = parquet_reader_next_chunk(
                    reader=reader,
                    parquet_type=ParquetType.QuoteTick,
                    reader_type=ParquetReaderType.File,
                )
            parquet_reader_free(
                reader=reader,
                parquet_type=ParquetType.QuoteTick,
                reader_type=ParquetReaderType.File,
            )
        ram_used_gb = psutil.Process(os.getpid()).memory_info().rss / BYTES_IN_GIGABYTE
        print(ram_used_gb)

    def test_cvec_frees(self):
        ram_used_gb = psutil.Process(os.getpid()).memory_info().rss / BYTES_IN_GIGABYTE
        print(ram_used_gb)
        cdef CVec cvec
        cdef list objs
        n = 10_000
        for _ in range(n):
            cvec = create_test_cvec()
            parquet_reader_drop_chunk(cvec, ParquetType.QuoteTick)
        ram_used_gb = psutil.Process(os.getpid()).memory_info().rss / BYTES_IN_GIGABYTE
        print(ram_used_gb)