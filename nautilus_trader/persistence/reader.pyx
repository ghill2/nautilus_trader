import os
from libc.stdint cimport uint64_t
from nautilus_trader.core.correctness cimport Condition
from nautilus_trader.core.string cimport pystr_to_cstr
from nautilus_trader.model.data.tick cimport QuoteTick
from nautilus_trader.core.rust.model cimport QuoteTick_t

from nautilus_trader.core.rust.persistence_g cimport parquet_reader_new
from nautilus_trader.core.rust.persistence_g cimport parquet_reader_next_chunk
from nautilus_trader.core.rust.persistence_g cimport parquet_reader_drop_chunk
from nautilus_trader.core.rust.persistence_g cimport parquet_reader_free

cdef class ParquetReader:
    def __init__(
        self,
        str file_path,
        uint64_t chunk_size=1000,  # TBD
    ):
        Condition.valid_string(file_path, "file_path")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at {file_path}")
        
        self._reader = parquet_reader_new(
            file_path=pystr_to_cstr(file_path),
            chunk_size=chunk_size,
        )
        
    def __iter__(self):
        cdef list objs = []
        while True:
            
            self._chunk = parquet_reader_next_chunk(reader=self._reader)
            if self._chunk.len == 0:
                parquet_reader_drop_chunk(self._chunk)
                return # stop iterating
            
            objs = []
            for i in range(0, self._chunk.len):
                objs.append(QuoteTick.from_mem_c((<QuoteTick_t *>self._chunk.ptr)[i]))
            yield objs
            
            parquet_reader_drop_chunk(self._chunk)

    def __del__(self) -> None:
        parquet_reader_free(reader=self._reader)
