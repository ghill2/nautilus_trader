import os

from libc.stdint cimport uint64_t
from nautilus_trader.core.correctness cimport Condition
from nautilus_trader.core.string cimport pystr_to_cstr
from nautilus_trader.model.data.tick cimport QuoteTick
from nautilus_trader.core.rust.persistence_g cimport parquet_reader_new
# from nautilus_trader.core.rust.persistence_g cimport quote_tick_clone
from nautilus_trader.core.rust.persistence_g cimport parquet_reader_next_chunk
from nautilus_trader.core.rust.persistence_g cimport Vec_QuoteTick
# from nautilus_trader.core.rust.persistence_g cimport index_chunk
from nautilus_trader.core.rust.persistence_g cimport free_chunk

from nautilus_trader.core.rust.model cimport QuoteTick_t

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

        cdef:
            QuoteTick_t _mem
            QuoteTick obj
            list objs
            uint64_t i

        while True:
            
            self._chunk = parquet_reader_next_chunk(reader=self._reader)
            print(self._chunk.cap)
            print(self._chunk.len)
            if self._chunk.len == 0:
                # self._drop_chunk()
                # free_chunk(chunk)
                return # Stop iterating
            
            # objs = []
            # for i in range(0, 10):
            #     obj = QuoteTick.__new__(QuoteTick)
            #     _mem = index_chunk(chunk, i)
            #     obj.ts_init = _mem.ts_init
            #     obj.ts_event = _mem.ts_event
            #     obj._mem = _mem
            #     objs.append(obj)
            
            yield [5]
            
            # free_chunk(chunk)
        exit()
        
    # def next(self):

    #     cdef Vec_QuoteTick chunk = parquet_reader_next_chunk(reader=self._reader)
    #     return _parse_quote_tick_chunk(chunk)
    #     free_chunk

    # """
    # Provides a parquet file reader implemented in Rust under the hood.
    # """
    # def __del__(self) -> None:
    #     parquet_reader_free(
    #         reader=self._reader,
    #         parquet_type=self._parquet_type,
    #         reader_type=self._reader_type,
    #     )
    #     self._drop_chunk()

# cdef inline list _parse_quote_tick_chunk(Vec_QuoteTick chunk):
#     cdef:
#         QuoteTick_t _mem
#         QuoteTick obj
#         list objs = []
#         uint64_t i
#     for i in range(0, chunk.len):
#         obj = QuoteTick.__new__(QuoteTick)
#         _mem = index_chunk(&chunk, i)
#         obj.ts_init = _mem.ts_init
#         obj.ts_event = _mem.ts_event
#         obj._mem = _mem
#         objs.append(obj)
#     return objs

# cdef inline list _parse_quote_tick_chunk(CVec chunk):
#     cdef:
#         QuoteTick_t _mem
#         QuoteTick obj
#         list objs = []
#         uint64_t i
#     for i in range(0, chunk.len):
#         obj = QuoteTick.__new__(QuoteTick)
#         _mem = (<QuoteTick_t *>chunk.ptr)[i]
#         obj.ts_init = _mem.ts_init
#         obj.ts_event = _mem.ts_event
#         obj._mem = data_clone(&_mem)
#         objs.append(obj)
#     return objs




# from nautilus_trader.core.rust.persistence cimport parquet_reader_drop_chunk
# from nautilus_trader.core.rust.persistence cimport parquet_reader_free
# from nautilus_trader.core.rust.persistence cimport parquet_reader_next_chunk
# from nautilus_trader.core.rust.persistence cimport ParquetReaderType
# from nautilus_trader.core.rust.persistence cimport ParquetType
# from nautilus_trader.persistence.catalog.rust.common import py_type_to_parquet_type
# ERROR!

# from nautilus_trader.core.rust.core cimport CVec
# from nautilus_trader.core.rust.model cimport TradeTick_t
# from nautilus_trader.core.rust.model cimport quote_tick_copy




        # cdef QuoteTick obj = QuoteTick.from_mem_c((<QuoteTick_t *>chunk.ptr)[0])
        # cdef uint64_t i = _mem.ts_event
        
        # obj._mem = quote_tick_clone(&_mem)

        
        # print(ts_event)
            # QuoteTick obj
            # list objs = []
            # uint64_t i
        
        # obj = QuoteTick.__new__(QuoteTick)

        # obj.ts_init = _mem.ts_init
        # obj.ts_event = _mem.ts_event
        # obj._mem = quote_tick_clone((<QuoteTick_t *>chunk.ptr)[0])
        # objs.append(obj)
        # cdef list test = _parse_quote_tick_chunk(chunk)
        # print(chunk.ptr)
        # print(chunk.len)

        # print(chunk.cap)
        # (<QuoteTick_t *>chunk.ptr).ts_event
        
        
        # print(_mem.ts_event)
        
        # exit()

        
        # if chunk.len == 0:
        #     # self._drop_chunk()
        #     return # Stop iterating
