from nautilus_trader.core.rust.core cimport CVec

cdef class ParquetReader:
    cdef void* _reader
    cdef CVec _chunk

    # cdef void _drop_chunk(self) except *

# cdef class ParquetFileReader(ParquetReader):
    # cdef str _file_path


# cdef class ParquetBufferReader(ParquetReader):
#     pass


# cdef list _parse_quote_tick_chunk(CVec chunk)
# cdef list _parse_trade_tick_chunk(CVec chunk)