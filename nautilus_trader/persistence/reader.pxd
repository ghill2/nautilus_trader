from nautilus_trader.core.rust.persistence_g cimport Vec_QuoteTick

cdef class ParquetReader:
    cdef void* _reader
    cdef Vec_QuoteTick * _chunk
    # cdef ParquetType _parquet_type
    # cdef ParquetReaderType _reader_type
    
    # cdef CVec _chunk

    # cdef void _drop_chunk(self) except *

# cdef class ParquetFileReader(ParquetReader):
    # cdef str _file_path


# cdef class ParquetBufferReader(ParquetReader):
#     pass


# cdef list _parse_quote_tick_chunk(CVec chunk)
# cdef list _parse_trade_tick_chunk(CVec chunk)