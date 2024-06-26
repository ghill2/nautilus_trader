#![allow(warnings)]

// arrow2 legacy imports
use std::{collections::BTreeMap, ffi::c_void, fs::File, io::Cursor, ptr::null_mut, slice};
use nautilus_core::cvec::CVec;
use nautilus_model::data::tick::{QuoteTick, TradeTick};
// use super::reader::{ParquetReader, GroupFilterArg};
use pyo3::types::PyBytes;
use pyo3::{prelude::*, types::PyCapsule};
use super::{ParquetType, ParquetReaderType};
use arrow2::{datatypes::Schema, io::parquet::read::FileReader};
use std::io::{Read, Seek};
use arrow2::io::parquet::read;
use arrow2::{
    array::{Array, Int64Array, UInt64Array},
    chunk::Chunk,
    datatypes::{DataType, Field},
    io::parquet::write::{transverse, Encoding},
};
use nautilus_model::{
    identifiers::instrument_id::InstrumentId,
    types::{price::Price, quantity::Quantity},
};

#[pyclass(name = "ParquetReader")]
pub struct ParquetReader {
    reader: *mut c_void,
    current_chunk: Option<CVec>,
}
unsafe impl Send for ParquetReader {} /// Empty derivation for Send to satisfy `pyclass` requirements, this is only designed for single threaded use for now.


#[pymethods]
impl ParquetReader {
    #[new]
    #[pyo3(signature = (file_path, chunk_size))]
    fn new(
        file_path: String,
        chunk_size: usize,
    ) -> Self {
        
        let mut file = File::open(&file_path)
            .unwrap_or_else(|_| panic!("Unable to open parquet file {file_path}"));

        // let reader_ptr = (Box::into_raw(reader) as *mut c_void);
        // let reader = ParquetReader::<QuoteTick, File>::new(file, chunk_size, group_filter);
        let metadata = read::read_metadata(&mut file).expect("Unable to read metadata");
        let schema = read::infer_schema(&metadata).expect("Unable to infer schema");
        let reader = FileReader::new(file, metadata.row_groups, schema, Some(chunk_size), None, None);
        let reader_ = Box::into_raw(Box::new(reader)) as *mut c_void;

        ParquetReader {
            reader: reader_,
            current_chunk: None,
        }
        
    }

    /// The reader implements an iterator.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Each iteration returns a chunk of values read from the parquet file.
    unsafe fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        // slf.drop_chunk();

        if let Some(CVec { ptr, len, cap }) = slf.current_chunk {
            let data: Vec<QuoteTick> =
                unsafe { Vec::from_raw_parts(ptr as *mut QuoteTick, len, cap) };
            drop(data);
            // reset current chunk field
            slf.current_chunk = None;
        };
        
        let chunk: Option<CVec> = {
            let mut reader = Box::from_raw(slf.reader as *mut FileReader<File>);
            let chunk = reader.next();
            if chunk.is_none() {
                return None
            }
            // Leak reader value back otherwise it will be dropped after this function
            let quotes = decode_quotes(reader.schema(), chunk.unwrap().unwrap());

            Box::into_raw(reader);

            Some(CVec::from(quotes))
            
        };

        slf.current_chunk = chunk;
        
        match chunk {
            Some(cvec) => Python::with_gil(|py| {
                Some(PyCapsule::new::<CVec>(py, cvec, None).unwrap().into_py(py))
            }),
            None => None,
        }

    }

    unsafe fn drop(slf: PyRefMut<'_, Self>) {
        drop(slf)
    }

    unsafe fn drop_chunk(&mut self) {
        if let Some(CVec { ptr, len, cap }) = self.current_chunk {
            let data: Vec<QuoteTick> =
                unsafe { Vec::from_raw_parts(ptr as *mut QuoteTick, len, cap) };
            drop(data);
            // reset current chunk field
            self.current_chunk = None;
        };
    }
}

// impl ParquetReader {
    /// Chunks generated by iteration must be dropped after use, otherwise
    /// it will leak memory. Current chunk is held by the reader,
    /// drop if exists and reset the field.
    
// }

fn decode_quotes(schema: &Schema, cols: Chunk<Box<dyn Array>>) -> Vec<QuoteTick> {
    let instrument_id =
        InstrumentId::from(schema.metadata.get("instrument_id").unwrap().as_str());
    let price_precision = schema
        .metadata
        .get("price_precision")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    let size_precision = schema
        .metadata
        .get("size_precision")
        .unwrap()
        .parse::<u8>()
        .unwrap();

    // Extract field value arrays from chunk separately
    let bid_values = cols.arrays()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ask_values = cols.arrays()[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ask_size_values = cols.arrays()[2]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let bid_size_values = cols.arrays()[3]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let ts_event_values = cols.arrays()[4]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let ts_init_values = cols.arrays()[5]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    // Construct iterator of values from field value arrays
    let values = bid_values
        .into_iter()
        .zip(ask_values.into_iter())
        .zip(ask_size_values.into_iter())
        .zip(bid_size_values.into_iter())
        .zip(ts_event_values.into_iter())
        .zip(ts_init_values.into_iter())
        .map(
            |(((((bid, ask), ask_size), bid_size), ts_event), ts_init)| QuoteTick {
                instrument_id: instrument_id.clone(),
                bid: Price::from_raw(*bid.unwrap(), price_precision),
                ask: Price::from_raw(*ask.unwrap(), price_precision),
                bid_size: Quantity::from_raw(*bid_size.unwrap(), size_precision),
                ask_size: Quantity::from_raw(*ask_size.unwrap(), size_precision),
                ts_event: *ts_event.unwrap(),
                ts_init: *ts_init.unwrap(),
            },
        );

    values.collect()
}


// /// pyo3 automatically calls drop on the underlying rust struct when the python object is deallocated. so answer: https://stackoverflow.com/q/66401814
// impl Drop for ParquetReader {
//     fn drop(&mut self) {
//         self.drop_chunk();
//         match (self.parquet_type, self.reader_type) {
//             (ParquetType::QuoteTick, ParquetReaderType::File) => {
//                 let reader =
//                     unsafe { Box::from_raw(self.reader as *mut ParquetReader<QuoteTick, File>) };
//                 drop(reader);
//             }
//             (ParquetType::TradeTick, ParquetReaderType::File) => {
//                 let reader =
//                     unsafe { Box::from_raw(self.reader as *mut ParquetReader<TradeTick, File>) };
//                 drop(reader);
//             }
//             (ParquetType::QuoteTick, ParquetReaderType::Buffer) => {
//                 let reader = unsafe {
//                     Box::from_raw(self.reader as *mut ParquetReader<QuoteTick, Cursor<&[u8]>>)
//                 };
//                 drop(reader);
//             }
//             (ParquetType::TradeTick, ParquetReaderType::Buffer) => {
//                 let reader = unsafe {
//                     Box::from_raw(self.reader as *mut ParquetReader<TradeTick, Cursor<&[u8]>>)
//                 };
//                 drop(reader);
//             }
//         }

//         self.reader = null_mut();
//     }
// }

