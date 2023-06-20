// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------


// /// Loaded as nautilus_pyo3.persistence_g
// /// causes linker error on windows 11
// use parquet::reader::ParquetReader; // legacy arrow2
// #[pymodule]
// pub fn persistence(_: Python<'_>, m: &PyModule) -> PyResult<()> {
//     m.add_class::<ParquetReader>()?; // legacy arrow2
//     Ok(())
// }

use std::ffi::{c_char, c_void};
use nautilus_core::string::cstr_to_string;
use std::fs::File;
use arrow2::io::parquet::read::FileReader;
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
use nautilus_model::data::tick::QuoteTick;
use arrow2::datatypes::Schema;
use nautilus_core::cvec::CVec;

#[no_mangle]
pub unsafe extern "C" fn parquet_reader_new(
    file_path: *const c_char,
    chunk_size: usize,
) -> *mut c_void {
    let file_path = cstr_to_string(file_path);
    let mut file = File::open(&file_path)
        .unwrap_or_else(|_| panic!("Unable to open parquet file {file_path}"));
    let metadata = read::read_metadata(&mut file).expect("Unable to read metadata");
    let schema = read::infer_schema(&metadata).expect("Unable to infer schema");
    let reader = read::FileReader::new(file, metadata.row_groups, schema, Some(chunk_size), None, None);
    Box::into_raw(Box::new(reader)) as *mut c_void
}

// #[no_mangle]
// pub extern "C" fn quote_tick_clone(data: &QuoteTick) -> QuoteTick {
//     data.clone()
// }

#[no_mangle]
pub unsafe extern "C" fn parquet_reader_next_chunk(
    reader: *mut c_void,
) -> CVec {
    let mut reader = Box::from_raw(reader as *mut read::FileReader::<File>);
    let chunk = reader.next();
    if chunk.is_none() {
        let quotes = Vec::<QuoteTick>::with_capacity(0);
        Box::into_raw(reader);  // Leak reader value back otherwise it will be dropped after this function
        return CVec::from(quotes);
    }
    let quotes = decode_quotes(reader.schema(), chunk.unwrap().expect("ERROR"));
    Box::into_raw(reader);  // Leak reader value back otherwise it will be dropped after this function
    CVec::from(quotes)
    // .iter().map(|data| data.clone()).collect::<Vec<_>>()
    

    // chunk.map_or_else(CVec::empty, |data| data.into())
    // }
        
    // if chunk.is_none() {
    //     Box::into_raw(reader);  // Leak reader value back otherwise it will be dropped after this function
    //     CVec::from(Vec::<QuoteTick>::new())
    // } else {
}

#[no_mangle]
pub unsafe extern "C" fn parquet_reader_drop_chunk(
    chunk: CVec,
) {
    let CVec { ptr, len, cap } = chunk;
    let data: Vec<QuoteTick> = Vec::from_raw_parts(ptr as *mut QuoteTick, len, cap);
    drop(data);
}

/// # Safety
/// - Assumes `reader` is a valid `*mut ParquetReader<Struct>` where the struct
/// has a corresponding [ParquetType] enum.
#[no_mangle]
pub unsafe extern "C" fn parquet_reader_free(
    reader: *mut c_void,
) {
    let mut reader = Box::from_raw(reader as *mut read::FileReader::<File>);
    drop(reader);
}

fn decode_quotes(schema: &Schema, cols: Chunk<Box<dyn Array>>) -> Vec<QuoteTick> {
    let instrument_id =
        InstrumentId::from(schema.metadata.get("instrument_id").unwrap().as_str());
    // let instrument_id = InstrumentId::from("EUR/USD.DUKA");
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



//////////////////////////////////////////////////////////////////////
    // let mut vec: Vec<QuoteTick> = Vec::from_raw_parts(ptr as *mut QuoteTick, len, len);
    // let mut vec: Vec<QuoteTick> = Vec::<QuoteTick>::with_capacity(1);
    // let mut vec: Vec<QuoteTick> = Vec::<QuoteTick>::new();
    // let tick = QuoteTick {
    //     instrument_id: InstrumentId::from("ETH-PERP.FTX"),
    //     bid: Price::new(10000.0, 4),
    //     ask: Price::new(10001.0, 4),
    //     bid_size: Quantity::new(1.0, 7),
    //     ask_size: Quantity::new(1.0, 7),
    //     ts_event: 25,
    //     ts_init: 25,
    // };
    // vec.push(tick);

    // // let static_ref: &'static mut [QuoteTick] = vec.clone().leak();
    // // let vec_ = vec.iter().map(|data| (*data).into()).collect();
    // // Box::into_raw(Box::new(vec));  // Leak vec back otherwise it will be dropped after this function
    // vec
    // .as_mut_ptr()
    
    //////////////////////////////////////////////////////////////////////
    // Some(CVec::from(quotes))


// #[repr(C)]
// pub struct Vec_QuoteTick {
//     ptr: *mut QuoteTick,
//     cap: usize,
//     len: usize,
// }

// #[no_mangle]
// pub unsafe extern "C" fn index_chunk(data: Vec_QuoteTick, idx: usize) -> QuoteTick {
//     let Vec_QuoteTick { ptr, cap, len } = data;
//     let vec: Vec<QuoteTick> = unsafe { Vec::from_raw_parts(ptr, cap, len )};
//     vec[idx].clone()
// }