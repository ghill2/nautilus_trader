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

use std::ffi::{c_char, c_void};
use nautilus_core::string::cstr_to_string;
use std::fs::File;
use arrow2::io::parquet::read;
use arrow2::{
    array::{Array, Int64Array, UInt64Array},
    chunk::Chunk,
};
use nautilus_model::{
    identifiers::instrument_id::InstrumentId,
    types::{price::Price, quantity::Quantity},

};
// use nautilus_model::data::tick::QuoteTick;
use nautilus_model::data::quote::QuoteTick;
use arrow2::datatypes::Schema;
use nautilus_core::cvec::CVec;
use std::str::FromStr;

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
}

#[no_mangle]
pub unsafe extern "C" fn parquet_reader_drop_chunk(
    chunk: CVec,
) {
    let CVec { ptr, len, cap } = chunk;
    let data: Vec<QuoteTick> = Vec::from_raw_parts(ptr as *mut QuoteTick, len, cap);
    drop(data);
}

#[no_mangle]
pub unsafe extern "C" fn parquet_reader_free(
    reader: *mut c_void,
) {
    let reader = Box::from_raw(reader as *mut read::FileReader::<File>);
    drop(reader);
}

fn decode_quotes(schema: &Schema, cols: Chunk<Box<dyn Array>>) -> Vec<QuoteTick> {
    let instrument_id =
        InstrumentId::from_str(schema.metadata.get("instrument_id").unwrap()).unwrap();
    // let instrument_id = InstrumentId::from_str("AAPL.NASDAQ").unwrap();

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
            |(((((bid_price, ask_price), ask_size), bid_size), ts_event), ts_init)| QuoteTick {
                instrument_id,
                bid_price: Price::from_raw(*bid_price.unwrap(), price_precision),
                ask_price: Price::from_raw(*ask_price.unwrap(), price_precision),
                bid_size: Quantity::from_raw(*bid_size.unwrap(), size_precision),
                ask_size: Quantity::from_raw(*ask_size.unwrap(), size_precision),
                ts_event: *ts_event.unwrap(),
                ts_init: *ts_init.unwrap(),
            },
        );

    values.collect()
}