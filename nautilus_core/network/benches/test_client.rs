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

use std::collections::HashMap;

<<<<<<< HEAD:nautilus_core/network/benches/test_client.rs
use hyper::Method;
use nautilus_network::http::HttpClient;

const CONCURRENCY: usize = 256;
const TOTAL: usize = 1_000_000;
=======
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use nautilus_model::data::tick::Data;
use pyo3::prelude::*;

#[repr(C)]
#[pyclass]
#[derive(Debug, Clone, Copy)]
pub enum ParquetType {
    QuoteTick = 0,
    TradeTick = 1,
}
>>>>>>> da8e52efa (Vec<QuoteTick>):nautilus_core/persistence/src/parquet/mod.rs

#[tokio::main]
async fn main() {
    let client = HttpClient::py_new(Vec::new());
    let mut reqs = Vec::new();
    for _ in 0..(TOTAL / CONCURRENCY) {
        for _ in 0..CONCURRENCY {
            reqs.push(client.send_request(
                Method::GET,
                "http://127.0.0.1:3000".to_string(),
                HashMap::new(),
                None,
            ));
        }

<<<<<<< HEAD:nautilus_core/network/benches/test_client.rs
        let resp = futures::future::join_all(reqs.drain(0..)).await;
        assert!(resp.iter().all(|res| if let Ok(resp) = res {
            resp.status == 200
        } else {
            false
        }));
    }
}
=======
pub trait DecodeDataFromRecordBatch
where
    Self: Sized + Into<Data>,
{
    fn decode_batch(metadata: &HashMap<String, String>, record_batch: RecordBatch) -> Vec<Data>;
    fn get_schema(metadata: HashMap<String, String>) -> SchemaRef;
}
>>>>>>> da8e52efa (Vec<QuoteTick>):nautilus_core/persistence/src/parquet/mod.rs
