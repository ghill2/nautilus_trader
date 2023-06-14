#![allow(warnings)]
use std::vec::IntoIter;
use datafusion::prelude::*;
use compare::Compare;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::executor::block_on;
use futures::{Stream, StreamExt};
use nautilus_core::cvec::CVec;
use nautilus_model::data::tick::QuoteTick;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_asyncio::tokio::get_runtime;
use std::collections::HashMap;
use pyo3::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use nautilus_model::{
    identifiers::instrument_id::InstrumentId,
    types::{price::Price, quantity::Quantity},
};
use datafusion::execution::context::ExecutionContext;

// use futures::stream::{self};
use std::process;
use stream_kmerge::kmerge_by;

pub async fn iterate_batch_stream(batch_stream: SendableRecordBatchStream) {
    

    
    
}

pub async fn create_batch_stream() -> SendableRecordBatchStream {
    let table_name = "quotes";
    let session_ctx = SessionContext::default();

    let mut file_path: &str = "/Users/g1/BU/STORE/CATALOG/EURUSD-DUKA-QuoteTick-1-TICK-ASK-2019.parquet";

    let parquet_options = ParquetReadOptions::<'_> {
        skip_metadata: Some(false),
        ..Default::default()
    };
    session_ctx
        .register_parquet(table_name, file_path, parquet_options)
        .await;

    // let mut batch_stream: SendableRecordBatchStream = 
    session_ctx
        .sql(&format!("SELECT * FROM {} ORDER BY ts_init", &table_name))
        .await
        .expect("REASON")
        .execute_stream()
        .await
        .unwrap()
        // ;

}


#[tokio::main]
async fn main() {
    // Step 1: Create ExecutionContext
    let mut ctx = ExecutionContext::new();

    // Step 2: Register Parquet file as a table
    ctx.register_parquet("my_table", "path/to/parquet_file.parquet", None)?;

    // Step 3: Build logical plan
    let logical_plan = ctx.create_logical_plan("SELECT * FROM my_table")?;

    // Step 4: Optimize logical plan
    let optimized_plan = ctx.optimize(&logical_plan)?;

    // Step 5: Create physical plan
    let physical_plan = ctx.create_physical_plan(&optimized_plan)?;

    // Step 6: Convert physical plan to SendableRecordBatchStream
    let stream = physical_plan.execute().await?;

}







// #[tokio::main]
// async fn main() {
//     let table_name = "quotes";
//     let session_ctx = SessionContext::default();

//     let mut file_path: &str = "/Users/g1/BU/STORE/CATALOG/EURUSD-DUKA-QuoteTick-1-TICK-ASK-2019.parquet";

//     let parquet_options = ParquetReadOptions::<'_> {
//         skip_metadata: Some(false),
//         ..Default::default()
//     };
//     session_ctx
//         .register_parquet(table_name, file_path, parquet_options)
//         .await;

//     // let mut batch_stream: SendableRecordBatchStream = 
//     session_ctx
//         .sql(&format!("SELECT * FROM {} ORDER BY ts_init", &table_name))
//         .await
//         .expect("REASON")

//         .execute_stream()
//         .await
//         .unwrap();

//     println!("GOT TO HERE2");

//     while let Some(result) = stream.next().await {
//         match result {
//             Ok(batch) => {
//                 println!("{}", batch.num_rows());
//             }
//             Err(error) => {
//                 // Handle the error
//                 println!("Error retrieving record batch: {}", error);
//             }
//         }
//     }

//     println!("GOT TO HERE3");
// }



