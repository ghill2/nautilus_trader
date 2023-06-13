use std::vec::IntoIter;

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

use crate::kmerge_batch::{KMerge, PeekElementBatchStream};
use crate::parquet::{DecodeDataFromRecordBatch, ParquetType};

pub struct PersistenceCatalog {
    session_ctx: SessionContext,
    batch_streams: Vec<Box<dyn Stream<Item = IntoIter<QuoteTick>> + Unpin>>,
    chunk_size: usize,
}

impl PersistenceCatalog {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            session_ctx: SessionContext::default(),
            batch_streams: Vec::default(),
            chunk_size,
        }
    }

    pub async fn add_file_with_query(
        &mut self,
        table_name: &str,
        file_path: &str,
        sql_query: &str,
    ) -> Result<()>
    {
        // default query: &format!("SELECT * FROM {} ORDER BY ts_init", &table_name)
        let parquet_options = ParquetReadOptions::<'_> {
            skip_metadata: Some(false),
            ..Default::default()
        };
        self.session_ctx
            .register_parquet(table_name, file_path, parquet_options)
            .await?;

        let batch_stream = self
            .session_ctx
            .sql(sql_query)
            .await?
            .execute_stream()
            .await?;

        self.add_batch_stream(batch_stream);
        Ok(())
    }

    fn add_batch_stream(&mut self, stream: SendableRecordBatchStream)
    {
        let transform = stream.map(|result| match result {
            Ok(batch) => T::decode_batch(batch.schema().metadata(), batch).into_iter(),
            Err(_err) => panic!("Error getting next batch from RecordBatchStream"),
        });

        self.batch_streams.push(Box::new(transform));
    }

    // Consumes the registered queries and returns a [QueryResult].
    // Passes the output of the query though the a KMerge which sorts the
    // queries in ascending order of `ts_init`.
    // QueryResult is an iterator that return Vec<Data>.
    pub fn to_query_result(&mut self) -> QueryResult<QuoteTick> {
        
        let mut kmerge: KMerge<_, _, _> = KMerge::new(TsInitComparator);

        Iterator::for_each(self.batch_streams.drain(..), |batch_stream| {
            block_on(kmerge.push_stream(batch_stream));
        });

        QueryResult {
            data: Box::new(kmerge.chunks(self.chunk_size)),
        }

    }
}

pub struct QueryResult {
    data: Box<dyn Stream<Item = Vec<QuoteTick>> + Unpin>,
}

// returns Vec<QuoteTick>
impl Iterator for QueryResult {
    type Item = Vec<Data>;

    fn next(&mut self) -> Option<Self::Item> {
        block_on(self.data.next())
    }
}