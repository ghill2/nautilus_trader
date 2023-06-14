// stream.for_each_concurrent(1, |batch| async {
//     println!("{}", batch.expect("REASON").num_rows());
// }).await;


// Example usage
// fn main() {
//     block_on(create_batch_stream());
        
//     println!("END");
// }


// use crate::kmerge::{KMerge};
// use crate::kmerge_batch::{KMerge, PeekElementBatchStream};
// use stream_kmerge::kmerge_by;
// use nautilus_persistence::kmerge_batch::{KMerge, PeekElementBatchStream};

fn decode_batch(metadata: &HashMap<String, String>, record_batch: RecordBatch) -> Vec<QuoteTick> {
    let instrument_id = InstrumentId::from(metadata.get("instrument_id").unwrap().as_str());
    let price_precision = metadata
        .get("price_precision")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    let size_precision = metadata
        .get("size_precision")
        .unwrap()
        .parse::<u8>()
        .unwrap();

    // Extract field value arrays from record batch
    let cols = record_batch.columns();
    let bid_values = cols[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let ask_values = cols[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let ask_size_values = cols[2].as_any().downcast_ref::<UInt64Array>().unwrap();
    let bid_size_values = cols[3].as_any().downcast_ref::<UInt64Array>().unwrap();
    let ts_event_values = cols[4].as_any().downcast_ref::<UInt64Array>().unwrap();
    let ts_init_values = cols[5].as_any().downcast_ref::<UInt64Array>().unwrap();

    // Construct iterator of values from field value arrays
    let values = bid_values
        .into_iter()
        .zip(ask_values.iter())
        .zip(ask_size_values.iter())
        .zip(bid_size_values.iter())
        .zip(ts_event_values.iter())
        .zip(ts_init_values.iter())
        .map(
            |(((((bid, ask), ask_size), bid_size), ts_event), ts_init)| {
                QuoteTick {
                    instrument_id: instrument_id.clone(),
                    bid: Price::from_raw(bid.unwrap(), price_precision),
                    ask: Price::from_raw(ask.unwrap(), price_precision),
                    bid_size: Quantity::from_raw(bid_size.unwrap(), size_precision),
                    ask_size: Quantity::from_raw(ask_size.unwrap(), size_precision),
                    ts_event: ts_event.unwrap(),
                    ts_init: ts_init.unwrap(),
                }
                .into()
            },
        );

    values.collect()
}

pub async fn iterate() {
    
    let mut batch_streams: _ = Vec::default();

    let session_ctx = SessionContext::default();

    let mut file_paths: Vec<&str> = Vec::new();
    file_paths.push("/Users/g1/BU/STORE/CATALOG/EURUSD-DUKA-QuoteTick-1-TICK-ASK-2019.parquet");
    // file_paths.push("/Users/g1/BU/STORE/CATALOG/GBPUSD-DUKA-QuoteTick-1-TICK-ASK-2019.parquet");
    
    for file_path in &file_paths {
        let table_name = "quotes";

        let parquet_options = ParquetReadOptions::<'_> {
            skip_metadata: Some(false),
            ..Default::default()
        };
        session_ctx
            .register_parquet(table_name, file_path, parquet_options)
            .await;

        let batch_stream: SendableRecordBatchStream = session_ctx
            .sql(&format!("SELECT * FROM {} ORDER BY ts_init", &table_name))
            .await
            .expect("REASON")
            .execute_stream()
            .await
            .unwrap()
            ;
        
        let transform = batch_stream.map(|result| match result {
            Ok(batch) => decode_batch(batch.schema().metadata(), batch).into_iter(),
            Err(_err) => panic!("Error getting next batch from RecordBatchStream"),
        });
        // futures::stream::Map<Pin<Box<dyn RecordBatchStream<Item = Result<datafusion::arrow::record_batch::RecordBatch, DataFusionError>> + std::marker::Send>>
        

        batch_streams.push(transform);

    }
    
//     // println!("{:}", transform);

//     // Iterator::for_each(self.batch_streams.drain(..), |batch_stream| {
//     //     block_on(kmerge.push_stream(batch_stream));
//     // });
    
//     // let batch_stream_ = &batch_streams[0];
    
//     // for item in batch_stream_.iter() {
//     //     println!("{:?}", item);
//     // }
//         // .await
//         ;

// }
    // Iterator::for_each(batch_streams.drain(..), |batch_stream| {
        // block_on(kmerge.push_stream(batch_stream));
        // println!("{:}", batch_stream);
        // StreamExt::for_each
        // batch_stream.next();
    // });

    // kmerge_by(batch_streams,
    //         |iter_x: &std::vec::IntoIter<QuoteTick>, iter_y: &std::vec::IntoIter<QuoteTick>| {
    //             // Access the QuoteTick instances from the iterators
    //             // let x = iter_x.as_slice();
    //             // let y = iter_y.as_slice();
    //             println!("{:}", iter_x.len());
    //             true
    //         }
    //     );

    





// for streams in batch_streams.drain(..) {
    //     // `dyn Stream<Item = std::vec::IntoIter<nautilus_model::data::tick::QuoteTick>> + Unpin`
    //     // println!("{:}", item.len());
        
    // }
    
        // .collect::<Vec<QuoteTick>>()
        // .await
        ;
    // let mut kmerge: KMerge<
    //                     _,
    //                     QuoteTick,
    //                     TsInitComparator,
    //                 > = KMerge::new(TsInitComparator);


#[derive(Debug, Default)]
pub struct TsInitComparator;

impl<S> Compare<PeekElementBatchStream<S, QuoteTick>> for TsInitComparator
where
    S: Stream<Item = IntoIter<QuoteTick>>,
{
    fn compare(
        &self,
        l: &PeekElementBatchStream<S, QuoteTick>,
        r: &PeekElementBatchStream<S, QuoteTick>,
    ) -> std::cmp::Ordering {
        // Max heap ordering must be reversed
        l.item.ts_init.cmp(&r.item.ts_init).reverse()
    }
}

struct OrdComparator;
impl<S> Compare<PeekElementBatchStream<S, i32>> for OrdComparator
where
    S: Stream<Item = IntoIter<i32>>,
{
    fn compare(
        &self,
        l: &PeekElementBatchStream<S, i32>,
        r: &PeekElementBatchStream<S, i32>,
    ) -> std::cmp::Ordering {
        // Max heap ordering must be reversed
        l.item.cmp(&r.item).reverse()
    }
}
        // #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_session_g() {
//         println!("SUCCESS");
//         // process::exit(1);
//         assert!(false, "This test intentionally fails");
//     }
// }
        // how to make a stream of QuoteTicks?
    // let batch_stream = self
    //     .session_ctx
    //     .sql(sql_query)
    //     .execute_stream()
    // self.add_batch_stream::<T>(batch_stream);
    // Create an iterator of QuoteTicks
    // Create chunks from the iterator
    // let batch_streams: Stream<Vec<QuoteTick>> = Vec::default();
    // let mut kmerge: KMerge<_, i32, OrdComparator> = KMerge::new(OrdComparator);
    
    // let mut kmerge: KMerge<Stream<Item = IntoIter<QuoteTick>, QuoteTick>, QuoteTick, TsInitComparator> = KMerge::new(TsInitComparator);
    // println!("{:}", batch_streams.len());
    // for item in batch_streams.drain(..) {
    //     // `dyn Stream<Item = std::vec::IntoIter<nautilus_model::data::tick::QuoteTick>> + Unpin`
    //     println!("{:}", item.len());
    // }

    // Iterator::for_each(batch_streams.drain(..), |batch_stream| {
    //     block_on(kmerge.push_stream(batch_stream));
    // });

    
    // transform.for_each(|chunk| async move {
    //     println!("{}", chunk.len());
    //     // for quote in &quotes {
    //     //     println!("{}", quote);
    //     // }

    // }).await;
    // self.batch_streams.push(Box::new(transform));


// let batch_stream = session_ctx
    //     .sql(&format!("SELECT * FROM {} ORDER BY ts_init", &table_name))
    //     .await?
    //     .execute_stream()
    //     .await?;

    // let transform = batch_stream.map(|result| match result {
    //     Ok(batch) => decode_batch(batch.schema().metadata(), batch).into_iter(),
    //     Err(_err) => panic!("Error getting next batch from RecordBatchStream"),
    // });

    // batch_streams.push(Box::new(transform));
    


#[derive(Debug, Default)]
pub struct TsInitComparator;

impl<S> Compare<PeekElementBatchStream<S, QuoteTick>> for TsInitComparator
where
    S: Stream<Item = IntoIter<QuoteTick>>,
{
    fn compare(
        &self,
        l: &PeekElementBatchStream<S, QuoteTick>,
        r: &PeekElementBatchStream<S, QuoteTick>,
    ) -> std::cmp::Ordering {
        // Max heap ordering must be reversed
        l.item.ts_init.cmp(&r.item.ts_init).reverse()
    }
}

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
            Ok(batch) => decode_batch(batch.schema().metadata(), batch).into_iter(),
            Err(_err) => panic!("Error getting next batch from RecordBatchStream"),
        });

        self.batch_streams.push(Box::new(transform));
    }

    pub fn to_query_result(&mut self)  {  // -> QueryResult
        let result = kmerge_by(self.batch_streams, |x: &QuoteTick, y: &QuoteTick| y.ts_init.cmp(&x.ts_init));
                    // .collect::<Vec<QuoteTick>>()
    }
}
// Make a draining iterator over the chunks
// let mut kmerge = KMerge::<Stream<Item = IntoIter<QuoteTick>>, QuoteTick, TsInitComparator>::new(TsInitComparator);
// let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];
// Iterator::for_each(self.batch_streams.drain(..), |batch_stream| {
//     block_on(kmerge.push_stream(batch_stream));
// });

// QueryResult {
//     data: Box::new(kmerge.chunks(self.chunk_size)),
// }
// pub struct QueryResult {
//     data: Box<dyn Stream<Item = Vec<QuoteTick>> + Unpin>,
// }

// impl Iterator for QueryResult {
//     type Item = Vec<QuoteTick>;

//     fn next(&mut self) -> Option<Self::Item> {
//         block_on(self.data.next())
//     }

// }
