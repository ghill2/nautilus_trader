use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::{Read, Seek};
use std::marker::PhantomData;

use arrow2::array::UInt64Array;
use arrow2::io::parquet::read::{self, RowGroupMetaData};
use arrow2::io::parquet::write::FileMetaData;
use arrow2::{datatypes::Schema, io::parquet::read::FileReader};
use pyo3::types::PyInt;
use pyo3::FromPyObject;
use arrow2::{array::Array, chunk::Chunk};

use nautilus_model::data::tick::QuoteTick;
use nautilus_model::{
    identifiers::instrument_id::InstrumentId,
    types::{price::Price, quantity::Quantity},
};
use arrow2::{
    array::{Array, Int64Array, UInt64Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    io::parquet::write::{transverse, Encoding},
};

pub trait DecodeFromChunk
where
    Self: Sized,
{
    fn decode(schema: &Schema, cols: Chunk<Box<dyn Array>>) -> Vec<Self>;
}

#[repr(C)]
/// Filter groups based on a field's metadata values.
pub enum GroupFilterArg {
    /// Select groups that have minimum ts_init less than limit.
    TsInitLt(u64),
    /// Select groups that have maximum ts_init greater than limit.
    TsInitGt(u64),
    /// No group filtering applied (to avoid `Option).
    None,
}

impl<'source> FromPyObject<'source> for GroupFilterArg {
    fn extract(ob: &'source pyo3::PyAny) -> pyo3::PyResult<Self> {
        let filter_arg: i64 = ob.downcast::<PyInt>()?.extract()?;
        match filter_arg.cmp(&0) {
            Ordering::Less => Ok(GroupFilterArg::TsInitLt(filter_arg.unsigned_abs())),
            Ordering::Equal => Ok(GroupFilterArg::None),
            Ordering::Greater => Ok(GroupFilterArg::TsInitGt(filter_arg.unsigned_abs())),
        }
    }
}

impl From<i64> for GroupFilterArg {
    fn from(value: i64) -> Self {
        match value.cmp(&0) {
            Ordering::Less => GroupFilterArg::TsInitLt(value.unsigned_abs()),
            Ordering::Equal => GroupFilterArg::None,
            Ordering::Greater => GroupFilterArg::TsInitGt(value.unsigned_abs()),
        }
    }
}

impl GroupFilterArg {
    /// Scan metadata and choose which chunks to filter and returns a HashSet
    /// holding the indexes of the selected chunks.
    fn selected_groups(&self, metadata: FileMetaData, schema: &Schema) -> Vec<RowGroupMetaData> {
        match self {
            // select groups that have minimum ts_init less than limit
            GroupFilterArg::TsInitLt(limit) => {
                if let Some(ts_init_field) =
                    schema.fields.iter().find(|field| field.name.eq("ts_init"))
                {
                    let statistics =
                        read::statistics::deserialize(ts_init_field, &metadata.row_groups)
                            .expect("Cannot extract ts_init statistics");
                    let min_values = statistics
                        .min_value
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("Unable to unwrap minimum value metadata for ts_init statistics");
                    let selected_groups: HashSet<usize> = min_values
                        .iter()
                        .enumerate()
                        .filter_map(|(i, ts_group_min)| {
                            let min = ts_group_min.unwrap_or(&u64::MAX);
                            if min < limit {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();
                    metadata
                        .row_groups
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _row_group)| selected_groups.contains(i))
                        .map(|(_i, row_group)| row_group)
                        .collect()
                } else {
                    metadata.row_groups
                }
            }
            // select groups that have maximum ts_init time greater than limit
            GroupFilterArg::TsInitGt(limit) => {
                if let Some(ts_init_field) =
                    schema.fields.iter().find(|field| field.name.eq("ts_init"))
                {
                    let statistics =
                        read::statistics::deserialize(ts_init_field, &metadata.row_groups)
                            .expect("Cannot extract ts_init statistics");
                    let max_values = statistics
                        .max_value
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("Unable to unwrap maximum value metadata for ts_init statistics");
                    let selected_groups: HashSet<usize> = max_values
                        .iter()
                        .enumerate()
                        .filter_map(|(i, ts_group_max)| {
                            let max = ts_group_max.unwrap_or(&u64::MAX);
                            if max > limit {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();
                    metadata
                        .row_groups
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _row_group)| selected_groups.contains(i))
                        .map(|(_i, row_group)| row_group)
                        .collect()
                } else {
                    metadata.row_groups
                }
            }
            GroupFilterArg::None => metadata.row_groups,
        }
    }
}

pub struct ParquetReader
{
    file_reader: FileReader,
}

impl<A, R> ParquetReader
where
    R: Read + Seek,
{
    #[must_use]
    pub fn new(mut reader: R, chunk_size: usize, filter_arg: GroupFilterArg) -> Self {
        let metadata = read::read_metadata(&mut reader).expect("Unable to read metadata");
        let schema = read::infer_schema(&metadata).expect("Unable to infer schema");
        let row_groups = filter_arg.selected_groups(metadata, &schema);
        let fr = FileReader::new(reader, row_groups, schema, Some(chunk_size), None, None);
        ParquetReader {
            file_reader: fr,
        }
    }
}

impl Iterator for ParquetReader
{
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Ok(chunk)) = self.file_reader.next() {
            Some(decode_quotes(self.file_reader.schema(), chunk))
        } else {
            None
        }
    }
}

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