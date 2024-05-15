use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::Float64Type;
use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use recv::TickDataStructure;

pub struct TickArray {
    data: Vec<(&'static str, PrimitiveBuilder<Float64Type>)>,
}

impl Default for TickArray {
    fn default() -> Self {
        Self::new()
    }
}

impl TickArray {
    pub fn new() -> Self {
        TickArray {
            data: Self::revoke_array(),
        }
    }
    fn revoke_array() -> Vec<(&'static str, PrimitiveBuilder<Float64Type>)> {
        vec![
            ("last_price", Float64Array::builder(0)),
            ("mid_price", Float64Array::builder(0)),
            ("volume", Float64Array::builder(0)),
            ("turnover", Float64Array::builder(0)),
            ("buy_volume", Float64Array::builder(0)),
            ("sell_volume", Float64Array::builder(0)),
            ("open_interest", Float64Array::builder(0)),
            ("snap_time", Float64Array::builder(0)),
            ("ms", Float64Array::builder(0)),
            ("ask_price_1", Float64Array::builder(0)),
            ("ask_price_2", Float64Array::builder(0)),
            ("ask_price_3", Float64Array::builder(0)),
            ("ask_price_4", Float64Array::builder(0)),
            ("ask_price_5", Float64Array::builder(0)),
            ("ask_volume_1", Float64Array::builder(0)),
            ("ask_volume_2", Float64Array::builder(0)),
            ("ask_volume_3", Float64Array::builder(0)),
            ("ask_volume_4", Float64Array::builder(0)),
            ("ask_volume_5", Float64Array::builder(0)),
            ("bid_price_1", Float64Array::builder(0)),
            ("bid_price_2", Float64Array::builder(0)),
            ("bid_price_3", Float64Array::builder(0)),
            ("bid_price_4", Float64Array::builder(0)),
            ("bid_price_5", Float64Array::builder(0)),
            ("bid_volume_1", Float64Array::builder(0)),
            ("bid_volume_2", Float64Array::builder(0)),
            ("bid_volume_3", Float64Array::builder(0)),
            ("bid_volume_4", Float64Array::builder(0)),
            ("bid_volume_5", Float64Array::builder(0)),
        ]
    }


    pub fn push(&mut self, tick: &impl TickDataStructure<f64>) {
        for (index, value) in tick.display().iter().enumerate() {
            self.data[index].1.append_value(*value);
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }
    pub fn output(&mut self, file_path: impl Into<PathBuf>) {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path.into())
            .unwrap();
        let array = self
            .data
            .iter_mut()
            .map(|(code, record)| (*code, Arc::new(record.finish()) as ArrayRef))
            .collect::<Vec<(&'static str, ArrayRef)>>();
        let batch = RecordBatch::try_from_iter(array).unwrap();
        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
        self.clear()
    }

    fn clear(&mut self) {
        self.data = Self::revoke_array();
    }
}
